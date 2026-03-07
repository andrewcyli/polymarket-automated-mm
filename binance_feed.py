"""
V16: Binance WebSocket Feed — Real-time CEX price data for Smart Exit Engine.

Provides sub-second price updates for BTC, ETH, SOL, XRP via Binance WebSocket.
Runs as a background daemon thread. No API key required (public market data).

Usage:
    feed = BinanceFeed(assets=["btc", "eth", "sol", "xrp"])
    feed.start()
    
    # Get current price
    price = feed.get_price("btc")  # -> 67234.50
    
    # Get momentum (% change over lookback seconds)
    mom = feed.get_momentum("btc", lookback=30)  # -> 0.0012 (0.12%)
    
    # Get directional signal for smart exit
    signal = feed.get_directional_signal("btc", lookback=30)
    # -> {"momentum": 0.0012, "direction": "up", "strength": "weak",
    #     "price": 67234.50, "samples": 45, "age_ms": 120}
    
    feed.stop()
"""

import json
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple

try:
    import websocket  # websocket-client package
    HAS_WEBSOCKET = True
except ImportError:
    HAS_WEBSOCKET = False

logger = logging.getLogger("binance_feed")


# Binance symbol mapping
ASSET_TO_SYMBOL = {
    "btc": "btcusdt",
    "eth": "ethusdt",
    "sol": "solusdt",
    "xrp": "xrpusdt",
    "doge": "dogeusdt",
    "ada": "adausdt",
    "matic": "maticusdt",
    "avax": "avaxusdt",
    "link": "linkusdt",
    "dot": "dotusdt",
}

SYMBOL_TO_ASSET = {v: k for k, v in ASSET_TO_SYMBOL.items()}


@dataclass
class PricePoint:
    """A single price observation."""
    timestamp: float  # Unix timestamp (seconds)
    price: float
    volume: float = 0.0


class BinanceFeed:
    """
    Real-time Binance price feed via WebSocket.
    
    Runs as a daemon thread. Provides:
    - Current price per asset
    - Price momentum (% change over N seconds)
    - Directional signal (up/down/flat with strength)
    - Price history for custom analysis
    
    Thread-safe: all reads use snapshot copies of deques.
    """
    
    # Binance WebSocket endpoints (try .com first, fall back to .us)
    WS_ENDPOINTS = [
        "wss://stream.binance.com:9443/stream",
        "wss://stream.binance.us:9443/stream",
    ]
    
    def __init__(
        self,
        assets: List[str] = None,
        history_seconds: int = 300,    # Keep 5 min of price history
        reconnect_delay: float = 5.0,  # Seconds between reconnect attempts
        max_reconnects: int = 50,      # Max consecutive reconnect attempts
        log_level: str = "INFO",
    ):
        self.assets = [a.lower() for a in (assets or ["btc", "eth", "sol", "xrp"])]
        self.history_seconds = history_seconds
        self.reconnect_delay = reconnect_delay
        self.max_reconnects = max_reconnects
        
        # Validate assets
        self.symbols = {}
        for asset in self.assets:
            if asset not in ASSET_TO_SYMBOL:
                logger.warning(f"Unknown asset '{asset}', skipping")
                continue
            self.symbols[asset] = ASSET_TO_SYMBOL[asset]
        
        if not self.symbols:
            raise ValueError(f"No valid assets provided: {self.assets}")
        
        # State (thread-safe via GIL for simple reads, deque is thread-safe)
        self._prices: Dict[str, float] = {}           # Latest price per asset
        self._history: Dict[str, deque] = {}           # Price history per asset
        self._last_update: Dict[str, float] = {}       # Last update timestamp
        self._trade_count: Dict[str, int] = {}         # Trade count since start
        
        for asset in self.symbols:
            self._prices[asset] = 0.0
            self._history[asset] = deque(maxlen=history_seconds * 20)  # ~20 updates/sec max
            self._last_update[asset] = 0.0
            self._trade_count[asset] = 0
        
        # Connection state
        self._ws = None
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._connected = False
        self._reconnect_count = 0
        self._start_time = 0.0
        self._endpoint_idx = 0
        self._lock = threading.Lock()
        
        # Stats
        self._total_messages = 0
        self._errors = 0
    
    def start(self):
        """Start the WebSocket feed in a background daemon thread."""
        if not HAS_WEBSOCKET:
            logger.warning(
                "websocket-client not installed. Install with: pip install websocket-client. "
                "BinanceFeed will run in degraded mode (no data)."
            )
            return False
        
        if self._running:
            logger.warning("BinanceFeed already running")
            return True
        
        self._running = True
        self._start_time = time.time()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="binance-feed",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            f"BinanceFeed started for {list(self.symbols.keys())} "
            f"(history={self.history_seconds}s)"
        )
        return True
    
    def stop(self):
        """Stop the WebSocket feed."""
        self._running = False
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)
        self._connected = False
        logger.info("BinanceFeed stopped")
    
    # ── Public API ─────────────────────────────────────────────────────
    
    def get_price(self, asset: str) -> float:
        """Get the latest price for an asset. Returns 0.0 if no data."""
        return self._prices.get(asset.lower(), 0.0)
    
    def get_prices(self) -> Dict[str, float]:
        """Get all latest prices. Returns a copy."""
        return dict(self._prices)
    
    def get_momentum(self, asset: str, lookback: int = 30) -> float:
        """
        Get price momentum (% change) over the last N seconds.
        
        Returns:
            float: Percentage change (e.g., 0.0012 = 0.12% up, -0.003 = 0.3% down)
            Returns 0.0 if insufficient data.
        """
        asset = asset.lower()
        history = self._history.get(asset)
        if not history or len(history) < 2:
            return 0.0
        
        current_price = self._prices.get(asset, 0.0)
        if current_price <= 0:
            return 0.0
        
        cutoff = time.time() - lookback
        # Find the oldest price within the lookback window
        old_price = None
        for pp in history:
            if pp.timestamp <= cutoff:
                old_price = pp.price
            else:
                break
        
        if old_price is None or old_price <= 0:
            # Use the oldest available price if all within lookback
            old_price = history[0].price
            if old_price <= 0:
                return 0.0
        
        return (current_price - old_price) / old_price
    
    def get_directional_signal(
        self, asset: str, lookback: int = 30
    ) -> Dict:
        """
        Get a directional signal for the smart exit engine.
        
        Returns dict with:
            - momentum: float (% change)
            - direction: "up" | "down" | "flat"
            - strength: "strong" | "moderate" | "weak" | "none"
            - price: float (current price)
            - samples: int (number of data points in lookback)
            - age_ms: int (ms since last update)
            - available: bool (whether data is available)
        """
        asset = asset.lower()
        momentum = self.get_momentum(asset, lookback)
        current_price = self._prices.get(asset, 0.0)
        last_update = self._last_update.get(asset, 0.0)
        age_ms = int((time.time() - last_update) * 1000) if last_update > 0 else -1
        
        # Count samples in lookback window
        history = self._history.get(asset)
        samples = 0
        if history:
            cutoff = time.time() - lookback
            samples = sum(1 for pp in history if pp.timestamp >= cutoff)
        
        # Classify direction and strength
        abs_mom = abs(momentum)
        if abs_mom < 0.0003:  # < 0.03%
            direction = "flat"
            strength = "none"
        elif abs_mom < 0.001:  # < 0.1%
            direction = "up" if momentum > 0 else "down"
            strength = "weak"
        elif abs_mom < 0.003:  # < 0.3%
            direction = "up" if momentum > 0 else "down"
            strength = "moderate"
        else:  # >= 0.3%
            direction = "up" if momentum > 0 else "down"
            strength = "strong"
        
        return {
            "momentum": momentum,
            "direction": direction,
            "strength": strength,
            "price": current_price,
            "samples": samples,
            "age_ms": age_ms,
            "available": current_price > 0 and samples >= 3 and age_ms < 10000,
        }
    
    def get_price_history(
        self, asset: str, lookback: int = 60
    ) -> List[Tuple[float, float]]:
        """
        Get price history as [(timestamp, price), ...] for the last N seconds.
        Useful for custom analysis.
        """
        asset = asset.lower()
        history = self._history.get(asset)
        if not history:
            return []
        
        cutoff = time.time() - lookback
        return [(pp.timestamp, pp.price) for pp in history if pp.timestamp >= cutoff]
    
    def get_stats(self) -> Dict:
        """Get feed statistics for monitoring/logging."""
        uptime = time.time() - self._start_time if self._start_time > 0 else 0
        return {
            "connected": self._connected,
            "running": self._running,
            "uptime_seconds": int(uptime),
            "total_messages": self._total_messages,
            "errors": self._errors,
            "reconnects": self._reconnect_count,
            "assets": {
                asset: {
                    "price": self._prices.get(asset, 0.0),
                    "trades": self._trade_count.get(asset, 0),
                    "history_size": len(self._history.get(asset, [])),
                    "last_update_ms": int(
                        (time.time() - self._last_update.get(asset, 0)) * 1000
                    ) if self._last_update.get(asset, 0) > 0 else -1,
                }
                for asset in self.symbols
            },
        }
    
    @property
    def is_connected(self) -> bool:
        return self._connected
    
    @property
    def is_running(self) -> bool:
        return self._running
    
    # ── Internal WebSocket Logic ───────────────────────────────────────
    
    def _run_loop(self):
        """Main reconnection loop (runs in daemon thread)."""
        while self._running:
            try:
                self._connect()
            except Exception as e:
                self._errors += 1
                logger.error(f"BinanceFeed connection error: {e}")
            
            if not self._running:
                break
            
            self._reconnect_count += 1
            if self._reconnect_count > self.max_reconnects:
                logger.error(
                    f"BinanceFeed exceeded max reconnects ({self.max_reconnects}). "
                    f"Stopping feed."
                )
                self._running = False
                break
            
            # Exponential backoff with cap
            delay = min(self.reconnect_delay * (1.5 ** min(self._reconnect_count, 10)), 60.0)
            logger.info(
                f"BinanceFeed reconnecting in {delay:.1f}s "
                f"(attempt {self._reconnect_count}/{self.max_reconnects})"
            )
            time.sleep(delay)
    
    def _connect(self):
        """Establish WebSocket connection and run until disconnect."""
        # Build combined stream URL
        streams = [f"{sym}@trade" for sym in self.symbols.values()]
        stream_param = "/".join(streams)
        
        endpoint = self.WS_ENDPOINTS[self._endpoint_idx % len(self.WS_ENDPOINTS)]
        url = f"{endpoint}?streams={stream_param}"
        
        logger.info(f"BinanceFeed connecting to {endpoint} ({len(streams)} streams)")
        
        self._ws = websocket.WebSocketApp(
            url,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
            on_open=self._on_open,
        )
        
        # run_forever blocks until connection closes
        self._ws.run_forever(
            ping_interval=20,
            ping_timeout=10,
            reconnect=0,  # We handle reconnection ourselves
        )
    
    def _on_open(self, ws):
        """Called when WebSocket connection is established."""
        self._connected = True
        self._reconnect_count = 0  # Reset on successful connect
        logger.info(f"BinanceFeed connected ({len(self.symbols)} assets)")
    
    def _on_message(self, ws, message):
        """Process incoming trade message."""
        try:
            data = json.loads(message)
            self._total_messages += 1
            
            # Combined stream format: {"stream": "btcusdt@trade", "data": {...}}
            if "data" in data:
                trade = data["data"]
            else:
                trade = data
            
            # Extract trade data
            symbol = trade.get("s", "").lower()
            price = float(trade.get("p", 0))
            volume = float(trade.get("q", 0))
            
            if not symbol or price <= 0:
                return
            
            asset = SYMBOL_TO_ASSET.get(symbol)
            if not asset:
                return
            
            now = time.time()
            
            # Update current price
            self._prices[asset] = price
            self._last_update[asset] = now
            self._trade_count[asset] = self._trade_count.get(asset, 0) + 1
            
            # Add to history
            self._history[asset].append(PricePoint(
                timestamp=now,
                price=price,
                volume=volume,
            ))
            
            # Prune old history (keep only history_seconds worth)
            cutoff = now - self.history_seconds
            while self._history[asset] and self._history[asset][0].timestamp < cutoff:
                self._history[asset].popleft()
                
        except Exception as e:
            self._errors += 1
            if self._errors % 100 == 1:  # Log every 100th error
                logger.warning(f"BinanceFeed message parse error: {e}")
    
    def _on_error(self, ws, error):
        """Called on WebSocket error."""
        self._errors += 1
        self._connected = False
        logger.warning(f"BinanceFeed WebSocket error: {error}")
    
    def _on_close(self, ws, close_status_code, close_msg):
        """Called when WebSocket connection closes."""
        self._connected = False
        if self._running:
            logger.info(
                f"BinanceFeed disconnected (code={close_status_code}, msg={close_msg}). "
                f"Will reconnect."
            )
            # Try alternate endpoint on next connect
            self._endpoint_idx += 1
    
    # ── Cleanup ────────────────────────────────────────────────────────
    
    def __del__(self):
        self.stop()
    
    def __repr__(self):
        status = "connected" if self._connected else "disconnected"
        prices = ", ".join(
            f"{a}=${p:.2f}" for a, p in self._prices.items() if p > 0
        )
        return f"<BinanceFeed {status} [{prices}]>"
