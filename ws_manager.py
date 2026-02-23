"""
WebSocket Manager for Polymarket Trading Bot
=============================================
Phase 1 of the async/WebSocket migration.

Manages persistent WebSocket connections to Polymarket's three channels:
1. Market: wss://ws-subscriptions-clob.polymarket.com/ws/market
   - Orderbook updates, price changes, trades/fills, new markets, resolutions
2. User: wss://ws-subscriptions-clob.polymarket.com/ws/user
   - Real-time order status updates (requires API key auth)
3. RTDS: wss://ws-live-data.polymarket.com
   - Real-time Binance + Chainlink prices for BTC/ETH/SOL/XRP

Features:
- Automatic reconnection with exponential backoff
- PING/PONG heartbeat monitoring
- Dynamic subscribe/unsubscribe per channel
- Thread-safe state store for sharing data with synchronous bot
- Graceful degradation: bot falls back to REST polling if WS disconnects

Architecture:
- WebSocket connections run in a background asyncio event loop (separate thread)
- The synchronous bot reads from the shared StateStore (thread-safe)
- No changes to the existing bot's synchronous execution model in Phase 1
"""

import asyncio
import json
import logging
import threading
import time
import traceback
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import warnings
try:
    import websockets
    from websockets.exceptions import ConnectionClosed
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        try:
            from websockets.exceptions import InvalidStatusCode
        except ImportError:
            InvalidStatusCode = ConnectionClosed
    HAS_WEBSOCKETS = True
except ImportError:
    HAS_WEBSOCKETS = False

logger = logging.getLogger("polybot_v15_1.ws")


# ─────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────

class Channel(Enum):
    MARKET = "market"
    USER = "user"
    RTDS = "rtds"


WS_URLS = {
    Channel.MARKET: "wss://ws-subscriptions-clob.polymarket.com/ws/market",
    Channel.USER: "wss://ws-subscriptions-clob.polymarket.com/ws/user",
    Channel.RTDS: "wss://ws-live-data.polymarket.com",
}

# Reconnection parameters
INITIAL_RECONNECT_DELAY = 1.0      # seconds
MAX_RECONNECT_DELAY = 60.0         # seconds
RECONNECT_BACKOFF_FACTOR = 2.0
HEARTBEAT_INTERVAL = 30.0          # seconds between PINGs
HEARTBEAT_TIMEOUT = 10.0           # seconds to wait for PONG
CONNECTION_TIMEOUT = 15.0          # seconds to establish connection

# RTDS subscription messages
RTDS_SUBSCRIBE_TEMPLATE = {
    "type": "subscribe",
    "channel": "price",
}


# ─────────────────────────────────────────────────────────────────
# Event Types (for the event bus)
# ─────────────────────────────────────────────────────────────────

class EventType(Enum):
    # Market channel events
    BOOK_UPDATE = "book_update"          # Orderbook snapshot/delta
    PRICE_CHANGE = "price_change"        # Market price change
    TRADE = "trade"                      # Trade/fill on the market
    NEW_MARKET = "new_market"            # New market created
    MARKET_RESOLVED = "market_resolved"  # Market resolved (UP/DOWN)

    # User channel events
    ORDER_UPDATE = "order_update"        # Order status change (placed, filled, cancelled)
    ORDER_FILL = "order_fill"           # Order filled (partial or full)

    # RTDS channel events
    CRYPTO_PRICE = "crypto_price"        # Real-time crypto price (Binance/Chainlink)

    # Connection events
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    RECONNECTING = "reconnecting"
    ERROR = "error"


# ─────────────────────────────────────────────────────────────────
# Shared State Store (Thread-Safe)
# ─────────────────────────────────────────────────────────────────

class StateStore:
    """
    Thread-safe state store for WebSocket data.
    
    The synchronous bot reads from this store instead of making REST calls.
    WebSocket handlers write to this store from the async event loop.
    All reads/writes are protected by threading.Lock.
    """

    def __init__(self):
        self._lock = threading.Lock()

        # ── Market Channel State ──
        # Orderbooks: {token_id: {"bids": [...], "asks": [...], "timestamp": float}}
        self._orderbooks: Dict[str, dict] = {}
        # Market prices: {condition_id: {"price_up": float, "price_down": float, "ts": float}}
        self._market_prices: Dict[str, dict] = {}
        # Recent trades: {token_id: [{"price": float, "size": float, "side": str, "ts": float}]}
        self._recent_trades: Dict[str, list] = defaultdict(list)
        # Resolved markets: {condition_id: {"outcome": str, "ts": float}}
        self._resolved_markets: Dict[str, dict] = {}

        # ── User Channel State ──
        # Order statuses: {order_id: {"status": str, "filled_size": float, "ts": float}}
        self._order_statuses: Dict[str, dict] = {}
        # Recent fills: [{"order_id": str, "price": float, "size": float, "ts": float}]
        self._recent_fills: list = []

        # ── RTDS Channel State ──
        # Crypto prices: {asset: {"price": float, "source": str, "ts": float}}
        self._crypto_prices: Dict[str, dict] = {}

        # ── Connection State ──
        self._connection_status: Dict[str, dict] = {
            Channel.MARKET.value: {"connected": False, "last_message": 0, "reconnects": 0},
            Channel.USER.value: {"connected": False, "last_message": 0, "reconnects": 0},
            Channel.RTDS.value: {"connected": False, "last_message": 0, "reconnects": 0},
        }

        # ── Metrics ──
        self._messages_received: Dict[str, int] = defaultdict(int)
        self._errors: Dict[str, int] = defaultdict(int)
        self._start_time = time.time()

    # ── Market Channel Accessors ──

    def update_orderbook(self, token_id: str, book_data: dict):
        with self._lock:
            self._orderbooks[token_id] = {
                **book_data,
                "timestamp": time.time(),
            }

    def get_orderbook(self, token_id: str) -> Optional[dict]:
        with self._lock:
            entry = self._orderbooks.get(token_id)
            if entry and time.time() - entry.get("timestamp", 0) < 30:
                return entry
            return None

    def update_market_price(self, condition_id: str, price_up: float, price_down: float):
        with self._lock:
            self._market_prices[condition_id] = {
                "price_up": price_up,
                "price_down": price_down,
                "ts": time.time(),
            }

    def get_market_price(self, condition_id: str) -> Optional[dict]:
        with self._lock:
            entry = self._market_prices.get(condition_id)
            if entry and time.time() - entry.get("ts", 0) < 30:
                return entry
            return None

    def record_trade(self, token_id: str, trade: dict):
        with self._lock:
            self._recent_trades[token_id].append({
                **trade,
                "ts": time.time(),
            })
            # Keep only last 50 trades per token
            if len(self._recent_trades[token_id]) > 50:
                self._recent_trades[token_id] = self._recent_trades[token_id][-50:]

    def get_recent_trades(self, token_id: str, max_age: float = 60.0) -> list:
        with self._lock:
            cutoff = time.time() - max_age
            return [t for t in self._recent_trades.get(token_id, []) if t.get("ts", 0) > cutoff]

    def mark_resolved(self, condition_id: str, outcome: str):
        with self._lock:
            self._resolved_markets[condition_id] = {
                "outcome": outcome,
                "ts": time.time(),
            }

    def is_resolved(self, condition_id: str) -> Optional[str]:
        with self._lock:
            entry = self._resolved_markets.get(condition_id)
            return entry.get("outcome") if entry else None

    # ── User Channel Accessors ──

    def update_order_status(self, order_id: str, status: str, filled_size: float = 0):
        with self._lock:
            self._order_statuses[order_id] = {
                "status": status,
                "filled_size": filled_size,
                "ts": time.time(),
            }

    def get_order_status(self, order_id: str) -> Optional[dict]:
        with self._lock:
            return self._order_statuses.get(order_id)

    def record_fill(self, fill_data: dict):
        with self._lock:
            self._recent_fills.append({
                **fill_data,
                "ts": time.time(),
            })
            # Keep only last 200 fills
            if len(self._recent_fills) > 200:
                self._recent_fills = self._recent_fills[-200:]

    def get_recent_fills(self, since: float = 0) -> list:
        with self._lock:
            return [f for f in self._recent_fills if f.get("ts", 0) > since]

    def get_pending_fills(self) -> list:
        """Get fills that haven't been processed by the bot yet."""
        with self._lock:
            return [f for f in self._recent_fills if not f.get("processed")]

    def mark_fill_processed(self, fill_ts: float):
        with self._lock:
            for f in self._recent_fills:
                if f.get("ts") == fill_ts:
                    f["processed"] = True

    # ── RTDS Channel Accessors ──

    def update_crypto_price(self, asset: str, price: float, source: str = "binance"):
        with self._lock:
            self._crypto_prices[asset] = {
                "price": price,
                "source": source,
                "ts": time.time(),
            }

    def get_crypto_price(self, asset: str, max_age: float = 10.0) -> Optional[float]:
        """Get real-time crypto price. Returns None if stale (>max_age seconds)."""
        with self._lock:
            entry = self._crypto_prices.get(asset)
            if entry and time.time() - entry.get("ts", 0) < max_age:
                return entry["price"]
            return None

    def get_all_crypto_prices(self) -> Dict[str, dict]:
        with self._lock:
            return dict(self._crypto_prices)

    # ── Connection State ──

    def set_connected(self, channel: str, connected: bool):
        with self._lock:
            self._connection_status[channel]["connected"] = connected
            if connected:
                self._connection_status[channel]["last_message"] = time.time()

    def record_reconnect(self, channel: str):
        with self._lock:
            self._connection_status[channel]["reconnects"] += 1

    def is_connected(self, channel: str) -> bool:
        with self._lock:
            return self._connection_status[channel]["connected"]

    def record_message(self, channel: str):
        with self._lock:
            self._connection_status[channel]["last_message"] = time.time()
            self._messages_received[channel] += 1

    def record_error(self, channel: str):
        with self._lock:
            self._errors[channel] += 1

    # ── Diagnostics ──

    def get_status(self) -> dict:
        with self._lock:
            uptime = time.time() - self._start_time
            return {
                "uptime_seconds": uptime,
                "connections": dict(self._connection_status),
                "messages_received": dict(self._messages_received),
                "errors": dict(self._errors),
                "orderbooks_cached": len(self._orderbooks),
                "crypto_prices_cached": len(self._crypto_prices),
                "order_statuses_tracked": len(self._order_statuses),
                "recent_fills": len(self._recent_fills),
            }


# ─────────────────────────────────────────────────────────────────
# Event Bus (Thread-Safe)
# ─────────────────────────────────────────────────────────────────

class EventBus:
    """
    Simple pub/sub event bus for WebSocket events.
    
    Handlers can be registered from the synchronous bot thread.
    Events are emitted from the async WebSocket thread.
    Thread-safe via Lock.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._handlers: Dict[EventType, List[Callable]] = defaultdict(list)
        self._event_log: list = []
        self._max_log_size = 500

    def subscribe(self, event_type: EventType, handler: Callable):
        """Register a handler for an event type. Thread-safe."""
        with self._lock:
            self._handlers[event_type].append(handler)

    def unsubscribe(self, event_type: EventType, handler: Callable):
        """Remove a handler. Thread-safe."""
        with self._lock:
            if handler in self._handlers[event_type]:
                self._handlers[event_type].remove(handler)

    def emit(self, event_type: EventType, data: dict):
        """Emit an event to all registered handlers. Thread-safe."""
        with self._lock:
            handlers = list(self._handlers.get(event_type, []))
            self._event_log.append({
                "type": event_type.value,
                "ts": time.time(),
                "data_keys": list(data.keys()) if isinstance(data, dict) else [],
            })
            if len(self._event_log) > self._max_log_size:
                self._event_log = self._event_log[-self._max_log_size:]

        # Call handlers outside the lock to prevent deadlocks
        for handler in handlers:
            try:
                handler(event_type, data)
            except Exception as e:
                logger.error(f"Event handler error for {event_type.value}: {e}")

    def get_event_log(self, last_n: int = 50) -> list:
        with self._lock:
            return self._event_log[-last_n:]


# ─────────────────────────────────────────────────────────────────
# WebSocket Connection (Single Channel)
# ─────────────────────────────────────────────────────────────────

class WSConnection:
    """
    Manages a single WebSocket connection with:
    - Automatic reconnection with exponential backoff
    - PING/PONG heartbeat monitoring
    - Dynamic subscribe/unsubscribe
    - Message routing to handlers
    """

    def __init__(
        self,
        channel: Channel,
        state_store: StateStore,
        event_bus: EventBus,
        auth_headers: Optional[dict] = None,
    ):
        self.channel = channel
        self.url = WS_URLS[channel]
        self.state_store = state_store
        self.event_bus = event_bus
        self.auth_headers = auth_headers or {}

        self._ws = None
        self._running = False
        self._reconnect_delay = INITIAL_RECONNECT_DELAY
        self._subscriptions: Set[str] = set()
        self._pending_subscribes: list = []
        self._pending_unsubscribes: list = []
        self._last_pong = time.time()
        self._message_count = 0

    async def connect(self):
        """Main connection loop with auto-reconnection."""
        self._running = True
        while self._running:
            try:
                logger.info(f"WS [{self.channel.value}] Connecting to {self.url}...")
                extra_headers = {}
                if self.auth_headers:
                    extra_headers.update(self.auth_headers)

                async with websockets.connect(
                    self.url,
                    extra_headers=extra_headers,
                    ping_interval=HEARTBEAT_INTERVAL,
                    ping_timeout=HEARTBEAT_TIMEOUT,
                    close_timeout=5,
                    open_timeout=CONNECTION_TIMEOUT,
                    max_size=2**20,  # 1MB max message size
                ) as ws:
                    self._ws = ws
                    self._reconnect_delay = INITIAL_RECONNECT_DELAY
                    self._last_pong = time.time()

                    self.state_store.set_connected(self.channel.value, True)
                    self.event_bus.emit(EventType.CONNECTED, {
                        "channel": self.channel.value,
                    })
                    logger.info(f"WS [{self.channel.value}] Connected!")

                    # Re-subscribe to any active subscriptions after reconnect
                    await self._resubscribe_all()

                    # Process pending subscribes
                    await self._process_pending()

                    # Message receive loop
                    async for message in ws:
                        if not self._running:
                            break
                        try:
                            await self._handle_message(message)
                        except Exception as e:
                            logger.error(
                                f"WS [{self.channel.value}] Message handler error: {e}\n"
                                f"{traceback.format_exc()}"
                            )
                            self.state_store.record_error(self.channel.value)

            except ConnectionClosed as e:
                logger.warning(
                    f"WS [{self.channel.value}] Connection closed: code={e.code} reason={e.reason}"
                )
            except InvalidStatusCode as e:
                logger.error(
                    f"WS [{self.channel.value}] Invalid status code: {e.status_code}"
                )
            except asyncio.CancelledError:
                logger.info(f"WS [{self.channel.value}] Connection cancelled")
                break
            except Exception as e:
                logger.error(
                    f"WS [{self.channel.value}] Connection error: {e}\n"
                    f"{traceback.format_exc()}"
                )

            # Mark disconnected
            self._ws = None
            self.state_store.set_connected(self.channel.value, False)
            self.state_store.record_reconnect(self.channel.value)
            self.event_bus.emit(EventType.DISCONNECTED, {
                "channel": self.channel.value,
            })

            if not self._running:
                break

            # Exponential backoff
            logger.info(
                f"WS [{self.channel.value}] Reconnecting in {self._reconnect_delay:.1f}s..."
            )
            self.event_bus.emit(EventType.RECONNECTING, {
                "channel": self.channel.value,
                "delay": self._reconnect_delay,
            })
            await asyncio.sleep(self._reconnect_delay)
            self._reconnect_delay = min(
                self._reconnect_delay * RECONNECT_BACKOFF_FACTOR,
                MAX_RECONNECT_DELAY,
            )

    async def disconnect(self):
        """Gracefully disconnect."""
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        self._ws = None
        self.state_store.set_connected(self.channel.value, False)

    async def subscribe(self, subscription_msg: dict):
        """Subscribe to a topic on this channel."""
        if self._ws:
            try:
                await self._ws.send(json.dumps(subscription_msg))
                sub_key = json.dumps(subscription_msg, sort_keys=True)
                self._subscriptions.add(sub_key)
                logger.debug(f"WS [{self.channel.value}] Subscribed: {subscription_msg}")
            except Exception as e:
                logger.error(f"WS [{self.channel.value}] Subscribe error: {e}")
                self._pending_subscribes.append(subscription_msg)
        else:
            self._pending_subscribes.append(subscription_msg)

    async def unsubscribe(self, unsubscription_msg: dict):
        """Unsubscribe from a topic on this channel."""
        if self._ws:
            try:
                await self._ws.send(json.dumps(unsubscription_msg))
                sub_key = json.dumps(unsubscription_msg, sort_keys=True)
                self._subscriptions.discard(sub_key)
                logger.debug(f"WS [{self.channel.value}] Unsubscribed: {unsubscription_msg}")
            except Exception as e:
                logger.error(f"WS [{self.channel.value}] Unsubscribe error: {e}")

    async def _resubscribe_all(self):
        """Re-subscribe to all active subscriptions after reconnect."""
        for sub_key in list(self._subscriptions):
            try:
                msg = json.loads(sub_key)
                await self._ws.send(json.dumps(msg))
                logger.debug(f"WS [{self.channel.value}] Re-subscribed: {msg}")
            except Exception as e:
                logger.error(f"WS [{self.channel.value}] Re-subscribe error: {e}")

    async def _process_pending(self):
        """Process any pending subscribe/unsubscribe requests."""
        while self._pending_subscribes:
            msg = self._pending_subscribes.pop(0)
            await self.subscribe(msg)
        while self._pending_unsubscribes:
            msg = self._pending_unsubscribes.pop(0)
            await self.unsubscribe(msg)

    async def _handle_message(self, raw_message: str):
        """Route incoming message to the appropriate handler."""
        self._message_count += 1
        self.state_store.record_message(self.channel.value)

        try:
            data = json.loads(raw_message)
        except json.JSONDecodeError:
            logger.debug(f"WS [{self.channel.value}] Non-JSON message: {raw_message[:100]}")
            return

        if self.channel == Channel.MARKET:
            await self._handle_market_message(data)
        elif self.channel == Channel.USER:
            await self._handle_user_message(data)
        elif self.channel == Channel.RTDS:
            await self._handle_rtds_message(data)

    async def _handle_market_message(self, data: dict):
        """Handle Market channel messages."""
        msg_type = data.get("event_type", data.get("type", ""))

        if msg_type in ("book", "book_update"):
            # Orderbook update
            token_id = data.get("asset_id", data.get("market", ""))
            if token_id:
                book = {
                    "bids": data.get("bids", []),
                    "asks": data.get("asks", []),
                }
                self.state_store.update_orderbook(token_id, book)
                self.event_bus.emit(EventType.BOOK_UPDATE, {
                    "token_id": token_id,
                    "book": book,
                })

        elif msg_type in ("price_change", "last_trade_price"):
            # Price change
            market_id = data.get("market", data.get("condition_id", ""))
            if market_id:
                changes = data.get("changes", [])
                for change in changes:
                    if len(change) >= 2:
                        # [token_id, new_price]
                        self.event_bus.emit(EventType.PRICE_CHANGE, {
                            "market_id": market_id,
                            "token_id": change[0] if len(change) > 0 else "",
                            "price": float(change[1]) if len(change) > 1 else 0,
                        })

        elif msg_type in ("trade", "fill"):
            # Trade/fill on the market
            token_id = data.get("asset_id", data.get("market", ""))
            if token_id:
                trade = {
                    "price": float(data.get("price", 0)),
                    "size": float(data.get("size", data.get("count", 0))),
                    "side": data.get("side", ""),
                }
                self.state_store.record_trade(token_id, trade)
                self.event_bus.emit(EventType.TRADE, {
                    "token_id": token_id,
                    **trade,
                })

        elif msg_type == "market_resolved":
            condition_id = data.get("condition_id", "")
            outcome = data.get("outcome", "")
            if condition_id:
                self.state_store.mark_resolved(condition_id, outcome)
                self.event_bus.emit(EventType.MARKET_RESOLVED, {
                    "condition_id": condition_id,
                    "outcome": outcome,
                })

        elif msg_type == "new_market":
            self.event_bus.emit(EventType.NEW_MARKET, data)

    async def _handle_user_message(self, data: dict):
        """Handle User channel messages (order status updates)."""
        msg_type = data.get("event_type", data.get("type", ""))

        if msg_type in ("order", "order_update"):
            order_id = data.get("id", data.get("order_id", ""))
            status = data.get("status", data.get("order_status", ""))
            filled_size = float(data.get("size_matched", data.get("filled_size", 0)))

            if order_id:
                self.state_store.update_order_status(order_id, status, filled_size)
                self.event_bus.emit(EventType.ORDER_UPDATE, {
                    "order_id": order_id,
                    "status": status,
                    "filled_size": filled_size,
                    "raw": data,
                })

                # If the order is filled, also emit a fill event
                if status in ("MATCHED", "FILLED", "matched", "filled"):
                    fill_data = {
                        "order_id": order_id,
                        "price": float(data.get("price", 0)),
                        "size": filled_size,
                        "side": data.get("side", ""),
                        "token_id": data.get("asset_id", data.get("market", "")),
                    }
                    self.state_store.record_fill(fill_data)
                    self.event_bus.emit(EventType.ORDER_FILL, fill_data)

    async def _handle_rtds_message(self, data: dict):
        """Handle RTDS channel messages (real-time crypto prices)."""
        # RTDS format varies; handle both Binance and Chainlink formats
        msg_type = data.get("type", data.get("event", ""))

        if msg_type in ("price", "price_update", "ticker"):
            asset = data.get("asset", data.get("symbol", "")).lower()
            price = data.get("price", data.get("last_price", 0))
            source = data.get("source", data.get("provider", "binance"))

            # Normalize asset names (e.g., "BTCUSDT" -> "btc")
            asset = self._normalize_asset(asset)

            if asset and price:
                try:
                    price = float(price)
                    self.state_store.update_crypto_price(asset, price, source)
                    self.event_bus.emit(EventType.CRYPTO_PRICE, {
                        "asset": asset,
                        "price": price,
                        "source": source,
                    })
                except (ValueError, TypeError):
                    pass

        # Handle array-style updates (some RTDS formats)
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    await self._handle_rtds_message(item)

    @staticmethod
    def _normalize_asset(raw: str) -> str:
        """Normalize asset names from various formats to lowercase short form."""
        raw = raw.lower().strip()
        # Handle Binance-style pairs
        for suffix in ("usdt", "usd", "usdc"):
            if raw.endswith(suffix):
                raw = raw[:-len(suffix)]
                break
        # Map common names
        name_map = {
            "bitcoin": "btc", "ethereum": "eth", "solana": "sol", "ripple": "xrp",
            "btcusd": "btc", "ethusd": "eth", "solusd": "sol", "xrpusd": "xrp",
        }
        return name_map.get(raw, raw)

    @property
    def connected(self) -> bool:
        return self._ws is not None and self._running

    @property
    def message_count(self) -> int:
        return self._message_count


# ─────────────────────────────────────────────────────────────────
# WebSocket Manager (Orchestrator)
# ─────────────────────────────────────────────────────────────────

class WebSocketManager:
    """
    Orchestrates all WebSocket connections.
    
    Runs in a background thread with its own asyncio event loop.
    The synchronous bot interacts via:
    - state_store: Read latest data (thread-safe)
    - event_bus: Subscribe to events (thread-safe)
    - subscribe/unsubscribe: Manage channel subscriptions (thread-safe)
    
    Usage:
        manager = WebSocketManager(api_key="...", api_secret="...", api_passphrase="...")
        manager.start()
        
        # Read data from the state store
        price = manager.state_store.get_crypto_price("btc")
        book = manager.state_store.get_orderbook(token_id)
        
        # Subscribe to events
        manager.event_bus.subscribe(EventType.ORDER_FILL, my_handler)
        
        # Subscribe to market data
        manager.subscribe_market(token_id)
        
        # Graceful shutdown
        manager.stop()
    """

    def __init__(
        self,
        api_key: str = "",
        api_secret: str = "",
        api_passphrase: str = "",
        enable_market: bool = True,
        enable_user: bool = True,
        enable_rtds: bool = True,
        logger_instance=None,
    ):
        global logger
        if logger_instance:
            logger = logger_instance

        if not HAS_WEBSOCKETS:
            logger.error("websockets library not installed. WebSocket features disabled.")

        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase

        self.enable_market = enable_market
        self.enable_user = enable_user and bool(api_key)
        self.enable_rtds = enable_rtds

        # Shared state and event bus
        self.state_store = StateStore()
        self.event_bus = EventBus()

        # Connections
        self._connections: Dict[Channel, WSConnection] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False

        # Subscription tracking for thread-safe queue
        self._sub_queue: list = []
        self._unsub_queue: list = []
        self._queue_lock = threading.Lock()

    def start(self):
        """Start all WebSocket connections in a background thread."""
        if not HAS_WEBSOCKETS:
            logger.warning("WebSocket manager: websockets library not available. Skipping.")
            return False

        if self._running:
            logger.warning("WebSocket manager already running.")
            return True

        self._running = True
        self._thread = threading.Thread(
            target=self._run_event_loop,
            name="ws-manager",
            daemon=True,
        )
        self._thread.start()
        logger.info("WebSocket manager started (background thread)")
        return True

    def stop(self):
        """Stop all WebSocket connections and the background thread."""
        logger.info("WebSocket manager stopping...")
        self._running = False

        if self._loop:
            # Schedule disconnect tasks
            for conn in self._connections.values():
                asyncio.run_coroutine_threadsafe(conn.disconnect(), self._loop)

            # Stop the event loop
            self._loop.call_soon_threadsafe(self._loop.stop)

        if self._thread:
            self._thread.join(timeout=10)
            if self._thread.is_alive():
                logger.warning("WebSocket manager thread did not stop cleanly")

        self._connections.clear()
        logger.info("WebSocket manager stopped")

    def _run_event_loop(self):
        """Background thread: create and run the asyncio event loop."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        try:
            self._loop.run_until_complete(self._run_connections())
        except Exception as e:
            logger.error(f"WebSocket event loop error: {e}\n{traceback.format_exc()}")
        finally:
            try:
                self._loop.close()
            except Exception:
                pass
            self._loop = None

    async def _run_connections(self):
        """Start all enabled WebSocket connections as concurrent tasks."""
        tasks = []

        if self.enable_market:
            conn = WSConnection(
                Channel.MARKET, self.state_store, self.event_bus
            )
            self._connections[Channel.MARKET] = conn
            tasks.append(asyncio.create_task(conn.connect()))

        if self.enable_user:
            auth_headers = {}
            if self.api_key:
                auth_headers = {
                    "POLY_API_KEY": self.api_key,
                    "POLY_API_SECRET": self.api_secret,
                    "POLY_PASSPHRASE": self.api_passphrase,
                }
            conn = WSConnection(
                Channel.USER, self.state_store, self.event_bus,
                auth_headers=auth_headers,
            )
            self._connections[Channel.USER] = conn
            tasks.append(asyncio.create_task(conn.connect()))

        if self.enable_rtds:
            conn = WSConnection(
                Channel.RTDS, self.state_store, self.event_bus
            )
            self._connections[Channel.RTDS] = conn
            tasks.append(asyncio.create_task(conn.connect()))

        # Also run the subscription processor
        tasks.append(asyncio.create_task(self._process_subscription_queue()))

        if tasks:
            logger.info(
                f"WebSocket manager: starting {len(tasks) - 1} connections "
                f"(market={self.enable_market}, user={self.enable_user}, rtds={self.enable_rtds})"
            )
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _process_subscription_queue(self):
        """Process subscription requests from the synchronous thread."""
        while self._running:
            with self._queue_lock:
                subs = list(self._sub_queue)
                unsubs = list(self._unsub_queue)
                self._sub_queue.clear()
                self._unsub_queue.clear()

            for channel, msg in subs:
                conn = self._connections.get(channel)
                if conn:
                    await conn.subscribe(msg)

            for channel, msg in unsubs:
                conn = self._connections.get(channel)
                if conn:
                    await conn.unsubscribe(msg)

            await asyncio.sleep(0.5)

    # ── Thread-Safe Subscription Methods ──

    def subscribe_market(self, token_id: str):
        """Subscribe to orderbook updates for a specific token. Thread-safe."""
        msg = {
            "type": "subscribe",
            "channel": "book",
            "assets_id": token_id,
        }
        with self._queue_lock:
            self._sub_queue.append((Channel.MARKET, msg))

    def unsubscribe_market(self, token_id: str):
        """Unsubscribe from orderbook updates for a specific token. Thread-safe."""
        msg = {
            "type": "unsubscribe",
            "channel": "book",
            "assets_id": token_id,
        }
        with self._queue_lock:
            self._unsub_queue.append((Channel.MARKET, msg))

    def subscribe_user_orders(self):
        """Subscribe to user order updates. Thread-safe."""
        msg = {
            "type": "subscribe",
            "channel": "user",
        }
        with self._queue_lock:
            self._sub_queue.append((Channel.USER, msg))

    def subscribe_rtds_asset(self, asset: str):
        """Subscribe to real-time price data for a crypto asset. Thread-safe."""
        msg = {
            "type": "subscribe",
            "channel": "price",
            "asset": asset.upper(),
        }
        with self._queue_lock:
            self._sub_queue.append((Channel.RTDS, msg))

    def subscribe_rtds_all(self, assets: list = None):
        """Subscribe to RTDS for all configured assets. Thread-safe."""
        if assets is None:
            assets = ["BTC", "ETH", "SOL", "XRP"]
        for asset in assets:
            self.subscribe_rtds_asset(asset)

    # ── Status & Diagnostics ──

    def is_healthy(self) -> bool:
        """Check if all enabled connections are healthy."""
        if not self._running:
            return False
        for channel, conn in self._connections.items():
            if not conn.connected:
                return False
        return True

    def get_status(self) -> dict:
        """Get comprehensive status of all connections."""
        return {
            "running": self._running,
            "healthy": self.is_healthy(),
            "state_store": self.state_store.get_status(),
            "connections": {
                ch.value: {
                    "enabled": True,
                    "connected": conn.connected,
                    "messages": conn.message_count,
                }
                for ch, conn in self._connections.items()
            },
        }

    def get_connection_summary(self) -> str:
        """Human-readable connection summary for logging."""
        parts = []
        for ch, conn in self._connections.items():
            status = "OK" if conn.connected else "DOWN"
            parts.append(f"{ch.value}:{status}({conn.message_count})")
        return " | ".join(parts) if parts else "No connections"
