"""
WebSocket-Enhanced Price Feed
==============================
Wraps the existing PriceFeed with real-time RTDS data from WebSocket.

Priority chain:
1. RTDS WebSocket (Binance real-time) — sub-second latency
2. Chainlink on-chain (Polygon RPC) — ~2-3 second latency
3. CoinGecko REST API — ~10-30 second latency

The bot uses this as a drop-in replacement for PriceFeed.
If WebSocket is down, it automatically falls back to the existing REST-based feeds.
"""

import logging
import time
from typing import Optional, Dict

logger = logging.getLogger("polybot_v15_1.ws_price")


class WSPriceFeed:
    """
    Enhanced price feed that reads from WebSocket StateStore first,
    then falls back to the existing PriceFeed (Chainlink/CoinGecko).
    
    Drop-in replacement: same interface as PriceFeed.
    """

    def __init__(self, legacy_price_feed, state_store=None, logger_instance=None):
        """
        Args:
            legacy_price_feed: The existing PriceFeed instance (Chainlink + CoinGecko)
            state_store: The WebSocket StateStore (from WebSocketManager)
        """
        global logger
        if logger_instance:
            logger = logger_instance

        self.legacy = legacy_price_feed
        self.state_store = state_store
        self._ws_hit_count = 0
        self._fallback_count = 0
        self._price_source_log: Dict[str, str] = {}  # asset -> last source used

    def update(self):
        """
        Update prices from all sources.
        WebSocket prices are updated automatically via the background thread.
        This still calls the legacy update for Chainlink/CoinGecko as fallback.
        """
        # Legacy update still runs for Chainlink + CoinGecko
        self.legacy.update()

    def get_current_price(self, asset: str) -> Optional[float]:
        """
        Get the best available price for an asset.
        Priority: RTDS WebSocket > Chainlink > CoinGecko
        """
        # Try WebSocket RTDS first (freshest data, sub-second)
        if self.state_store:
            ws_price = self.state_store.get_crypto_price(asset, max_age=10.0)
            if ws_price is not None:
                self._ws_hit_count += 1
                self._price_source_log[asset] = "rtds_ws"
                return ws_price

        # Fall back to legacy (Chainlink then CoinGecko)
        self._fallback_count += 1
        price = self.legacy.get_current_price(asset)
        if price:
            if self.legacy.use_chainlink:
                self._price_source_log[asset] = "chainlink"
            else:
                self._price_source_log[asset] = "coingecko"
        return price

    def get_momentum(self, asset: str, lookback_minutes: int = 5) -> Optional[float]:
        """Get price momentum. Uses legacy for now (needs history)."""
        return self.legacy.get_momentum(asset, lookback_minutes)

    def predict_resolution(self, asset: str, window_start_ts, window_end_ts):
        """Predict market resolution. Uses legacy Chainlink for now."""
        return self.legacy.predict_resolution(asset, window_start_ts, window_end_ts)

    def get_price_source(self) -> str:
        """Get the current primary price source."""
        if self.state_store and any(
            self.state_store.get_crypto_price(a, max_age=10.0) is not None
            for a in ["btc", "eth", "sol", "xrp"]
        ):
            return "RTDS WebSocket (Binance) + Chainlink"
        return self.legacy.get_price_source()

    def get_stats(self) -> dict:
        """Get price feed statistics."""
        return {
            "ws_hits": self._ws_hit_count,
            "fallbacks": self._fallback_count,
            "hit_rate": (
                self._ws_hit_count / max(1, self._ws_hit_count + self._fallback_count)
            ),
            "sources": dict(self._price_source_log),
        }

    # ── Pass-through properties for backward compatibility ──

    @property
    def use_chainlink(self):
        return self.legacy.use_chainlink

    @property
    def chainlink(self):
        return self.legacy.chainlink

    @property
    def prices(self):
        return self.legacy.prices

    @property
    def price_history(self):
        return self.legacy.price_history


class WSOrderBookReader:
    """
    Enhanced order book reader that checks WebSocket StateStore first,
    then falls back to the existing REST-based OrderBookReader.
    
    Drop-in replacement: same interface as OrderBookReader.
    """

    def __init__(self, legacy_book_reader, state_store=None, logger_instance=None):
        global logger
        if logger_instance:
            logger = logger_instance

        self.legacy = legacy_book_reader
        self.state_store = state_store
        self._ws_hit_count = 0
        self._fallback_count = 0

    def get_book(self, token_id: str) -> Optional[dict]:
        """Get orderbook. WebSocket first, then REST."""
        if self.state_store:
            ws_book = self.state_store.get_orderbook(token_id)
            if ws_book:
                self._ws_hit_count += 1
                return ws_book

        self._fallback_count += 1
        return self.legacy.get_book(token_id)

    def get_spread(self, token_id: str) -> Optional[dict]:
        """Get spread data. Uses WebSocket book if available."""
        if self.state_store:
            ws_book = self.state_store.get_orderbook(token_id)
            if ws_book:
                # Compute spread from WebSocket book data
                bids = ws_book.get("bids", [])
                asks = ws_book.get("asks", [])
                if bids and asks:
                    spread_data = self._compute_spread(bids, asks)
                    if spread_data:
                        self._ws_hit_count += 1
                        return spread_data

        self._fallback_count += 1
        return self.legacy.get_spread(token_id)

    def _compute_spread(self, bids: list, asks: list) -> Optional[dict]:
        """Compute spread from raw bid/ask data (same logic as OrderBookReader)."""
        if not bids or not asks:
            return None

        sorted_bids = sorted(bids, key=lambda x: float(x.get("price", 0)), reverse=True)
        sorted_asks = sorted(asks, key=lambda x: float(x.get("price", 0)))

        real_bids = [b for b in sorted_bids if float(b.get("price", 0)) > 0.05]
        real_asks = [a for a in sorted_asks if float(a.get("price", 0)) < 0.95]

        if not real_bids or not real_asks:
            real_bids = sorted_bids
            real_asks = sorted_asks

        if not real_bids or not real_asks:
            return None

        best_bid = float(real_bids[0]["price"])
        best_ask = float(real_asks[0]["price"])
        spread = best_ask - best_bid
        midpoint = (best_bid + best_ask) / 2

        total_bid_size = sum(float(b.get("size", 0)) for b in bids)
        total_ask_size = sum(float(a.get("size", 0)) for a in asks)
        imbalance = 0
        if (total_bid_size + total_ask_size) > 0:
            imbalance = (total_bid_size - total_ask_size) / (total_bid_size + total_ask_size)

        return {
            "bid": best_bid,
            "ask": best_ask,
            "spread": spread,
            "midpoint": midpoint,
            "total_bid_size": total_bid_size,
            "total_ask_size": total_ask_size,
            "imbalance": imbalance,
        }

    def invalidate_cache(self, token_id=None):
        """Invalidate cache. WebSocket data is always fresh, but clear legacy cache too."""
        self.legacy.invalidate_cache(token_id)

    def get_available_liquidity(self, token_id, side, price, size_needed):
        """Get available liquidity. Falls back to legacy for now."""
        return self.legacy.get_available_liquidity(token_id, side, price, size_needed)

    def get_stats(self) -> dict:
        return {
            "ws_hits": self._ws_hit_count,
            "fallbacks": self._fallback_count,
            "hit_rate": (
                self._ws_hit_count / max(1, self._ws_hit_count + self._fallback_count)
            ),
        }
