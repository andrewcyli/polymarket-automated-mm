"""
WebSocket-Enhanced Price Feed & Order Book Reader
===================================================
Wraps the existing PriceFeed and OrderBookReader with real-time WebSocket data.

Price feed priority:
1. RTDS WebSocket (Binance real-time) — sub-second latency
2. Chainlink on-chain (Polygon RPC) — ~2-3 second latency
3. CoinGecko REST API — ~10-30 second latency

Orderbook priority:
1. WebSocket book snapshot + incremental deltas — sub-second
2. WebSocket best_bid_ask events — sub-second (spread only)
3. REST API polling — ~1-5 second latency

The bot uses these as drop-in replacements.
If WebSocket is down, they automatically fall back to the existing REST-based feeds.
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
    
    Three data sources (in priority order):
    1. Full orderbook from StateStore (snapshot + incremental deltas)
    2. best_bid_ask from StateStore (fast spread-only path)
    3. REST API (legacy fallback)
    
    Drop-in replacement: same interface as OrderBookReader.
    """

    def __init__(self, legacy_book_reader, state_store=None, logger_instance=None):
        global logger
        if logger_instance:
            logger = logger_instance

        self.legacy = legacy_book_reader
        self.state_store = state_store
        self._ws_book_hits = 0
        self._ws_bba_hits = 0  # best_bid_ask fast path hits
        self._fallback_count = 0
        self._last_source: Dict[str, str] = {}  # token_id -> last source used

    @property
    def _ws_hit_count(self):
        """Backward-compatible total WS hit count."""
        return self._ws_book_hits + self._ws_bba_hits

    def get_book(self, token_id: str) -> Optional[dict]:
        """
        Get orderbook. WebSocket first, then REST.
        
        The WS book includes the initial snapshot plus any incremental
        price_change deltas applied since the last snapshot.
        """
        if self.state_store:
            ws_book = self.state_store.get_orderbook(token_id)
            if ws_book:
                self._ws_book_hits += 1
                self._last_source[token_id] = "ws_book"
                return ws_book

        self._fallback_count += 1
        self._last_source[token_id] = "rest"
        return self.legacy.get_book(token_id)

    def get_spread(self, token_id: str) -> Optional[dict]:
        """
        Get spread data. Uses three-tier priority:
        1. Full WS book → compute spread (most accurate, includes depth)
        2. WS best_bid_ask → instant spread (no depth info)
        3. REST fallback
        """
        if self.state_store:
            # Tier 1: Full WS book (includes depth for imbalance calculation)
            ws_book = self.state_store.get_orderbook(token_id)
            if ws_book:
                bids = ws_book.get("bids", [])
                asks = ws_book.get("asks", [])
                if bids and asks:
                    spread_data = self._compute_spread(bids, asks)
                    if spread_data:
                        self._ws_book_hits += 1
                        self._last_source[token_id] = "ws_book"
                        return spread_data

            # Tier 2: best_bid_ask fast path (no depth, but instant)
            bba = self.state_store.get_best_bid_ask(token_id)
            if bba:
                try:
                    best_bid = float(bba["best_bid"])
                    best_ask = float(bba["best_ask"])
                    if best_bid > 0 and best_ask > 0 and best_ask > best_bid:
                        spread = best_ask - best_bid
                        midpoint = (best_bid + best_ask) / 2
                        self._ws_bba_hits += 1
                        self._last_source[token_id] = "ws_bba"
                        return {
                            "bid": best_bid,
                            "ask": best_ask,
                            "spread": spread,
                            "midpoint": midpoint,
                            "total_bid_size": 0,  # Not available from bba
                            "total_ask_size": 0,
                            "imbalance": 0,
                        }
                except (ValueError, TypeError, KeyError):
                    pass

        # Tier 3: REST fallback
        self._fallback_count += 1
        self._last_source[token_id] = "rest"
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
        """
        Get available liquidity. Uses WS book if available, else REST.
        
        This is the same algorithm as OrderBookReader.get_available_liquidity
        but uses the WS-maintained book instead of making a REST call.
        """
        # Try WS book first
        if self.state_store:
            ws_book = self.state_store.get_orderbook(token_id)
            if ws_book:
                return self._compute_liquidity(ws_book, side, price, size_needed)

        # Fall back to legacy REST
        return self.legacy.get_available_liquidity(token_id, side, price, size_needed)

    def _compute_liquidity(self, book: dict, side: str, price: float, size_needed: float) -> dict:
        """
        Compute available liquidity from book data.
        Same algorithm as OrderBookReader.get_available_liquidity.
        """
        if side == "BUY":
            orders = sorted(book.get("asks", []), key=lambda x: float(x.get("price", 0)))
        else:
            orders = sorted(book.get("bids", []), key=lambda x: float(x.get("price", 0)), reverse=True)

        filled = 0.0
        total_cost = 0.0
        for order in orders:
            op = float(order.get("price", 0))
            os_val = float(order.get("size", 0))
            can_fill = min(os_val, size_needed - filled)
            filled += can_fill
            total_cost += can_fill * op
            if filled >= size_needed:
                break

        avg_price = total_cost / filled if filled > 0 else price
        slippage = abs(avg_price - price) / price if price > 0 else 0
        return {
            "available": filled >= size_needed * 0.5,
            "fillable_size": filled,
            "avg_price": avg_price,
            "slippage": slippage,
        }

    def get_book_freshness(self, token_id: str) -> Optional[dict]:
        """Get book freshness info for diagnostics."""
        if self.state_store:
            return self.state_store.get_book_freshness(token_id)
        return None

    def get_stats(self) -> dict:
        """Get detailed orderbook reader statistics."""
        total = self._ws_book_hits + self._ws_bba_hits + self._fallback_count
        return {
            "ws_book_hits": self._ws_book_hits,
            "ws_bba_hits": self._ws_bba_hits,
            "ws_hits": self._ws_hit_count,  # backward compat
            "fallbacks": self._fallback_count,
            "hit_rate": (
                self._ws_hit_count / max(1, total)
            ),
            "sources": dict(self._last_source),
        }
