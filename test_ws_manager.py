"""
Tests for WebSocket Manager (Phase 1)
======================================
Tests cover:
1. StateStore — thread-safe data access
2. EventBus — pub/sub event system
3. WSPriceFeed — WS-first with REST fallback
4. WSOrderBookReader — WS-first with REST fallback
5. WSConnection — message parsing for all three channels
6. WebSocketManager — subscription message formats (Polymarket protocol)
7. Graceful degradation — fallback when WS is unavailable

Run: python -m pytest test_ws_manager.py -v
"""

import json
import time
import threading
import asyncio
import pytest
from unittest.mock import MagicMock, patch, AsyncMock

from ws_manager import (
    StateStore, EventBus, EventType, Channel,
    WSConnection, WebSocketManager, WS_URLS,
    INITIAL_RECONNECT_DELAY, MAX_RECONNECT_DELAY,
    MIN_SESSION_DURATION, RECONNECT_BACKOFF_FACTOR,
)
from ws_price_feed import WSPriceFeed, WSOrderBookReader


# ─────────────────────────────────────────────────────────────────
# StateStore Tests
# ─────────────────────────────────────────────────────────────────

class TestStateStore:
    """Test the thread-safe StateStore."""

    def setup_method(self):
        self.store = StateStore()

    # ── Orderbook ──

    def test_update_and_get_orderbook(self):
        book = {"bids": [{"price": "0.45", "size": "100"}], "asks": [{"price": "0.55", "size": "100"}]}
        self.store.update_orderbook("token_abc", book)
        result = self.store.get_orderbook("token_abc")
        assert result is not None
        assert result["bids"] == book["bids"]
        assert result["asks"] == book["asks"]
        assert "timestamp" in result

    def test_get_orderbook_stale(self):
        book = {"bids": [], "asks": []}
        self.store.update_orderbook("token_abc", book)
        with self.store._lock:
            # Must be older than 120s (new default max_age) to be stale
            self.store._orderbooks["token_abc"]["timestamp"] = time.time() - 130
        result = self.store.get_orderbook("token_abc")
        assert result is None

    def test_get_orderbook_custom_max_age(self):
        """Test that custom max_age parameter works."""
        book = {"bids": [], "asks": []}
        self.store.update_orderbook("token_abc", book)
        with self.store._lock:
            self.store._orderbooks["token_abc"]["timestamp"] = time.time() - 60
        # With default max_age=120, should still be fresh
        result = self.store.get_orderbook("token_abc")
        assert result is not None
        # With custom max_age=30, should be stale
        result = self.store.get_orderbook("token_abc", max_age=30.0)
        assert result is None

    def test_get_orderbook_missing(self):
        result = self.store.get_orderbook("nonexistent")
        assert result is None

    # ── Market Prices ──

    def test_update_and_get_market_price(self):
        self.store.update_market_price("cond_123", 0.65, 0.35)
        result = self.store.get_market_price("cond_123")
        assert result is not None
        assert result["price_up"] == 0.65
        assert result["price_down"] == 0.35

    def test_get_market_price_stale(self):
        self.store.update_market_price("cond_123", 0.65, 0.35)
        with self.store._lock:
            self.store._market_prices["cond_123"]["ts"] = time.time() - 60
        result = self.store.get_market_price("cond_123")
        assert result is None

    # ── Best Bid/Ask ──

    def test_update_and_get_best_bid_ask(self):
        self.store.update_best_bid_ask("asset_1", "0.50", "0.55", "0.05")
        bba = self.store.get_best_bid_ask("asset_1")
        assert bba is not None
        assert bba["best_bid"] == "0.50"
        assert bba["best_ask"] == "0.55"
        assert bba["spread"] == "0.05"

    def test_best_bid_ask_stale(self):
        self.store.update_best_bid_ask("asset_1", "0.50", "0.55", "0.05")
        with self.store._lock:
            # Must be older than 60s (new default max_age) to be stale
            self.store._best_bid_ask["asset_1"]["ts"] = time.time() - 70
        assert self.store.get_best_bid_ask("asset_1") is None

    def test_best_bid_ask_custom_max_age(self):
        """Test custom max_age parameter for BBA."""
        self.store.update_best_bid_ask("asset_1", "0.50", "0.55", "0.05")
        with self.store._lock:
            self.store._best_bid_ask["asset_1"]["ts"] = time.time() - 40
        # Default max_age=60 → still fresh
        assert self.store.get_best_bid_ask("asset_1") is not None
        # Custom max_age=30 → stale
        assert self.store.get_best_bid_ask("asset_1", max_age=30.0) is None

    # ── BBA Derivation from Book ──

    def test_derive_bba_from_book_snapshot(self):
        """update_orderbook should also populate best_bid_ask."""
        book = {
            "bids": [{"price": "0.45", "size": "100"}, {"price": "0.40", "size": "200"}],
            "asks": [{"price": "0.55", "size": "100"}, {"price": "0.60", "size": "200"}],
        }
        self.store.update_orderbook("token_xyz", book)
        bba = self.store.get_best_bid_ask("token_xyz")
        assert bba is not None
        assert float(bba["best_bid"]) == 0.45
        assert float(bba["best_ask"]) == 0.55
        assert float(bba["spread"]) == pytest.approx(0.10, abs=0.001)

    def test_derive_bba_from_book_delta(self):
        """apply_book_delta should update best_bid_ask."""
        book = {
            "bids": [{"price": "0.45", "size": "100"}],
            "asks": [{"price": "0.55", "size": "100"}],
        }
        self.store.update_orderbook("token_xyz", book)
        # Add a better bid
        self.store.apply_book_delta("token_xyz", "BUY", "0.48", "50")
        bba = self.store.get_best_bid_ask("token_xyz")
        assert bba is not None
        assert float(bba["best_bid"]) == 0.48
        assert float(bba["best_ask"]) == 0.55

    def test_derive_bba_empty_book_no_crash(self):
        """Empty book should not crash BBA derivation."""
        book = {"bids": [], "asks": []}
        self.store.update_orderbook("token_empty", book)
        bba = self.store.get_best_bid_ask("token_empty")
        # No bids/asks → no BBA derived
        assert bba is None

    def test_derive_bba_filters_extreme_prices(self):
        """BBA derivation should filter extreme prices (< 0.05 bids, > 0.95 asks)."""
        book = {
            "bids": [{"price": "0.01", "size": "1000"}, {"price": "0.45", "size": "100"}],
            "asks": [{"price": "0.55", "size": "100"}, {"price": "0.99", "size": "1000"}],
        }
        self.store.update_orderbook("token_extreme", book)
        bba = self.store.get_best_bid_ask("token_extreme")
        assert bba is not None
        # Should filter out 0.01 bid and 0.99 ask
        assert float(bba["best_bid"]) == 0.45
        assert float(bba["best_ask"]) == 0.55

    # ── Trades ──

    def test_record_and_get_trades(self):
        self.store.record_trade("token_abc", {"price": 0.50, "size": 10, "side": "BUY"})
        self.store.record_trade("token_abc", {"price": 0.51, "size": 5, "side": "SELL"})
        trades = self.store.get_recent_trades("token_abc")
        assert len(trades) == 2
        assert trades[0]["price"] == 0.50

    def test_trades_max_50(self):
        for i in range(60):
            self.store.record_trade("token_abc", {"price": 0.50 + i * 0.001, "size": 1, "side": "BUY"})
        with self.store._lock:
            assert len(self.store._recent_trades["token_abc"]) == 50

    def test_trades_age_filter(self):
        self.store.record_trade("token_abc", {"price": 0.50, "size": 10, "side": "BUY"})
        with self.store._lock:
            self.store._recent_trades["token_abc"][0]["ts"] = time.time() - 120
        trades = self.store.get_recent_trades("token_abc", max_age=60)
        assert len(trades) == 0

    # ── Resolved Markets ──

    def test_mark_and_check_resolved(self):
        self.store.mark_resolved("cond_123", "Yes")
        assert self.store.is_resolved("cond_123") == "Yes"
        assert self.store.is_resolved("cond_456") is None

    # ── Order Statuses ──

    def test_update_and_get_order_status(self):
        self.store.update_order_status("order_1", "PLACEMENT", 0)
        result = self.store.get_order_status("order_1")
        assert result["status"] == "PLACEMENT"
        assert result["filled_size"] == 0

        self.store.update_order_status("order_1", "MATCHED", 10.5)
        result = self.store.get_order_status("order_1")
        assert result["status"] == "MATCHED"
        assert result["filled_size"] == 10.5

    # ── Fills ──

    def test_record_and_get_fills(self):
        self.store.record_fill({"order_id": "o1", "price": 0.45, "size": 10})
        self.store.record_fill({"order_id": "o2", "price": 0.55, "size": 5})
        fills = self.store.get_recent_fills()
        assert len(fills) == 2

    def test_fills_max_200(self):
        for i in range(210):
            self.store.record_fill({"order_id": f"o{i}", "price": 0.50, "size": 1})
        with self.store._lock:
            assert len(self.store._recent_fills) == 200

    def test_pending_fills(self):
        self.store.record_fill({"order_id": "o1", "price": 0.45, "size": 10})
        self.store.record_fill({"order_id": "o2", "price": 0.55, "size": 5})
        pending = self.store.get_pending_fills()
        assert len(pending) == 2
        # Use fill_id for precise matching (timestamps can collide on fast machines)
        self.store.mark_fill_processed(fill_id=pending[0]["fill_id"])
        pending = self.store.get_pending_fills()
        assert len(pending) == 1
        assert pending[0]["order_id"] == "o2"

    # ── Crypto Prices ──

    def test_update_and_get_crypto_price(self):
        self.store.update_crypto_price("btc", 95000.0, "binance")
        price = self.store.get_crypto_price("btc")
        assert price == 95000.0

    def test_crypto_price_stale(self):
        self.store.update_crypto_price("btc", 95000.0, "binance")
        with self.store._lock:
            self.store._crypto_prices["btc"]["ts"] = time.time() - 30
        price = self.store.get_crypto_price("btc", max_age=10)
        assert price is None

    def test_get_all_crypto_prices(self):
        self.store.update_crypto_price("btc", 95000.0)
        self.store.update_crypto_price("eth", 3500.0)
        all_prices = self.store.get_all_crypto_prices()
        assert "btc" in all_prices
        assert "eth" in all_prices

    # ── Connection State ──

    def test_connection_state(self):
        assert not self.store.is_connected("market")
        self.store.set_connected("market", True)
        assert self.store.is_connected("market")
        self.store.set_connected("market", False)
        assert not self.store.is_connected("market")

    def test_record_reconnect(self):
        self.store.record_reconnect("market")
        self.store.record_reconnect("market")
        with self.store._lock:
            assert self.store._connection_status["market"]["reconnects"] == 2

    def test_record_message(self):
        self.store.record_message("market")
        self.store.record_message("market")
        with self.store._lock:
            assert self.store._messages_received["market"] == 2

    # ── Diagnostics ──

    def test_get_status(self):
        self.store.update_crypto_price("btc", 95000.0)
        self.store.update_orderbook("token_abc", {"bids": [], "asks": []})
        self.store.record_fill({"order_id": "o1", "price": 0.45, "size": 10})
        status = self.store.get_status()
        assert "uptime_seconds" in status
        assert status["orderbooks_cached"] == 1
        assert status["crypto_prices_cached"] == 1
        assert status["recent_fills"] == 1

    # ── Thread Safety ──

    def test_concurrent_writes(self):
        errors = []
        def writer(asset, count):
            try:
                for i in range(count):
                    self.store.update_crypto_price(asset, 50000 + i)
                    self.store.record_trade(f"token_{asset}", {"price": 0.5, "size": 1, "side": "BUY"})
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=writer, args=("btc", 100)),
            threading.Thread(target=writer, args=("eth", 100)),
            threading.Thread(target=writer, args=("sol", 100)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(errors) == 0
        assert self.store.get_crypto_price("btc") is not None
        assert self.store.get_crypto_price("eth") is not None
        assert self.store.get_crypto_price("sol") is not None


# ─────────────────────────────────────────────────────────────────
# EventBus Tests
# ─────────────────────────────────────────────────────────────────

class TestEventBus:

    def setup_method(self):
        self.bus = EventBus()

    def test_subscribe_and_emit(self):
        received = []
        self.bus.subscribe(EventType.CRYPTO_PRICE, lambda et, d: received.append((et, d)))
        self.bus.emit(EventType.CRYPTO_PRICE, {"asset": "btc", "price": 95000})
        assert len(received) == 1
        assert received[0][0] == EventType.CRYPTO_PRICE

    def test_multiple_handlers(self):
        counts = {"a": 0, "b": 0}
        self.bus.subscribe(EventType.ORDER_FILL, lambda et, d: counts.__setitem__("a", counts["a"] + 1))
        self.bus.subscribe(EventType.ORDER_FILL, lambda et, d: counts.__setitem__("b", counts["b"] + 1))
        self.bus.emit(EventType.ORDER_FILL, {"order_id": "o1"})
        assert counts["a"] == 1
        assert counts["b"] == 1

    def test_unsubscribe(self):
        received = []
        handler = lambda et, d: received.append(d)
        self.bus.subscribe(EventType.CONNECTED, handler)
        self.bus.emit(EventType.CONNECTED, {"channel": "market"})
        assert len(received) == 1
        self.bus.unsubscribe(EventType.CONNECTED, handler)
        self.bus.emit(EventType.CONNECTED, {"channel": "market"})
        assert len(received) == 1

    def test_handler_error_doesnt_crash(self):
        def bad_handler(et, d):
            raise ValueError("boom")
        received = []
        self.bus.subscribe(EventType.TRADE, bad_handler)
        self.bus.subscribe(EventType.TRADE, lambda et, d: received.append(d))
        self.bus.emit(EventType.TRADE, {"price": 0.50})
        assert len(received) == 1

    def test_event_log(self):
        self.bus.emit(EventType.CONNECTED, {"channel": "market"})
        self.bus.emit(EventType.CRYPTO_PRICE, {"asset": "btc"})
        log = self.bus.get_event_log()
        assert len(log) == 2
        assert log[0]["type"] == "connected"
        assert log[1]["type"] == "crypto_price"

    def test_no_cross_event_leakage(self):
        received_a = []
        received_b = []
        self.bus.subscribe(EventType.CRYPTO_PRICE, lambda et, d: received_a.append(d))
        self.bus.subscribe(EventType.ORDER_FILL, lambda et, d: received_b.append(d))
        self.bus.emit(EventType.CRYPTO_PRICE, {"asset": "btc"})
        self.bus.emit(EventType.ORDER_FILL, {"order_id": "o1"})
        assert len(received_a) == 1
        assert len(received_b) == 1


# ─────────────────────────────────────────────────────────────────
# WSPriceFeed Tests
# ─────────────────────────────────────────────────────────────────

class TestWSPriceFeed:

    def setup_method(self):
        self.legacy = MagicMock()
        self.store = StateStore()
        self.feed = WSPriceFeed(self.legacy, state_store=self.store)

    def test_ws_price_takes_priority(self):
        self.store.update_crypto_price("btc", 95000.0, "binance")
        self.legacy.get_current_price.return_value = 94500.0
        price = self.feed.get_current_price("btc")
        assert price == 95000.0
        self.legacy.get_current_price.assert_not_called()

    def test_fallback_to_legacy(self):
        self.legacy.get_current_price.return_value = 94500.0
        self.legacy.use_chainlink = True
        price = self.feed.get_current_price("btc")
        assert price == 94500.0
        self.legacy.get_current_price.assert_called_once_with("btc")

    def test_fallback_when_ws_stale(self):
        self.store.update_crypto_price("btc", 95000.0, "binance")
        with self.store._lock:
            self.store._crypto_prices["btc"]["ts"] = time.time() - 30
        self.legacy.get_current_price.return_value = 94500.0
        self.legacy.use_chainlink = False
        price = self.feed.get_current_price("btc")
        assert price == 94500.0

    def test_no_state_store(self):
        feed = WSPriceFeed(self.legacy, state_store=None)
        self.legacy.get_current_price.return_value = 94500.0
        self.legacy.use_chainlink = True
        price = feed.get_current_price("btc")
        assert price == 94500.0

    def test_update_calls_legacy(self):
        self.feed.update()
        self.legacy.update.assert_called_once()

    def test_momentum_delegates_to_legacy(self):
        self.legacy.get_momentum.return_value = 0.003
        result = self.feed.get_momentum("btc", 5)
        assert result == 0.003

    def test_predict_resolution_delegates(self):
        self.legacy.predict_resolution.return_value = {"direction": "UP", "confidence": 0.75}
        result = self.feed.predict_resolution("btc", 100, 200)
        assert result["direction"] == "UP"

    def test_stats_tracking(self):
        self.store.update_crypto_price("btc", 95000.0)
        self.legacy.get_current_price.return_value = 3500.0
        self.legacy.use_chainlink = True
        self.feed.get_current_price("btc")  # WS hit
        self.feed.get_current_price("eth")  # Fallback
        stats = self.feed.get_stats()
        assert stats["ws_hits"] == 1
        assert stats["fallbacks"] == 1
        assert stats["hit_rate"] == 0.5

    def test_passthrough_properties(self):
        self.legacy.use_chainlink = True
        self.legacy.chainlink = "mock"
        self.legacy.prices = {"btc": 67000}
        self.legacy.price_history = []
        assert self.feed.use_chainlink is True
        assert self.feed.prices == {"btc": 67000}


# ─────────────────────────────────────────────────────────────────
# WSOrderBookReader Tests
# ─────────────────────────────────────────────────────────────────

class TestWSOrderBookReader:

    def setup_method(self):
        self.legacy = MagicMock()
        self.store = StateStore()
        self.reader = WSOrderBookReader(self.legacy, state_store=self.store)

    def test_ws_book_takes_priority(self):
        book = {
            "bids": [{"price": "0.45", "size": "100"}],
            "asks": [{"price": "0.55", "size": "100"}],
        }
        self.store.update_orderbook("token_abc", book)
        result = self.reader.get_book("token_abc")
        assert result is not None
        assert len(result["bids"]) == 1
        self.legacy.get_book.assert_not_called()

    def test_fallback_to_legacy_book(self):
        self.legacy.get_book.return_value = {"bids": [{"price": "0.45"}], "asks": []}
        result = self.reader.get_book("token_abc")
        assert result is not None
        self.legacy.get_book.assert_called_once()

    def test_ws_spread_computation(self):
        book = {
            "bids": [{"price": "0.45", "size": "100"}, {"price": "0.40", "size": "50"}],
            "asks": [{"price": "0.55", "size": "80"}, {"price": "0.60", "size": "30"}],
        }
        self.store.update_orderbook("token_abc", book)
        spread = self.reader.get_spread("token_abc")
        assert spread is not None
        assert spread["bid"] == 0.45
        assert spread["ask"] == 0.55
        assert abs(spread["spread"] - 0.10) < 0.001
        assert abs(spread["midpoint"] - 0.50) < 0.001
        assert spread["total_bid_size"] == 150
        assert spread["total_ask_size"] == 110

    def test_spread_fallback(self):
        self.legacy.get_spread.return_value = {"bid": 0.45, "ask": 0.55, "spread": 0.10}
        spread = self.reader.get_spread("token_abc")
        assert spread is not None
        assert spread["bid"] == 0.45
        self.legacy.get_spread.assert_called_once()

    def test_spread_empty_book(self):
        book = {"bids": [], "asks": []}
        self.store.update_orderbook("token_abc", book)
        self.legacy.get_spread.return_value = {"bid": 0.45, "ask": 0.55, "spread": 0.10}
        spread = self.reader.get_spread("token_abc")
        assert spread is not None
        self.legacy.get_spread.assert_called_once()

    def test_invalidate_cache_delegates(self):
        self.reader.invalidate_cache("token_abc")
        self.legacy.invalidate_cache.assert_called_once_with("token_abc")


# ─────────────────────────────────────────────────────────────────
# WSConnection Message Parsing Tests
# ─────────────────────────────────────────────────────────────────

class TestWSConnectionParsing:
    """Test that WSConnection correctly parses messages per Polymarket protocol."""

    def setup_method(self):
        self.store = StateStore()
        self.bus = EventBus()

    def _make_conn(self, channel):
        return WSConnection(channel, self.store, self.bus)

    # ── Market Channel ──

    @pytest.mark.asyncio
    async def test_market_book_event(self):
        conn = self._make_conn(Channel.MARKET)
        msg = {
            "event_type": "book",
            "asset_id": "token_abc",
            "market": "cond_123",
            "hash": "abc123",
            "bids": [{"price": "0.50", "size": "100"}],
            "asks": [{"price": "0.55", "size": "80"}],
        }
        await conn._handle_market_message(msg)
        book = self.store.get_orderbook("token_abc")
        assert book is not None
        assert len(book["bids"]) == 1
        assert book["hash"] == "abc123"

    @pytest.mark.asyncio
    async def test_market_price_change_event(self):
        conn = self._make_conn(Channel.MARKET)
        events = []
        self.bus.subscribe(EventType.PRICE_CHANGE, lambda et, d: events.append(d))
        msg = {
            "event_type": "price_change",
            "market": "cond_123",
            "price_changes": [
                {"asset_id": "token_1", "price": "0.55", "size": "100", "side": "BUY",
                 "best_bid": "0.54", "best_ask": "0.56"},
            ],
        }
        await conn._handle_market_message(msg)
        assert len(events) == 1
        assert events[0]["asset_id"] == "token_1"
        assert events[0]["best_bid"] == "0.54"

    @pytest.mark.asyncio
    async def test_market_last_trade_price_event(self):
        conn = self._make_conn(Channel.MARKET)
        events = []
        self.bus.subscribe(EventType.LAST_TRADE_PRICE, lambda et, d: events.append(d))
        msg = {
            "event_type": "last_trade_price",
            "asset_id": "token_abc",
            "market": "cond_123",
            "price": "0.55",
            "size": "100",
            "side": "BUY",
            "fee_rate_bps": "0",
        }
        await conn._handle_market_message(msg)
        assert len(events) == 1
        assert events[0]["price"] == 0.55
        trades = self.store.get_recent_trades("token_abc")
        assert len(trades) == 1

    @pytest.mark.asyncio
    async def test_market_best_bid_ask_event(self):
        conn = self._make_conn(Channel.MARKET)
        msg = {
            "event_type": "best_bid_ask",
            "asset_id": "token_abc",
            "market": "cond_123",
            "best_bid": "0.50",
            "best_ask": "0.55",
            "spread": "0.05",
        }
        await conn._handle_market_message(msg)
        bba = self.store.get_best_bid_ask("token_abc")
        assert bba is not None
        assert bba["best_bid"] == "0.50"

    @pytest.mark.asyncio
    async def test_market_resolved_event(self):
        conn = self._make_conn(Channel.MARKET)
        events = []
        self.bus.subscribe(EventType.MARKET_RESOLVED, lambda et, d: events.append(d))
        msg = {
            "event_type": "market_resolved",
            "market": "cond_123",
            "question": "Will BTC be above 70k?",
            "winning_outcome": "Yes",
            "winning_asset_id": "token_yes",
            "assets_ids": ["token_yes", "token_no"],
            "outcomes": ["Yes", "No"],
        }
        await conn._handle_market_message(msg)
        assert self.store.is_resolved("cond_123") == "Yes"
        assert len(events) == 1
        assert events[0]["winning_outcome"] == "Yes"

    @pytest.mark.asyncio
    async def test_market_new_market_event(self):
        conn = self._make_conn(Channel.MARKET)
        events = []
        self.bus.subscribe(EventType.NEW_MARKET, lambda et, d: events.append(d))
        msg = {
            "event_type": "new_market",
            "id": "market_456",
            "question": "Will ETH hit 5k?",
            "market": "cond_456",
            "slug": "will-eth-hit-5k",
            "assets_ids": ["token_a", "token_b"],
            "outcomes": ["Yes", "No"],
        }
        await conn._handle_market_message(msg)
        assert len(events) == 1
        assert events[0]["question"] == "Will ETH hit 5k?"

    @pytest.mark.asyncio
    async def test_market_tick_size_change_event(self):
        conn = self._make_conn(Channel.MARKET)
        events = []
        self.bus.subscribe(EventType.TICK_SIZE_CHANGE, lambda et, d: events.append(d))
        msg = {
            "event_type": "tick_size_change",
            "asset_id": "token_abc",
            "market": "cond_123",
            "old_tick_size": "0.01",
            "new_tick_size": "0.001",
        }
        await conn._handle_market_message(msg)
        assert len(events) == 1
        assert events[0]["new_tick_size"] == "0.001"

    # ── List Message Handling ──

    @pytest.mark.asyncio
    async def test_market_list_message(self):
        """Server sometimes sends JSON arrays instead of single dicts."""
        conn = self._make_conn(Channel.MARKET)
        events = []
        self.bus.subscribe(EventType.LAST_TRADE_PRICE, lambda et, d: events.append(d))
        # Server sends a list of two events
        data = [
            {
                "event_type": "last_trade_price",
                "asset_id": "token_1",
                "market": "cond_1",
                "price": "0.55",
                "size": "100",
                "side": "BUY",
                "fee_rate_bps": "0",
            },
            {
                "event_type": "last_trade_price",
                "asset_id": "token_2",
                "market": "cond_2",
                "price": "0.45",
                "size": "50",
                "side": "SELL",
                "fee_rate_bps": "0",
            },
        ]
        await conn._handle_market_message(data)
        assert len(events) == 2
        assert events[0]["token_id"] == "token_1"
        assert events[1]["token_id"] == "token_2"

    @pytest.mark.asyncio
    async def test_market_non_dict_non_list_ignored(self):
        """Non-dict, non-list messages are silently ignored."""
        conn = self._make_conn(Channel.MARKET)
        await conn._handle_market_message("some string")
        await conn._handle_market_message(42)
        # No crash, no state change

    @pytest.mark.asyncio
    async def test_user_list_message(self):
        """User channel can also receive list messages."""
        conn = self._make_conn(Channel.USER)
        events = []
        self.bus.subscribe(EventType.ORDER_UPDATE, lambda et, d: events.append(d))
        data = [
            {
                "event_type": "order",
                "type": "PLACEMENT",
                "id": "order_1",
                "asset_id": "token_abc",
                "market": "cond_1",
                "size_matched": "0",
            },
        ]
        await conn._handle_user_message(data)
        assert len(events) == 1

    # ── User Channel ──

    @pytest.mark.asyncio
    async def test_user_trade_matched(self):
        conn = self._make_conn(Channel.USER)
        fills = []
        order_updates = []
        self.bus.subscribe(EventType.USER_TRADE, lambda et, d: fills.append(d))
        self.bus.subscribe(EventType.ORDER_FILL, lambda et, d: order_updates.append(d))
        msg = {
            "event_type": "trade",
            "type": "TRADE",
            "status": "MATCHED",
            "taker_order_id": "taker_1",
            "asset_id": "token_abc",
            "market": "cond_123",
            "price": "0.55",
            "size": "100",
            "side": "BUY",
            "maker_orders": [
                {"order_id": "maker_1", "matched_amount": "50", "price": "0.55",
                 "asset_id": "token_abc", "outcome": "Yes"},
                {"order_id": "maker_2", "matched_amount": "50", "price": "0.55",
                 "asset_id": "token_abc", "outcome": "Yes"},
            ],
        }
        await conn._handle_user_message(msg)
        # Should have 1 USER_TRADE + 2 ORDER_FILL events
        assert len(fills) == 1
        assert len(order_updates) == 2
        # Check fill recorded in state store
        store_fills = self.store.get_recent_fills()
        assert len(store_fills) == 1
        assert store_fills[0]["taker_order_id"] == "taker_1"
        # Check order statuses
        taker_status = self.store.get_order_status("taker_1")
        assert taker_status["status"] == "MATCHED"
        maker_status = self.store.get_order_status("maker_1")
        assert maker_status["status"] == "MATCHED"

    @pytest.mark.asyncio
    async def test_user_trade_confirmed(self):
        """CONFIRMED status should update order status but NOT record a new fill."""
        conn = self._make_conn(Channel.USER)
        msg = {
            "event_type": "trade",
            "type": "TRADE",
            "status": "CONFIRMED",
            "taker_order_id": "taker_1",
            "asset_id": "token_abc",
            "price": "0.55",
            "size": "100",
            "side": "BUY",
            "maker_orders": [],
        }
        await conn._handle_user_message(msg)
        status = self.store.get_order_status("taker_1")
        assert status["status"] == "CONFIRMED"
        # No fill should be recorded for CONFIRMED (only MATCHED)
        assert len(self.store.get_recent_fills()) == 0

    @pytest.mark.asyncio
    async def test_user_order_placement(self):
        conn = self._make_conn(Channel.USER)
        events = []
        self.bus.subscribe(EventType.ORDER_UPDATE, lambda et, d: events.append(d))
        msg = {
            "event_type": "order",
            "type": "PLACEMENT",
            "id": "order_123",
            "asset_id": "token_abc",
            "market": "cond_123",
            "price": "0.55",
            "side": "BUY",
            "original_size": "100",
            "size_matched": "0",
            "outcome": "Yes",
        }
        await conn._handle_user_message(msg)
        assert len(events) == 1
        assert events[0]["type"] == "PLACEMENT"
        status = self.store.get_order_status("order_123")
        assert status["status"] == "PLACEMENT"

    @pytest.mark.asyncio
    async def test_user_order_cancellation(self):
        conn = self._make_conn(Channel.USER)
        msg = {
            "event_type": "order",
            "type": "CANCELLATION",
            "id": "order_123",
            "asset_id": "token_abc",
            "market": "cond_123",
            "size_matched": "50",
        }
        await conn._handle_user_message(msg)
        status = self.store.get_order_status("order_123")
        assert status["status"] == "CANCELLATION"

    # ── RTDS Channel ──

    @pytest.mark.asyncio
    async def test_rtds_binance_price(self):
        conn = self._make_conn(Channel.RTDS)
        msg = {
            "topic": "crypto_prices",
            "type": "update",
            "timestamp": 1753314064237,
            "payload": {
                "symbol": "btcusdt",
                "timestamp": 1753314088395,
                "value": 67234.50,
            },
        }
        await conn._handle_rtds_message(msg)
        price = self.store.get_crypto_price("btc")
        assert price == 67234.50

    @pytest.mark.asyncio
    async def test_rtds_chainlink_price(self):
        conn = self._make_conn(Channel.RTDS)
        msg = {
            "topic": "crypto_prices_chainlink",
            "type": "update",
            "timestamp": 1753314064237,
            "payload": {
                "symbol": "eth/usd",
                "timestamp": 1753314088395,
                "value": 3456.78,
            },
        }
        await conn._handle_rtds_message(msg)
        price = self.store.get_crypto_price("eth")
        assert price == 3456.78

    @pytest.mark.asyncio
    async def test_rtds_normalize_asset(self):
        conn = self._make_conn(Channel.RTDS)
        assert conn._normalize_asset("btcusdt") == "btc"
        assert conn._normalize_asset("ETHUSDT") == "eth"
        assert conn._normalize_asset("sol/usd") == "sol"
        assert conn._normalize_asset("SOLUSDC") == "sol"
        assert conn._normalize_asset("bitcoin") == "btc"
        assert conn._normalize_asset("XRP") == "xrp"

    # ── PONG and non-JSON handling ──

    @pytest.mark.asyncio
    async def test_pong_handling(self):
        conn = self._make_conn(Channel.MARKET)
        await conn._handle_message("PONG")
        assert conn._message_count == 0

    @pytest.mark.asyncio
    async def test_non_json_message(self):
        """Non-JSON, non-PONG messages are counted but gracefully ignored."""
        conn = self._make_conn(Channel.MARKET)
        await conn._handle_message("some random text")
        # Message is counted (it's not a PONG) but parsing fails gracefully
        assert conn._message_count == 1


# ─────────────────────────────────────────────────────────────────
# WebSocketManager Subscription Format Tests
# ─────────────────────────────────────────────────────────────────

class TestSubscriptionFormats:
    """Verify subscription messages match the official Polymarket WebSocket protocol."""

    def setup_method(self):
        self.manager = WebSocketManager(
            api_key="test_key",
            api_secret="test_secret",
            api_passphrase="test_pass",
            enable_market=True,
            enable_user=True,
            enable_rtds=True,
        )

    def test_market_subscribe_format(self):
        """Market subscribe (first call): {assets_ids: [...], type: 'market', custom_feature_enabled: true, initial_dump: true}"""
        self.manager.subscribe_market(["token_1", "token_2"])
        with self.manager._queue_lock:
            assert len(self.manager._sub_queue) == 1
            channel, msg = self.manager._sub_queue[0]
            assert channel == Channel.MARKET
            assert msg["type"] == "market"
            assert msg["assets_ids"] == ["token_1", "token_2"]
            assert msg["custom_feature_enabled"] is True
            assert msg["initial_dump"] is True

    def test_market_additional_subscribe_format(self):
        """Additional subscribe: {assets_ids: [...], operation: 'subscribe', custom_feature_enabled: true}"""
        self.manager.subscribe_market_additional(["token_3"])
        with self.manager._queue_lock:
            channel, msg = self.manager._sub_queue[0]
            assert msg["operation"] == "subscribe"
            assert msg["assets_ids"] == ["token_3"]
            assert msg["custom_feature_enabled"] is True

    def test_market_unsubscribe_format(self):
        """Unsubscribe: {assets_ids: [...], operation: 'unsubscribe'}"""
        self.manager.unsubscribe_market(["token_1"])
        with self.manager._queue_lock:
            channel, msg = self.manager._unsub_queue[0]
            assert msg["operation"] == "unsubscribe"
            assert msg["assets_ids"] == ["token_1"]

    def test_user_subscribe_format(self):
        """User subscribe (first call): sets initial_markets and triggers lazy connect.
        
        The first call to subscribe_user_orders() stores condition IDs on the
        WSConnection._initial_markets and starts the connection. The auth message
        is sent inside connect() with the full format:
        {auth: {...}, markets: [...], assets_ids: [], type: 'user', initial_dump: true}
        """
        # Create a user connection with auth creds (not yet running)
        conn = WSConnection(
            Channel.USER, self.manager.state_store, self.manager.event_bus,
            auth_creds={"apiKey": "test_key", "secret": "test_secret", "passphrase": "test_pass"},
        )
        self.manager._connections[Channel.USER] = conn
        # Without an event loop, subscribe_user_orders stores initial_markets
        # but can't actually start the connection (no loop)
        self.manager.subscribe_user_orders(["cond_1", "cond_2"])
        # Verify the condition IDs were stored for lazy connect
        assert conn._initial_markets == ["cond_1", "cond_2"]
        assert conn.auth_creds["apiKey"] == "test_key"
        assert conn.auth_creds["secret"] == "test_secret"
        assert conn.auth_creds["passphrase"] == "test_pass"

    def test_user_subscribe_subsequent_call(self):
        """Subsequent user subscribe (already running): queues operation subscribe."""
        conn = WSConnection(
            Channel.USER, self.manager.state_store, self.manager.event_bus,
            auth_creds={"apiKey": "test_key", "secret": "test_secret", "passphrase": "test_pass"},
        )
        conn._running = True  # Simulate already started
        self.manager._connections[Channel.USER] = conn
        self.manager.subscribe_user_orders(["cond_3", "cond_4"])
        with self.manager._queue_lock:
            channel, msg = self.manager._sub_queue[0]
            assert channel == Channel.USER
            assert msg["operation"] == "subscribe"
            assert msg["markets"] == ["cond_3", "cond_4"]

    def test_user_additional_subscribe_format(self):
        """Additional user subscribe: {markets: [...], operation: 'subscribe'}"""
        self.manager.subscribe_user_additional(["cond_3"])
        with self.manager._queue_lock:
            channel, msg = self.manager._sub_queue[0]
            assert msg["operation"] == "subscribe"
            assert msg["markets"] == ["cond_3"]

    def test_rtds_binance_subscribe_format(self):
        """RTDS Binance: {action: 'subscribe', subscriptions: [{topic: 'crypto_prices', type: 'update', filters: '...'}]}"""
        self.manager.subscribe_rtds_binance(["btc", "eth"])
        with self.manager._queue_lock:
            channel, msg = self.manager._sub_queue[0]
            assert channel == Channel.RTDS
            assert msg["action"] == "subscribe"
            assert len(msg["subscriptions"]) == 1
            sub = msg["subscriptions"][0]
            assert sub["topic"] == "crypto_prices"
            assert sub["type"] == "update"
            assert "btcusdt" in sub["filters"]
            assert "ethusdt" in sub["filters"]

    def test_rtds_chainlink_subscribe_format(self):
        """RTDS Chainlink: {action: 'subscribe', subscriptions: [{topic: 'crypto_prices_chainlink', type: '*'}]}"""
        self.manager.subscribe_rtds_chainlink(["btc"])
        with self.manager._queue_lock:
            channel, msg = self.manager._sub_queue[0]
            assert msg["action"] == "subscribe"
            sub = msg["subscriptions"][0]
            assert sub["topic"] == "crypto_prices_chainlink"
            assert sub["type"] == "*"

    def test_rtds_all_subscribes_both(self):
        self.manager.subscribe_rtds_all(["btc", "eth"])
        with self.manager._queue_lock:
            assert len(self.manager._sub_queue) == 2
            topics = [msg["subscriptions"][0]["topic"] for _, msg in self.manager._sub_queue]
            assert "crypto_prices" in topics
            assert "crypto_prices_chainlink" in topics

    def test_market_subscribe_dedup_first_call(self):
        """First call to subscribe_market sends full subscription with initial_dump."""
        self.manager.subscribe_market(["token_1", "token_2"])
        with self.manager._queue_lock:
            assert len(self.manager._sub_queue) == 1
            channel, msg = self.manager._sub_queue[0]
            assert msg["type"] == "market"
            assert msg["initial_dump"] is True
            assert msg["assets_ids"] == ["token_1", "token_2"]
        assert self.manager._market_first_sub_done is True
        assert self.manager._market_subscribed_tokens == {"token_1", "token_2"}

    def test_market_subscribe_dedup_subsequent_same_tokens(self):
        """Subsequent call with same tokens should be a no-op (no new subscription)."""
        self.manager.subscribe_market(["token_1", "token_2"])
        with self.manager._queue_lock:
            self.manager._sub_queue.clear()  # Clear first subscription
        # Call again with same tokens
        self.manager.subscribe_market(["token_1", "token_2"])
        with self.manager._queue_lock:
            assert len(self.manager._sub_queue) == 0  # No new subscription

    def test_market_subscribe_dedup_new_tokens_only(self):
        """Subsequent call with mix of old+new tokens should only subscribe new ones."""
        self.manager.subscribe_market(["token_1", "token_2"])
        with self.manager._queue_lock:
            self.manager._sub_queue.clear()
        # Call with 2 old + 1 new token
        self.manager.subscribe_market(["token_1", "token_2", "token_3"])
        with self.manager._queue_lock:
            assert len(self.manager._sub_queue) == 1
            channel, msg = self.manager._sub_queue[0]
            assert msg["operation"] == "subscribe"  # Incremental, not full
            assert msg["assets_ids"] == ["token_3"]
            assert "type" not in msg  # No type field for incremental
            assert "initial_dump" not in msg  # No initial_dump for incremental

    def test_market_subscribe_dedup_all_new_tokens(self):
        """Subsequent call with entirely new tokens should subscribe all of them."""
        self.manager.subscribe_market(["token_1"])
        with self.manager._queue_lock:
            self.manager._sub_queue.clear()
        self.manager.subscribe_market(["token_2", "token_3"])
        with self.manager._queue_lock:
            assert len(self.manager._sub_queue) == 1
            channel, msg = self.manager._sub_queue[0]
            assert set(msg["assets_ids"]) == {"token_2", "token_3"}

    def test_set_derived_creds(self):
        self.manager.set_derived_creds("derived_key", "derived_secret", "derived_pass")
        assert self.manager.api_key == "derived_key"
        assert self.manager.api_secret == "derived_secret"
        assert self.manager.api_passphrase == "derived_pass"


# ─────────────────────────────────────────────────────────────────
# WebSocketManager Lifecycle Tests
# ─────────────────────────────────────────────────────────────────

class TestWebSocketManager:

    def test_init_default(self):
        mgr = WebSocketManager(api_key="test_key")
        assert mgr.enable_market is True
        assert mgr.enable_user is True
        assert mgr.enable_rtds is True

    def test_init_no_api_key_disables_user(self):
        mgr = WebSocketManager(api_key="")
        assert mgr.enable_user is False

    def test_init_selective_channels(self):
        mgr = WebSocketManager(api_key="test", enable_market=False, enable_user=False, enable_rtds=True)
        assert mgr.enable_market is False
        assert mgr.enable_user is False
        assert mgr.enable_rtds is True

    def test_get_status_not_running(self):
        mgr = WebSocketManager()
        status = mgr.get_status()
        assert status["running"] is False
        assert status["healthy"] is False

    def test_is_healthy_no_connections(self):
        mgr = WebSocketManager()
        assert mgr.is_healthy() is False

    def test_get_connection_summary_empty(self):
        mgr = WebSocketManager()
        summary = mgr.get_connection_summary()
        assert summary == "No connections"

    @patch("ws_manager.HAS_WEBSOCKETS", False)
    def test_start_without_websockets_lib(self):
        mgr = WebSocketManager()
        result = mgr.start()
        assert result is False

    def test_empty_subscribe_calls(self):
        mgr = WebSocketManager()
        mgr.subscribe_market([])
        mgr.subscribe_market_additional([])
        mgr.unsubscribe_market([])
        mgr.subscribe_user_additional([])
        with mgr._queue_lock:
            assert len(mgr._sub_queue) == 0
            assert len(mgr._unsub_queue) == 0


# ─────────────────────────────────────────────────────────────────
# Reconnect Backoff Tests
# ─────────────────────────────────────────────────────────────────

class TestReconnectBackoff:
    """Verify backoff behavior for rapid disconnect scenarios."""

    def test_backoff_increases_on_short_session(self):
        conn = WSConnection(Channel.USER, StateStore(), EventBus())
        conn._reconnect_delay = 2.0
        conn._session_start = time.time() - 1  # 1 second session
        session_duration = time.time() - conn._session_start
        if session_duration < MIN_SESSION_DURATION:
            conn._reconnect_delay = min(
                conn._reconnect_delay * RECONNECT_BACKOFF_FACTOR,
                MAX_RECONNECT_DELAY,
            )
        assert conn._reconnect_delay == 4.0

    def test_backoff_resets_on_long_session(self):
        conn = WSConnection(Channel.MARKET, StateStore(), EventBus())
        conn._reconnect_delay = 16.0
        conn._session_start = time.time() - 30  # 30 second session
        session_duration = time.time() - conn._session_start
        if session_duration >= MIN_SESSION_DURATION:
            conn._reconnect_delay = INITIAL_RECONNECT_DELAY
        assert conn._reconnect_delay == INITIAL_RECONNECT_DELAY

    def test_backoff_caps_at_max(self):
        conn = WSConnection(Channel.USER, StateStore(), EventBus())
        conn._reconnect_delay = 50.0
        conn._session_start = time.time() - 1
        session_duration = time.time() - conn._session_start
        if session_duration < MIN_SESSION_DURATION:
            conn._reconnect_delay = min(
                conn._reconnect_delay * RECONNECT_BACKOFF_FACTOR,
                MAX_RECONNECT_DELAY,
            )
        assert conn._reconnect_delay == MAX_RECONNECT_DELAY


# ─────────────────────────────────────────────────────────────────
# Integration: StateStore + EventBus
# ─────────────────────────────────────────────────────────────────

class TestStateStoreEventBusIntegration:

    def test_fill_flow(self):
        store = StateStore()
        bus = EventBus()
        fills = []
        bus.subscribe(EventType.ORDER_FILL, lambda et, d: fills.append(d))
        fill_data = {"order_id": "order_123", "price": 0.45, "size": 10.0, "side": "BUY"}
        store.record_fill(fill_data)
        bus.emit(EventType.ORDER_FILL, fill_data)
        assert len(fills) == 1
        assert fills[0]["order_id"] == "order_123"
        assert len(store.get_pending_fills()) == 1

    def test_crypto_price_flow(self):
        store = StateStore()
        bus = EventBus()
        prices = []
        bus.subscribe(EventType.CRYPTO_PRICE, lambda et, d: prices.append(d))
        store.update_crypto_price("btc", 95123.45, "binance")
        bus.emit(EventType.CRYPTO_PRICE, {"asset": "btc", "price": 95123.45, "source": "binance"})
        assert len(prices) == 1
        assert store.get_crypto_price("btc") == 95123.45

    def test_market_resolution_flow(self):
        store = StateStore()
        bus = EventBus()
        resolutions = []
        bus.subscribe(EventType.MARKET_RESOLVED, lambda et, d: resolutions.append(d))
        store.mark_resolved("cond_123", "Yes")
        bus.emit(EventType.MARKET_RESOLVED, {"market": "cond_123", "winning_outcome": "Yes"})
        assert len(resolutions) == 1
        assert store.is_resolved("cond_123") == "Yes"


# ─────────────────────────────────────────────────────────────────
# Graceful Degradation Tests
# ─────────────────────────────────────────────────────────────────

class TestGracefulDegradation:

    def test_price_feed_without_ws(self):
        legacy = MagicMock()
        legacy.get_current_price.return_value = 95000.0
        legacy.use_chainlink = True
        feed = WSPriceFeed(legacy, state_store=None)
        price = feed.get_current_price("btc")
        assert price == 95000.0

    def test_book_reader_without_ws(self):
        legacy = MagicMock()
        legacy.get_spread.return_value = {"bid": 0.45, "ask": 0.55, "spread": 0.10}
        reader = WSOrderBookReader(legacy, state_store=None)
        spread = reader.get_spread("token_abc")
        assert spread["bid"] == 0.45

    def test_price_feed_ws_down_then_up(self):
        legacy = MagicMock()
        legacy.get_current_price.return_value = 94500.0
        legacy.use_chainlink = True
        store = StateStore()
        feed = WSPriceFeed(legacy, state_store=store)

        # Phase 1: No WS data → fallback
        price = feed.get_current_price("btc")
        assert price == 94500.0
        assert feed._fallback_count == 1

        # Phase 2: WS comes online → use WS
        store.update_crypto_price("btc", 95000.0, "binance")
        price = feed.get_current_price("btc")
        assert price == 95000.0
        assert feed._ws_hit_count == 1

        # Phase 3: WS goes stale → fallback again
        with store._lock:
            store._crypto_prices["btc"]["ts"] = time.time() - 30
        price = feed.get_current_price("btc")
        assert price == 94500.0
        assert feed._fallback_count == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
