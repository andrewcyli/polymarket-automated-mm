"""
Tests for WebSocket Manager (Phase 1)
======================================
Tests the StateStore, EventBus, WSPriceFeed, WSOrderBookReader,
and WebSocketManager initialization/lifecycle.

Run: python -m pytest test_ws_manager.py -v
"""

import json
import time
import threading
import pytest
from unittest.mock import MagicMock, patch, AsyncMock

from ws_manager import (
    StateStore, EventBus, EventType, Channel,
    WSConnection, WebSocketManager, WS_URLS,
    INITIAL_RECONNECT_DELAY, MAX_RECONNECT_DELAY,
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
        # Manually set timestamp to 60 seconds ago
        with self.store._lock:
            self.store._orderbooks["token_abc"]["timestamp"] = time.time() - 60
        result = self.store.get_orderbook("token_abc")
        assert result is None  # Stale (>30s)

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

    # ── Trades ──

    def test_record_and_get_trades(self):
        self.store.record_trade("token_abc", {"price": 0.50, "size": 10, "side": "BUY"})
        self.store.record_trade("token_abc", {"price": 0.51, "size": 5, "side": "SELL"})
        trades = self.store.get_recent_trades("token_abc")
        assert len(trades) == 2
        assert trades[0]["price"] == 0.50
        assert trades[1]["side"] == "SELL"

    def test_trades_max_50(self):
        for i in range(60):
            self.store.record_trade("token_abc", {"price": 0.50 + i * 0.001, "size": 1, "side": "BUY"})
        with self.store._lock:
            assert len(self.store._recent_trades["token_abc"]) == 50

    def test_trades_age_filter(self):
        self.store.record_trade("token_abc", {"price": 0.50, "size": 10, "side": "BUY"})
        # Manually age the trade
        with self.store._lock:
            self.store._recent_trades["token_abc"][0]["ts"] = time.time() - 120
        trades = self.store.get_recent_trades("token_abc", max_age=60)
        assert len(trades) == 0

    # ── Resolved Markets ──

    def test_mark_and_check_resolved(self):
        self.store.mark_resolved("cond_123", "UP")
        assert self.store.is_resolved("cond_123") == "UP"
        assert self.store.is_resolved("cond_456") is None

    # ── Order Statuses ──

    def test_update_and_get_order_status(self):
        self.store.update_order_status("order_1", "OPEN", 0)
        result = self.store.get_order_status("order_1")
        assert result["status"] == "OPEN"
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

        # Mark one as processed
        self.store.mark_fill_processed(pending[0]["ts"])
        pending = self.store.get_pending_fills()
        assert len(pending) == 1

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
        assert all_prices["btc"]["price"] == 95000.0

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
        """Test that concurrent writes don't corrupt state."""
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
        # All prices should be written
        assert self.store.get_crypto_price("btc") is not None
        assert self.store.get_crypto_price("eth") is not None
        assert self.store.get_crypto_price("sol") is not None


# ─────────────────────────────────────────────────────────────────
# EventBus Tests
# ─────────────────────────────────────────────────────────────────

class TestEventBus:
    """Test the thread-safe EventBus."""

    def setup_method(self):
        self.bus = EventBus()

    def test_subscribe_and_emit(self):
        received = []
        def handler(event_type, data):
            received.append((event_type, data))

        self.bus.subscribe(EventType.CRYPTO_PRICE, handler)
        self.bus.emit(EventType.CRYPTO_PRICE, {"asset": "btc", "price": 95000})

        assert len(received) == 1
        assert received[0][0] == EventType.CRYPTO_PRICE
        assert received[0][1]["asset"] == "btc"

    def test_multiple_handlers(self):
        counts = {"a": 0, "b": 0}
        def handler_a(et, d):
            counts["a"] += 1
        def handler_b(et, d):
            counts["b"] += 1

        self.bus.subscribe(EventType.ORDER_FILL, handler_a)
        self.bus.subscribe(EventType.ORDER_FILL, handler_b)
        self.bus.emit(EventType.ORDER_FILL, {"order_id": "o1"})

        assert counts["a"] == 1
        assert counts["b"] == 1

    def test_unsubscribe(self):
        received = []
        def handler(et, d):
            received.append(d)

        self.bus.subscribe(EventType.CONNECTED, handler)
        self.bus.emit(EventType.CONNECTED, {"channel": "market"})
        assert len(received) == 1

        self.bus.unsubscribe(EventType.CONNECTED, handler)
        self.bus.emit(EventType.CONNECTED, {"channel": "market"})
        assert len(received) == 1  # No new events

    def test_handler_error_doesnt_crash(self):
        def bad_handler(et, d):
            raise ValueError("boom")

        received = []
        def good_handler(et, d):
            received.append(d)

        self.bus.subscribe(EventType.TRADE, bad_handler)
        self.bus.subscribe(EventType.TRADE, good_handler)

        # Should not raise, and good_handler should still fire
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
        def handler_a(et, d):
            received_a.append(d)
        def handler_b(et, d):
            received_b.append(d)

        self.bus.subscribe(EventType.CRYPTO_PRICE, handler_a)
        self.bus.subscribe(EventType.ORDER_FILL, handler_b)

        self.bus.emit(EventType.CRYPTO_PRICE, {"asset": "btc"})
        assert len(received_a) == 1
        assert len(received_b) == 0

        self.bus.emit(EventType.ORDER_FILL, {"order_id": "o1"})
        assert len(received_a) == 1
        assert len(received_b) == 1


# ─────────────────────────────────────────────────────────────────
# WSPriceFeed Tests
# ─────────────────────────────────────────────────────────────────

class TestWSPriceFeed:
    """Test the WS-enhanced price feed wrapper."""

    def setup_method(self):
        self.legacy = MagicMock()
        self.store = StateStore()
        self.feed = WSPriceFeed(self.legacy, state_store=self.store)

    def test_ws_price_takes_priority(self):
        """WebSocket price should be returned when fresh."""
        self.store.update_crypto_price("btc", 95000.0, "binance")
        self.legacy.get_current_price.return_value = 94500.0

        price = self.feed.get_current_price("btc")
        assert price == 95000.0
        # Legacy should NOT have been called
        self.legacy.get_current_price.assert_not_called()

    def test_fallback_to_legacy(self):
        """Should fall back to legacy when WS price is stale."""
        self.legacy.get_current_price.return_value = 94500.0
        self.legacy.use_chainlink = True

        price = self.feed.get_current_price("btc")
        assert price == 94500.0
        self.legacy.get_current_price.assert_called_once_with("btc")

    def test_fallback_when_ws_stale(self):
        """Should fall back when WS price is older than max_age."""
        self.store.update_crypto_price("btc", 95000.0, "binance")
        # Make it stale
        with self.store._lock:
            self.store._crypto_prices["btc"]["ts"] = time.time() - 30
        self.legacy.get_current_price.return_value = 94500.0
        self.legacy.use_chainlink = False

        price = self.feed.get_current_price("btc")
        assert price == 94500.0

    def test_no_state_store(self):
        """Should work without state store (REST-only mode)."""
        feed = WSPriceFeed(self.legacy, state_store=None)
        self.legacy.get_current_price.return_value = 94500.0
        self.legacy.use_chainlink = True

        price = feed.get_current_price("btc")
        assert price == 94500.0

    def test_update_calls_legacy(self):
        """update() should always call legacy update for Chainlink/CoinGecko."""
        self.feed.update()
        self.legacy.update.assert_called_once()

    def test_momentum_delegates_to_legacy(self):
        """Momentum calculation delegates to legacy (needs history)."""
        self.legacy.get_momentum.return_value = 0.003
        result = self.feed.get_momentum("btc", 5)
        assert result == 0.003
        self.legacy.get_momentum.assert_called_once_with("btc", 5)

    def test_predict_resolution_delegates(self):
        """predict_resolution delegates to legacy."""
        self.legacy.predict_resolution.return_value = {"direction": "UP", "confidence": 0.75}
        result = self.feed.predict_resolution("btc", 100, 200)
        assert result["direction"] == "UP"

    def test_stats_tracking(self):
        """Stats should track WS hits vs fallbacks."""
        self.store.update_crypto_price("btc", 95000.0)
        self.legacy.get_current_price.return_value = 3500.0
        self.legacy.use_chainlink = True

        self.feed.get_current_price("btc")  # WS hit
        self.feed.get_current_price("eth")  # Fallback

        stats = self.feed.get_stats()
        assert stats["ws_hits"] == 1
        assert stats["fallbacks"] == 1
        assert stats["hit_rate"] == 0.5


# ─────────────────────────────────────────────────────────────────
# WSOrderBookReader Tests
# ─────────────────────────────────────────────────────────────────

class TestWSOrderBookReader:
    """Test the WS-enhanced order book reader wrapper."""

    def setup_method(self):
        self.legacy = MagicMock()
        self.store = StateStore()
        self.reader = WSOrderBookReader(self.legacy, state_store=self.store)

    def test_ws_book_takes_priority(self):
        """WebSocket book should be returned when available."""
        book = {
            "bids": [{"price": "0.45", "size": "100"}],
            "asks": [{"price": "0.55", "size": "100"}],
        }
        self.store.update_orderbook("token_abc", book)
        self.legacy.get_book.return_value = {"bids": [], "asks": []}

        result = self.reader.get_book("token_abc")
        assert result is not None
        assert len(result["bids"]) == 1
        self.legacy.get_book.assert_not_called()

    def test_fallback_to_legacy_book(self):
        """Should fall back to legacy when no WS book."""
        self.legacy.get_book.return_value = {"bids": [{"price": "0.45"}], "asks": []}

        result = self.reader.get_book("token_abc")
        assert result is not None
        self.legacy.get_book.assert_called_once()

    def test_ws_spread_computation(self):
        """Should compute spread from WS book data."""
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
        """Should fall back to legacy spread when no WS book."""
        self.legacy.get_spread.return_value = {"bid": 0.45, "ask": 0.55, "spread": 0.10}

        spread = self.reader.get_spread("token_abc")
        assert spread is not None
        assert spread["bid"] == 0.45
        self.legacy.get_spread.assert_called_once()

    def test_spread_empty_book(self):
        """Should fall back when WS book has no bids/asks."""
        book = {"bids": [], "asks": []}
        self.store.update_orderbook("token_abc", book)
        self.legacy.get_spread.return_value = {"bid": 0.45, "ask": 0.55, "spread": 0.10}

        spread = self.reader.get_spread("token_abc")
        assert spread is not None
        self.legacy.get_spread.assert_called_once()

    def test_invalidate_cache_delegates(self):
        """invalidate_cache should delegate to legacy."""
        self.reader.invalidate_cache("token_abc")
        self.legacy.invalidate_cache.assert_called_once_with("token_abc")


# ─────────────────────────────────────────────────────────────────
# WSConnection Tests
# ─────────────────────────────────────────────────────────────────

class TestWSConnection:
    """Test the WSConnection message handling."""

    def setup_method(self):
        self.store = StateStore()
        self.bus = EventBus()

    def test_normalize_asset(self):
        """Test asset name normalization."""
        assert WSConnection._normalize_asset("BTCUSDT") == "btc"
        assert WSConnection._normalize_asset("ETHUSDC") == "eth"
        assert WSConnection._normalize_asset("SOLUSD") == "sol"
        assert WSConnection._normalize_asset("bitcoin") == "btc"
        assert WSConnection._normalize_asset("ethereum") == "eth"
        assert WSConnection._normalize_asset("BTC") == "btc"
        assert WSConnection._normalize_asset("xrp") == "xrp"

    def test_connection_properties(self):
        """Test connection initialization."""
        conn = WSConnection(Channel.MARKET, self.store, self.bus)
        assert conn.channel == Channel.MARKET
        assert conn.url == WS_URLS[Channel.MARKET]
        assert not conn.connected
        assert conn.message_count == 0


# ─────────────────────────────────────────────────────────────────
# WebSocketManager Tests
# ─────────────────────────────────────────────────────────────────

class TestWebSocketManager:
    """Test the WebSocketManager orchestrator."""

    def test_init_default(self):
        """Test default initialization."""
        mgr = WebSocketManager(api_key="test_key")
        assert mgr.enable_market is True
        assert mgr.enable_user is True
        assert mgr.enable_rtds is True
        assert mgr.state_store is not None
        assert mgr.event_bus is not None

    def test_init_no_api_key_disables_user(self):
        """User channel should be disabled without API key."""
        mgr = WebSocketManager(api_key="")
        assert mgr.enable_user is False

    def test_init_selective_channels(self):
        """Test selective channel enabling."""
        mgr = WebSocketManager(
            api_key="test",
            enable_market=False,
            enable_user=False,
            enable_rtds=True,
        )
        assert mgr.enable_market is False
        assert mgr.enable_user is False
        assert mgr.enable_rtds is True

    def test_subscribe_market_queues(self):
        """subscribe_market should queue the subscription."""
        mgr = WebSocketManager()
        mgr.subscribe_market("token_abc")
        with mgr._queue_lock:
            assert len(mgr._sub_queue) == 1
            channel, msg = mgr._sub_queue[0]
            assert channel == Channel.MARKET
            assert msg["assets_id"] == "token_abc"

    def test_subscribe_rtds_all(self):
        """subscribe_rtds_all should queue subscriptions for all assets."""
        mgr = WebSocketManager()
        mgr.subscribe_rtds_all(["BTC", "ETH", "SOL"])
        with mgr._queue_lock:
            assert len(mgr._sub_queue) == 3

    def test_subscribe_user_orders(self):
        """subscribe_user_orders should queue the subscription."""
        mgr = WebSocketManager(api_key="test")
        mgr.subscribe_user_orders()
        with mgr._queue_lock:
            assert len(mgr._sub_queue) == 1
            channel, msg = mgr._sub_queue[0]
            assert channel == Channel.USER

    def test_get_status_not_running(self):
        """Status should show not running before start."""
        mgr = WebSocketManager()
        status = mgr.get_status()
        assert status["running"] is False
        assert status["healthy"] is False

    def test_is_healthy_no_connections(self):
        """is_healthy should be False when not running."""
        mgr = WebSocketManager()
        assert mgr.is_healthy() is False

    def test_get_connection_summary_empty(self):
        """Connection summary should handle no connections."""
        mgr = WebSocketManager()
        summary = mgr.get_connection_summary()
        assert summary == "No connections"

    @patch("ws_manager.HAS_WEBSOCKETS", False)
    def test_start_without_websockets_lib(self):
        """Should return False if websockets library is not installed."""
        mgr = WebSocketManager()
        result = mgr.start()
        assert result is False


# ─────────────────────────────────────────────────────────────────
# Integration: StateStore + EventBus
# ─────────────────────────────────────────────────────────────────

class TestStateStoreEventBusIntegration:
    """Test that StateStore updates trigger EventBus events correctly."""

    def test_fill_flow(self):
        """Simulate a fill flowing through the system."""
        store = StateStore()
        bus = EventBus()

        fills_received = []
        def on_fill(et, data):
            fills_received.append(data)

        bus.subscribe(EventType.ORDER_FILL, on_fill)

        # Simulate what WSConnection._handle_user_message does
        fill_data = {
            "order_id": "order_123",
            "price": 0.45,
            "size": 10.0,
            "side": "BUY",
            "token_id": "token_abc",
        }
        store.record_fill(fill_data)
        bus.emit(EventType.ORDER_FILL, fill_data)

        assert len(fills_received) == 1
        assert fills_received[0]["order_id"] == "order_123"

        # Verify store has the fill
        pending = store.get_pending_fills()
        assert len(pending) == 1

    def test_crypto_price_flow(self):
        """Simulate crypto price update flowing through the system."""
        store = StateStore()
        bus = EventBus()

        prices_received = []
        def on_price(et, data):
            prices_received.append(data)

        bus.subscribe(EventType.CRYPTO_PRICE, on_price)

        # Simulate RTDS message
        store.update_crypto_price("btc", 95123.45, "binance")
        bus.emit(EventType.CRYPTO_PRICE, {"asset": "btc", "price": 95123.45, "source": "binance"})

        assert len(prices_received) == 1
        assert store.get_crypto_price("btc") == 95123.45

    def test_market_resolution_flow(self):
        """Simulate market resolution flowing through the system."""
        store = StateStore()
        bus = EventBus()

        resolutions = []
        def on_resolved(et, data):
            resolutions.append(data)

        bus.subscribe(EventType.MARKET_RESOLVED, on_resolved)

        store.mark_resolved("cond_123", "UP")
        bus.emit(EventType.MARKET_RESOLVED, {"condition_id": "cond_123", "outcome": "UP"})

        assert len(resolutions) == 1
        assert store.is_resolved("cond_123") == "UP"


# ─────────────────────────────────────────────────────────────────
# Graceful Degradation Tests
# ─────────────────────────────────────────────────────────────────

class TestGracefulDegradation:
    """Test that the system degrades gracefully when WS is unavailable."""

    def test_price_feed_without_ws(self):
        """WSPriceFeed should work fine without state_store."""
        legacy = MagicMock()
        legacy.get_current_price.return_value = 95000.0
        legacy.use_chainlink = True

        feed = WSPriceFeed(legacy, state_store=None)
        price = feed.get_current_price("btc")
        assert price == 95000.0

    def test_book_reader_without_ws(self):
        """WSOrderBookReader should work fine without state_store."""
        legacy = MagicMock()
        legacy.get_spread.return_value = {"bid": 0.45, "ask": 0.55, "spread": 0.10}

        reader = WSOrderBookReader(legacy, state_store=None)
        spread = reader.get_spread("token_abc")
        assert spread["bid"] == 0.45

    def test_price_feed_ws_down_then_up(self):
        """WSPriceFeed should seamlessly switch between WS and REST."""
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
