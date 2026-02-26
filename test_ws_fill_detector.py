"""
Tests for WSFillDetector — Phases 2-4 real-time fill detection.

Tests cover:
1. Event subscription and handler registration
2. ORDER_FILL event processing (maker fills)
3. USER_TRADE event processing (taker fills)
4. Deduplication of fills
5. Unknown order handling
6. Fill processing into TradingEngine state
7. Health check and REST fallback logic
8. Thread safety
9. Stats tracking
10. Phase 4: ORDER_UPDATE lifecycle tracking (PLACEMENT, CANCELLATION)
11. Phase 4: Partial fill detection from cancellation events
12. Phase 4: Periodic REST reconciliation
13. Phase 4: WS order confirmation tracking
"""

import time
import threading
import pytest
from unittest.mock import MagicMock, patch
from collections import defaultdict

# Import the module under test
from ws_fill_detector import WSFillDetector
from ws_manager import EventBus, EventType, Channel


class MockEngine:
    """Mock TradingEngine with the same attributes WSFillDetector accesses."""

    def __init__(self):
        self.active_orders = {}
        self.orders_by_window = {}
        self.window_fill_cost = {}
        self.filled_windows = set()
        self.window_entry_count = {}
        self.window_fill_tokens = {}
        self.window_fill_sides = {}
        self.paired_windows = set()
        self.expired_windows_pending_claim = {}
        self.closed_windows = set()
        self._pending_hedges = []
        self._is_up_token_cache = {}
        self._recently_cancelled = {}
        self.token_holdings = {}
        self.capital_in_positions = 0
        self.session_total_spent = 0
        self.capital_deployed = 0
        self.total_capital_used = 0
        self.total_exposure = 0
        self.asset_exposure = {}
        self.window_exposure = {}
        self.config = MagicMock()
        self.config.hedge_completion_enabled = True
        self.config.dry_run = False

        # Track calls
        self._record_fill_calls = []
        self._recalc_exposure_calls = 0

    def record_fill(self, token_id, side, price, size, fee=0):
        self._record_fill_calls.append({
            "token_id": token_id, "side": side,
            "price": price, "size": size, "fee": fee,
        })
        if side == "BUY":
            if token_id not in self.token_holdings:
                self.token_holdings[token_id] = {"size": 0, "cost": 0}
            self.token_holdings[token_id]["size"] += size
            self.token_holdings[token_id]["cost"] += price * size + fee
            self.capital_in_positions += price * size + fee
            self.session_total_spent += price * size + fee

    def _recalc_exposure(self):
        self._recalc_exposure_calls += 1

    def _update_total_capital(self):
        self.total_capital_used = self.capital_deployed + self.capital_in_positions

    def check_fills(self):
        """Original REST-based check_fills — returns 0 for mock."""
        return 0

    # Mock client for REST reconciliation tests
    client = None


class MockWSManager:
    """Mock WebSocketManager with EventBus and connection tracking."""

    def __init__(self):
        self.event_bus = EventBus()
        self._connections = {}

    def set_user_connected(self, connected: bool):
        """Helper to simulate user channel connection state."""
        conn = MagicMock()
        conn.connected = connected
        self._connections[Channel.USER] = conn


class TestWSFillDetectorInit:
    """Test initialization and lifecycle."""

    def test_init(self):
        ws = MockWSManager()
        engine = MockEngine()
        detector = WSFillDetector(ws, engine)
        assert not detector._started
        assert detector._ws_fills_total == 0

    def test_start_subscribes_to_events(self):
        ws = MockWSManager()
        engine = MockEngine()
        detector = WSFillDetector(ws, engine)
        detector.start()
        assert detector._started
        # Verify event subscriptions by checking EventBus has handlers
        assert len(ws.event_bus._handlers.get(EventType.ORDER_FILL, [])) == 1
        assert len(ws.event_bus._handlers.get(EventType.USER_TRADE, [])) == 1
        assert len(ws.event_bus._handlers.get(EventType.ORDER_UPDATE, [])) == 1

    def test_stop_unsubscribes(self):
        ws = MockWSManager()
        engine = MockEngine()
        detector = WSFillDetector(ws, engine)
        detector.start()
        detector.stop()
        assert not detector._started
        assert len(ws.event_bus._handlers.get(EventType.ORDER_FILL, [])) == 0
        assert len(ws.event_bus._handlers.get(EventType.USER_TRADE, [])) == 0
        assert len(ws.event_bus._handlers.get(EventType.ORDER_UPDATE, [])) == 0

    def test_start_without_ws_manager(self):
        engine = MockEngine()
        detector = WSFillDetector(None, engine)
        detector.start()
        assert not detector._started  # Should not start without WS


class TestOrderFillProcessing:
    """Test ORDER_FILL event handling and processing."""

    def setup_method(self):
        self.ws = MockWSManager()
        self.engine = MockEngine()
        self.detector = WSFillDetector(self.ws, self.engine)
        self.detector.start()

    def test_order_fill_queued(self):
        """ORDER_FILL for a known active order should be queued."""
        self.engine.active_orders["order-1"] = {
            "window_id": "BTC-w1", "side": "BUY", "price": 0.47,
            "size": 31.2, "token_id": "token-abc", "strategy": "mm",
        }
        self.ws.event_bus.emit(EventType.ORDER_FILL, {
            "order_id": "order-1",
            "price": 0.47,
            "size": 31.2,
            "asset_id": "token-abc",
            "status": "MATCHED",
        })
        assert len(self.detector._fill_queue) == 1

    def test_order_fill_unknown_order_skipped(self):
        """ORDER_FILL for an unknown order should be skipped."""
        self.ws.event_bus.emit(EventType.ORDER_FILL, {
            "order_id": "unknown-order",
            "price": 0.50,
            "size": 10,
            "asset_id": "token-xyz",
            "status": "MATCHED",
        })
        assert len(self.detector._fill_queue) == 0

    def test_order_fill_no_order_id_skipped(self):
        """ORDER_FILL without order_id should be skipped."""
        self.ws.event_bus.emit(EventType.ORDER_FILL, {
            "price": 0.50,
            "size": 10,
        })
        assert len(self.detector._fill_queue) == 0

    def test_order_fill_deduplication(self):
        """Same order_id should not be queued twice."""
        self.engine.active_orders["order-1"] = {
            "window_id": "BTC-w1", "side": "BUY", "price": 0.47,
            "size": 31.2, "token_id": "token-abc",
        }
        fill_data = {
            "order_id": "order-1",
            "price": 0.47,
            "size": 31.2,
            "asset_id": "token-abc",
            "status": "MATCHED",
        }
        self.ws.event_bus.emit(EventType.ORDER_FILL, fill_data)
        self.ws.event_bus.emit(EventType.ORDER_FILL, fill_data)
        # First one queued, second deduplicated (but not yet processed)
        # Both should be in queue since dedup happens at process time for queued items
        # Actually, the dedup check is in _on_order_fill using _processed_order_ids
        # Since we haven't called check_fills_ws yet, the order hasn't been marked processed
        # But the queue should have 2 entries (dedup happens at processing)
        # Wait — looking at the code, the dedup in _on_order_fill checks _processed_order_ids
        # which is only populated after check_fills_ws processes the fill.
        # So both events get queued, but check_fills_ws will only process the first one.
        assert len(self.detector._fill_queue) == 2

        # Process fills — only 1 should be processed
        fills = self.detector.check_fills_ws()
        assert fills == 1

    def test_check_fills_ws_processes_buy(self):
        """check_fills_ws should process BUY fills correctly."""
        self.engine.active_orders["order-1"] = {
            "window_id": "BTC-w1", "side": "BUY", "price": 0.47,
            "size": 31.2, "token_id": "token-abc", "strategy": "mm",
        }
        self.engine._is_up_token_cache["token-abc"] = True

        self.ws.event_bus.emit(EventType.ORDER_FILL, {
            "order_id": "order-1",
            "price": 0.47,
            "size": 31.2,
            "asset_id": "token-abc",
            "status": "MATCHED",
        })

        fills = self.detector.check_fills_ws()
        assert fills == 1

        # Verify engine state was updated
        assert "order-1" not in self.engine.active_orders
        assert len(self.engine._record_fill_calls) == 1
        assert self.engine._record_fill_calls[0]["token_id"] == "token-abc"
        assert self.engine._record_fill_calls[0]["side"] == "BUY"
        assert self.engine._record_fill_calls[0]["price"] == 0.47
        assert self.engine._record_fill_calls[0]["size"] == 31.2
        assert "BTC-w1" in self.engine.filled_windows
        assert self.engine.window_entry_count["BTC-w1"] == 1
        assert self.engine._recalc_exposure_calls == 1

    def test_check_fills_ws_processes_sell(self):
        """check_fills_ws should process SELL fills correctly."""
        self.engine.active_orders["order-2"] = {
            "window_id": "BTC-w1", "side": "SELL", "price": 0.53,
            "size": 20.0, "token_id": "token-def", "strategy": "mm",
        }

        self.ws.event_bus.emit(EventType.ORDER_FILL, {
            "order_id": "order-2",
            "price": 0.53,
            "size": 20.0,
            "asset_id": "token-def",
            "status": "MATCHED",
        })

        fills = self.detector.check_fills_ws()
        assert fills == 1
        assert "order-2" not in self.engine.active_orders
        assert "BTC-w1" not in self.engine.filled_windows  # SELL doesn't mark filled

    def test_paired_window_detection(self):
        """Both UP and DOWN fills in same window should mark as paired."""
        # Fill UP side
        self.engine.active_orders["order-up"] = {
            "window_id": "BTC-w1", "side": "BUY", "price": 0.47,
            "size": 31.2, "token_id": "token-up", "strategy": "mm",
        }
        self.engine._is_up_token_cache["token-up"] = True

        self.ws.event_bus.emit(EventType.ORDER_FILL, {
            "order_id": "order-up", "price": 0.47, "size": 31.2,
            "asset_id": "token-up", "status": "MATCHED",
        })
        self.detector.check_fills_ws()

        # Fill DOWN side
        self.engine.active_orders["order-down"] = {
            "window_id": "BTC-w1", "side": "BUY", "price": 0.53,
            "size": 31.2, "token_id": "token-down", "strategy": "mm",
        }
        self.engine._is_up_token_cache["token-down"] = False

        self.ws.event_bus.emit(EventType.ORDER_FILL, {
            "order_id": "order-down", "price": 0.53, "size": 31.2,
            "asset_id": "token-down", "status": "MATCHED",
        })
        self.detector.check_fills_ws()

        assert "BTC-w1" in self.engine.paired_windows

    def test_hedge_pending_on_single_side_fill(self):
        """Single-side BUY fill should create pending hedge."""
        self.engine.active_orders["order-up"] = {
            "window_id": "BTC-w1", "side": "BUY", "price": 0.47,
            "size": 31.2, "token_id": "token-up", "strategy": "mm",
        }
        self.engine._is_up_token_cache["token-up"] = True

        self.ws.event_bus.emit(EventType.ORDER_FILL, {
            "order_id": "order-up", "price": 0.47, "size": 31.2,
            "asset_id": "token-up", "status": "MATCHED",
        })
        self.detector.check_fills_ws()

        assert len(self.engine._pending_hedges) == 1
        hedge = self.engine._pending_hedges[0]
        assert hedge["window_id"] == "BTC-w1"
        assert hedge["filled_side"] == "UP"

    def test_orders_by_window_updated(self):
        """Filled order should be removed from orders_by_window."""
        self.engine.active_orders["order-1"] = {
            "window_id": "BTC-w1", "side": "BUY", "price": 0.47,
            "size": 31.2, "token_id": "token-abc",
        }
        self.engine.orders_by_window["BTC-w1"] = ["order-1", "order-2"]

        self.ws.event_bus.emit(EventType.ORDER_FILL, {
            "order_id": "order-1", "price": 0.47, "size": 31.2,
            "asset_id": "token-abc", "status": "MATCHED",
        })
        self.detector.check_fills_ws()

        assert self.engine.orders_by_window["BTC-w1"] == ["order-2"]


class TestUserTradeProcessing:
    """Test USER_TRADE event handling (taker fills)."""

    def setup_method(self):
        self.ws = MockWSManager()
        self.engine = MockEngine()
        self.detector = WSFillDetector(self.ws, self.engine)
        self.detector.start()

    def test_user_trade_queued(self):
        """USER_TRADE for a known active order should be queued."""
        self.engine.active_orders["taker-1"] = {
            "window_id": "BTC-w1", "side": "BUY", "price": 0.47,
            "size": 31.2, "token_id": "token-abc",
        }
        self.ws.event_bus.emit(EventType.USER_TRADE, {
            "taker_order_id": "taker-1",
            "price": 0.47,
            "size": 31.2,
            "side": "BUY",
            "asset_id": "token-abc",
            "market": "condition-1",
            "status": "MATCHED",
        })
        assert len(self.detector._fill_queue) == 1

    def test_user_trade_unknown_order_skipped(self):
        """USER_TRADE for unknown taker order should be skipped."""
        self.ws.event_bus.emit(EventType.USER_TRADE, {
            "taker_order_id": "unknown-taker",
            "price": 0.50,
            "size": 10,
        })
        assert len(self.detector._fill_queue) == 0

    def test_user_trade_processed(self):
        """USER_TRADE should be processed into engine state."""
        self.engine.active_orders["taker-1"] = {
            "window_id": "BTC-w1", "side": "BUY", "price": 0.47,
            "size": 31.2, "token_id": "token-abc",
        }
        self.engine._is_up_token_cache["token-abc"] = True

        self.ws.event_bus.emit(EventType.USER_TRADE, {
            "taker_order_id": "taker-1",
            "price": 0.47,
            "size": 31.2,
            "side": "BUY",
            "asset_id": "token-abc",
            "status": "MATCHED",
        })

        fills = self.detector.check_fills_ws()
        assert fills == 1
        assert "taker-1" not in self.engine.active_orders


class TestHealthAndFallback:
    """Test health checking and REST fallback logic."""

    def setup_method(self):
        self.ws = MockWSManager()
        self.engine = MockEngine()
        self.detector = WSFillDetector(self.ws, self.engine)
        self.detector.start()

    def test_healthy_when_user_connected(self):
        self.ws.set_user_connected(True)
        assert self.detector.is_healthy()

    def test_unhealthy_when_user_disconnected(self):
        self.ws.set_user_connected(False)
        assert not self.detector.is_healthy()

    def test_unhealthy_when_no_user_connection(self):
        # No user connection object at all
        assert not self.detector.is_healthy()

    def test_should_fallback_when_not_started(self):
        detector = WSFillDetector(self.ws, self.engine)
        assert detector.should_fallback_to_rest()

    def test_should_fallback_when_user_disconnected(self):
        self.ws.set_user_connected(False)
        assert self.detector.should_fallback_to_rest()

    def test_should_not_fallback_when_user_connected(self):
        self.ws.set_user_connected(True)
        assert not self.detector.should_fallback_to_rest()

    def test_rest_fallback_counter(self):
        self.detector.record_rest_fallback()
        self.detector.record_rest_fallback()
        stats = self.detector.get_stats()
        assert stats["rest_fallback_count"] == 2


class TestStats:
    """Test statistics tracking."""

    def setup_method(self):
        self.ws = MockWSManager()
        self.engine = MockEngine()
        self.detector = WSFillDetector(self.ws, self.engine)
        self.detector.start()

    def test_stats_initial(self):
        stats = self.detector.get_stats()
        assert stats["ws_fills_total"] == 0
        assert stats["ws_fills_session"] == 0
        assert stats["rest_fallback_count"] == 0
        assert stats["queue_size"] == 0

    def test_stats_after_fills(self):
        self.engine.active_orders["order-1"] = {
            "window_id": "BTC-w1", "side": "BUY", "price": 0.47,
            "size": 31.2, "token_id": "token-abc",
        }
        self.engine._is_up_token_cache["token-abc"] = True

        self.ws.event_bus.emit(EventType.ORDER_FILL, {
            "order_id": "order-1", "price": 0.47, "size": 31.2,
            "asset_id": "token-abc", "status": "MATCHED",
        })
        self.detector.check_fills_ws()

        stats = self.detector.get_stats()
        assert stats["ws_fills_total"] == 1
        assert stats["ws_fills_session"] == 1
        assert stats["queue_size"] == 0
        assert stats["processed_ids_count"] == 1

    def test_stats_fallback_mode(self):
        self.ws.set_user_connected(True)
        stats = self.detector.get_stats()
        assert not stats["fallback_mode"]

        self.ws.set_user_connected(False)
        stats = self.detector.get_stats()
        assert stats["fallback_mode"]


class TestThreadSafety:
    """Test thread safety of fill queue operations."""

    def test_concurrent_fill_events(self):
        """Multiple threads emitting fills should not corrupt state."""
        ws = MockWSManager()
        engine = MockEngine()
        detector = WSFillDetector(ws, engine)
        detector.start()

        # Pre-populate active orders
        for i in range(100):
            engine.active_orders[f"order-{i}"] = {
                "window_id": f"BTC-w{i}", "side": "BUY", "price": 0.47,
                "size": 10.0, "token_id": f"token-{i}",
            }
            engine._is_up_token_cache[f"token-{i}"] = True

        # Emit fills from multiple threads
        def emit_fills(start, end):
            for i in range(start, end):
                ws.event_bus.emit(EventType.ORDER_FILL, {
                    "order_id": f"order-{i}",
                    "price": 0.47,
                    "size": 10.0,
                    "asset_id": f"token-{i}",
                    "status": "MATCHED",
                })

        threads = [
            threading.Thread(target=emit_fills, args=(0, 25)),
            threading.Thread(target=emit_fills, args=(25, 50)),
            threading.Thread(target=emit_fills, args=(50, 75)),
            threading.Thread(target=emit_fills, args=(75, 100)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Process all fills
        fills = detector.check_fills_ws()
        assert fills == 100
        assert detector._ws_fills_total == 100

    def test_concurrent_emit_and_process(self):
        """Emitting and processing fills concurrently should be safe."""
        ws = MockWSManager()
        engine = MockEngine()
        detector = WSFillDetector(ws, engine)
        detector.start()

        total_fills = [0]

        # Pre-populate active orders
        for i in range(50):
            engine.active_orders[f"order-{i}"] = {
                "window_id": f"BTC-w{i}", "side": "BUY", "price": 0.47,
                "size": 10.0, "token_id": f"token-{i}",
            }
            engine._is_up_token_cache[f"token-{i}"] = True

        # Emit fills in background
        def emit_fills():
            for i in range(50):
                ws.event_bus.emit(EventType.ORDER_FILL, {
                    "order_id": f"order-{i}",
                    "price": 0.47,
                    "size": 10.0,
                    "asset_id": f"token-{i}",
                    "status": "MATCHED",
                })
                time.sleep(0.001)

        # Process fills in foreground
        def process_fills():
            for _ in range(100):
                fills = detector.check_fills_ws()
                total_fills[0] += fills
                time.sleep(0.001)

        t1 = threading.Thread(target=emit_fills)
        t2 = threading.Thread(target=process_fills)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # Final drain
        total_fills[0] += detector.check_fills_ws()

        assert total_fills[0] == 50


class TestEdgeCases:
    """Test edge cases and error handling."""

    def setup_method(self):
        self.ws = MockWSManager()
        self.engine = MockEngine()
        self.detector = WSFillDetector(self.ws, self.engine)
        self.detector.start()

    def test_check_fills_when_not_started(self):
        detector = WSFillDetector(self.ws, self.engine)
        assert detector.check_fills_ws() == 0

    def test_fill_with_zero_price(self):
        """Fill with zero price should still be processed."""
        self.engine.active_orders["order-1"] = {
            "window_id": "BTC-w1", "side": "BUY", "price": 0,
            "size": 10, "token_id": "token-abc",
        }
        self.ws.event_bus.emit(EventType.ORDER_FILL, {
            "order_id": "order-1", "price": 0, "size": 10,
            "asset_id": "token-abc", "status": "MATCHED",
        })
        fills = self.detector.check_fills_ws()
        assert fills == 1

    def test_fill_uses_ws_data_over_order_info(self):
        """WS fill data should take priority over stored order info."""
        self.engine.active_orders["order-1"] = {
            "window_id": "BTC-w1", "side": "BUY", "price": 0.47,
            "size": 31.2, "token_id": "token-abc",
        }
        self.engine._is_up_token_cache["token-abc"] = True

        # WS reports different price (partial fill at better price)
        self.ws.event_bus.emit(EventType.ORDER_FILL, {
            "order_id": "order-1", "price": 0.45, "size": 15.0,
            "asset_id": "token-abc", "status": "MATCHED",
        })
        self.detector.check_fills_ws()

        assert self.engine._record_fill_calls[0]["price"] == 0.45
        assert self.engine._record_fill_calls[0]["size"] == 15.0

    def test_fill_falls_back_to_order_info(self):
        """If WS fill data is missing, fall back to stored order info."""
        self.engine.active_orders["order-1"] = {
            "window_id": "BTC-w1", "side": "BUY", "price": 0.47,
            "size": 31.2, "token_id": "token-abc",
        }
        self.engine._is_up_token_cache["token-abc"] = True

        # WS reports fill with no price/size (unlikely but defensive)
        self.ws.event_bus.emit(EventType.ORDER_FILL, {
            "order_id": "order-1", "price": 0, "size": 0,
            "asset_id": "token-abc", "status": "MATCHED",
        })
        self.detector.check_fills_ws()

        # Should fall back to order info since WS price/size are 0 (falsy)
        assert self.engine._record_fill_calls[0]["price"] == 0.47
        assert self.engine._record_fill_calls[0]["size"] == 31.2

    def test_processed_ids_trimming(self):
        """Processed IDs set should be trimmed when it exceeds max size."""
        self.detector._max_processed_ids = 10

        for i in range(15):
            self.engine.active_orders[f"order-{i}"] = {
                "window_id": f"BTC-w{i}", "side": "SELL", "price": 0.5,
                "size": 10, "token_id": f"token-{i}",
            }
            self.ws.event_bus.emit(EventType.ORDER_FILL, {
                "order_id": f"order-{i}", "price": 0.5, "size": 10,
                "asset_id": f"token-{i}", "status": "MATCHED",
            })

        self.detector.check_fills_ws()
        assert len(self.detector._processed_order_ids) <= 10

    def test_order_removed_between_queue_and_process(self):
        """Order removed from active_orders between queue and process.
        If it's in _recently_cancelled, the fill should be recovered.
        If not, it should be skipped."""
        # Case 1: Order removed but NOT in _recently_cancelled → skip
        self.engine.active_orders["order-1"] = {
            "window_id": "BTC-w1", "side": "BUY", "price": 0.47,
            "size": 31.2, "token_id": "token-abc",
        }
        self.ws.event_bus.emit(EventType.ORDER_FILL, {
            "order_id": "order-1", "price": 0.47, "size": 31.2,
            "asset_id": "token-abc", "status": "MATCHED",
        })
        # Remove order before processing (simulates REST fallback already processed it)
        del self.engine.active_orders["order-1"]
        fills = self.detector.check_fills_ws()
        assert fills == 0  # Should skip — not in active_orders or _recently_cancelled

    def test_order_recovered_from_recently_cancelled(self):
        """Order cancelled but filled on exchange should be recovered."""
        self.engine.active_orders["order-2"] = {
            "window_id": "BTC-w2", "side": "BUY", "price": 0.48,
            "size": 20.0, "token_id": "token-def",
        }
        self.engine._is_up_token_cache["token-def"] = True
        self.ws.event_bus.emit(EventType.ORDER_FILL, {
            "order_id": "order-2", "price": 0.48, "size": 20.0,
            "asset_id": "token-def", "status": "MATCHED",
        })
        # Simulate cancel: move from active_orders to _recently_cancelled
        import time as _time
        info = self.engine.active_orders.pop("order-2")
        self.engine._recently_cancelled["order-2"] = {**info, "cancelled_at": _time.time()}
        fills = self.detector.check_fills_ws()
        assert fills == 1  # Should recover from _recently_cancelled
        assert "BTC-w2" in self.engine.filled_windows
        assert self.engine.window_fill_cost.get("BTC-w2", 0) > 0
        # Verify it was removed from _recently_cancelled after processing
        assert "order-2" not in self.engine._recently_cancelled

    def test_multiple_fills_same_cycle(self):
        """Multiple fills in the same cycle should all be processed."""
        for i in range(5):
            self.engine.active_orders[f"order-{i}"] = {
                "window_id": f"BTC-w{i}", "side": "BUY", "price": 0.47,
                "size": 10.0, "token_id": f"token-{i}",
            }
            self.engine._is_up_token_cache[f"token-{i}"] = True
            self.ws.event_bus.emit(EventType.ORDER_FILL, {
                "order_id": f"order-{i}", "price": 0.47, "size": 10.0,
                "asset_id": f"token-{i}", "status": "MATCHED",
            })

        fills = self.detector.check_fills_ws()
        assert fills == 5
        assert self.engine._recalc_exposure_calls == 1  # Only called once at end


class TestIntegrationWithMainLoop:
    """Test the WS-first, REST-fallback pattern used in main.py."""

    def test_ws_healthy_uses_ws(self):
        """When WS is healthy, should use WS fill detection."""
        ws = MockWSManager()
        ws.set_user_connected(True)
        engine = MockEngine()
        detector = WSFillDetector(ws, engine)
        detector.start()

        # Simulate main loop logic
        if not detector.should_fallback_to_rest():
            fills = detector.check_fills_ws()
            source = "WS"
        else:
            fills = engine.check_fills()
            source = "REST"

        assert source == "WS"

    def test_ws_unhealthy_falls_back_to_rest(self):
        """When WS is unhealthy, should fall back to REST."""
        ws = MockWSManager()
        ws.set_user_connected(False)
        engine = MockEngine()
        detector = WSFillDetector(ws, engine)
        detector.start()

        if not detector.should_fallback_to_rest():
            fills = detector.check_fills_ws()
            source = "WS"
        else:
            fills = engine.check_fills()
            source = "REST"
            detector.record_rest_fallback()

        assert source == "REST"
        assert detector.get_stats()["rest_fallback_count"] == 1

    def test_ws_reconnects_switches_back_to_ws(self):
        """After WS reconnects, should switch back to WS fill detection."""
        ws = MockWSManager()
        engine = MockEngine()
        detector = WSFillDetector(ws, engine)
        detector.start()

        # Start disconnected
        ws.set_user_connected(False)
        assert detector.should_fallback_to_rest()

        # Reconnect
        ws.set_user_connected(True)
        assert not detector.should_fallback_to_rest()


class TestPhase4OrderLifecycle:
    """Phase 4: Test ORDER_UPDATE lifecycle tracking."""

    def setup_method(self):
        self.ws = MockWSManager()
        self.engine = MockEngine()
        self.detector = WSFillDetector(self.ws, self.engine)
        self.detector.start()

    def test_placement_confirmation_tracked(self):
        """PLACEMENT event should add order to confirmed set."""
        self.ws.event_bus.emit(EventType.ORDER_UPDATE, {
            "event_type": "order",
            "type": "PLACEMENT",
            "order_id": "order-1",
            "asset_id": "token-abc",
            "price": "0.47",
            "side": "BUY",
            "original_size": "31.2",
            "size_matched": "0",
        })
        assert "order-1" in self.detector._ws_confirmed_orders
        assert self.detector._ws_placements_confirmed == 1

    def test_cancellation_removes_from_confirmed(self):
        """CANCELLATION should remove order from confirmed set."""
        # First confirm the order
        self.ws.event_bus.emit(EventType.ORDER_UPDATE, {
            "event_type": "order",
            "type": "PLACEMENT",
            "order_id": "order-1",
            "asset_id": "token-abc",
            "price": "0.47",
            "side": "BUY",
            "original_size": "31.2",
            "size_matched": "0",
        })
        assert "order-1" in self.detector._ws_confirmed_orders

        # Now cancel it
        self.engine.active_orders["order-1"] = {
            "window_id": "BTC-w1", "side": "BUY", "price": 0.47,
            "size": 31.2, "token_id": "token-abc",
        }
        self.ws.event_bus.emit(EventType.ORDER_UPDATE, {
            "event_type": "order",
            "type": "CANCELLATION",
            "order_id": "order-1",
            "size_matched": "0",
        })
        assert "order-1" not in self.detector._ws_confirmed_orders
        assert self.detector._ws_cancellations_detected == 1

    def test_cancellation_removes_from_active_orders(self):
        """CANCELLATION event should remove order from engine.active_orders."""
        self.engine.active_orders["order-1"] = {
            "window_id": "BTC-w1", "side": "BUY", "price": 0.47,
            "size": 31.2, "token_id": "token-abc",
        }
        self.engine.orders_by_window["BTC-w1"] = ["order-1", "order-2"]

        self.ws.event_bus.emit(EventType.ORDER_UPDATE, {
            "event_type": "order",
            "type": "CANCELLATION",
            "order_id": "order-1",
            "size_matched": "0",
        })

        assert "order-1" not in self.engine.active_orders
        assert self.engine.orders_by_window["BTC-w1"] == ["order-2"]

    def test_cancellation_with_partial_fill(self):
        """CANCELLATION with size_matched > 0 should queue partial fill."""
        self.engine.active_orders["order-1"] = {
            "window_id": "BTC-w1", "side": "BUY", "price": 0.47,
            "size": 31.2, "token_id": "token-abc",
        }
        self.engine._is_up_token_cache["token-abc"] = True

        self.ws.event_bus.emit(EventType.ORDER_UPDATE, {
            "event_type": "order",
            "type": "CANCELLATION",
            "order_id": "order-1",
            "size_matched": "15.6",  # Half filled before cancel
        })

        # Should have queued a partial fill
        assert len(self.detector._partial_fill_queue) == 1
        pfill = self.detector._partial_fill_queue[0]
        assert pfill["size"] == 15.6
        assert pfill["price"] == 0.47
        assert pfill["window_id"] == "BTC-w1"

        # Process the partial fill
        fills = self.detector.check_fills_ws()
        assert fills == 1
        assert self.engine._record_fill_calls[0]["size"] == 15.6
        assert "BTC-w1" in self.engine.filled_windows

    def test_cancellation_full_fill_no_partial(self):
        """CANCELLATION where size_matched == original_size should NOT queue partial."""
        self.engine.active_orders["order-1"] = {
            "window_id": "BTC-w1", "side": "BUY", "price": 0.47,
            "size": 31.2, "token_id": "token-abc",
        }

        self.ws.event_bus.emit(EventType.ORDER_UPDATE, {
            "event_type": "order",
            "type": "CANCELLATION",
            "order_id": "order-1",
            "size_matched": "31.2",  # Fully filled — not a partial
        })

        # Full fill should NOT be queued as partial (it will come via ORDER_FILL)
        assert len(self.detector._partial_fill_queue) == 0

    def test_cancellation_zero_matched_no_partial(self):
        """CANCELLATION with size_matched=0 should not queue partial fill."""
        self.engine.active_orders["order-1"] = {
            "window_id": "BTC-w1", "side": "BUY", "price": 0.47,
            "size": 31.2, "token_id": "token-abc",
        }

        self.ws.event_bus.emit(EventType.ORDER_UPDATE, {
            "event_type": "order",
            "type": "CANCELLATION",
            "order_id": "order-1",
            "size_matched": "0",
        })

        assert len(self.detector._partial_fill_queue) == 0

    def test_cancellation_unknown_order_no_crash(self):
        """CANCELLATION for unknown order should not crash."""
        self.ws.event_bus.emit(EventType.ORDER_UPDATE, {
            "event_type": "order",
            "type": "CANCELLATION",
            "order_id": "unknown-order",
            "size_matched": "10",
        })
        # Should not crash, partial fill queue should be empty
        assert len(self.detector._partial_fill_queue) == 0
        assert self.detector._ws_cancellations_detected == 1

    def test_no_order_id_skipped(self):
        """ORDER_UPDATE without order_id should be skipped."""
        self.ws.event_bus.emit(EventType.ORDER_UPDATE, {
            "event_type": "order",
            "type": "PLACEMENT",
            "order_id": "",
        })
        assert self.detector._ws_placements_confirmed == 0

    def test_trade_failed_logged(self):
        """FAILED trade status should be handled without crash."""
        self.ws.event_bus.emit(EventType.ORDER_UPDATE, {
            "event_type": "trade",
            "status": "FAILED",
            "taker_order_id": "taker-1",
        })
        # Should not crash — just logged

    def test_stats_include_phase4_fields(self):
        """get_stats should include Phase 4 fields."""
        self.ws.event_bus.emit(EventType.ORDER_UPDATE, {
            "event_type": "order",
            "type": "PLACEMENT",
            "order_id": "order-1",
            "size_matched": "0",
        })
        stats = self.detector.get_stats()
        assert "ws_placements_confirmed" in stats
        assert "ws_cancellations_detected" in stats
        assert stats["ws_placements_confirmed"] == 1

    def test_purge_ws_cancelled_orders(self):
        """Stale cancelled order entries should be purged."""
        self.detector._ws_cancelled_orders["old-order"] = {
            "size_matched": 0, "ts": time.time() - 400,
        }
        self.detector._ws_cancelled_orders["new-order"] = {
            "size_matched": 0, "ts": time.time() - 10,
        }
        self.detector.purge_ws_cancelled_orders(max_age=300)
        assert "old-order" not in self.detector._ws_cancelled_orders
        assert "new-order" in self.detector._ws_cancelled_orders


class TestPhase4RESTReconciliation:
    """Phase 4: Test periodic REST reconciliation."""

    def setup_method(self):
        self.ws = MockWSManager()
        self.engine = MockEngine()
        # Set up a mock client for REST calls
        self.engine.client = MagicMock()
        self.detector = WSFillDetector(self.ws, self.engine)
        self.detector.start()

    def test_reconcile_skips_non_interval_cycles(self):
        """REST reconciliation should only run every N cycles."""
        self.detector._rest_reconcile_interval = 10
        # Cycles 1-9 should skip
        for i in range(9):
            result = self.detector.rest_reconcile()
            assert result == 0
        # Cycle 10 should run (but no fills)
        self.engine.client.get_orders.return_value = []
        result = self.detector.rest_reconcile()
        assert result == 0
        assert self.engine.client.get_orders.called

    def test_reconcile_detects_missing_fill(self):
        """REST reconciliation should detect fills missed by WS."""
        self.detector._rest_reconcile_interval = 1  # Run every cycle for testing
        self.engine.active_orders["order-1"] = {
            "window_id": "BTC-w1", "side": "BUY", "price": 0.47,
            "size": 31.2, "token_id": "token-abc",
        }
        self.engine._is_up_token_cache["token-abc"] = True

        # REST says order-1 is NOT in open orders (it was filled)
        self.engine.client.get_orders.return_value = []

        result = self.detector.rest_reconcile()
        assert result == 1
        assert "order-1" not in self.engine.active_orders
        assert "BTC-w1" in self.engine.filled_windows

    def test_reconcile_skips_ws_cancelled_orders(self):
        """REST reconciliation should skip orders already cancelled via WS."""
        self.detector._rest_reconcile_interval = 1
        self.engine.active_orders["order-1"] = {
            "window_id": "BTC-w1", "side": "BUY", "price": 0.47,
            "size": 31.2, "token_id": "token-abc",
        }
        # Mark as cancelled via WS
        self.detector._ws_cancelled_orders["order-1"] = {
            "size_matched": 0, "ts": time.time(),
        }

        self.engine.client.get_orders.return_value = []

        result = self.detector.rest_reconcile()
        assert result == 0  # Should skip — already handled by WS cancel

    def test_reconcile_skips_already_processed(self):
        """REST reconciliation should skip already-processed fills."""
        self.detector._rest_reconcile_interval = 1
        self.engine.active_orders["order-1"] = {
            "window_id": "BTC-w1", "side": "BUY", "price": 0.47,
            "size": 31.2, "token_id": "token-abc",
        }
        self.detector._processed_order_ids.add("order-1")

        self.engine.client.get_orders.return_value = []

        result = self.detector.rest_reconcile()
        assert result == 0

    def test_reconcile_skips_still_open_orders(self):
        """REST reconciliation should skip orders still open on exchange."""
        self.detector._rest_reconcile_interval = 1
        self.engine.active_orders["order-1"] = {
            "window_id": "BTC-w1", "side": "BUY", "price": 0.47,
            "size": 31.2, "token_id": "token-abc",
        }

        # REST says order-1 IS still open
        self.engine.client.get_orders.return_value = [{"id": "order-1"}]

        result = self.detector.rest_reconcile()
        assert result == 0
        assert "order-1" in self.engine.active_orders  # Still active

    def test_reconcile_recovers_recently_cancelled(self):
        """REST reconciliation should check _recently_cancelled buffer too."""
        self.detector._rest_reconcile_interval = 1
        self.engine._recently_cancelled["order-rc"] = {
            "window_id": "BTC-w2", "side": "BUY", "price": 0.48,
            "size": 20.0, "token_id": "token-def",
            "cancelled_at": time.time(),
        }
        self.engine._is_up_token_cache["token-def"] = True

        self.engine.client.get_orders.return_value = []

        result = self.detector.rest_reconcile()
        assert result == 1
        assert "order-rc" not in self.engine._recently_cancelled
        assert "BTC-w2" in self.engine.filled_windows

    def test_reconcile_skips_dry_run(self):
        """REST reconciliation should skip in dry_run mode."""
        self.detector._rest_reconcile_interval = 1
        self.engine.config.dry_run = True
        result = self.detector.rest_reconcile()
        assert result == 0

    def test_reconcile_handles_api_error(self):
        """REST reconciliation should handle API errors gracefully."""
        self.detector._rest_reconcile_interval = 1
        self.engine.client.get_orders.side_effect = Exception("API timeout")
        result = self.detector.rest_reconcile()
        assert result == 0  # Should not crash


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
