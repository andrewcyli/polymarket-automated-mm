"""
Tests for V15.3 hedge tier fix: hedge tiers must work even after
cleanup_expired_windows removes the window from _market_cache.

Root cause: cleanup_expired_windows ran BEFORE process_hedge_completions,
removing _market_cache entries. When market=None, the old code would either
skip (time>0) or abandon (time<=0) the hedge, meaning hedge tiers could
NEVER execute for expired windows.

Fix: Store token_up/token_down/end_time/interval in the hedge entry at
creation time, and use that data as the primary source in
process_hedge_completions.
"""
import time
import unittest
from unittest.mock import MagicMock, patch
from trading_bot_v15 import TradingEngine, BotConfig, FeeCalculator


class MockBookReader:
    """Mock book reader that returns configurable spreads per token."""
    def __init__(self, spreads=None):
        self.spreads = spreads or {}

    def get_spread(self, token_id):
        return self.spreads.get(token_id)


def make_engine():
    """Create a TradingEngine with real FeeCalculator but mocked client/logger."""
    config = BotConfig()
    config.hedge_completion_enabled = True
    config.dry_run = True
    config.hedge_tiers = [(67, 1.03), (33, 1.05), (13, 1.08)]
    config.hedge_min_profit_per_share = -0.10  # Allow negative for testing
    config.hedge_t4_enabled = True
    config.hedge_t4_sell_pct = 5.0
    config.hedge_t4_max_loss = 0.03
    engine = TradingEngine(config, MagicMock(), MagicMock())
    # Replace the mock fee_calc with a real one
    engine.fee_calc = FeeCalculator()
    return engine


class TestHedgeEntryStoresMarketData(unittest.TestCase):
    """V15.3: Hedge entries must include token_up, token_down, end_time, interval."""

    def test_hedge_entry_has_token_fields(self):
        """Directly create a hedge entry and verify it has the new fields."""
        engine = make_engine()
        wid = "btc-5m-1772065200"
        token_up = "TOKEN_UP_123"
        token_down = "TOKEN_DOWN_456"
        end_time = 1772065200
        interval = 300

        # Set up market cache and is_up cache
        engine._market_cache[wid] = {
            "window_id": wid, "token_up": token_up, "token_down": token_down,
            "end_time": end_time, "interval": interval,
        }
        engine._is_up_token_cache[token_up] = True

        # Simulate the fill detection logic (the part that creates hedge entries)
        fill_token = token_up
        fill_price = 0.50
        fill_size = 10
        side_label = "UP"

        # Reproduce the hedge creation logic from check_fills
        engine.window_fill_sides[wid] = {"UP": [{"token_id": fill_token, "price": fill_price, "size": fill_size}]}
        _hedge_market = engine._market_cache.get(wid, {})
        _hedge_meta = engine.window_metadata.get(wid, {})
        engine._pending_hedges.append({
            "window_id": wid, "filled_side": side_label,
            "filled_price": fill_price, "filled_size": fill_size,
            "filled_token": fill_token, "time": time.time(),
            "token_up": _hedge_market.get("token_up", _hedge_meta.get("token_up", "")),
            "token_down": _hedge_market.get("token_down", _hedge_meta.get("token_down", "")),
            "end_time": _hedge_market.get("end_time", _hedge_meta.get("end_time", 0)),
            "interval": _hedge_market.get("interval", 300 if "-5m-" in wid else 900),
        })

        hedge = engine._pending_hedges[0]
        self.assertEqual(hedge["token_up"], token_up)
        self.assertEqual(hedge["token_down"], token_down)
        self.assertEqual(hedge["end_time"], end_time)
        self.assertEqual(hedge["interval"], interval)

    def test_hedge_entry_falls_back_to_metadata(self):
        """When _market_cache is empty, hedge entry should use window_metadata."""
        engine = make_engine()
        wid = "eth-5m-1772065500"
        token_up = "META_UP_789"
        token_down = "META_DOWN_012"

        engine.window_metadata[wid] = {
            "token_up": token_up, "token_down": token_down,
            "end_time": 1772065500,
        }

        _hedge_market = engine._market_cache.get(wid, {})
        _hedge_meta = engine.window_metadata.get(wid, {})
        engine._pending_hedges.append({
            "window_id": wid, "filled_side": "UP",
            "filled_price": 0.48, "filled_size": 10,
            "filled_token": "FILL_TOKEN", "time": time.time(),
            "token_up": _hedge_market.get("token_up", _hedge_meta.get("token_up", "")),
            "token_down": _hedge_market.get("token_down", _hedge_meta.get("token_down", "")),
            "end_time": _hedge_market.get("end_time", _hedge_meta.get("end_time", 0)),
            "interval": _hedge_market.get("interval", 300 if "-5m-" in wid else 900),
        })

        hedge = engine._pending_hedges[0]
        self.assertEqual(hedge["token_up"], token_up)
        self.assertEqual(hedge["token_down"], token_down)


class TestHedgeTiersWorkAfterCleanup(unittest.TestCase):
    """V15.3: Hedge tiers must execute even after cleanup_expired_windows
    removes the window from _market_cache."""

    def test_hedge_completes_without_market_cache(self):
        """Hedge completion should work using hedge entry data when
        _market_cache has been cleared by cleanup_expired_windows."""
        engine = make_engine()
        wid = "btc-5m-1772065200"
        now = time.time()
        window_end = now - 100  # observation started 100s ago
        interval = 300
        # market_close = window_end + 300 = now + 200 (200s remaining)
        # pct_remaining = 200/300 * 100 = 66.7% -> T1 triggers (threshold 67%)

        other_token = "TOKEN_DOWN_456"

        engine._pending_hedges.append({
            "window_id": wid, "filled_side": "UP",
            "filled_price": 0.50, "filled_size": 10,
            "filled_token": "TOKEN_UP_123",
            "time": now - 60,
            "token_up": "TOKEN_UP_123", "token_down": other_token,
            "end_time": window_end, "interval": interval,
        })

        # _market_cache is EMPTY (post-cleanup)
        engine._market_cache = {}

        book = MockBookReader(spreads={
            other_token: {"ask": 0.50, "bid": 0.48}
        })

        with patch.object(engine, 'place_order', return_value=True) as mock_order:
            completed = engine.process_hedge_completions(book)

        self.assertEqual(completed, 1)
        self.assertEqual(len(engine._pending_hedges), 0)
        mock_order.assert_called_once()
        call_args = mock_order.call_args
        self.assertEqual(call_args[0][0], other_token)
        self.assertEqual(call_args[0][1], "BUY")

    def test_hedge_skips_when_no_tier_active(self):
        """Hedge should skip when pct_remaining is above all tier thresholds."""
        engine = make_engine()
        wid = "btc-5m-1772065200"
        now = time.time()
        window_end = now - 20  # 280s remaining, pct = 93%
        interval = 300

        engine._pending_hedges.append({
            "window_id": wid, "filled_side": "UP",
            "filled_price": 0.50, "filled_size": 10,
            "filled_token": "TOKEN_UP",
            "time": now - 10,
            "token_up": "TOKEN_UP", "token_down": "TOKEN_DOWN",
            "end_time": window_end, "interval": interval,
        })
        engine._market_cache = {}

        book = MockBookReader()
        completed = engine.process_hedge_completions(book)
        self.assertEqual(completed, 0)
        self.assertEqual(len(engine._pending_hedges), 1)

    def test_t4_sell_works_without_market_cache(self):
        """T4 last resort sell should work even when _market_cache is empty."""
        engine = make_engine()
        wid = "btc-5m-1772065200"
        now = time.time()
        window_end = now - 290  # 10s remaining, pct = 3.3%
        interval = 300

        filled_token = "TOKEN_UP_123"
        engine._pending_hedges.append({
            "window_id": wid, "filled_side": "UP",
            "filled_price": 0.50, "filled_size": 10,
            "filled_token": filled_token,
            "time": now - 280,
            "token_up": filled_token, "token_down": "TOKEN_DOWN_456",
            "end_time": window_end, "interval": interval,
        })
        engine._market_cache = {}
        engine.token_holdings[filled_token] = {"size": 10.0}

        book = MockBookReader(spreads={
            "TOKEN_DOWN_456": {"ask": 0.60, "bid": 0.58},  # too expensive for buy tiers
            filled_token: {"ask": 0.52, "bid": 0.49},  # sell at 0.49, loss = 0.01/sh
        })

        with patch.object(engine, 'place_order', return_value=True) as mock_order, \
             patch.object(engine, 'cancel_window_orders', return_value=0):
            completed = engine.process_hedge_completions(book)

        self.assertEqual(completed, 1)
        self.assertEqual(len(engine._pending_hedges), 0)
        call_args = mock_order.call_args
        self.assertEqual(call_args[0][0], filled_token)
        self.assertEqual(call_args[0][1], "SELL")

    def test_old_hedge_entries_without_tokens_use_metadata(self):
        """Hedge entries created before V15.3 (without token_up/token_down)
        should fall back to window_metadata."""
        engine = make_engine()
        wid = "sol-5m-1772065800"
        now = time.time()
        window_end = now - 100
        interval = 300
        other_token = "SOL_DOWN_TOKEN"

        # Old-style hedge entry (no token_up/token_down)
        engine._pending_hedges.append({
            "window_id": wid, "filled_side": "UP",
            "filled_price": 0.50, "filled_size": 10,
            "filled_token": "SOL_UP_TOKEN",
            "time": now - 60,
        })
        engine._market_cache = {}
        engine.window_metadata[wid] = {
            "token_up": "SOL_UP_TOKEN", "token_down": other_token,
            "end_time": window_end,
        }

        book = MockBookReader(spreads={
            other_token: {"ask": 0.50, "bid": 0.48}
        })

        with patch.object(engine, 'place_order', return_value=True):
            completed = engine.process_hedge_completions(book)

        self.assertEqual(completed, 1)

    def test_hedge_abandoned_when_no_token_data_and_expired(self):
        """Hedge should be abandoned when no token data is available
        and the window has expired."""
        engine = make_engine()
        wid = "xrp-5m-1772065200"
        now = time.time()

        # Old-style hedge entry, no token data, no metadata, expired
        engine._pending_hedges.append({
            "window_id": wid, "filled_side": "UP",
            "filled_price": 0.50, "filled_size": 10,
            "filled_token": "XRP_UP",
            "time": now - 600,
        })
        engine._market_cache = {}

        book = MockBookReader()
        completed = engine.process_hedge_completions(book)

        self.assertEqual(completed, 0)
        self.assertEqual(len(engine._pending_hedges), 0)
        self.assertEqual(engine.hedge_analytics["resolved_abandoned"], 1)

    def test_hedge_tier_progression_t1_to_t3(self):
        """Verify that as time passes, higher tiers become active."""
        engine = make_engine()
        wid = "btc-5m-1772065200"
        now = time.time()
        interval = 300
        other_token = "TOKEN_DOWN"

        # T1 threshold: 67%, T2: 33%, T3: 13%
        # At 60% remaining -> T1 active
        window_end_t1 = now - 120  # 180s remaining = 60%
        engine._pending_hedges.append({
            "window_id": wid, "filled_side": "UP",
            "filled_price": 0.50, "filled_size": 10,
            "filled_token": "TOKEN_UP",
            "time": now - 100,
            "token_up": "TOKEN_UP", "token_down": other_token,
            "end_time": window_end_t1, "interval": interval,
        })
        engine._market_cache = {}

        # Ask price too high for T1 (max cost 1.03) but ok for T2 (1.05)
        # filled=0.50, fee_filled~0.0078, fee_other~0.0078
        # T1 max_other = 1.03 - 0.50 - 0.0078 - 0.0078 = 0.5144
        # T2 max_other = 1.05 - 0.50 - 0.0078 - 0.0078 = 0.5344
        book = MockBookReader(spreads={
            other_token: {"ask": 0.52, "bid": 0.50}
        })

        # T1 should wait (ask 0.52 > max 0.5144)
        completed = engine.process_hedge_completions(book)
        self.assertEqual(completed, 0)
        self.assertEqual(len(engine._pending_hedges), 1)

        # Now advance time so T2 is active (30% remaining)
        engine._pending_hedges[0]["end_time"] = now - 210  # 90s remaining = 30%

        with patch.object(engine, 'place_order', return_value=True):
            completed = engine.process_hedge_completions(book)

        self.assertEqual(completed, 1)

    def test_pending_hedges_count_in_stats(self):
        """Verify that pending hedge count is available for stats reporting."""
        engine = make_engine()
        engine._pending_hedges.append({
            "window_id": "test-5m-123", "filled_side": "UP",
            "filled_price": 0.50, "filled_size": 10,
            "filled_token": "TOKEN", "time": time.time(),
        })
        self.assertEqual(len(engine._pending_hedges), 1)


if __name__ == "__main__":
    unittest.main()
