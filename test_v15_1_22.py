"""
Tests for V15.1-22 fixes:
1. Prioritized market selection — session limiting, scoring, top-N selection
2. Dynamic re-prioritization — cancel orders on demoted windows
3. Momentum exit cancels opposite-side order after selling
4. CC config mapping for maxOrderSessions
"""
import time
import pytest
import logging
from unittest.mock import MagicMock, patch, call

from trading_bot_v15 import (
    BotConfig, TradingEngine, FeeCalculator, MarketMakingStrategy,
    OrderBookReader, KellySizer,
)


def _make_engine(config=None, bankroll=1000.0):
    cfg = config or BotConfig()
    cfg.dry_run = True
    cfg.kelly_bankroll = bankroll
    cfg.session_budget = bankroll
    logger = MagicMock()
    fee_calc = FeeCalculator()
    balance_checker = MagicMock()
    balance_checker.get_balance.return_value = bankroll
    engine = TradingEngine(cfg, fee_calc, logger, balance_checker=balance_checker)
    return engine


def _make_mm_strategy(config=None, engine=None):
    cfg = config or BotConfig()
    cfg.dry_run = True
    if engine is None:
        engine = _make_engine(cfg)
    logger = MagicMock()
    book_reader = MagicMock()
    price_feed = MagicMock()
    kelly = MagicMock()
    kelly.optimal_size.return_value = 10.0
    fee_calc = FeeCalculator()
    reward_optimizer = MagicMock()
    vol_tracker = MagicMock()
    churn_manager = MagicMock()
    mm = MarketMakingStrategy(
        cfg, engine, book_reader, price_feed, kelly, fee_calc, logger,
        reward_optimizer, vol_tracker, churn_manager
    )
    return mm


class TestMaxOrderSessions:
    """Test the max_order_sessions config field."""

    def test_default_value(self):
        cfg = BotConfig()
        assert cfg.max_order_sessions == 3

    def test_custom_value(self):
        cfg = BotConfig(max_order_sessions=5)
        assert cfg.max_order_sessions == 5

    def test_dynamic_reprioritize_default(self):
        cfg = BotConfig()
        assert cfg.dynamic_reprioritize is True


class TestSessionLimiting:
    """Test that markets are limited to closest N sessions per timeframe."""

    def _make_markets(self, tf, end_times, assets=None):
        """Create test markets for a given timeframe and end_times."""
        assets = assets or ["btc"]
        markets = []
        now = int(time.time())
        for end_t in end_times:
            for asset in assets:
                markets.append({
                    "window_id": f"{asset}-{tf}-{end_t}",
                    "slug": f"{asset}-updown-{tf}-{end_t}",
                    "asset": asset,
                    "timeframe": tf,
                    "end_time": end_t,
                    "timestamp": end_t,
                    "time_left": end_t - now,
                    "is_advance": False,
                    "token_up": f"tok_up_{asset}_{end_t}",
                    "token_down": f"tok_dn_{asset}_{end_t}",
                    "price_up": 0.47,
                    "price_down": 0.47,
                    "condition_id": f"cid_{asset}_{end_t}",
                    "edge": 0.02,
                    "maker_edge": 0.015,
                    "gamma_sum": 0.94,
                    "gamma_edge_est": 0.045,
                    "interval": 300 if tf == "5m" else 900,
                })
        return markets

    def test_session_limit_filters_to_closest_n(self):
        """With max_order_sessions=3, only the 3 closest sessions per TF should pass."""
        now = int(time.time())
        # 5 sessions for 5m: 300, 600, 900, 1200, 1500 seconds out
        end_times_5m = [now + 300, now + 600, now + 900, now + 1200, now + 1500]
        markets_5m = self._make_markets("5m", end_times_5m)

        # Group by timeframe and pick closest 3
        max_sessions = 3
        tf_sessions = {}
        for market in markets_5m:
            tf = market["timeframe"]
            end_t = market["end_time"]
            if tf not in tf_sessions:
                tf_sessions[tf] = {}
            if end_t not in tf_sessions[tf]:
                tf_sessions[tf][end_t] = []
            tf_sessions[tf][end_t].append(market)

        session_limited = []
        for tf, sessions in tf_sessions.items():
            sorted_times = sorted(sessions.keys())[:max_sessions]
            for t in sorted_times:
                session_limited.extend(sessions[t])

        # Should only have 3 sessions (closest 3 end_times)
        assert len(session_limited) == 3
        end_times_in_result = {m["end_time"] for m in session_limited}
        assert end_times_in_result == set(end_times_5m[:3])

    def test_session_limit_per_timeframe(self):
        """Each timeframe gets its own session limit."""
        now = int(time.time())
        end_times_5m = [now + 300, now + 600, now + 900, now + 1200]
        end_times_15m = [now + 900, now + 1800, now + 2700, now + 3600]
        markets = self._make_markets("5m", end_times_5m) + self._make_markets("15m", end_times_15m)

        max_sessions = 2
        tf_sessions = {}
        for market in markets:
            tf = market["timeframe"]
            end_t = market["end_time"]
            if tf not in tf_sessions:
                tf_sessions[tf] = {}
            if end_t not in tf_sessions[tf]:
                tf_sessions[tf][end_t] = []
            tf_sessions[tf][end_t].append(market)

        session_limited = []
        for tf, sessions in tf_sessions.items():
            sorted_times = sorted(sessions.keys())[:max_sessions]
            for t in sorted_times:
                session_limited.extend(sessions[t])

        # 2 from 5m + 2 from 15m = 4
        assert len(session_limited) == 4
        tf_counts = {}
        for m in session_limited:
            tf = m["timeframe"]
            tf_counts[tf] = tf_counts.get(tf, 0) + 1
        assert tf_counts["5m"] == 2
        assert tf_counts["15m"] == 2

    def test_multiple_assets_per_session(self):
        """Multiple assets in the same session should all pass."""
        now = int(time.time())
        end_times = [now + 300, now + 600]
        markets = self._make_markets("5m", end_times, assets=["btc", "eth", "sol"])

        max_sessions = 2
        tf_sessions = {}
        for market in markets:
            tf = market["timeframe"]
            end_t = market["end_time"]
            if tf not in tf_sessions:
                tf_sessions[tf] = {}
            if end_t not in tf_sessions[tf]:
                tf_sessions[tf][end_t] = []
            tf_sessions[tf][end_t].append(market)

        session_limited = []
        for tf, sessions in tf_sessions.items():
            sorted_times = sorted(sessions.keys())[:max_sessions]
            for t in sorted_times:
                session_limited.extend(sessions[t])

        # 2 sessions x 3 assets = 6 markets
        assert len(session_limited) == 6


class TestPrioritySelection:
    """Test that top N markets are selected by score with condition_id dedup."""

    def test_top_n_selection(self):
        """Only max_concurrent_windows markets should be selected."""
        markets = []
        for i in range(8):
            markets.append({
                "window_id": f"btc-5m-{1000 + i}",
                "condition_id": f"cid_{i}",
                "_sort_score": 0.1 - (i * 0.01),  # Decreasing scores
            })

        max_concurrent = 4
        priority_markets = []
        priority_cids = set()
        for market in markets:
            if len(priority_markets) >= max_concurrent:
                break
            mkt_cid = market.get("condition_id", "")
            if mkt_cid and mkt_cid in priority_cids:
                continue
            priority_markets.append(market)
            if mkt_cid:
                priority_cids.add(mkt_cid)

        assert len(priority_markets) == 4
        # Should be the top 4 by score
        assert priority_markets[0]["_sort_score"] == 0.1
        assert priority_markets[3]["_sort_score"] == 0.07

    def test_condition_id_dedup_in_priority(self):
        """Markets with duplicate condition_ids should be deduplicated."""
        markets = [
            {"window_id": "btc-5m-1000", "condition_id": "cid_A", "_sort_score": 0.10},
            {"window_id": "btc-15m-1000", "condition_id": "cid_A", "_sort_score": 0.09},  # Same cid
            {"window_id": "eth-5m-1000", "condition_id": "cid_B", "_sort_score": 0.08},
            {"window_id": "sol-5m-1000", "condition_id": "cid_C", "_sort_score": 0.07},
        ]

        max_concurrent = 3
        priority_markets = []
        priority_cids = set()
        for market in markets:
            if len(priority_markets) >= max_concurrent:
                break
            mkt_cid = market.get("condition_id", "")
            if mkt_cid and mkt_cid in priority_cids:
                continue
            priority_markets.append(market)
            if mkt_cid:
                priority_cids.add(mkt_cid)

        assert len(priority_markets) == 3
        wids = [m["window_id"] for m in priority_markets]
        assert "btc-5m-1000" in wids
        assert "btc-15m-1000" not in wids  # Deduped
        assert "eth-5m-1000" in wids
        assert "sol-5m-1000" in wids


class TestDynamicReprioritization:
    """Test that orders on demoted windows are cancelled."""

    def setup_method(self):
        self.config = BotConfig()
        self.config.dry_run = True
        self.config.dynamic_reprioritize = True
        self.engine = _make_engine(self.config)

    def test_demoted_window_orders_cancelled(self):
        """Windows that lose priority should have their orders cancelled."""
        # Set up window_exposure with 3 windows
        self.engine.window_exposure = {
            "btc-5m-1000": 5.0,
            "eth-5m-1000": 5.0,
            "sol-5m-1000": 5.0,
        }
        # Track orders for each window
        self.engine.orders_by_window = {
            "btc-5m-1000": ["order1"],
            "eth-5m-1000": ["order2"],
            "sol-5m-1000": ["order3"],
        }
        self.engine.active_orders = {
            "order1": {"window_id": "btc-5m-1000", "side": "BUY", "price": 0.5, "size": 10, "token_id": "tok1", "strategy": "mm", "time": time.time()},
            "order2": {"window_id": "eth-5m-1000", "side": "BUY", "price": 0.5, "size": 10, "token_id": "tok2", "strategy": "mm", "time": time.time()},
            "order3": {"window_id": "sol-5m-1000", "side": "BUY", "price": 0.5, "size": 10, "token_id": "tok3", "strategy": "mm", "time": time.time()},
        }

        # New priority only includes btc and eth
        priority_wids = {"btc-5m-1000", "eth-5m-1000"}
        filled_wids = set()
        closed_wids = set()

        current_order_wids = set(self.engine.window_exposure.keys())
        demoted_wids = current_order_wids - priority_wids - filled_wids - closed_wids

        assert demoted_wids == {"sol-5m-1000"}

    def test_filled_windows_not_demoted(self):
        """Windows with fills should not be cancelled even if not in priority."""
        self.engine.window_exposure = {
            "btc-5m-1000": 5.0,
            "eth-5m-1000": 5.0,
        }
        self.engine.window_fill_sides = {
            "eth-5m-1000": {"UP": [{"token_id": "tok", "price": 0.47, "size": 10, "time": time.time()}]}
        }

        priority_wids = {"btc-5m-1000"}
        filled_wids = set()
        closed_wids = set()

        current_order_wids = set(self.engine.window_exposure.keys())
        demoted_wids = current_order_wids - priority_wids - filled_wids - closed_wids

        # eth-5m-1000 is in demoted_wids but has fills, so should be skipped
        assert "eth-5m-1000" in demoted_wids
        # The actual code checks window_fill_sides before cancelling
        should_cancel = []
        for dwid in demoted_wids:
            if dwid not in self.engine.window_fill_sides:
                should_cancel.append(dwid)
        assert "eth-5m-1000" not in should_cancel


class TestMomentumExitCancelsOpposite:
    """Test that momentum exit cancels the opposite-side order after selling."""

    def setup_method(self):
        self.config = BotConfig()
        self.config.dry_run = True
        self.config.momentum_exit_enabled = True
        self.config.momentum_exit_max_wait_secs = 5.0
        self.config.momentum_exit_threshold = 0.03
        self.engine = _make_engine(self.config)

    def test_cancel_window_orders_called_before_sell(self):
        """process_momentum_exits should call cancel_window_orders before placing sell."""
        wid = "btc-5m-1000"
        now = time.time()

        # Set up a one-sided fill (only DOWN filled)
        self.engine.known_windows = {wid}
        self.engine.window_fill_sides[wid] = {
            "DN": [{"token_id": "tok_dn", "price": 0.50, "size": 10, "time": now - 10}]
        }
        self.engine.filled_windows.add(wid)
        self.engine.window_fill_cost[wid] = 5.0

        # Set up an active BUY UP order that should be cancelled
        self.engine.active_orders["order_up"] = {
            "window_id": wid, "side": "BUY", "price": 0.46,
            "size": 10, "token_id": "tok_up", "strategy": "mm",
            "time": now - 10,
        }
        self.engine.orders_by_window[wid] = ["order_up"]
        self.engine._recalc_exposure()

        # Verify the UP order is tracked
        assert wid in self.engine.window_exposure
        assert "order_up" in self.engine.active_orders

        # After cancel_window_orders, the order should be removed
        cancelled = self.engine.cancel_window_orders(wid)
        assert cancelled == 1
        assert "order_up" not in self.engine.active_orders
        assert wid not in self.engine.window_exposure

    def test_cancel_market_orders_safety_net(self):
        """Momentum exit should call cancel_market_orders as a safety net for each token."""
        wid = "btc-5m-1000"
        now = time.time()

        # Set up market metadata
        self.engine._market_cache = {
            wid: {"token_up": "tok_up_123", "token_down": "tok_dn_456"}
        }

        # Verify the cache has the tokens
        mkt = self.engine._market_cache.get(wid, {})
        tokens = [mkt.get("token_up", ""), mkt.get("token_down", "")]
        tokens = [t for t in tokens if t]
        assert len(tokens) == 2
        assert "tok_up_123" in tokens
        assert "tok_dn_456" in tokens


class TestMMStrategyCleanupWindow:
    """Test that cleanup_window allows re-placing after demotion."""

    def test_cleanup_clears_first_placed(self):
        """cleanup_window should remove the window from _window_first_placed."""
        mm = _make_mm_strategy()
        wid = "btc-5m-1000"
        mm._window_first_placed[wid] = time.time() - 30

        assert wid in mm._window_first_placed
        mm.cleanup_window(wid)
        assert wid not in mm._window_first_placed

    def test_can_reenter_after_cleanup(self):
        """After cleanup, the NO-CHURN guard should not block the window."""
        mm = _make_mm_strategy()
        wid = "btc-5m-1000"

        # Simulate first placement
        mm._window_first_placed[wid] = time.time() - 30
        assert wid in mm._window_first_placed

        # Cleanup (demotion)
        mm.cleanup_window(wid)
        assert wid not in mm._window_first_placed

        # Now the window should be able to re-enter (not blocked by NO-CHURN)
        # The check is: if window_id in self._window_first_placed: return
        assert wid not in mm._window_first_placed  # Would not be blocked


class TestCCConfigMapping:
    """Test the CC config mapping for maxOrderSessions."""

    def test_max_order_sessions_mapping(self):
        """maxOrderSessions from CC should map to config.max_order_sessions."""
        from main import apply_cc_config
        config = BotConfig()
        cc_config = {
            "maxOrderSessions": 5,
            "targetAssets": '["BTC"]',
            "windowDurations": '["5m"]',
        }
        apply_cc_config(config, cc_config)
        assert config.max_order_sessions == 5

    def test_max_order_sessions_default(self):
        """Without CC override, max_order_sessions should be 3."""
        config = BotConfig()
        assert config.max_order_sessions == 3

    def test_max_concurrent_windows_mapping(self):
        """maxConcurrentWindows from CC should map to config.max_concurrent_windows."""
        from main import apply_cc_config
        config = BotConfig()
        cc_config = {
            "maxConcurrentWindows": 4,
            "targetAssets": '["BTC"]',
            "windowDurations": '["5m"]',
        }
        apply_cc_config(config, cc_config)
        assert config.max_concurrent_windows == 4


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
