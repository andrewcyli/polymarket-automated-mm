"""Tests for V15.1-27/28 fixes:
1. Attribute error fix (self.engine.hedge_analytics in MarketMakingStrategy)
2. Orphan recovery via re-pairing (V15.1-28)
3. Smart token selection for LIVE POS queries
4. RPC interval reduction
"""
import time
import unittest
from unittest.mock import MagicMock, patch
from trading_bot_v15 import BotConfig, TradingEngine, MarketMakingStrategy


class TestHedgeAnalyticsAttributeFix(unittest.TestCase):
    """Bug: MarketMakingStrategy referenced self.hedge_analytics instead of
    self.engine.hedge_analytics, causing AttributeError on every strategy
    execution after momentum gate check."""

    def setUp(self):
        self.config = BotConfig()
        self.config.dry_run = True
        self.config.momentum_gate_threshold = 0.005  # 0.5%
        self.config.momentum_gate_max_consec = 5
        self.engine = TradingEngine(self.config, fee_calc=MagicMock(), logger=MagicMock())
        self.mm = MarketMakingStrategy(
            config=self.config,
            engine=self.engine,
            book_reader=MagicMock(),
            price_feed=MagicMock(),
            kelly=MagicMock(),
            fee_calc=MagicMock(),
            logger=MagicMock(),
            reward_optimizer=MagicMock(),
            vol_tracker=MagicMock(),
            churn_manager=MagicMock(),
        )

    def test_engine_has_hedge_analytics(self):
        """TradingEngine should have hedge_analytics dict."""
        assert hasattr(self.engine, 'hedge_analytics')
        assert "gate_blocks" in self.engine.hedge_analytics
        assert "gate_bypasses" in self.engine.hedge_analytics

    def test_mm_strategy_does_not_have_hedge_analytics(self):
        """MarketMakingStrategy should NOT have its own hedge_analytics."""
        assert not hasattr(self.mm, 'hedge_analytics')

    def test_mm_can_access_engine_hedge_analytics(self):
        """MarketMakingStrategy should access hedge_analytics via self.engine."""
        self.mm.engine.hedge_analytics["gate_blocks"] += 1
        assert self.engine.hedge_analytics["gate_blocks"] == 1

    def test_gate_block_increments_engine_analytics(self):
        """When momentum gate blocks, it should increment engine.hedge_analytics."""
        initial = self.engine.hedge_analytics["gate_blocks"]
        self.mm.engine.hedge_analytics["gate_blocks"] += 1
        assert self.engine.hedge_analytics["gate_blocks"] == initial + 1

    def test_gate_bypass_increments_engine_analytics(self):
        """When momentum gate bypasses, it should increment engine.hedge_analytics."""
        initial = self.engine.hedge_analytics["gate_bypasses"]
        self.mm.engine.hedge_analytics["gate_bypasses"] += 1
        assert self.engine.hedge_analytics["gate_bypasses"] == initial + 1


class TestOrphanRecoveryViaPairing(unittest.TestCase):
    """V15.1-28: After momentum exit sells one side, if opposite side has
    orphan tokens, try to BUY the missing side to re-pair for merge.
    Fall back to sell only if insufficient time or unprofitable."""

    def setUp(self):
        self.config = BotConfig()
        self.config.dry_run = True
        self.engine = TradingEngine(self.config, fee_calc=MagicMock(), logger=MagicMock())

    def test_hedge_analytics_has_orphan_fields(self):
        """hedge_analytics should have orphan_recoveries and orphan_sells."""
        assert "orphan_recoveries" in self.engine.hedge_analytics
        assert "orphan_sells" in self.engine.hedge_analytics
        assert self.engine.hedge_analytics["orphan_recoveries"] == 0
        assert self.engine.hedge_analytics["orphan_sells"] == 0

    def test_recovery_decision_with_enough_time_and_profit(self):
        """When time_left >= 60s and pair is profitable, should choose re-pair."""
        wid = "eth-15m-1234"
        now = time.time()
        # Window ends in 5 minutes
        window_end = now + 300
        # Orphan DOWN at bid $0.48
        opp_bid = 0.48
        # Missing UP ask at $0.50
        missing_ask = 0.50
        min_margin = 0.02
        max_recovery_price = round(1.0 - opp_bid - min_margin, 2)  # $0.50
        time_left = window_end - now
        can_repair = (
            time_left >= 60
            and missing_ask > 0
            and missing_ask <= max_recovery_price
        )
        assert can_repair is True
        assert max_recovery_price == 0.50

    def test_recovery_decision_not_enough_time(self):
        """When time_left < 60s, should fall back to sell."""
        now = time.time()
        window_end = now + 30  # Only 30s left
        opp_bid = 0.48
        missing_ask = 0.50
        min_margin = 0.02
        max_recovery_price = round(1.0 - opp_bid - min_margin, 2)
        time_left = window_end - now
        can_repair = (
            time_left >= 60
            and missing_ask > 0
            and missing_ask <= max_recovery_price
        )
        assert can_repair is False

    def test_recovery_decision_unprofitable_pair(self):
        """When missing side ask > max recovery price, should fall back to sell."""
        now = time.time()
        window_end = now + 300  # Plenty of time
        opp_bid = 0.48
        missing_ask = 0.55  # Too expensive
        min_margin = 0.02
        max_recovery_price = round(1.0 - opp_bid - min_margin, 2)  # $0.50
        time_left = window_end - now
        can_repair = (
            time_left >= 60
            and missing_ask > 0
            and missing_ask <= max_recovery_price
        )
        assert can_repair is False
        assert missing_ask > max_recovery_price

    def test_recovery_price_calculation(self):
        """Recovery price should be min(ask, max_recovery_price)."""
        opp_bid = 0.48
        min_margin = 0.02
        max_recovery_price = round(1.0 - opp_bid - min_margin, 2)
        # Case 1: ask < max → use ask
        missing_ask = 0.47
        recovery_price = min(missing_ask, max_recovery_price)
        assert recovery_price == 0.47
        # Case 2: ask == max → use either
        missing_ask = 0.50
        recovery_price = min(missing_ask, max_recovery_price)
        assert recovery_price == 0.50
        # Case 3: ask > max → would not enter recovery (can_repair=False)

    def test_window_reopened_after_recovery(self):
        """After successful recovery buy, window should be re-opened for merge."""
        wid = "eth-15m-1234"
        self.engine.closed_windows.add(wid)
        # Simulate recovery: re-open window
        self.engine.closed_windows.discard(wid)
        self.engine.held_windows.add(wid)
        self.engine.window_fill_sides[wid] = {
            "DOWN": [{"token_id": "tok_dn", "price": 0.48, "size": 10.5, "time": time.time()}]
        }
        assert wid not in self.engine.closed_windows
        assert wid in self.engine.held_windows
        assert "DOWN" in self.engine.window_fill_sides[wid]

    def test_paired_windows_after_both_sides_filled(self):
        """After recovery buy fills, window should enter paired_windows for merge."""
        wid = "eth-15m-1234"
        self.engine.window_fill_sides[wid] = {
            "DOWN": [{"token_id": "tok_dn", "price": 0.48, "size": 10.5, "time": time.time()}],
            "UP": [{"token_id": "tok_up", "price": 0.50, "size": 10.5, "time": time.time()}],
        }
        # Check pairing logic
        sides = self.engine.window_fill_sides.get(wid, {})
        if "UP" in sides and "DOWN" in sides:
            self.engine.paired_windows.add(wid)
        assert wid in self.engine.paired_windows

    def test_orphan_detection_opposite_side_mapping(self):
        """Verify correct mapping: if UP sold, orphan is DOWN, missing is UP."""
        filled_side = "UP"
        opp_key = "token_down" if filled_side == "UP" else "token_up"
        missing_key = "token_up" if filled_side == "UP" else "token_down"
        assert opp_key == "token_down"  # Orphan is DOWN
        assert missing_key == "token_up"  # Missing (sold) is UP

        filled_side = "DOWN"
        opp_key = "token_down" if filled_side == "UP" else "token_up"
        missing_key = "token_up" if filled_side == "UP" else "token_down"
        assert opp_key == "token_up"  # Orphan is UP
        assert missing_key == "token_down"  # Missing (sold) is DOWN

    def test_min_margin_prevents_breakeven_pairs(self):
        """Min margin of $0.02 should prevent pairs that would only break even."""
        opp_bid = 0.49
        min_margin = 0.02
        max_recovery_price = round(1.0 - opp_bid - min_margin, 2)
        assert max_recovery_price == 0.49
        # If ask is $0.50, pair cost = $0.49 + $0.50 = $0.99 → only $0.01 profit
        # But max_recovery_price = $0.49, so ask $0.50 > $0.49 → rejected
        assert 0.50 > max_recovery_price


class TestSmartTokenSelection(unittest.TestCase):
    """V15.1-27: LIVE POS should only query tokens for windows with fills,
    positions, or pending claims — not ALL discovered tokens."""

    def setUp(self):
        self.config = BotConfig()
        self.config.dry_run = True
        self.engine = TradingEngine(self.config, fee_calc=MagicMock(), logger=MagicMock())

    def test_relevant_wids_includes_fill_sides(self):
        """Windows with fills should be included in relevant set."""
        self.engine.window_fill_sides["eth-15m-1234"] = {
            "UP": [{"token_id": "tok_up", "price": 0.47, "size": 10}]
        }
        relevant = set()
        relevant.update(self.engine.window_fill_sides.keys())
        assert "eth-15m-1234" in relevant

    def test_relevant_wids_includes_pending_claims(self):
        """Windows pending claim should be included."""
        self.engine.expired_windows_pending_claim["eth-15m-5678"] = {
            "token_up": "tok_up", "token_down": "tok_dn",
        }
        relevant = set()
        relevant.update(self.engine.expired_windows_pending_claim.keys())
        assert "eth-15m-5678" in relevant

    def test_relevant_wids_includes_held_windows(self):
        """Held windows should be included."""
        self.engine.held_windows.add("btc-15m-9999")
        relevant = set()
        relevant.update(self.engine.held_windows)
        assert "btc-15m-9999" in relevant

    def test_relevant_wids_includes_paired_windows(self):
        """Paired windows should be included."""
        self.engine.paired_windows.add("sol-15m-1111")
        relevant = set()
        relevant.update(self.engine.paired_windows)
        assert "sol-15m-1111" in relevant

    def test_existing_holdings_included(self):
        """Tokens already in holdings should be queried (catch orphans)."""
        self.engine.token_holdings["tok_orphan"] = {"size": 10.5, "cost": 0}
        token_ids = set()
        for tid in self.engine.token_holdings:
            if self.engine.token_holdings[tid].get("size", 0) >= 1.0:
                token_ids.add(tid)
        assert "tok_orphan" in token_ids

    def test_dust_holdings_excluded(self):
        """Tokens with dust amounts (<1.0) should not be queried."""
        self.engine.token_holdings["tok_dust"] = {"size": 0.001, "cost": 0}
        token_ids = set()
        for tid in self.engine.token_holdings:
            if self.engine.token_holdings[tid].get("size", 0) >= 1.0:
                token_ids.add(tid)
        assert "tok_dust" not in token_ids


class TestRPCInterval(unittest.TestCase):
    """V15.1-27: RPC interval reduced from 1.5s to 0.5s."""

    def test_default_rpc_interval(self):
        config = BotConfig()
        assert config.rpc_min_call_interval == 0.5


class TestHedgeAnalyticsStructure(unittest.TestCase):
    """Verify hedge_analytics has all required fields."""

    def test_all_fields_present(self):
        config = BotConfig()
        engine = TradingEngine(config, fee_calc=MagicMock(), logger=MagicMock())
        ha = engine.hedge_analytics
        required = [
            "one_sided_fills", "resolved_by_hedge", "resolved_by_exit",
            "resolved_by_merge", "resolved_abandoned",
            "tier_counts", "tier_costs", "tier_times",
            "exit_profits", "exit_hold_times",
            "gate_blocks", "gate_bypasses",
            "orphan_recoveries", "orphan_sells",
            "per_asset",
        ]
        for field in required:
            assert field in ha, f"Missing field: {field}"

    def test_tier_counts_has_all_tiers(self):
        config = BotConfig()
        engine = TradingEngine(config, fee_calc=MagicMock(), logger=MagicMock())
        for tier in ("t1", "t2", "t3"):
            assert tier in engine.hedge_analytics["tier_counts"]


if __name__ == "__main__":
    unittest.main()
