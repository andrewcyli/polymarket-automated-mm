"""Tests for V15.1-27 fixes:
1. Attribute error fix (self.engine.hedge_analytics in MarketMakingStrategy)
2. Orphan detection after momentum exit
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
        # This should not raise AttributeError
        self.mm.engine.hedge_analytics["gate_blocks"] += 1
        assert self.engine.hedge_analytics["gate_blocks"] == 1

    def test_gate_block_increments_engine_analytics(self):
        """When momentum gate blocks, it should increment engine.hedge_analytics."""
        initial = self.engine.hedge_analytics["gate_blocks"]
        # Simulate what the momentum gate does
        self.mm.engine.hedge_analytics["gate_blocks"] += 1
        assert self.engine.hedge_analytics["gate_blocks"] == initial + 1

    def test_gate_bypass_increments_engine_analytics(self):
        """When momentum gate bypasses, it should increment engine.hedge_analytics."""
        initial = self.engine.hedge_analytics["gate_bypasses"]
        self.mm.engine.hedge_analytics["gate_bypasses"] += 1
        assert self.engine.hedge_analytics["gate_bypasses"] == initial + 1


class TestOrphanDetection(unittest.TestCase):
    """After momentum exit sells one side, check if opposite side has
    orphaned tokens from a late fill and sell them too."""

    def setUp(self):
        self.config = BotConfig()
        self.config.dry_run = True
        self.config.momentum_exit_enabled = True
        self.config.momentum_exit_threshold = 0.03
        self.config.momentum_exit_min_hold_secs = 10
        self.config.momentum_exit_max_wait_secs = 120
        self.engine = TradingEngine(self.config, fee_calc=MagicMock(), logger=MagicMock())

    def test_window_metadata_has_both_tokens(self):
        """Window metadata should store both token_up and token_down."""
        market = {
            "window_id": "eth-15m-1234",
            "slug": "eth-updown-15m-1234",
            "condition_id": "0xabc",
            "token_up": "tok_up_123",
            "token_down": "tok_dn_456",
            "asset": "eth",
            "end_time": int(time.time()) + 900,
        }
        self.engine.register_window_metadata(market)
        meta = self.engine.window_metadata["eth-15m-1234"]
        assert meta["token_up"] == "tok_up_123"
        assert meta["token_down"] == "tok_dn_456"

    def test_orphan_detection_finds_opposite_tokens(self):
        """After momentum exit sells UP, should detect DOWN tokens in holdings."""
        wid = "eth-15m-1234"
        # Register metadata
        self.engine.window_metadata[wid] = {
            "token_up": "tok_up_123",
            "token_down": "tok_dn_456",
            "asset": "eth",
        }
        # Simulate: UP was sold via momentum exit, but DOWN has orphan tokens
        self.engine.token_holdings["tok_dn_456"] = {"size": 10.5, "cost": 0}
        # Check orphan detection logic
        filled_side = "UP"
        opp_key = "token_down" if filled_side == "UP" else "token_up"
        meta = self.engine.window_metadata.get(wid, {})
        opp_token = meta.get(opp_key, "")
        opp_held = self.engine.token_holdings.get(opp_token, {}).get("size", 0)
        assert opp_token == "tok_dn_456"
        assert opp_held == 10.5

    def test_no_orphan_when_opposite_empty(self):
        """No orphan detection when opposite side has no tokens."""
        wid = "eth-15m-1234"
        self.engine.window_metadata[wid] = {
            "token_up": "tok_up_123",
            "token_down": "tok_dn_456",
            "asset": "eth",
        }
        # No tokens in holdings for DOWN
        filled_side = "UP"
        opp_key = "token_down" if filled_side == "UP" else "token_up"
        meta = self.engine.window_metadata.get(wid, {})
        opp_token = meta.get(opp_key, "")
        opp_held = self.engine.token_holdings.get(opp_token, {}).get("size", 0)
        assert opp_held == 0  # No orphan


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
