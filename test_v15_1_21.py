"""
Tests for V15.1-21 fixes:
1. Momentum exit cleanup race — window_fill_sides preserved for one-sided fills
2. Merge approval check — isApprovedForAll and setApprovalForAll in ABI
3. Verbose merge error logging
4. _recently_cancelled TTL extended to 900s
"""
import time
import pytest
import logging
from unittest.mock import MagicMock, patch

# Import the trading bot components
from trading_bot_v15 import BotConfig, TradingEngine, FeeCalculator, CTF_FULL_ABI


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


class TestMomentumExitCleanupRace:
    """Test that cleanup_expired_windows preserves window_fill_sides for one-sided fills."""

    def setup_method(self):
        self.config = BotConfig()
        self.config.dry_run = True
        self.config.momentum_exit_enabled = True
        self.config.momentum_exit_max_wait_secs = 5.0
        self.config.momentum_exit_threshold = 0.03
        self.engine = _make_engine(self.config)

    def test_one_sided_fill_preserved_after_cleanup(self):
        """When a window has only one side filled, cleanup should NOT pop window_fill_sides."""
        wid = "BTC-15m-1234"
        # Set up a one-sided fill (only UP filled)
        self.engine.known_windows = {wid}
        self.engine.window_fill_sides[wid] = {
            "UP": [{"token_id": "tok_up", "price": 0.47, "size": 10, "time": time.time() - 30}]
        }
        self.engine.window_fill_cost[wid] = 4.70
        self.engine.filled_windows.add(wid)
        self.engine.window_entry_count[wid] = 1

        # Simulate window expiring (not in active_markets)
        active_markets = []  # No active markets = window expired
        self.engine.cleanup_expired_windows(active_markets)

        # window_fill_sides should be PRESERVED for momentum exit
        assert wid in self.engine.window_fill_sides, \
            "window_fill_sides should be preserved for one-sided fills"
        assert "UP" in self.engine.window_fill_sides[wid]
        # filled_windows should also be preserved
        assert wid in self.engine.filled_windows, \
            "filled_windows should be preserved for one-sided fills"

    def test_paired_fill_cleaned_after_cleanup(self):
        """When a window has both sides filled (paired), cleanup should pop window_fill_sides."""
        wid = "BTC-15m-1234"
        self.engine.known_windows = {wid}
        self.engine.window_fill_sides[wid] = {
            "UP": [{"token_id": "tok_up", "price": 0.47, "size": 10, "time": time.time()}],
            "DOWN": [{"token_id": "tok_dn", "price": 0.51, "size": 10, "time": time.time()}],
        }
        self.engine.paired_windows.add(wid)
        self.engine.window_fill_cost[wid] = 9.80
        self.engine.filled_windows.add(wid)

        active_markets = []
        self.engine.cleanup_expired_windows(active_markets)

        # Paired windows should be cleaned up
        assert wid not in self.engine.window_fill_sides
        assert wid not in self.engine.filled_windows

    def test_no_fill_cleaned_after_cleanup(self):
        """When a window has no fills, cleanup should pop window_fill_sides (empty dict)."""
        wid = "BTC-15m-1234"
        self.engine.known_windows = {wid}
        self.engine.window_fill_sides[wid] = {}  # No fills

        active_markets = []
        self.engine.cleanup_expired_windows(active_markets)

        assert wid not in self.engine.window_fill_sides

    def test_momentum_exit_can_process_preserved_fills(self):
        """After cleanup preserves one-sided fills, momentum exit should be able to process them."""
        wid = "BTC-15m-1234"
        fill_time = time.time() - 200  # 200s ago, well past max_wait
        self.engine.window_fill_sides[wid] = {
            "UP": [{"token_id": "tok_up", "price": 0.47, "size": 10, "time": fill_time}]
        }
        self.engine.filled_windows.add(wid)
        self.engine.token_holdings["tok_up"] = {"size": 10, "cost": 4.70}

        # Mock book_reader to return a spread with price increase
        book_reader = MagicMock()
        book_reader.get_spread.return_value = {"bid": 0.50, "ask": 0.52, "spread": 0.02}

        exits = self.engine.process_momentum_exits(book_reader)
        # With 0.47 -> 0.50 = 6.4% increase > 3% threshold, should exit
        assert exits == 1, "Momentum exit should fire on preserved one-sided fill"


class TestRecentlyCancelledTTL:
    """Test that _recently_cancelled TTL is 900s (15 min) to catch late fills."""

    def test_ttl_is_900_seconds(self):
        engine = _make_engine()
        assert engine._recently_cancelled_ttl == 900, \
            "TTL should be 900s (15 min) to catch late fills"

    def test_purge_respects_ttl(self):
        engine = _make_engine()

        # Add a recently cancelled order 600s ago (within TTL)
        engine._recently_cancelled["order1"] = {
            "window_id": "w1", "cancelled_at": time.time() - 600
        }
        # Add one 1000s ago (past TTL)
        engine._recently_cancelled["order2"] = {
            "window_id": "w2", "cancelled_at": time.time() - 1000
        }

        engine.purge_recently_cancelled()

        assert "order1" in engine._recently_cancelled, \
            "Order within TTL should be preserved"
        assert "order2" not in engine._recently_cancelled, \
            "Order past TTL should be purged"


class TestCTFABIApproval:
    """Test that CTF ABI includes isApprovedForAll and setApprovalForAll."""

    def test_abi_has_is_approved_for_all(self):
        fn_names = [entry.get("name") for entry in CTF_FULL_ABI]
        assert "isApprovedForAll" in fn_names, \
            "CTF ABI should include isApprovedForAll for merge approval checking"

    def test_abi_has_set_approval_for_all(self):
        fn_names = [entry.get("name") for entry in CTF_FULL_ABI]
        assert "setApprovalForAll" in fn_names, \
            "CTF ABI should include setApprovalForAll for setting merge approval"

    def test_abi_has_merge_positions(self):
        fn_names = [entry.get("name") for entry in CTF_FULL_ABI]
        assert "mergePositions" in fn_names

    def test_abi_has_redeem_positions(self):
        fn_names = [entry.get("name") for entry in CTF_FULL_ABI]
        assert "redeemPositions" in fn_names

    def test_abi_has_balance_of(self):
        fn_names = [entry.get("name") for entry in CTF_FULL_ABI]
        assert "balanceOf" in fn_names

    def test_abi_has_payout_denominator(self):
        fn_names = [entry.get("name") for entry in CTF_FULL_ABI]
        assert "payoutDenominator" in fn_names


class TestCancellationHandlerPreservesForRecovery:
    """Test that the CANCELLATION handler moves orders to _recently_cancelled
    instead of just deleting them, so late ORDER_FILL events can recover fills."""

    def setup_method(self):
        self.config = BotConfig()
        self.config.dry_run = True
        self.engine = _make_engine(self.config)

    def test_cancel_window_orders_saves_to_recently_cancelled(self):
        """cancel_window_orders should save order info to _recently_cancelled before deleting."""
        wid = "BTC-15m-1234"
        oid = "DRY-1234-1"
        self.engine.active_orders[oid] = {
            "window_id": wid, "side": "BUY", "price": 0.47,
            "size": 10, "token_id": "tok_up", "strategy": "mm",
            "time": time.time(),
        }
        self.engine.orders_by_window[wid] = [oid]

        self.engine.cancel_window_orders(wid)

        # Order should be in _recently_cancelled
        assert oid in self.engine._recently_cancelled, \
            "Cancelled order should be in _recently_cancelled for fill recovery"
        assert self.engine._recently_cancelled[oid]["window_id"] == wid
        assert self.engine._recently_cancelled[oid]["price"] == 0.47
        # Order should NOT be in active_orders
        assert oid not in self.engine.active_orders

    def test_prune_stale_orders_saves_to_recently_cancelled(self):
        """prune_stale_orders should save order info to _recently_cancelled."""
        wid = "BTC-15m-1234"
        oid = "DRY-1234-1"
        self.engine.active_orders[oid] = {
            "window_id": wid, "side": "BUY", "price": 0.47,
            "size": 10, "token_id": "tok_up", "strategy": "mm",
            "time": time.time() - 9999,  # Very old
        }
        self.engine.orders_by_window[wid] = [oid]
        self.engine.known_windows = {wid}

        self.engine.prune_stale_orders()

        assert oid in self.engine._recently_cancelled
        assert oid not in self.engine.active_orders
