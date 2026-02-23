"""
Test suite for V15.1-18 Bankroll Management.

Tests the exact scenarios described by the user:
  1) $100 bankroll, 4 concurrent markets, $30 max each
  2) Bot should run first 3 trades (3x$30=$90 < $100)
  3) Bot should NOT execute 4th trade ($120 > $100)
  4) After merge/claim returns $40 ($30 cost + $10 profit), bankroll frees up
  5) 4th trade now allowed ($50 available > $30), but 5th trade blocked
"""
import sys
import os
import unittest
from unittest.mock import MagicMock, patch

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from trading_bot_v15 import BotConfig, TradingEngine, FeeCalculator
import logging


def make_engine(bankroll=100, budget_per_market=30, max_concurrent=4,
                dry_run=True, check_wallet=False):
    """Create a TradingEngine with the given bankroll settings."""
    config = BotConfig()
    config.kelly_bankroll = bankroll
    config.max_position_per_market = budget_per_market
    config.mm_order_size = budget_per_market / 2.0
    config.max_total_exposure = bankroll  # V15.1-18: bankroll IS the cap
    config.max_concurrent_windows = max_concurrent
    config.dry_run = dry_run
    config.check_wallet_balance = check_wallet
    config.deploy_reserve_pct = 0.0  # No reserve — bankroll is fully deployable
    config.max_daily_loss = 999  # Don't interfere with tests
    config.max_asset_exposure_pct = 1.0  # Don't interfere with tests
    config.strategy_budget_enabled = False
    fee_calc = FeeCalculator()
    logger = logging.getLogger("test_bankroll")
    logger.setLevel(logging.WARNING)  # Suppress info logs during tests
    engine = TradingEngine(config, fee_calc, logger)
    return engine


class TestBankrollAvailableCapital(unittest.TestCase):
    """Test get_available_capital() returns correct values."""

    def test_initial_available_equals_bankroll(self):
        """At start, available capital = full bankroll."""
        engine = make_engine(bankroll=100)
        self.assertEqual(engine.get_available_capital(), 100.0)

    def test_available_after_open_orders(self):
        """After placing open orders, available decreases by order cost."""
        engine = make_engine(bankroll=100)
        # Simulate placing a $30 BUY order (open, not yet filled)
        engine.active_orders["order1"] = {
            "side": "BUY", "price": 0.50, "size": 60,  # $30 cost
            "window_id": "btc-15m-w1", "strategy": "mm"
        }
        engine._recalc_exposure()
        self.assertAlmostEqual(engine.capital_deployed, 30.0, places=1)
        self.assertAlmostEqual(engine.get_available_capital(), 70.0, places=1)

    def test_available_after_fill(self):
        """After a fill, capital moves from deployed to positions. Available stays same."""
        engine = make_engine(bankroll=100)
        # Place order
        engine.active_orders["order1"] = {
            "side": "BUY", "price": 0.50, "size": 60,
            "window_id": "btc-15m-w1", "strategy": "mm"
        }
        engine._recalc_exposure()
        avail_before = engine.get_available_capital()

        # Fill happens: order removed from active, position recorded
        del engine.active_orders["order1"]
        engine.record_fill("token_up_1", "BUY", 0.50, 60)
        engine._recalc_exposure()

        # Available should be the same — capital moved from deployed to positions
        self.assertAlmostEqual(engine.get_available_capital(), avail_before, places=1)
        self.assertAlmostEqual(engine.capital_deployed, 0.0, places=1)
        self.assertAlmostEqual(engine.capital_in_positions, 30.0, places=1)

    def test_available_after_merge_returns_capital(self):
        """After merge/claim, capital_in_positions decreases, freeing up available."""
        engine = make_engine(bankroll=100)
        # Simulate: $30 in positions
        engine.capital_in_positions = 30.0
        engine._update_total_capital()
        self.assertAlmostEqual(engine.get_available_capital(), 70.0, places=1)

        # Merge returns $30 (cost basis) — capital_in_positions drops
        engine.capital_in_positions = max(0, engine.capital_in_positions - 30.0)
        engine._update_total_capital()
        self.assertAlmostEqual(engine.get_available_capital(), 100.0, places=1)

    def test_available_capped_by_wallet(self):
        """Available capital is capped by actual wallet balance."""
        engine = make_engine(bankroll=100, check_wallet=True)
        engine.balance_checker = MagicMock()
        engine.balance_checker.get_balance.return_value = 50.0
        # Bankroll says $100 available, but wallet only has $50
        self.assertAlmostEqual(engine.get_available_capital(), 50.0, places=1)


class TestBankrollScenario(unittest.TestCase):
    """
    Full scenario from user:
    $100 bankroll, 4 concurrent markets, $30 max per market.
    """

    def test_scenario_step_by_step(self):
        engine = make_engine(bankroll=100, budget_per_market=30, max_concurrent=4)

        # --- Step 1: Initial state ---
        self.assertEqual(engine.get_available_capital(), 100.0)

        # --- Step 2: Trade 1 — $30 on btc-15m-w1 ---
        # Place UP order ($15) and DOWN order ($15)
        engine.active_orders["order1_up"] = {
            "side": "BUY", "price": 0.50, "size": 30,  # $15
            "window_id": "btc-15m-w1", "strategy": "mm"
        }
        engine.active_orders["order1_down"] = {
            "side": "BUY", "price": 0.50, "size": 30,  # $15
            "window_id": "btc-15m-w1", "strategy": "mm"
        }
        engine._recalc_exposure()
        self.assertAlmostEqual(engine.capital_deployed, 30.0, places=1)
        self.assertAlmostEqual(engine.get_available_capital(), 70.0, places=1)

        # --- Step 3: Trade 2 — $30 on btc-15m-w2 ---
        engine.active_orders["order2_up"] = {
            "side": "BUY", "price": 0.50, "size": 30,
            "window_id": "btc-15m-w2", "strategy": "mm"
        }
        engine.active_orders["order2_down"] = {
            "side": "BUY", "price": 0.50, "size": 30,
            "window_id": "btc-15m-w2", "strategy": "mm"
        }
        engine._recalc_exposure()
        self.assertAlmostEqual(engine.capital_deployed, 60.0, places=1)
        self.assertAlmostEqual(engine.get_available_capital(), 40.0, places=1)

        # --- Step 4: Trade 3 — $30 on btc-15m-w3 ---
        engine.active_orders["order3_up"] = {
            "side": "BUY", "price": 0.50, "size": 30,
            "window_id": "btc-15m-w3", "strategy": "mm"
        }
        engine.active_orders["order3_down"] = {
            "side": "BUY", "price": 0.50, "size": 30,
            "window_id": "btc-15m-w3", "strategy": "mm"
        }
        engine._recalc_exposure()
        self.assertAlmostEqual(engine.capital_deployed, 90.0, places=1)
        avail = engine.get_available_capital()
        self.assertAlmostEqual(avail, 10.0, places=1)
        # $10 available < $30 per market → 4th trade should be REJECTED
        self.assertLess(avail, 30.0, "4th trade should be rejected: avail < budget_per_market")

        # --- Step 5: Trade 1 fills (both sides) ---
        # Remove open orders, record fills
        del engine.active_orders["order1_up"]
        del engine.active_orders["order1_down"]
        engine.record_fill("token_up_w1", "BUY", 0.50, 30)  # $15
        engine.record_fill("token_down_w1", "BUY", 0.50, 30)  # $15
        engine.filled_windows.add("btc-15m-w1")
        engine.window_fill_cost["btc-15m-w1"] = 30.0
        engine._recalc_exposure()

        # capital_deployed = $60 (w2+w3), capital_in_positions = $30 (w1 filled)
        # total_capital_used = $90, available = $10
        self.assertAlmostEqual(engine.capital_deployed, 60.0, places=1)
        self.assertAlmostEqual(engine.capital_in_positions, 30.0, places=1)
        self.assertAlmostEqual(engine.get_available_capital(), 10.0, places=1)
        # Still can't trade 4th market — capital is tied up in positions

        # --- Step 6: Merge/claim Trade 1 — returns $40 ($30 cost + $10 profit) ---
        # Merge reduces capital_in_positions by cost basis ($30)
        engine.capital_in_positions = max(0, engine.capital_in_positions - 30.0)
        engine.session_total_spent = max(0, engine.session_total_spent - 30.0)
        # Clean up filled window tracking (merge completed)
        engine.filled_windows.discard("btc-15m-w1")
        engine.window_fill_cost.pop("btc-15m-w1", None)
        engine._update_total_capital()

        # Now: capital_deployed=$60, capital_in_positions=$0, total=$60
        # available = $100 - $60 = $40
        avail_after_merge = engine.get_available_capital()
        self.assertAlmostEqual(avail_after_merge, 40.0, places=1)
        # $40 available >= $30 per market → 4th trade ALLOWED
        self.assertGreaterEqual(avail_after_merge, 30.0,
                                "4th trade should be allowed after merge")

        # --- Step 7: Trade 4 — $30 on btc-15m-w4 ---
        engine.active_orders["order4_up"] = {
            "side": "BUY", "price": 0.50, "size": 30,
            "window_id": "btc-15m-w4", "strategy": "mm"
        }
        engine.active_orders["order4_down"] = {
            "side": "BUY", "price": 0.50, "size": 30,
            "window_id": "btc-15m-w4", "strategy": "mm"
        }
        engine._recalc_exposure()
        self.assertAlmostEqual(engine.capital_deployed, 90.0, places=1)
        avail_after_4 = engine.get_available_capital()
        self.assertAlmostEqual(avail_after_4, 10.0, places=1)
        # $10 available < $30 → 5th trade REJECTED
        self.assertLess(avail_after_4, 30.0, "5th trade should be rejected")


class TestBankrollBudgetCheck(unittest.TestCase):
    """Test that place_order() correctly enforces bankroll limits."""

    def test_order_rejected_when_exceeds_available(self):
        """place_order should reject BUY when cost > available capital."""
        engine = make_engine(bankroll=100, budget_per_market=30)
        # Use up $90 of capital
        engine.capital_in_positions = 90.0
        engine._update_total_capital()
        # Try to place $30 order — only $10 available
        result = engine.place_order(
            token_id="token_up", side="BUY", price=0.50, size=60,
            window_id="btc-15m-w4", strategy="mm"
        )
        self.assertIsNone(result, "Order should be rejected: $30 > $10 available")

    def test_order_rejected_when_exceeds_per_market(self):
        """place_order should reject BUY when window spend exceeds per-market cap."""
        engine = make_engine(bankroll=100, budget_per_market=30)
        # Already spent $30 on this window (filled)
        engine.window_fill_cost["btc-15m-w1"] = 30.0
        engine.filled_windows.add("btc-15m-w1")
        # Try to place another $15 on same window
        result = engine.place_order(
            token_id="token_up", side="BUY", price=0.50, size=30,
            window_id="btc-15m-w1", strategy="mm"
        )
        self.assertIsNone(result, "Order should be rejected: window spend exceeds per-market cap")

    def test_order_rejected_when_exceeds_total_exposure(self):
        """place_order should reject BUY when total exposure exceeds max."""
        engine = make_engine(bankroll=100, budget_per_market=50)
        # Place $90 in open orders across 2 windows
        engine.active_orders["o1"] = {
            "side": "BUY", "price": 0.50, "size": 90,  # $45
            "window_id": "btc-15m-w1", "strategy": "mm"
        }
        engine.active_orders["o2"] = {
            "side": "BUY", "price": 0.50, "size": 90,  # $45
            "window_id": "btc-15m-w2", "strategy": "mm"
        }
        engine._recalc_exposure()
        # Try to place $15 more — total would be $105 > $100
        result = engine.place_order(
            token_id="token_up", side="BUY", price=0.50, size=30,
            window_id="btc-15m-w3", strategy="mm"
        )
        self.assertIsNone(result, "Order should be rejected: total exposure > bankroll")


class TestMergeClaimFlowsBackToBankroll(unittest.TestCase):
    """Test that merge/claim proceeds correctly increase available capital."""

    def test_record_claim_frees_capital(self):
        """record_claim should reduce capital_in_positions, increasing available."""
        engine = make_engine(bankroll=100)
        engine.capital_in_positions = 60.0
        engine._update_total_capital()
        self.assertAlmostEqual(engine.get_available_capital(), 40.0, places=1)

        # Claim returns $30
        engine.record_claim(30.0)
        self.assertAlmostEqual(engine.capital_in_positions, 30.0, places=1)
        self.assertAlmostEqual(engine.get_available_capital(), 70.0, places=1)

    def test_merge_frees_capital(self):
        """After merge reduces capital_in_positions, available increases."""
        engine = make_engine(bankroll=100)
        engine.capital_in_positions = 30.0
        engine.session_total_spent = 30.0
        engine._update_total_capital()

        # Merge returns $30 cost basis
        engine.capital_in_positions = max(0, engine.capital_in_positions - 30.0)
        engine.session_total_spent = max(0, engine.session_total_spent - 30.0)
        engine._update_total_capital()

        self.assertAlmostEqual(engine.capital_in_positions, 0.0, places=1)
        self.assertAlmostEqual(engine.get_available_capital(), 100.0, places=1)

    def test_sell_frees_capital(self):
        """Selling tokens (CLOB-SELL claim) reduces capital_in_positions."""
        engine = make_engine(bankroll=100)
        engine.capital_in_positions = 30.0
        engine.token_holdings["token_up_1"] = {"size": 60, "cost": 30.0}
        engine._update_total_capital()

        # Sell 60 tokens at $0.95 each = $57 revenue
        engine.record_fill("token_up_1", "SELL", 0.95, 60)
        # capital_in_positions should decrease by sell revenue
        # $30 - ($0.95 * 60) = $30 - $57 → max(0, -$27) = $0
        self.assertAlmostEqual(engine.capital_in_positions, 0.0, places=1)
        self.assertAlmostEqual(engine.get_available_capital(), 100.0, places=1)


class TestNoReserveDeduction(unittest.TestCase):
    """Test that deploy_reserve_pct no longer reduces available capital."""

    def test_reserve_pct_has_no_effect(self):
        """Even with deploy_reserve_pct > 0, available = bankroll - used."""
        engine = make_engine(bankroll=100)
        engine.config.deploy_reserve_pct = 0.20  # Old 20% reserve
        # V15.1-18: get_available_capital ignores deploy_reserve_pct
        self.assertAlmostEqual(engine.get_available_capital(), 100.0, places=1)

    def test_full_bankroll_deployable(self):
        """All of bankroll is deployable, not just 80%."""
        engine = make_engine(bankroll=100)
        # Can deploy up to $100, not $80
        engine.active_orders["o1"] = {
            "side": "BUY", "price": 0.50, "size": 180,  # $90
            "window_id": "btc-15m-w1", "strategy": "mm"
        }
        engine._recalc_exposure()
        self.assertAlmostEqual(engine.get_available_capital(), 10.0, places=1)
        # Not $-10 as it would be with 80% deployable


class TestReconcilePreservesCapital(unittest.TestCase):
    """Test that reconcile doesn't zero capital_in_positions when tokens are held."""

    def test_reconcile_preserves_filled_window_capital(self):
        """reconcile should NOT zero capital_in_positions for filled windows."""
        engine = make_engine(bankroll=100)
        engine.capital_in_positions = 30.0
        engine.filled_windows.add("btc-15m-w1")
        engine.window_fill_cost["btc-15m-w1"] = 30.0
        engine._update_total_capital()

        # Reconcile runs (no active orders)
        engine.reconcile_capital_from_wallet()

        # Capital should be preserved (tokens still held)
        self.assertAlmostEqual(engine.capital_in_positions, 30.0, places=1)
        self.assertAlmostEqual(engine.get_available_capital(), 70.0, places=1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
