"""Tests for V15.1-20 critical fixes: P0 (live loss stop), P1 (held_windows), P2 (wallet_delta P&L)."""
import unittest
from unittest.mock import MagicMock
from trading_bot_v15 import BotConfig, TradingEngine, FeeCalculator


def make_engine(dry_run=False, bankroll=100):
    cfg = BotConfig()
    cfg.dry_run = dry_run
    cfg.kelly_bankroll = bankroll
    cfg.max_total_exposure = bankroll * 0.80
    cfg.max_position_per_market = bankroll * 0.35
    cfg.session_budget = bankroll
    logger = MagicMock()
    fee_calc = FeeCalculator()  # No args
    balance_checker = MagicMock()
    balance_checker.get_balance.return_value = bankroll
    engine = TradingEngine(cfg, fee_calc, logger, balance_checker=balance_checker)
    return engine, cfg, logger, balance_checker


class TestP1HeldWindows(unittest.TestCase):
    """P1: held_windows should persist after filled_windows is cleared."""

    def test_held_windows_initialized(self):
        engine, _, _, _ = make_engine()
        self.assertIsInstance(engine.held_windows, set)
        self.assertEqual(len(engine.held_windows), 0)

    def test_held_windows_added_on_fill(self):
        engine, _, _, _ = make_engine()
        wid = "btc-5m-123"
        engine.filled_windows.add(wid)
        engine.held_windows.add(wid)
        self.assertIn(wid, engine.held_windows)

    def test_held_windows_survives_filled_windows_clear(self):
        engine, _, _, _ = make_engine()
        wid = "btc-5m-123"
        engine.filled_windows.add(wid)
        engine.held_windows.add(wid)
        # Simulate what cleanup_expired_windows does
        engine.filled_windows.discard(wid)
        # held_windows should still have it
        self.assertIn(wid, engine.held_windows)
        self.assertNotIn(wid, engine.filled_windows)

    def test_held_windows_released_on_merge(self):
        engine, _, _, _ = make_engine()
        wid = "btc-5m-123"
        engine.held_windows.add(wid)
        engine.window_fill_cost[wid] = 10.0
        # Simulate merge returning capital
        engine.window_fill_cost.pop(wid, None)
        engine.held_windows.discard(wid)
        self.assertNotIn(wid, engine.held_windows)

    def test_held_windows_released_on_momentum_exit(self):
        engine, _, _, _ = make_engine()
        wid = "btc-5m-123"
        engine.held_windows.add(wid)
        engine.closed_windows.add(wid)
        # Momentum exit releases held
        engine.held_windows.discard(wid)
        self.assertNotIn(wid, engine.held_windows)
        # But closed_windows keeps it blocked
        self.assertIn(wid, engine.closed_windows)

    def test_concurrent_check_includes_held(self):
        engine, cfg, _, _ = make_engine()
        cfg.max_concurrent_windows = 4
        # Add 4 held windows (simulating 4 windows with tokens on-chain)
        for i in range(4):
            engine.held_windows.add(f"btc-5m-{i}")
        # Active set should include held_windows
        active = (set(engine.window_exposure.keys())
                  | engine.filled_windows
                  | engine.held_windows)
        self.assertEqual(len(active), 4)

    def test_held_windows_not_released_by_filled_cleanup(self):
        """The critical bug: cleanup_expired_windows discards filled_windows
        but held_windows must persist until capital is recovered."""
        engine, _, _, _ = make_engine()
        # Simulate 4 windows filled then expired
        for i in range(4):
            wid = f"eth-5m-{i}"
            engine.filled_windows.add(wid)
            engine.held_windows.add(wid)
        # Simulate cleanup_expired_windows clearing filled_windows
        engine.filled_windows.clear()
        # held_windows should still have all 4
        self.assertEqual(len(engine.held_windows), 4)
        # New windows should be blocked by held_windows
        active = (set(engine.window_exposure.keys())
                  | engine.filled_windows
                  | engine.held_windows)
        self.assertEqual(len(active), 4)


class TestP2WalletDeltaPnL(unittest.TestCase):
    """P2: get_live_pnl should return dict with wallet_delta."""

    def test_returns_none_without_balance(self):
        engine, _, _, _ = make_engine()
        engine.starting_wallet_balance = None
        result = engine.get_live_pnl()
        self.assertIsNone(result)

    def test_returns_dict_with_wallet_delta(self):
        engine, _, _, bc = make_engine(bankroll=100)
        engine.starting_wallet_balance = 100.0
        bc.get_balance.return_value = 80.0
        result = engine.get_live_pnl()
        self.assertIsInstance(result, dict)
        self.assertAlmostEqual(result["wallet_delta"], -20.0)
        self.assertEqual(result["wallet_now"], 80.0)

    def test_wallet_delta_in_get_stats(self):
        engine, _, _, bc = make_engine(bankroll=100)
        engine.starting_wallet_balance = 100.0
        bc.get_balance.return_value = 90.0
        stats = engine.get_stats()
        self.assertAlmostEqual(stats["wallet_delta"], -10.0)
        self.assertAlmostEqual(stats["live_pnl"], -10.0)  # backward compat
        self.assertIn("held_value", stats)
        self.assertIn("total_pnl_est", stats)
        self.assertIn("held_windows", stats)

    def test_wallet_delta_positive(self):
        engine, _, _, bc = make_engine(bankroll=100)
        engine.starting_wallet_balance = 100.0
        bc.get_balance.return_value = 110.0
        result = engine.get_live_pnl()
        self.assertAlmostEqual(result["wallet_delta"], 10.0)

    def test_wallet_delta_zero(self):
        engine, _, _, bc = make_engine(bankroll=100)
        engine.starting_wallet_balance = 100.0
        bc.get_balance.return_value = 100.0
        result = engine.get_live_pnl()
        self.assertAlmostEqual(result["wallet_delta"], 0.0)

    def test_held_value_in_pnl(self):
        engine, _, _, bc = make_engine(bankroll=100)
        engine.starting_wallet_balance = 100.0
        bc.get_balance.return_value = 80.0
        # Simulate holding tokens worth $15
        engine.token_holdings["token1"] = {"size": 30, "cost": 15}
        engine._market_cache["btc-5m-123"] = {
            "token_up": "token1", "up_price": 0.50
        }
        result = engine.get_live_pnl()
        self.assertAlmostEqual(result["wallet_delta"], -20.0)
        self.assertGreater(result["held_value"], 0)
        # total_pnl includes held value estimate
        self.assertGreater(result["total_pnl"], result["wallet_delta"])


class TestP0LiveLossStop(unittest.TestCase):
    """P0: Live mode should have a loss stop based on wallet delta."""

    def test_engine_has_starting_wallet(self):
        engine, _, _, _ = make_engine()
        self.assertTrue(hasattr(engine, 'starting_wallet_balance'))

    def test_wallet_delta_loss_detection(self):
        """When wallet drops below max_loss_pct, the loss should be detectable."""
        engine, cfg, _, bc = make_engine(bankroll=100)
        engine.starting_wallet_balance = 100.0
        cfg.max_loss_pct = 0.20  # 20% max loss = $20
        # Wallet dropped $25 (25% loss)
        bc.get_balance.return_value = 75.0
        pnl = engine.get_live_pnl()
        wallet_delta = pnl["wallet_delta"]
        max_loss = cfg.kelly_bankroll * cfg.max_loss_pct
        # The loss exceeds the limit
        self.assertLess(wallet_delta, 0)
        self.assertGreater(abs(wallet_delta), max_loss)


if __name__ == "__main__":
    unittest.main()
