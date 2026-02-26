"""
PolyMaker Bot Launcher
======================
Wraps the v15.1 trading bot with Command Center integration for:
- Config loading from Command Center (targetAssets, bankroll, etc.)
- Run lifecycle management (start/stop/update)
- Periodic metrics push to dashboard
- Session-based logging (logs/{run_id}/bot.log)

Usage:
    python main.py
"""

import os
import sys
import time
import signal
import logging
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv

load_dotenv()

# Import v15 bot components
from trading_bot_v15 import (
    PolymarketBot, BotConfig, setup_logging, LOG_DIR
)

# Import Command Center client
from polymaker_client import cc

# Import WebSocket manager (Phase 1 async foundation)
try:
    from ws_manager import WebSocketManager, EventType, Channel
    from ws_price_feed import WSPriceFeed, WSOrderBookReader
    from ws_fill_detector import WSFillDetector
    HAS_WS_MANAGER = True
except ImportError:
    HAS_WS_MANAGER = False


def setup_session_logging(run_id: int, logger: logging.Logger):
    """
    Configure session-based logging: logs/{run_id}/bot.log
    """
    session_dir = os.path.join("logs", str(run_id))
    os.makedirs(session_dir, exist_ok=True)
    session_log_path = os.path.join(session_dir, "bot.log")

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
    fh = logging.FileHandler(session_log_path, mode="w")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    logger.info(f"Session logs: {os.path.abspath(session_log_path)}")
    return session_log_path


def apply_cc_config(config: BotConfig, cc_config: dict):
    """
    Apply Command Center config overrides to the v15 BotConfig.
    
    Hybrid Budget System (Option D):
    - CC controls high-level budget: sessionBudget, budgetPerMarket
    - mmOrderSize is auto-derived: budgetPerMarket / 2
    - v15's internal strategy budget system is disabled (100% to MM)
    - Only exposure caps are kept for safety
    """
    # Target assets - handle both list and comma-separated string
    target_assets = cc_config.get("targetAssets", [])
    if isinstance(target_assets, str):
        target_assets = [a.strip() for a in target_assets.split(",") if a.strip()]
    if target_assets:
        assets_lower = [a.lower() for a in target_assets]
        config.assets_15m = assets_lower
        config.assets_5m = assets_lower

    # Window durations - handle both list and comma-separated string
    window_durations = cc_config.get("windowDurations", [])
    if isinstance(window_durations, str):
        try:
            import json
            window_durations = json.loads(window_durations)
        except Exception:
            window_durations = [w.strip() for w in window_durations.split(",") if w.strip()]
    if window_durations:
        config.timeframes = window_durations

    # ── Hybrid Budget System ────────────────────────────────────────
    # CC provides: kellyBankroll, sessionBudget, budgetPerMarket
    # Bot derives: mm_order_size = budgetPerMarket / 2
    
    bankroll = cc_config.get("kellyBankroll")
    if bankroll and bankroll > 0:
        config.kelly_bankroll = float(bankroll)

    session_budget = cc_config.get("sessionBudget")
    budget_per_market = cc_config.get("budgetPerMarket")
    
    # Derive mm_order_size from budgetPerMarket (half for UP, half for DOWN)
    if budget_per_market and budget_per_market > 0:
        config.mm_order_size = round(float(budget_per_market) / 2.0, 2)
        # Set per-market position cap to full budgetPerMarket (allows both sides)
        config.max_position_per_market = float(budget_per_market)
        print(f"  Budget per market: ${budget_per_market:.2f} -> order size: ${config.mm_order_size:.2f}/side")

    # Session budget controls total exposure cap
    if session_budget and session_budget > 0:
        config.max_total_exposure = float(session_budget)
        print(f"  Session budget: ${session_budget:.2f} (max total exposure)")

    # Spread parameters
    mm_spread_min = cc_config.get("mmSpreadMin")
    if mm_spread_min and mm_spread_min > 0:
        config.mm_base_spread = float(mm_spread_min)
    
    mm_spread_max = cc_config.get("mmSpreadMax")
    if mm_spread_max and mm_spread_max > 0:
        config.mm_max_spread = float(mm_spread_max)

    # Risk parameters
    max_concurrent = cc_config.get("maxConcurrentWindows")
    if max_concurrent and max_concurrent > 0:
        config.max_concurrent_windows = int(max_concurrent)

    # V15.1-22: Max time sessions to consider per timeframe
    max_sessions = cc_config.get("maxOrderSessions")
    if max_sessions and max_sessions > 0:
        config.max_order_sessions = int(max_sessions)

    max_loss_pct = cc_config.get("maxLossPct")
    if max_loss_pct is not None and max_loss_pct > 0:
        config.hard_loss_stop_pct = float(max_loss_pct)

    loss_cooldown = cc_config.get("lossCooldownSec")
    if loss_cooldown is not None and loss_cooldown > 0:
        config.hard_loss_cooloff = float(loss_cooldown)

    reserve_ratio = cc_config.get("reserveRatio")
    if reserve_ratio is not None and reserve_ratio >= 0:
        config.deploy_reserve_pct = float(reserve_ratio)

    # Dry run mode
    mode = cc_config.get("mode", "dry_run")
    config.dry_run = (mode != "live")

    # ── Per-Strategy Toggles & Budgets from CC ──────────────────────
    # CC is the authority for which strategies are active and their budgets.
    # If not toggled on, budget = $0 and strategy is disabled.
    
    mm_enabled = cc_config.get("mmEnabled", True)
    arb_enabled = cc_config.get("arbEnabled", False)
    sniper_enabled = cc_config.get("sniperEnabled", False)
    contrarian_enabled = cc_config.get("contrarianEnabled", False)
    pre_exit_enabled = cc_config.get("preExitEnabled", False)
    
    config.mm_enabled = bool(mm_enabled)
    config.arb_enabled = bool(arb_enabled)
    config.sniper_enabled = bool(sniper_enabled)
    config.contrarian_enabled = bool(contrarian_enabled)
    
    # Per-strategy budgets (disabled = $0)
    mm_budget = float(cc_config.get("mmBudget", 0)) if mm_enabled else 0
    arb_budget = float(cc_config.get("arbBudget", 0)) if arb_enabled else 0
    sniper_budget = float(cc_config.get("sniperBudget", 0)) if sniper_enabled else 0
    contrarian_budget = float(cc_config.get("contrarianBudget", 0)) if contrarian_enabled else 0
    
    # Map CC budgets to v15's strategy_budget_pct system
    total_budget = mm_budget + arb_budget + sniper_budget + contrarian_budget
    if total_budget > 0:
        config.strategy_budget_pct = {
            "mm": mm_budget / total_budget,
            "arb": arb_budget / total_budget,
            "sniper": sniper_budget / total_budget,
            "contrarian": contrarian_budget / total_budget,
        }
        config.strategy_budget_enabled = True
    else:
        config.strategy_budget_pct = {"mm": 1.0, "sniper": 0.0, "arb": 0.0, "contrarian": 0.0}
    
    # Arb-specific parameters
    arb_min_profit = cc_config.get("arbMinProfit")
    if arb_min_profit is not None:
        config.arb_min_profit = float(arb_min_profit)
    arb_max_size = cc_config.get("arbMaxSize")
    if arb_max_size is not None:
        config.arb_max_size = float(arb_max_size)
    
    # Pre-exit: disabled by default (erodes margins in live trading)
    # When disabled, positions resolve naturally via auto-claim for full $1 payout
    if not pre_exit_enabled:
        config.pre_exit_enabled = False
    
    print(f"  Strategies: MM={'ON' if mm_enabled else 'OFF'} (${mm_budget:.0f}) | "
          f"Arb={'ON' if arb_enabled else 'OFF'} (${arb_budget:.0f}) | "
          f"Sniper={'ON' if sniper_enabled else 'OFF'} (${sniper_budget:.0f}) | "
          f"Contrarian={'ON' if contrarian_enabled else 'OFF'} (${contrarian_budget:.0f}) | "
          f"PreExit={'ON' if pre_exit_enabled else 'OFF'}")

    # ── Cash management: enable auto-merge for paired positions ──────
    # When both UP+DOWN sides of a pair are filled, merging them on-chain
    # returns ~$1/share as USDC, freeing capital for the next window.
    # This is essential for capital efficiency in MM.
    config.auto_merge_enabled = True
    config.blind_redeem_enabled = True   # Try redeem even before resolution confirmed
    config.immediate_pair_completion = False  # Not needed with Option C skip
    # V15.1-13: Hedge completion — auto-buy other side after one side fills.
    # V15.1-23: Tiered hedge pricing — progressively wider max cost as time passes.
    config.hedge_completion_enabled = bool(cc_config.get("hedgeEnabled", True))
    config.hedge_min_profit_per_share = float(cc_config.get("hedgeMinProfit", 0.005))
    config.hedge_max_loss_per_share = 0.02    # Legacy fallback threshold
    # Tiered hedge: list of (pct_remaining_threshold, max_combined_cost)
    # pct_remaining is the % of window time left — tier triggers when remaining < threshold
    config.hedge_tiers = [
        (float(cc_config.get("hedgeTier1Pct", 67)), float(cc_config.get("hedgeTier1Cost", 1.03))),
        (float(cc_config.get("hedgeTier2Pct", 33)), float(cc_config.get("hedgeTier2Cost", 1.05))),
        (float(cc_config.get("hedgeTier3Pct", 13)), float(cc_config.get("hedgeTier3Cost", 1.08))),
    ]
    # Sort tiers by pct descending (T1=67% triggers first, T3=13% triggers last)
    config.hedge_tiers = sorted(config.hedge_tiers, key=lambda t: t[0], reverse=True)
    # Keep legacy fields for backward compat (use tier 1 as default)
    config.hedge_completion_delay = 30  # Legacy fallback
    config.hedge_max_combined_cost = config.hedge_tiers[0][1]

    # V15.2-T4: Last Resort Sell — sell filled side at market bid when all buy-tiers exhausted
    config.hedge_t4_enabled = bool(cc_config.get("hedgeT4Enabled", True))
    config.hedge_t4_sell_pct = float(cc_config.get("hedgeTier4Pct", 33.0))
    config.hedge_t4_max_loss = float(cc_config.get("hedgeTier4MaxLoss", 0.30))

    # V15.1-14: Momentum exit — sell one-sided fill if price rises >X%
    config.momentum_exit_enabled = bool(cc_config.get("momentumExitEnabled", True))
    config.momentum_exit_threshold = float(cc_config.get("momentumExitThreshold", 0.03))
    config.momentum_exit_max_wait_secs = float(cc_config.get("momentumExitMaxWait", 120.0))
    config.momentum_exit_min_hold_secs = 10.0  # Fixed at 10s

    # V15.2: Max order horizon — how far before observation start to place orders
    max_order_horizon = cc_config.get("maxOrderHorizon")
    if max_order_horizon is not None and max_order_horizon > 0:
        config.max_order_horizon = float(max_order_horizon)

    # Pause control: CC can pause new order placement each cycle
    config.pause_orders = bool(cc_config.get("pauseOrders", False))

    print(f"  Hedge: {'ON' if config.hedge_completion_enabled else 'OFF'} "
          f"(tiers: {', '.join(f'${t[1]:.2f}@<{t[0]:.0f}%rem' for t in config.hedge_tiers)}, "
          f"minProfit=${config.hedge_min_profit_per_share:.3f})")
    print(f"  T4 Last Resort: {'ON' if config.hedge_t4_enabled else 'OFF'} "
          f"(trigger=<{config.hedge_t4_sell_pct:.0f}%rem, maxLoss=${config.hedge_t4_max_loss:.3f}/sh)")
    print(f"  Momentum Exit: {'ON' if config.momentum_exit_enabled else 'OFF'} "
          f"(threshold={config.momentum_exit_threshold:.1%}, "
          f"maxWait={config.momentum_exit_max_wait_secs:.0f}s)")
    if config.pause_orders:
        print(f"  ⚠️  PAUSE ORDERS: enabled from CC — no new orders will be placed")

    # V15.1-19: Pre-entry filters for orphan reduction
    config.momentum_gate_threshold = float(cc_config.get("momentumGate", 0.01))
    config.momentum_gate_max_consec = int(cc_config.get("momentumGateMaxConsec", 3))
    config.min_book_depth = float(cc_config.get("minBookDepth", 5.0))
    config.max_spread_asymmetry = float(cc_config.get("maxSpreadAsymmetry", 0.02))
    # V15.1-29: Enhanced pre-entry filters
    config.midpoint_skew_limit = float(cc_config.get("midpointSkewLimit", 0.03))
    config.momentum_gate_asset_scale = {
        "BTC": float(cc_config.get("momentumGateScaleBtc", 1.0)),
        "ETH": float(cc_config.get("momentumGateScaleEth", 1.2)),
        "SOL": float(cc_config.get("momentumGateScaleSol", 1.8)),
        "XRP": float(cc_config.get("momentumGateScaleXrp", 1.8)),
    }
    # Session blackout windows: list of [start_hour_utc, end_hour_utc] pairs
    blackout_raw = cc_config.get("tradingBlackoutWindows", [])
    config.trading_blackout_windows = blackout_raw if isinstance(blackout_raw, list) else []

    print(f"  Pre-entry filters: MomGate={config.momentum_gate_threshold:.3f} (bypass@{config.momentum_gate_max_consec}) | "
          f"MidSkew={config.midpoint_skew_limit:.2f} | MinDepth=${config.min_book_depth:.0f} | MaxSpreadAsym={config.max_spread_asymmetry:.3f}")
    print(f"  Gate asset scale: " + " | ".join(f"{a}={s:.1f}x" for a, s in config.momentum_gate_asset_scale.items()))
    if config.trading_blackout_windows:
        print(f"  Blackout windows: {config.trading_blackout_windows}")

    # ── Auto-claim/redeem: reclaim USDC after market resolution ──────────
    # After a 15-min market resolves, winning shares are worth $1 each.
    # Auto-claim redeems them back to USDC so capital returns to bankroll.
    # This is FREE (gasless) through Polymarket's relayer system.
    config.auto_claim_enabled = True
    config.claim_delay_seconds = 15.0     # Wait 15s after window ends before checking
    config.claim_check_interval = 10.0    # Check every 10s (fast for 15-min markets)
    config.claim_max_attempts = 120       # Try for up to 20 minutes
    config.claim_timeout_seconds = 1800.0 # Give up after 30 minutes
    config.claim_fallback_sell = True      # Sell winning shares on CLOB if redeem fails
    config.claim_sell_min_price = 0.95    # Minimum price for fallback sell

    # ── Disable v15 bankroll auto-detect ───────────────────────────────
    # CC is the authority for bankroll. Don't let v15 override it from wallet.
    config.auto_detect_bankroll = False

    # ── Pre-flight validation ──────────────────────────────────────────
    _preflight_validate(config, cc_config)

    return config


def _preflight_validate(config: BotConfig, cc_config: dict):
    """
    Validate that CC config makes sense before the bot starts trading.
    Prints warnings for any issues that could cause problems.
    """
    issues = []
    
    session_budget = cc_config.get("sessionBudget", 0)
    budget_per_market = cc_config.get("budgetPerMarket", 0)
    bankroll = cc_config.get("kellyBankroll", 0)
    max_concurrent = cc_config.get("maxConcurrentWindows", 4)
    max_loss_pct = cc_config.get("maxLossPct", 0.2)
    
    # Check: sessionBudget should not exceed bankroll
    if session_budget > bankroll:
        issues.append(
            f"  ⚠️  sessionBudget (${session_budget}) > kellyBankroll (${bankroll}). "
            f"Session budget should not exceed total bankroll."
        )
    
    # Check: budgetPerMarket * maxConcurrentWindows should not exceed sessionBudget
    max_deploy = budget_per_market * max_concurrent
    if max_deploy > session_budget and session_budget > 0:
        issues.append(
            f"  ⚠️  budgetPerMarket (${budget_per_market}) x {max_concurrent} markets = ${max_deploy:.0f} "
            f"exceeds sessionBudget (${session_budget}). Bot may run out of capital."
        )
    
    # Check: budgetPerMarket should be at least $10 for viable pair orders
    if budget_per_market < 10:
        issues.append(
            f"  ⚠️  budgetPerMarket (${budget_per_market}) is below $10 minimum. "
            f"Each market needs at least $5/side for viable pair orders."
        )
    
    # Check: max loss makes sense
    max_loss_dollar = max_loss_pct * bankroll
    if max_loss_dollar > session_budget and session_budget > 0:
        issues.append(
            f"  ⚠️  maxLoss (${max_loss_dollar:.0f} = {max_loss_pct:.0%} of ${bankroll}) "
            f"exceeds sessionBudget (${session_budget}). Loss stop may not trigger before budget exhausted."
        )
    
    if issues:
        print("\n  ╔══ PRE-FLIGHT VALIDATION ══════════════════════════════════╗")
        for issue in issues:
            print(issue)
        print("  ╚══════════════════════════════════════════════════════════╝\n")
    else:
        print("  ✓ Pre-flight validation passed")


class PolyMakerBot(PolymarketBot):
    """
    Extended v15 bot with Command Center integration.
    Inherits all trading logic from PolymarketBot and adds:
    - CC config loading
    - Run lifecycle management
    - Periodic metrics push
    - Session-based logging
    """

    def __init__(self):
        # Load CC config BEFORE parent init so BotConfig gets overrides
        self._cc_config = None
        self._cc_config_id = None
        self._cc_run_id = None
        self._metrics_interval = 30.0
        self._last_metrics_push = 0
        self._peak_pnl = 0.0
        self._max_drawdown = 0.0

        # Fetch CC config first
        if cc._is_ready():
            config = cc.get_config()
            if config and config.get("id"):
                self._cc_config = config
                self._cc_config_id = config.get("id", 1)
                print(f"Command Center config loaded (ID: {self._cc_config_id})")
                print(f"  targetAssets: {config.get('targetAssets', [])}")
                print(f"  windowDurations: {config.get('windowDurations', [])}")
                print(f"  mode: {config.get('mode', 'dry_run')}")
            else:
                print("No active config from Command Center, using defaults")
        else:
            print("Command Center not configured, using defaults")

        # Now call parent init (which creates BotConfig)
        super().__init__()

        # Apply CC config overrides after parent init
        if self._cc_config:
            apply_cc_config(self.config, self._cc_config)
            self.logger.info(f"CC config applied: assets={self.config.assets_15m}, "
                           f"timeframes={self.config.timeframes}, "
                           f"bankroll=${self.config.kelly_bankroll:.0f}, "
                           f"session_budget=${self.config.max_total_exposure:.0f}, "
                           f"budget_per_market=${self.config.max_position_per_market:.0f}, "
                           f"order_size=${self.config.mm_order_size:.0f}/side, "
                           f"max_loss={self.config.hard_loss_stop_pct:.0%} (${self.config.hard_loss_stop_pct * self.config.kelly_bankroll:.0f}), "
                           f"cooldown={self.config.hard_loss_cooloff:.0f}s, "
                           f"dry_run={self.config.dry_run}")

        # ── WebSocket Manager (Phase 1 async foundation) ──
        self.ws_manager = None
        self.ws_fill_detector = None
        self._ws_enabled = os.getenv("WS_ENABLED", "true").lower() == "true"
        if HAS_WS_MANAGER and self._ws_enabled:
            try:
                self.ws_manager = WebSocketManager(
                    api_key=self.config.api_key,  # L1 keys initially; L2 set later via set_derived_creds()
                    api_secret=self.config.api_secret,
                    api_passphrase=self.config.api_passphrase,
                    enable_market=True,
                    enable_user=bool(self.config.api_key),
                    enable_rtds=True,
                    logger_instance=self.logger,
                )
                # Wrap price_feed and book_reader with WS-enhanced versions
                self.price_feed = WSPriceFeed(
                    self.price_feed,
                    state_store=self.ws_manager.state_store,
                    logger_instance=self.logger,
                )
                self.book_reader = WSOrderBookReader(
                    self.book_reader,
                    state_store=self.ws_manager.state_store,
                    logger_instance=self.logger,
                )
                # Re-inject wrapped readers into strategies that hold references
                self.mm_strategy.book_reader = self.book_reader
                self.mm_strategy.price_feed = self.price_feed
                self.sniper.book_reader = self.book_reader
                self.sniper.price_feed = self.price_feed
                self.arb.book_reader = self.book_reader
                self.contrarian.book_reader = self.book_reader
                self.contrarian.price_feed = self.price_feed
                # Pass L2 derived creds to WS manager for user channel auth
                if hasattr(self, 'engine') and self.engine.client and self.engine.client.creds:
                    derived = self.engine.client.creds
                    self.ws_manager.set_derived_creds(
                        derived.api_key, derived.api_secret, derived.api_passphrase
                    )
                self.logger.info("  WebSocket manager initialized (market+user+rtds)")
                # Phase 2: WS-based fill detection
                self.ws_fill_detector = WSFillDetector(
                    ws_manager=self.ws_manager,
                    engine=self.engine,
                    logger=self.logger,
                )
            except Exception as e:
                self.logger.warning(f"  WebSocket manager init failed: {e}. Using REST-only mode.")
                self.ws_manager = None
                self.ws_fill_detector = None
        elif not HAS_WS_MANAGER:
            self.logger.info("  WebSocket manager: module not available, using REST-only mode")
        else:
            self.logger.info("  WebSocket manager: disabled via WS_ENABLED=false")

        # Validate credentials for live mode
        if not self.config.dry_run:
            missing = []
            if not self.config.private_key:
                missing.append("PRIVATE_KEY (or POLY_PRIVATE_KEY or PK)")
            if not self.config.proxy_wallet:
                missing.append("PROXY_WALLET (or POLY_PROXY_WALLET or POLYMARKET_PROXY_ADDRESS)")
            if not self.config.api_key:
                missing.append("API_KEY (or POLY_API_KEY or CLOB_API_KEY)")
            if missing:
                self.logger.error("\n  *** MISSING CREDENTIALS FOR LIVE MODE ***")
                for m in missing:
                    self.logger.error(f"    - {m}")
                self.logger.error("  Set these in your .env file and restart.\n")
                sys.exit(1)

    def _shutdown(self, signum, frame):
        """Override shutdown to also stop CC run."""
        if not self.running:
            # Second Ctrl+C: force exit immediately
            self.logger.info("\nForce exit (second signal). Goodbye.")
            os._exit(1)
        self.logger.info("\nShutdown signal received. Cancelling all orders...")
        self.logger.info("  (Press Ctrl+C again to force-quit immediately)")
        self.running = False
        try:
            self.engine.cancel_all()
        except Exception:
            pass
        self._print_summary("FINAL")
        self._print_claim_summary()
        self._print_v15_1_summary()

        # Stop WS fill detector
        if hasattr(self, 'ws_fill_detector') and self.ws_fill_detector:
            try:
                self.ws_fill_detector.stop()
                stats = self.ws_fill_detector.get_stats()
                self.logger.info(
                    f"WS fill detector stopped. "
                    f"WS fills: {stats['ws_fills_session']}, "
                    f"REST fallbacks: {stats['rest_fallback_count']}"
                )
            except Exception:
                pass

        # Stop WebSocket manager
        if self.ws_manager:
            try:
                self.ws_manager.stop()
                self.logger.info("WebSocket manager stopped.")
            except Exception:
                pass

        # Stop CC run with final metrics
        try:
            self._push_final_metrics("stopped")
        except Exception:
            pass

        self.logger.info("All orders cancelled. Exiting.")
        os._exit(0)

    def _push_metrics(self):
        """Push current metrics to Command Center periodically."""
        now = time.time()
        if now - self._last_metrics_push < self._metrics_interval:
            return
        self._last_metrics_push = now

        if not self._cc_run_id:
            return

        stats = self.engine.get_stats()

        # V15.1-20: Calculate PnL: wallet_delta is primary (hard fact)
        total_pnl = 0.0
        wallet_delta = stats.get("wallet_delta")
        if wallet_delta is not None:
            total_pnl = wallet_delta  # Ground truth: actual wallet change
        elif self.sim_engine:
            s = self.sim_engine.get_summary()
            total_pnl = s.get("realized_pnl", 0)
        else:
            total_pnl = stats.get("live_pnl", 0) or 0

        # Track peak PnL and drawdown
        if total_pnl > self._peak_pnl:
            self._peak_pnl = total_pnl
        drawdown = self._peak_pnl - total_pnl
        if drawdown > self._max_drawdown:
            self._max_drawdown = drawdown

        # Include wallet balance in metrics for CC dashboard
        wallet_bal = getattr(self, '_current_wallet_balance', None)
        starting_bal = getattr(self, '_starting_wallet_balance', None)
        ending_bankroll = wallet_bal if wallet_bal is not None else (self.config.kelly_bankroll + total_pnl)

        # Include merge stats for CC dashboard
        merge_stats = self.auto_merger.get_stats()
        # Include claim/redeem stats for CC dashboard
        claim_stats = self.claim_manager.get_claim_stats()
        # V15.1-16: Include position value for accurate portfolio P&L
        pos_value = getattr(self, '_position_value', None)
        if pos_value is None:
            pos_value = self.engine.get_position_value()
        # V15.2: Pair/orphan tracking for CC dashboard
        hedge_summary = stats.get("hedge_analytics", {})
        realized_pnl = stats.get("session_realized_pnl", 0)
        success = cc.update_run(
            total_cycles=cc.cycle_count,
            total_orders=stats.get("total_placed", 0),
            total_fills=stats.get("total_filled", 0),
            total_pnl=total_pnl,
            peak_pnl=self._peak_pnl,
            max_drawdown=self._max_drawdown,
            ending_bankroll=ending_bankroll,
            max_capital=stats.get("total_exposure", 0),
            wallet_balance=wallet_bal,
            starting_wallet=starting_bal,
            position_value=pos_value,
            merges_completed=merge_stats.get("merges_completed", 0),
            total_merged_usd=merge_stats.get("total_merged_usd", 0),
            claims_completed=claim_stats.get("claimed_total", 0),
            total_claimed_usd=claim_stats.get("total_claimed_usd", 0),
            claims_pending=claim_stats.get("pending_claims", 0),
            hedge_analytics=stats.get("hedge_analytics"),
            realized_pnl=realized_pnl,
            paired_windows=stats.get("paired_windows", 0),
            one_sided_fills=hedge_summary.get("one_sided_fills", 0),
        )
        if not success:
            self.logger.warning("  CC PUSH FAILED | update_run returned False | C{}".format(cc.cycle_count))

    def _push_final_metrics(self, status="completed"):
        """Push final metrics when run ends."""
        if not self._cc_run_id:
            return

        stats = self.engine.get_stats()
        # V15.1-20: wallet_delta is primary P&L metric
        total_pnl = 0.0
        wallet_delta = stats.get("wallet_delta")
        if wallet_delta is not None:
            total_pnl = wallet_delta
        elif self.sim_engine:
            s = self.sim_engine.get_summary()
            total_pnl = s.get("realized_pnl", 0)
        else:
            total_pnl = stats.get("live_pnl", 0) or 0

        wallet_bal = getattr(self, '_current_wallet_balance', None)
        starting_bal = getattr(self, '_starting_wallet_balance', None)
        ending_bankroll = wallet_bal if wallet_bal is not None else (self.config.kelly_bankroll + total_pnl)

        merge_stats = self.auto_merger.get_stats()
        claim_stats = self.claim_manager.get_claim_stats()
        # V15.2: Pair/orphan tracking for CC dashboard
        hedge_summary = stats.get("hedge_analytics", {})
        realized_pnl = stats.get("session_realized_pnl", 0)
        self.logger.info("  CC FINAL PUSH | Sending stop_run to CC (status={}, C{}, M:{}, Cl:{})".format(
            status, cc.cycle_count,
            merge_stats.get("merges_completed", 0),
            claim_stats.get("claimed_total", 0)))
        result = cc.stop_run(
            status=status,
            total_cycles=cc.cycle_count,
            total_orders=stats.get("total_placed", 0),
            total_fills=stats.get("total_filled", 0),
            total_pnl=total_pnl,
            peak_pnl=self._peak_pnl,
            max_drawdown=self._max_drawdown,
            ending_bankroll=ending_bankroll,
            max_capital=stats.get("total_exposure", 0),
            wallet_balance=wallet_bal,
            starting_wallet=starting_bal,
            merges_completed=merge_stats.get("merges_completed", 0),
            total_merged_usd=merge_stats.get("total_merged_usd", 0),
            claims_completed=claim_stats.get("claimed_total", 0),
            total_claimed_usd=claim_stats.get("total_claimed_usd", 0),
            claims_pending=claim_stats.get("pending_claims", 0),
            hedge_analytics=stats.get("hedge_analytics"),
            realized_pnl=realized_pnl,
            paired_windows=stats.get("paired_windows", 0),
            one_sided_fills=hedge_summary.get("one_sided_fills", 0),
        )
        if result:
            self.logger.info("  CC FINAL PUSH OK | Run #{} stopped successfully".format(self._cc_run_id))
        else:
            self.logger.warning("  CC FINAL PUSH FAILED | stop_run returned None for run #{}".format(self._cc_run_id))

    def run(self):
        """Override run() to add CC run lifecycle and session logging."""
        # Start CC run
        if cc._is_ready() and self._cc_config_id:
            mode = "live" if not self.config.dry_run else "dry_run"
            self._cc_run_id = cc.start_run(
                config_id=self._cc_config_id,
                mode=mode,
                bankroll=self.config.kelly_bankroll,
            )
            if self._cc_run_id:
                self.logger.info(f"Command Center run #{self._cc_run_id} started")
                # Setup session logging
                setup_session_logging(self._cc_run_id, self.logger)
            else:
                self.logger.warning("Failed to start Command Center run")

        # Print banner
        self.logger.info("\n" + "=" * 70)
        self.logger.info("  POLYMAKER BOT v15.1 + Command Center")
        self.logger.info("=" * 70)
        self.logger.info("  Mode: {}".format("DRY RUN (simulated)" if self.config.dry_run else "LIVE"))
        self.logger.info("  Assets 15m: {}".format(", ".join(a.upper() for a in self.config.assets_15m)))
        self.logger.info("  Assets 5m:  {}".format(", ".join(a.upper() for a in self.config.assets_5m)))
        self.logger.info("  Bankroll:   ${:.0f}".format(self.config.kelly_bankroll))
        self.logger.info("  Kelly:      {:.0%} fraction".format(self.config.kelly_fraction))
        self.logger.info("  MM spread:  {:.3f} | Size: ${:.0f}/side | Per-market: ${:.0f}".format(
            self.config.mm_base_spread, self.config.mm_order_size,
            self.config.max_position_per_market))
        self.logger.info("  Strategies: MM={} | Sniper={} | Arb={} | Contrarian={}".format(
            "ON" if self.config.mm_enabled else "OFF",
            "ON" if self.config.sniper_enabled else "OFF",
            "ON" if self.config.arb_enabled else "OFF",
            "ON" if self.config.contrarian_enabled else "OFF"))
        self.logger.info("  Auto-merge: {} | Pair-IMM: {} | Blind-redeem: {} | "
                         "Hedge: ${:.3f} | CB: {:.1%}".format(
            "ON" if self.config.auto_merge_enabled else "OFF",
            "ON" if self.config.immediate_pair_completion else "OFF",
            "ON" if self.config.blind_redeem_enabled else "OFF",
            self.config.hedge_max_loss_per_share,
            self.config.vol_circuit_breaker))
        self.logger.info("  Equal shares: {} | Scan ahead: {} windows | Horizon: {:.0f}s".format(
            "ON" if self.config.pair_sizing_equal_shares else "OFF",
            self.config.scan_windows_ahead,
            self.config.max_order_horizon))
        if self._cc_run_id:
            self.logger.info(f"  Command Center: run #{self._cc_run_id}")
        self.logger.info("=" * 70)

        # ── Start WebSocket Manager ──────────────────────────────────────
        if self.ws_manager:
            ws_started = self.ws_manager.start()
            if ws_started:
                self.logger.info("  WebSocket manager started (background thread)")
                # Subscribe to RTDS for all configured assets
                all_assets = list(set(self.config.assets_15m + self.config.assets_5m))
                self.ws_manager.subscribe_rtds_all([a.lower() for a in all_assets])
                # Subscribe to user order updates if authenticated
                # Note: subscribe_user_orders needs condition_ids, called after market discovery
                # For now, just log that user channel will be subscribed later
                if self.config.api_key:
                    self.logger.info("  User channel: will subscribe after market discovery")
                # Give WebSocket connections a moment to establish
                time.sleep(2)
                ws_status = self.ws_manager.get_connection_summary()
                self.logger.info(f"  WebSocket status: {ws_status}")
                # Start WS fill detector (Phase 2)
                if hasattr(self, 'ws_fill_detector') and self.ws_fill_detector:
                    self.ws_fill_detector.start()
                    self.logger.info("  WS fill detector started — real-time fill detection active")
            else:
                self.logger.warning("  WebSocket manager failed to start. Using REST-only mode.")
                self.ws_manager = None
                if hasattr(self, 'ws_fill_detector'):
                    self.ws_fill_detector = None

        # ── Live Wallet Check at Startup ─────────────────────────────────
        # CC sets kellyBankroll as the session budget ceiling.
        # We read the actual wallet to: (1) validate we have enough, 
        # (2) record starting balance for real P&L tracking.
        self._starting_wallet_balance = None
        self._wallet_read_failures = 0
        self._max_wallet_failures = 10  # alert after this many consecutive failures
        
        if self.balance_checker and not self.config.dry_run:
            wallet_bal = None
            for _attempt in range(5):
                self.balance_checker._cache_time = 0
                wallet_bal = self.balance_checker.get_balance()
                if wallet_bal is not None:
                    break
                self.logger.info("  Wallet read attempt {} failed, retrying...".format(_attempt + 1))
                time.sleep(3)
            
            if wallet_bal is not None and wallet_bal > 0:
                self._starting_wallet_balance = wallet_bal
                self.engine.starting_wallet_balance = wallet_bal
                self.logger.info("  Wallet balance: ${:.2f} (USDC.e on Polygon)".format(wallet_bal))
                
                # Validate: wallet must have enough for the session
                session_budget = self.config.max_total_exposure  # CC sessionBudget
                if wallet_bal < session_budget:
                    self.logger.warning(
                        "  ⚠️  INSUFFICIENT FUNDS: wallet ${:.2f} < sessionBudget ${:.0f}. "
                        "Bot may not be able to deploy full budget.".format(
                            wallet_bal, session_budget))
                
                cc_bankroll = self.config.kelly_bankroll
                if wallet_bal < cc_bankroll:
                    self.logger.warning(
                        "  ⚠️  WALLET < BANKROLL: wallet ${:.2f} < kellyBankroll ${:.0f}. "
                        "Max loss protection uses CC bankroll (${:.0f}), not wallet.".format(
                            wallet_bal, cc_bankroll, cc_bankroll))
                else:
                    self.logger.info("  ✓ Wallet ${:.2f} >= bankroll ${:.0f} — funds sufficient".format(
                        wallet_bal, cc_bankroll))
            else:
                self.logger.warning(
                    "  ⚠️  WALLET READ FAILED at startup. Real P&L tracking disabled. "
                    "Max loss will use simulated P&L only. Check PROXY_WALLET and POLYGON_RPC_URL.")
        elif self.config.dry_run:
            self.logger.info("  Wallet: DRY RUN mode — no live wallet monitoring")
        else:
            self.logger.warning("  ⚠️  No balance checker available. Live wallet monitoring disabled.")

        # V15.1-1: Exposure limits
        # If CC set sessionBudget, max_total_exposure is already set by apply_cc_config.
        # If CC set budgetPerMarket, max_position_per_market is already set.
        # Only apply v15 defaults if CC didn't set them.
        # V15.1-18: max_total_exposure = bankroll (the user sets bankroll to what they
        # want to deploy). No more 80% default — bankroll IS the deployment cap.
        if not self._cc_config or not self._cc_config.get("sessionBudget"):
            self.config.max_total_exposure = self.config.kelly_bankroll
        if not self._cc_config or not self._cc_config.get("budgetPerMarket"):
            self.config.max_position_per_market = min(
                self.config.max_position_per_market,
                self.config.max_total_exposure * 0.45)
        self.logger.info("  Bankroll: ${:.0f} | max_total_exposure=${:.0f} | per_market=${:.0f} | order_size=${:.0f}/side".format(
            self.config.kelly_bankroll, self.config.max_total_exposure,
            self.config.max_position_per_market, self.config.mm_order_size))
        self.logger.info("  Risk controls: max_loss={:.0%} (${:.0f}) | cooldown={:.0f}s".format(
            self.config.hard_loss_stop_pct,
            self.config.hard_loss_stop_pct * self.config.kelly_bankroll,
            self.config.hard_loss_cooloff))

        self.running = True
        cycle = 0
        # V15.1-22: Track which windows had priority last cycle for re-prioritization
        self._prev_priority_wids = set()

        while self.running:
            cycle += 1
            cc.increment_cycle()
            try:
                # V15.1-30: Fetch pauseOrders EVERY cycle for instant response.
                # Full config refresh still every 5 cycles (heavier payload).
                if cc._is_ready():
                    fresh_config = cc.get_config()
                    if fresh_config:
                        # Pause is checked every cycle for instant response
                        was_paused = getattr(self.config, 'pause_orders', False)
                        self.config.pause_orders = bool(fresh_config.get("pauseOrders", False))
                        # Log state transitions immediately
                        if self.config.pause_orders and not was_paused:
                            self.logger.info("  \u26a0\ufe0f  PAUSED from CC \u2014 no new orders will be placed")
                        elif not self.config.pause_orders and was_paused:
                            self.logger.info("  \u2705  RESUMED from CC \u2014 order placement re-enabled")
                        # Full config refresh every 5 cycles
                        if cycle % 5 == 0:
                            self.config.hedge_completion_enabled = bool(fresh_config.get("hedgeEnabled", True))
                            self.config.hedge_min_profit_per_share = float(fresh_config.get("hedgeMinProfit", 0.005))
                            self.config.hedge_tiers = sorted([
                                (float(fresh_config.get("hedgeTier1Pct", 67)), float(fresh_config.get("hedgeTier1Cost", 1.03))),
                                (float(fresh_config.get("hedgeTier2Pct", 33)), float(fresh_config.get("hedgeTier2Cost", 1.05))),
                                (float(fresh_config.get("hedgeTier3Pct", 13)), float(fresh_config.get("hedgeTier3Cost", 1.08))),
                            ], key=lambda t: t[0], reverse=True)
                            # V15.2-T4: Last Resort Sell config
                            self.config.hedge_t4_enabled = bool(fresh_config.get("hedgeT4Enabled", True))
                            self.config.hedge_t4_sell_pct = float(fresh_config.get("hedgeTier4Pct", 33.0))
                            self.config.hedge_t4_max_loss = float(fresh_config.get("hedgeTier4MaxLoss", 0.30))
                            # V15.2: Max order horizon
                            moh = fresh_config.get("maxOrderHorizon")
                            if moh is not None and moh > 0:
                                self.config.max_order_horizon = float(moh)
                            self.config.momentum_exit_enabled = bool(fresh_config.get("momentumExitEnabled", True))
                            self.config.momentum_exit_threshold = float(fresh_config.get("momentumExitThreshold", 0.03))
                            self.config.momentum_exit_max_wait_secs = float(fresh_config.get("momentumExitMaxWait", 120.0))
                            self.config.momentum_gate_threshold = float(fresh_config.get("momentumGate", 0.01))
                            self.config.momentum_gate_max_consec = int(fresh_config.get("momentumGateMaxConsec", 3))
                            self.config.min_book_depth = float(fresh_config.get("minBookDepth", 5.0))
                            self.config.max_spread_asymmetry = float(fresh_config.get("maxSpreadAsymmetry", 0.02))

                self.engine.check_daily_reset()
                self.engine.sync_exchange_balance()
                self.engine.reset_cycle_counters()
                stats = self.engine.get_stats()

                wallet_str = ""
                pnl_str = ""
                self._current_wallet_balance = None
                self._real_pnl = None
                if self.balance_checker and not self.config.dry_run and self.running:
                    bal = self.balance_checker.get_balance()
                    if bal is not None:
                        self._current_wallet_balance = bal
                        self._wallet_read_failures = 0
                        wallet_str = " | W:${:.0f}".format(bal)
                        
                        # V15.1-16: Real P&L = (wallet + live_position_value) - starting_wallet
                        # Uses actual market prices for positions, not cost basis.
                        # Wallet-only P&L is misleading: buying tokens reduces wallet
                        # but creates positions with value. Must include both.
                        if self._starting_wallet_balance is not None:
                            wallet_change = bal - self._starting_wallet_balance
                            pos_value = self.engine.get_position_value()
                            pos_cost = getattr(self.engine, 'capital_in_positions', 0)
                            # V15.1-P5: Portfolio PnL = wallet change + position value
                            # (informational only, NOT used for loss stop)
                            self._portfolio_pnl = wallet_change + pos_value
                            self._wallet_only_pnl = wallet_change
                            self._position_value = pos_value
                            # V15.1-P5: Realized PnL = returns from merges/claims - cost
                            # This is the ONLY PnL used for loss stop decisions.
                            realized = stats.get("session_realized_pnl", 0)
                            self._real_pnl = realized
                            pnl_str = " | P&L:${:+.2f}r/${:+.2f}p (spent:${:.0f} pos:${:.2f})".format(
                                realized, self._portfolio_pnl,
                                self.engine.session_total_spent, pos_value)
                    else:
                        self._wallet_read_failures += 1
                        if self._wallet_read_failures >= self._max_wallet_failures:
                            if self._wallet_read_failures == self._max_wallet_failures:
                                self.logger.warning(
                                    "  ⚠️  WALLET READ FAILED {} consecutive times. "
                                    "Real P&L tracking unreliable. Check RPC connection.".format(
                                        self._wallet_read_failures))
                        # V15.1-20: Use wallet_delta as primary P&L
                        wallet_delta = stats.get("wallet_delta")
                        if wallet_delta is not None:
                            pnl_str = " | W\u0394:${:+.2f}".format(wallet_delta)
                            held_val = stats.get("held_value", 0)
                            if held_val > 0:
                                pnl_str += " +${:.0f}held".format(held_val)

                cs = self.claim_manager.get_claim_stats()
                claim_str = ""
                if cs["pending_claims"] > 0 or cs["claimed_total"] > 0:
                    claim_str = " | Cl:{}ok/{}p".format(cs["claimed_total"], cs["pending_claims"])

                hedge_str = ""
                if stats["hedges_completed"] > 0 or stats["hedges_skipped"] > 0:
                    hedge_str = " | H:{}/{}".format(
                        stats["hedges_completed"], stats["hedges_skipped"])

                merge_stats = self.auto_merger.get_stats()
                merge_str = ""
                if merge_stats["merges_completed"] > 0:
                    merge_str = " | M:{}/${:.0f}".format(
                        merge_stats["merges_completed"], merge_stats["total_merged_usd"])

                churn_str = ""
                if cycle % 10 == 0:
                    cs2 = self.churn_manager.get_stats()
                    if cs2["suppressed"] > 0:
                        churn_str = " | Churn:-{:.0f}%".format(cs2["reduction_pct"])

                # WebSocket status (every 10 cycles)
                ws_str = ""
                if self.ws_manager and cycle % 10 == 1:
                    ws_str = " | WS:{}".format(self.ws_manager.get_connection_summary())
                    if hasattr(self, 'ws_fill_detector') and self.ws_fill_detector:
                        fd_stats = self.ws_fill_detector.get_stats()
                        fill_mode = "WS" if not fd_stats["fallback_mode"] else "REST"
                        ws_str += " | Fills:{}(ws:{},rest:{},plc:{},cnl:{})".format(
                            fill_mode, fd_stats["ws_fills_session"],
                            fd_stats["rest_fallback_count"],
                            fd_stats.get("ws_placements_confirmed", 0),
                            fd_stats.get("ws_cancellations_detected", 0))
                    # Orderbook stats
                    if hasattr(self.book_reader, 'get_stats'):
                        bk_stats = self.book_reader.get_stats()
                        ws_str += " | Book:ws{}/bba{}/rest{}".format(
                            bk_stats.get("ws_book_hits", bk_stats.get("ws_hits", 0)),
                            bk_stats.get("ws_bba_hits", 0),
                            bk_stats.get("fallbacks", 0))

                self.logger.info(
                    "\n{}\n  C{} | {} | Ord:{} | Exp:${:.0f} | Avail:${:.0f} | MaxExp:${:.0f}"
                    "{}{}{}{}{}{}{}\n{}".format(
                        "_" * 60, cycle,
                        datetime.now(timezone.utc).strftime("%H:%M:%S"),
                        stats["active_orders"], stats["total_exposure"],
                        stats["available_capital"],
                        self.config.max_total_exposure,
                        wallet_str, pnl_str, claim_str, hedge_str,
                        merge_str, churn_str, ws_str,
                        "_" * 60))

                self.price_feed.update()
                all_assets = list(set(self.config.assets_15m + self.config.assets_5m))
                for asset in all_assets:
                    price = self.price_feed.get_current_price(asset)
                    if price:
                        self.vol_tracker.update_price(asset, price)

                self.book_reader.invalidate_cache()
                markets = self.market_discovery.discover()
                ws_token_ids = []
                ws_condition_ids = []

                for market in markets:
                    cid = market.get("condition_id", "")
                    if cid:
                        self.window_conditions[market["window_id"]] = cid
                        self.vol_tracker.register_condition(cid, market["asset"])
                    self.vol_tracker.register_token(market["token_up"], market["asset"])
                    self.vol_tracker.register_token(market["token_down"], market["asset"])
                    self.engine.register_window_metadata(market)
                    # Phase 3: Populate _is_up_token_cache BEFORE check_fills_ws
                    # so that recovered fills can correctly classify UP vs DOWN.
                    self.engine._is_up_token_cache[market["token_up"]] = True
                    self.engine._is_up_token_cache[market["token_down"]] = False
                    # Collect token IDs for batch WS subscription
                    if self.ws_manager:
                        ws_token_ids.append(market["token_up"])
                        ws_token_ids.append(market["token_down"])
                        ws_condition_ids.append(market["condition_id"])

                # Batch subscribe to WS market + user channels for discovered markets
                if self.ws_manager and ws_token_ids:
                    self.ws_manager.subscribe_market(ws_token_ids)
                    if self.config.api_key and ws_condition_ids:
                        self.ws_manager.subscribe_user_orders(ws_condition_ids)

                self._compute_market_edges(markets)
                self._resolve_expired_windows(markets)

                # V15.1-11: CRITICAL — check_fills() MUST run BEFORE cleanup/prune.
                # cleanup_expired_windows() and prune_stale_orders() delete orders
                # from active_orders. If a filled order is deleted first, check_fills()
                # will never detect it, causing capital_in_positions to stay at 0
                # and triggering false loss-stop warnings.
                #
                 # Phase 4: WS-primary fill detection with periodic REST reconciliation.
                # WSFillDetector processes real-time fill events AND order lifecycle
                # events (PLACEMENT, CANCELLATION) from the user channel.
                # REST reconciliation runs every 10 cycles as a safety net.
                # If WS is completely unhealthy, fall back to REST every cycle.
                if not self.config.dry_run:
                    live_fills = 0
                    fill_source = "REST"
                    # Try WS fill detection first
                    if hasattr(self, 'ws_fill_detector') and self.ws_fill_detector and not self.ws_fill_detector.should_fallback_to_rest():
                        live_fills = self.ws_fill_detector.check_fills_ws()
                        fill_source = "WS"
                        # Phase 4: Periodic REST reconciliation (every 10 cycles)
                        reconciled = self.ws_fill_detector.rest_reconcile()
                        if reconciled:
                            live_fills += reconciled
                            fill_source = "WS+RECONCILE"
                    else:
                        # Full fallback to REST polling (WS unhealthy)
                        live_fills = self.engine.check_fills()
                        fill_source = "REST"
                        if hasattr(self, 'ws_fill_detector') and self.ws_fill_detector:
                            self.ws_fill_detector.record_rest_fallback()

                    if live_fills:
                        self.logger.info("  {} orders filled [{}]".format(live_fills, fill_source))
                        for wid in self.engine.window_fill_sides:
                            self.churn_manager.force_allow(wid)
                        imm_completed = self._process_immediate_pair_completions()
                        if imm_completed:
                            self.logger.info("  {} immediate pair completions".format(imm_completed))
                    hedges = self.engine.process_hedge_completions(
                        self.book_reader, self.vol_tracker)
                    if hedges:
                        self.logger.info("  {} hedges completed".format(hedges))
                    # V15.1-14: Momentum exit — sell one-sided fills if price rises >X%
                    mom_exits = self.engine.process_momentum_exits(self.book_reader)
                    if mom_exits:
                        self.logger.info("  {} momentum exits".format(mom_exits))
                        if self.balance_checker:
                            self.balance_checker._cache_time = 0

                # V15.1-17: Merge BEFORE cleanup so _market_cache still has
                # expired window data (token_up/token_down needed for matching).
                if not self.config.dry_run:
                    merged = self.auto_merger.check_and_merge_all(
                        self.engine._market_cache, self.engine.token_holdings)
                    if merged:
                        self.logger.info("  Auto-merged {} positions | ${:.2f} returned".format(
                            merged, self.auto_merger.total_merged_usd))
                        if self.balance_checker:
                            self.balance_checker._cache_time = 0
                self.engine.cleanup_expired_windows(markets, self.churn_manager)
                # V15.1-P5: Clean up MM strategy tracking for expired windows
                for wid in list(self.mm_strategy._window_first_placed.keys()):
                    if wid not in self.engine.known_windows:
                        self.mm_strategy.cleanup_window(wid)
                self.engine.prune_stale_orders()
                self.engine.purge_recently_cancelled()
                # Phase 4: Purge stale WS order lifecycle tracking data
                if hasattr(self, 'ws_fill_detector') and self.ws_fill_detector:
                    self.ws_fill_detector.purge_ws_cancelled_orders()
                if not self.config.dry_run:
                    self.engine.reconcile_capital_from_wallet()
                    self._schedule_live_claims()
                    claimed = self.claim_manager.process_claims()
                    if claimed > 0:
                        self.logger.info("  Auto-claimed {} positions".format(claimed))
                        if self.balance_checker:
                            self.balance_checker._cache_time = 0
                    exits = self.claim_manager.execute_pre_exits(
                        markets, self.price_feed, self.book_reader)
                    if exits > 0:
                        self.logger.info("  Pre-exit: {} sells placed".format(exits))

                for market in markets:
                    if self.sim_engine:
                        price = self.price_feed.get_current_price(market["asset"])
                        if price:
                            self.sim_engine.record_window_start_price(
                                market["window_id"], market["asset"], price)

                if self.sim_engine:
                    fills = self.sim_engine.simulate_fills(self.book_reader, markets)
                    if fills > 0:
                        self.logger.info("  Simulated {} fills".format(fills))
                    merged = self.auto_merger.check_and_merge_all(
                        self.engine._market_cache, self.engine.token_holdings)
                    if merged:
                        self.logger.info("  Sim-merged {} positions".format(merged))

                # V15.1-24: Report active markets to CC for the Markets page
                if cycle % 10 == 0 and cc._is_ready():
                    try:
                        market_report = []
                        for wid, mkt in self.engine._market_cache.items():
                            market_report.append({
                                "tokenId": mkt.get("token_up", ""),
                                "tokenIdDown": mkt.get("token_down", ""),
                                "asset": mkt.get("asset", "?"),
                                "window": mkt.get("window_duration", "15m"),
                                "windowId": wid,
                                "conditionId": mkt.get("condition_id", ""),
                                "expiresAt": str(mkt.get("expiration", "")),
                            })
                        if market_report:
                            cc.report_markets(market_report)
                    except Exception as e:
                        self.logger.debug("  Market report failed: {}".format(e))

                if cycle % 5 == 0:
                    # V15.1-P4: Sync live on-chain positions to token_holdings.
                    # This ensures PnL calc uses real position data, not just
                    # fill-recorded data which can miss fills (race conditions).
                    # query_live_positions also feeds the merge scan with accurate data.
                    if not self.config.dry_run and self.auto_merger.w3:
                        self.auto_merger.query_live_positions(self.engine._market_cache)
                    self.merge_detector.check_merges(
                        self.engine.token_holdings, self.engine._market_cache)

                # ── Loss Protection: Realized PnL Only ────────────────────
                # V15.1-P5: Loss stop uses REALIZED PnL only (merges + claims).
                # Unrealized position value fluctuates as market moves, causing
                # false loss stops when one side fills before the other.
                # The session exposure limit controls max capital at risk.
                # Realized PnL only turns negative when positions are resolved
                # at a loss (e.g., one-sided fill after resolution).
                realized_pnl = self.engine.session_realized_returns - self.engine.session_realized_cost
                self._real_pnl = realized_pnl
                trading_halted = False
                now = time.time()
                loss_limit = -self.config.hard_loss_stop_pct * self.config.kelly_bankroll
                
                if now < self._loss_stop_until:
                    if cycle % 10 == 1:
                        self.logger.info("  LOSS COOLOFF -- {}s remaining".format(
                            int(self._loss_stop_until - now)))
                    trading_halted = True
                else:
                    # Source 1: Realized PnL from merges/claims (primary)
                    if realized_pnl < loss_limit:
                        self.logger.warning(
                            "  *** LOSS STOP (REALIZED) *** realizedP&L: ${:.2f} < limit ${:.2f} "
                            "(maxLoss={:.0%} x bankroll=${:.0f}). "
                            "Returns: ${:.2f}, Cost: ${:.2f}. "
                            "Halting for {:.0f}s.".format(
                                realized_pnl, loss_limit,
                                self.config.hard_loss_stop_pct,
                                self.config.kelly_bankroll,
                                self.engine.session_realized_returns,
                                self.engine.session_realized_cost,
                                self.config.hard_loss_cooloff))
                        self._loss_stop_until = now + self.config.hard_loss_cooloff
                        trading_halted = True
                    
                    # Source 2: Simulated P&L (fallback for dry_run mode)
                    elif self.sim_engine and self.config.dry_run:
                        s = self.sim_engine.get_summary()
                        if s["realized_pnl"] < loss_limit:
                            self.logger.warning(
                                "  *** LOSS STOP (SIM) *** simP&L: ${:.2f} < limit ${:.2f} "
                                "(maxLoss={:.0%} x bankroll=${:.0f}). "
                                "Halting for {:.0f}s. (dry_run mode)".format(
                                    s["realized_pnl"], loss_limit,
                                    self.config.hard_loss_stop_pct,
                                    self.config.kelly_bankroll,
                                    self.config.hard_loss_cooloff))
                            self._loss_stop_until = now + self.config.hard_loss_cooloff
                            trading_halted = True

                # V15.1-30: CC pause control — stop new order placement only.
                # Existing orders are LEFT on the book so pending pairs can
                # complete naturally (cancelling them would orphan filled sides).
                if getattr(self.config, 'pause_orders', False):
                    trading_halted = True

                # ═══════════════════════════════════════════════════════════
                # V15.1-22: PRIORITIZED MARKET SELECTION
                # 1. Filter to closest N sessions per timeframe
                # 2. Remove filled/closed windows
                # 3. Score and rank all candidates
                # 4. Pick top max_concurrent_windows by score
                # 5. Cancel orders on windows that lost priority
                # 6. Execute strategies only on priority windows
                # ═══════════════════════════════════════════════════════════
                filled_wids = self.engine.filled_windows
                closed_wids = self.engine.closed_windows

                # Step 1: Group markets by timeframe and pick closest N sessions
                tf_sessions = {}  # {timeframe: {end_time: [markets]}}
                for market in markets:
                    tf = market.get("timeframe", "")
                    end_t = market.get("end_time", 0)
                    if tf not in tf_sessions:
                        tf_sessions[tf] = {}
                    if end_t not in tf_sessions[tf]:
                        tf_sessions[tf][end_t] = []
                    tf_sessions[tf][end_t].append(market)

                session_limited_markets = []
                max_sessions = self.config.max_order_sessions
                for tf, sessions in tf_sessions.items():
                    sorted_times = sorted(sessions.keys())[:max_sessions]
                    for t in sorted_times:
                        session_limited_markets.extend(sessions[t])
                    if len(sorted_times) < len(sessions):
                        skipped = len(sessions) - len(sorted_times)
                        self.logger.debug(
                            "  SESSION LIMIT | {} | {} sessions -> {} (skipped {})".format(
                                tf, len(sessions), len(sorted_times), skipped))

                # Step 2: Basic eligibility filter
                eligible_markets = []
                active_condition_ids = set()
                # Track condition_ids from FILLED windows (positions we hold)
                for fwid in filled_wids:
                    cid = self.window_conditions.get(fwid, "")
                    if cid:
                        active_condition_ids.add(cid)
                for cwid in closed_wids:
                    cid = self.window_conditions.get(cwid, "")
                    if cid:
                        active_condition_ids.add(cid)

                for market in session_limited_markets:
                    wid = market["window_id"]
                    # Skip filled/closed windows
                    if wid in filled_wids or wid in closed_wids:
                        continue
                    # Skip advance windows if disabled
                    if market.get("is_advance", False) and not self.config.trade_advance_windows:
                        continue
                    # Edge filter
                    edge = market.get("edge", 0)
                    maker_edge = market.get("maker_edge", edge)
                    if (edge < self.config.min_pair_edge
                            and maker_edge < self.config.pair_min_profit):
                        continue
                    # Time horizon filter
                    if market.get("time_left", 0) > self.config.max_order_horizon:
                        continue
                    # Condition_id dedup: skip if same underlying already has
                    # a filled/closed position
                    mkt_cid = market.get("condition_id", "")
                    if mkt_cid and mkt_cid in active_condition_ids:
                        self.logger.debug(
                            "  DEDUP SKIP | {} | condition {} already has position".format(
                                wid, mkt_cid[:16]))
                        continue
                    eligible_markets.append(market)

                # Step 3: Score all eligible markets
                eligible_markets = self._score_and_sort_markets(eligible_markets)

                # Step 4: Pick top N markets (max_concurrent_windows)
                # Dedup by condition_id within the priority list
                max_concurrent = self.config.max_concurrent_windows
                priority_markets = []
                priority_cids = set()
                for market in eligible_markets:
                    if len(priority_markets) >= max_concurrent:
                        break
                    mkt_cid = market.get("condition_id", "")
                    if mkt_cid and mkt_cid in priority_cids:
                        self.logger.debug(
                            "  DEDUP PRIORITY | {} | condition {} already in priority list".format(
                                market["window_id"], mkt_cid[:16]))
                        continue
                    priority_markets.append(market)
                    if mkt_cid:
                        priority_cids.add(mkt_cid)

                priority_wids = {m["window_id"] for m in priority_markets}

                # Step 5: Cancel orders on windows that LOST priority
                # Only if dynamic_reprioritize is enabled
                if self.config.dynamic_reprioritize:
                    current_order_wids = set(self.engine.window_exposure.keys())
                    demoted_wids = current_order_wids - priority_wids - filled_wids - closed_wids
                    for dwid in demoted_wids:
                        # Don't cancel if the window has fills (it's being held)
                        if dwid in self.engine.window_fill_sides:
                            continue
                        self.logger.info(
                            "  REPRIORITIZE | {} | Lost priority, cancelling orders".format(dwid))
                        self.engine.cancel_window_orders(dwid)
                        # Clean up MM strategy tracking so it can re-place if promoted again
                        self.mm_strategy.cleanup_window(dwid)

                # Log priority changes
                if priority_wids != self._prev_priority_wids and cycle > 1:
                    new_wids = priority_wids - self._prev_priority_wids
                    lost_wids = self._prev_priority_wids - priority_wids
                    if new_wids or lost_wids:
                        self.logger.info(
                            "  PRIORITY CHANGE | +{} -{} | Active: {} | Top: {}".format(
                                len(new_wids), len(lost_wids),
                                ", ".join(sorted(priority_wids)),
                                ", ".join(
                                    "{} ({:.3f})".format(m["window_id"], m.get("_sort_score", 0))
                                    for m in priority_markets[:4])))
                self._prev_priority_wids = priority_wids

                if cycle % self.config.edge_map_interval == 1:
                    self._print_edge_map(markets, priority_markets)

                # Step 6: Execute strategies ONLY on priority markets
                for market in priority_markets:
                    try:
                        if trading_halted:
                            continue
                        wid = market["window_id"]
                        # Dynamic filled window guard
                        if wid in self.engine.filled_windows:
                            continue
                        # Dynamic concurrent check (belt-and-suspenders)
                        current_active = set(self.engine.window_exposure.keys())
                        if (len(current_active) >= max_concurrent
                                and wid not in current_active):
                            self.logger.debug(
                                "  CONCURRENT SKIP | {} | {}/{} windows active".format(
                                    wid, len(current_active), max_concurrent))
                            continue
                        if self.config.mm_enabled:
                            self.mm_strategy.execute(market)
                        if self.config.arb_enabled:
                            self.arb.execute(market)
                        if self.config.sniper_enabled:
                            self.sniper.execute(market)
                        if self.config.contrarian_enabled:
                            self.contrarian.execute(market)
                    except Exception as e:
                        self.logger.error("  Strategy error on {}: {}".format(
                            market["slug"], e))

                if self.sim_engine:
                    s = self.sim_engine.get_summary()
                    self.logger.info(
                        "  Sim: ${:,.2f} | P&L: ${:+,.2f} | Fills: {} | Open: {}".format(
                            s["current_bankroll"], s["realized_pnl"],
                            s["total_fills"], s["open_positions"]))

                if cycle % self.config.summary_interval == 0:
                    self._print_summary("(Cycle {})".format(cycle))
                    self._print_v15_1_summary()

                # Push metrics to Command Center
                self._push_metrics()

            except KeyboardInterrupt:
                self.logger.info("\nKeyboardInterrupt received. Shutting down...")
                self.running = False
                self.engine.cancel_all()
                self._print_summary("FINAL")
                self._push_final_metrics("stopped")
                break
            except Exception as e:
                self.logger.error("  Cycle error: {}".format(e))

            if self.running:
                time.sleep(self.config.cycle_interval)


# -----------------------------------------------------------------
# Entry Point
# -----------------------------------------------------------------

if __name__ == "__main__":
    try:
        bot = PolyMakerBot()
        bot.run()
    except KeyboardInterrupt:
        print("\nBot stopped by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\nFatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
