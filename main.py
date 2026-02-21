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
    Only overrides values that are explicitly set in the CC config.
    """
    # Target assets
    target_assets = cc_config.get("targetAssets", [])
    if target_assets:
        assets_lower = [a.lower() for a in target_assets]
        config.assets_15m = assets_lower
        config.assets_5m = assets_lower

    # Window durations
    window_durations = cc_config.get("windowDurations", [])
    if window_durations:
        config.timeframes = window_durations

    # Bankroll
    bankroll = cc_config.get("kellyBankroll")
    if bankroll and bankroll > 0:
        config.kelly_bankroll = float(bankroll)

    # Market Making parameters
    mm_order_size = cc_config.get("mmOrderSize")
    if mm_order_size and mm_order_size > 0:
        config.mm_order_size = float(mm_order_size)

    mm_spread = cc_config.get("mmSpread")
    if mm_spread and mm_spread > 0:
        config.mm_base_spread = float(mm_spread)

    # Risk parameters
    max_concurrent = cc_config.get("maxConcurrentWindows")
    if max_concurrent and max_concurrent > 0:
        config.max_concurrent_windows = int(max_concurrent)

    stop_loss = cc_config.get("stopLossThreshold")
    if stop_loss is not None:
        config.hard_loss_stop_pct = abs(float(stop_loss)) / 100.0 if abs(stop_loss) > 1 else abs(float(stop_loss))

    # Dry run mode
    mode = cc_config.get("mode", "dry_run")
    config.dry_run = (mode != "live")

    return config


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
                           f"dry_run={self.config.dry_run}")

    def _shutdown(self, signum, frame):
        """Override shutdown to also stop CC run."""
        self.logger.info("\nShutdown signal received. Cancelling all orders...")
        self.running = False
        self.engine.cancel_all()
        self._print_summary("FINAL")
        self._print_claim_summary()
        self._print_v15_1_summary()

        # Stop CC run with final metrics
        self._push_final_metrics("stopped")

        self.logger.info("All orders cancelled. Exiting.")
        sys.exit(0)

    def _push_metrics(self):
        """Push current metrics to Command Center periodically."""
        now = time.time()
        if now - self._last_metrics_push < self._metrics_interval:
            return
        self._last_metrics_push = now

        if not self._cc_run_id:
            return

        stats = self.engine.get_stats()

        # Calculate PnL from sim engine or live
        total_pnl = 0.0
        if self.sim_engine:
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

        cc.update_run(
            total_cycles=cc.cycle_count,
            total_orders=stats.get("total_placed", 0),
            total_fills=stats.get("total_placed", 0),
            total_pnl=total_pnl,
            peak_pnl=self._peak_pnl,
            max_drawdown=self._max_drawdown,
            ending_bankroll=self.config.kelly_bankroll + total_pnl,
            max_capital=stats.get("total_exposure", 0),
        )

    def _push_final_metrics(self, status="completed"):
        """Push final metrics when run ends."""
        if not self._cc_run_id:
            return

        stats = self.engine.get_stats()
        total_pnl = 0.0
        if self.sim_engine:
            s = self.sim_engine.get_summary()
            total_pnl = s.get("realized_pnl", 0)
        else:
            total_pnl = stats.get("live_pnl", 0) or 0

        cc.stop_run(
            status=status,
            total_cycles=cc.cycle_count,
            total_orders=stats.get("total_placed", 0),
            total_fills=stats.get("total_placed", 0),
            total_pnl=total_pnl,
            peak_pnl=self._peak_pnl,
            max_drawdown=self._max_drawdown,
            ending_bankroll=self.config.kelly_bankroll + total_pnl,
            max_capital=stats.get("total_exposure", 0),
        )

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
        self.logger.info("  MM spread:  {:.3f} | Size: ${:.0f}".format(
            self.config.mm_base_spread, self.config.mm_order_size))
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

        # V15.1-5: Bankroll auto-detect
        if self.config.auto_detect_bankroll and self.balance_checker and not self.config.dry_run:
            wallet_bal = None
            for _attempt in range(5):
                self.balance_checker._cache_time = 0
                wallet_bal = self.balance_checker.get_balance()
                if wallet_bal is not None:
                    break
                self.logger.info("  Bankroll detect attempt {} failed, retrying...".format(_attempt + 1))
                time.sleep(3)
            if wallet_bal is not None and wallet_bal > 0:
                old_bankroll = self.config.kelly_bankroll
                self.config.kelly_bankroll = wallet_bal
                self.engine.starting_wallet_balance = wallet_bal
                self.logger.info("  Bankroll: ${:.2f} (was ${:.2f})".format(
                    wallet_bal, old_bankroll))
            else:
                self.logger.warning("  Bankroll auto-detect FAILED. Using KELLY_BANKROLL=${:.0f}".format(
                    self.config.kelly_bankroll))

        # V15.1-1: Scale exposure with bankroll
        self.config.max_total_exposure = self.config.kelly_bankroll * 0.80
        self.config.max_position_per_market = min(
            self.config.max_position_per_market,
            self.config.max_total_exposure * 0.45)
        self.logger.info("  Exposure limits: max_total=${:.0f} | max_per_market=${:.0f} | bankroll=${:.0f}".format(
            self.config.max_total_exposure, self.config.max_position_per_market,
            self.config.kelly_bankroll))

        self.running = True
        cycle = 0

        while self.running:
            cycle += 1
            cc.increment_cycle()
            try:
                self.engine.check_daily_reset()
                self.engine.sync_exchange_balance()
                self.engine.reset_cycle_counters()
                stats = self.engine.get_stats()

                wallet_str = ""
                pnl_str = ""
                if self.balance_checker and not self.config.dry_run:
                    bal = self.balance_checker.get_balance()
                    if bal is not None:
                        wallet_str = " | W:${:.0f}".format(bal)
                    live_pnl = stats.get("live_pnl")
                    if live_pnl is not None:
                        pnl_str = " | P&L:${:+.2f}".format(live_pnl)

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

                self.logger.info(
                    "\n{}\n  C{} | {} | Ord:{} | Exp:${:.0f} | Avail:${:.0f} | MaxExp:${:.0f}"
                    "{}{}{}{}{}{}\n{}".format(
                        "_" * 60, cycle,
                        datetime.now(timezone.utc).strftime("%H:%M:%S"),
                        stats["active_orders"], stats["total_exposure"],
                        stats["available_capital"],
                        self.config.max_total_exposure,
                        wallet_str, pnl_str, claim_str, hedge_str,
                        merge_str, churn_str,
                        "_" * 60))

                self.price_feed.update()
                all_assets = list(set(self.config.assets_15m + self.config.assets_5m))
                for asset in all_assets:
                    price = self.price_feed.get_current_price(asset)
                    if price:
                        self.vol_tracker.update_price(asset, price)

                self.book_reader.invalidate_cache()
                markets = self.market_discovery.discover()

                for market in markets:
                    cid = market.get("condition_id", "")
                    if cid:
                        self.window_conditions[market["window_id"]] = cid
                        self.vol_tracker.register_condition(cid, market["asset"])
                    self.vol_tracker.register_token(market["token_up"], market["asset"])
                    self.vol_tracker.register_token(market["token_down"], market["asset"])
                    self.engine.register_window_metadata(market)

                self._compute_market_edges(markets)
                self._resolve_expired_windows(markets)
                self.engine.cleanup_expired_windows(markets, self.churn_manager)
                self.engine.prune_stale_orders()

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
                    live_fills = self.engine.check_fills()
                    if live_fills:
                        self.logger.info("  {} orders filled".format(live_fills))
                        for wid in self.engine.window_fill_sides:
                            self.churn_manager.force_allow(wid)
                        imm_completed = self._process_immediate_pair_completions()
                        if imm_completed:
                            self.logger.info("  {} immediate pair completions".format(imm_completed))
                    hedges = self.engine.process_hedge_completions(
                        self.book_reader, self.vol_tracker)
                    if hedges:
                        self.logger.info("  {} hedges completed".format(hedges))
                    merged = self.auto_merger.check_and_merge_all(
                        self.engine._market_cache, self.engine.token_holdings)
                    if merged:
                        self.logger.info("  Auto-merged {} positions | ${:.2f} returned".format(
                            merged, self.auto_merger.total_merged_usd))
                        if self.balance_checker:
                            self.balance_checker._cache_time = 0

                for market in markets:
                    self.engine._is_up_token_cache[market["token_up"]] = True
                    self.engine._is_up_token_cache[market["token_down"]] = False
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

                if cycle % 5 == 0:
                    self.merge_detector.check_merges(
                        self.engine.token_holdings, self.engine._market_cache)

                trading_halted = False
                now = time.time()
                if now < self._loss_stop_until:
                    if cycle % 10 == 1:
                        self.logger.info("  LOSS COOLOFF -- {}s remaining".format(
                            int(self._loss_stop_until - now)))
                    trading_halted = True
                elif self.sim_engine:
                    s = self.sim_engine.get_summary()
                    loss_limit = -self.config.hard_loss_stop_pct * self.config.kelly_bankroll
                    if s["realized_pnl"] < loss_limit:
                        self._loss_stop_until = now + self.config.hard_loss_cooloff
                        trading_halted = True

                # V15-2 + V15.1-6: Dual-path tradeable filter
                tradeable_markets = []
                active_window_ids = set(self.engine.window_exposure.keys())
                for market in markets:
                    edge = market.get("edge", 0)
                    maker_edge = market.get("maker_edge", edge)
                    if (edge < self.config.min_pair_edge
                            and maker_edge < self.config.pair_min_profit):
                        if market["window_id"] not in active_window_ids:
                            continue
                    if market.get("is_advance", False) and not self.config.trade_advance_windows:
                        continue
                    if market.get("time_left", 0) > self.config.max_order_horizon:
                        if market["window_id"] not in active_window_ids:
                            continue
                    if len(active_window_ids) >= self.config.max_concurrent_windows:
                        if market["window_id"] not in active_window_ids:
                            continue
                    tradeable_markets.append(market)

                tradeable_markets = self._score_and_sort_markets(tradeable_markets)

                if cycle % self.config.edge_map_interval == 1:
                    self._print_edge_map(markets, tradeable_markets)

                for market in tradeable_markets:
                    try:
                        if trading_halted:
                            continue
                        self.mm_strategy.execute(market)
                        self.sniper.execute(market)
                        self.arb.execute(market)
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
