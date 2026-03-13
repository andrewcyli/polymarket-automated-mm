"""
Polymarket Crypto Trading Bot v15.1
Pair Integrity Edition

Building from v15, fixing critical issue where DOWN orders were silently
rejected due to max_total_exposure not scaling with kelly_bankroll when
wallet auto-detect fails.

V15.1 Changes from V15:
  V15.1-1. EXPOSURE SCALES WITH BANKROLL: max_total_exposure is always set
           to kelly_bankroll * 0.80 at startup, not hardcoded $80. Previously
           only updated when wallet auto-detect succeeded, leaving $80 default
           when it failed — causing SOL DOWN orders to be silently rejected
           after $75 of exposure.
  V15.1-2. PAIR CAPITAL PRE-CHECK: Before placing UP order, verify capital
           exists for BOTH sides. If total pair cost exceeds available capital
           or would breach exposure limits, skip the entire window. Prevents
           one-sided deployments that can't hedge.
  V15.1-3. FLOATING POINT EPSILON: Pair profit check uses 0.0001 epsilon to
           avoid rejecting pairs at exactly the boundary. Fixes "Profit:
           $0.005 < min $0.005" caused by 0.004999 rounding.
  V15.1-4. VERBOSE ORDER REJECTION: place_order() now logs the specific
           reason when returning None (exposure limit, budget limit, capital
           limit). Previously failed silently, making debugging impossible.
  V15.1-5. BANKROLL AUTO-DETECT RELIABILITY: 5 attempts with 3s delay (was
           3 attempts, 2s). Invalidates balance cache between retries.
  V15.1-6. CURRENT-WINDOW FOCUS: Only place orders on windows expiring within
           max_order_horizon (default 2700s = 45min). Prevents burning capital
           and API calls on windows 90+ minutes away.

  V15.1-18. BANKROLL MANAGEMENT REDESIGN:
            get_available_capital() now returns bankroll - total_capital_used
            (no deploy_reserve_pct deduction). Bankroll = what user wants to
            deploy. max_total_exposure defaults to bankroll (not 80%).
            After merge/claim reduces capital_in_positions, available capital
            increases, allowing new trades. Example: $100 bankroll, 3x$30
            trades = $90 used, $10 available. After merge returns $40,
            capital_in_positions drops by $30, available = $40 for next trade.

  V15.1-17. FIX MERGE + REDEEM CASHFLOW:
            MERGE: _find_mergeable now builds combined lookup from _market_cache
            + expired_windows_pending_claim + window_metadata + window_fill_tokens.
            Previously, cleanup_expired_windows removed windows from _market_cache
            BEFORE merge ran, so merge could never find paired UP+DOWN holdings.
            Now merge runs BEFORE cleanup in the main loop.
            REDEEM: process_claims now checks resolution FIRST before any redeem
            attempt. Previously tried blind redeem every 3rd attempt regardless
            of resolution status, wasting RPC calls. New priority:
            1) CLOB-SELL (fastest, no gas) 2) CTF-DIRECT 3) CTF-PROXY 4) BLIND-REDEEM.
            V15.1-17b: After successful merge, reduce window_fill_cost by merged
            amount to keep reconcile accounting accurate. filled_windows guard
            remains (window stays blocked from re-entry even after merge).

  V15.1-16. PER-MARKET BUDGET INCLUDES FILLS: place_order() per-market check
            now uses window_exposure (open orders) + window_fill_cost (filled orders)
            instead of just window_exposure. Previously, after a fill removed the
            order from active_orders, window_exposure dropped, allowing hedge buys
            to pass the cap. Total spend could reach 2-3x budget_per_market.
            Also fixes reconcile_capital_from_wallet() to NOT zero
            capital_in_positions when filled_windows exist — tokens are still held
            and must be counted in P&L. The old code caused wallet-only P&L to
            trigger false loss stops (wallet down $57 but tokens worth $57 = $0
            actual loss), halting the bot for 6+ hours overnight.

  V15.1-15. PERSISTENT FILLED WINDOW GUARD: Adds filled_windows set that
            tracks windows with ANY fill. Survives reconcile_capital_from_wallet()
            which previously wiped window_fill_cost={}, allowing re-entry into
            windows that already had fills. Root cause of 5x $30 DOWN orders on
            same BTC 15m window ($150 on $100 bankroll). Guard is checked in:
            (1) MM execute, (2) Sniper/Arb/Contrarian execute, (3) tradeable
            filter, (4) strategy execution loop. Only released on window expiry
            or momentum exit. Also preserves window_fill_cost for filled windows
            during reconciliation instead of blanket reset.
  Carried from V15:
  V15-1 through V15-6, V14.1-1 through V14.1-8, V14-1 through V14-10,
  V13.1-1 through V13.1-2, V13-1 through V13-12, V8-1 through V8-13,
  V7-1 through V7-15, V5-1 through V5-24, V6-FIX
"""

import os
import sys
import time
import signal
import datetime
from datetime import datetime as dt, timezone

from bot_engine import (
    BotConfig,
    setup_logging,
    FeeCalculator,
    KellySizer,
    RewardOptimizer,
    VolatilityTracker,
    OrderChurnManager,
    MergeDetector,
    PriceFeed,
    MarketDiscovery,
    OrderBookReader,
    WalletBalanceChecker,
    AutoMerger,
    AutoClaimManager,
    TradingEngine,
    SimulatedFillEngine,
    MarketMakingStrategy,
    LateSniper,
    CombinedProbArb,
    ContrarianFade,
)


# -----------------------------------------------------------------
# Main Bot (V15.1)
# -----------------------------------------------------------------

class PolymarketBot:
    def __init__(self):
        self.config = BotConfig()
        errors = self.config.validate()
        if errors:
            print("CONFIG ERRORS:")
            for e in errors:
                print("  - " + e)
            sys.exit(1)

        self.logger = setup_logging(self.config.log_level)
        self.running = False
        self.fee_calc = FeeCalculator()
        self.price_feed = PriceFeed(self.config, self.logger)
        self.market_discovery = MarketDiscovery(self.config, self.logger)
        self.book_reader = OrderBookReader(self.logger)
        self.balance_checker = WalletBalanceChecker(self.config, self.logger)
        self.claim_manager = AutoClaimManager(self.config, self.logger)
        self.engine = TradingEngine(
            self.config, self.fee_calc, self.logger, self.balance_checker)
        self.claim_manager.set_engine(self.engine)
        self.engine._claim_manager_ref = self.claim_manager  # V15.7: for mom-exit fallback claims
        self.kelly = KellySizer(self.config, self.fee_calc)

        self.reward_optimizer = RewardOptimizer(self.config, self.fee_calc, self.logger)
        self.vol_tracker = VolatilityTracker(self.config, self.logger)
        self.churn_manager = OrderChurnManager(self.config, self.logger)
        self.merge_detector = MergeDetector(self.config, self.logger)
        self.auto_merger = AutoMerger(self.config, self.logger, self.engine)

        if self.config.dry_run:
            self.sim_engine = SimulatedFillEngine(
                self.config, self.fee_calc, self.logger, self.engine)
            self.engine.sim_engine = self.sim_engine
        else:
            self.sim_engine = None

        self.window_conditions = {}

        # V16: Smart Exit Engine + Binance Feed initialization
        self._binance_feed = None
        self._smart_exit_engine = None
        if getattr(self.config, 'exit_mode', 'tiers') == 'smart':
            try:
                from binance_feed import BinanceFeed
                from smart_exit import SmartExitEngine
                # Start Binance WebSocket feed for CEX momentum data
                if getattr(self.config, 'smart_exit_binance_enabled', True):
                    symbols = []
                    for a in getattr(self.config, 'assets_5m', getattr(self.config, 'assets_15m', ['btc', 'eth', 'sol', 'xrp'])):
                        sym = {'btc': 'btcusdt', 'eth': 'ethusdt', 'sol': 'solusdt', 'xrp': 'xrpusdt'}.get(a.lower())
                        if sym:
                            symbols.append(sym)
                    if symbols:
                        # BinanceFeed expects 'assets' (e.g. ['btc','eth']), not 'symbols' (e.g. ['btcusdt'])
                        feed_assets = list(set(a.lower() for a in getattr(self.config, 'assets_5m', []) + getattr(self.config, 'assets_15m', ['btc', 'eth'])))
                        self._binance_feed = BinanceFeed(
                            assets=feed_assets,
                            history_seconds=getattr(self.config, 'smart_exit_binance_history', 300))
                        self._binance_feed.start()
                        self.logger.info("  V16: Binance feed started for {}".format(symbols))
                # Initialize Smart Exit Engine
                self._smart_exit_engine = SmartExitEngine(
                    immediate_sell_ask=getattr(self.config, 'smart_exit_immediate_sell_ask', 0.85),
                    sell_score_threshold=getattr(self.config, 'smart_exit_sell_threshold', 0.55),
                    hedge_score_threshold=getattr(self.config, 'smart_exit_hedge_threshold', 0.40),
                    velocity_window=getattr(self.config, 'smart_exit_velocity_window', 15.0),
                    ask_weight=getattr(self.config, 'smart_exit_ask_weight', 0.40),
                    velocity_weight=getattr(self.config, 'smart_exit_velocity_weight', 0.20),
                    time_weight=getattr(self.config, 'smart_exit_time_weight', 0.20),
                    cex_weight=getattr(self.config, 'smart_exit_cex_weight', 0.20),
                    binance_feed=self._binance_feed)
                self.logger.info("  V16: Smart Exit Engine initialized (mode=smart)")
            except ImportError as e:
                self.logger.warning("  V16: Smart Exit import failed: {} — falling back to tiers".format(e))
                self._smart_exit_engine = None
            except Exception as e:
                self.logger.warning("  V16: Smart Exit init failed: {} — falling back to tiers".format(e))
                self._smart_exit_engine = None
        # Wire smart exit engine to the trade engine for hedge decisions
        self.engine._smart_exit_engine = self._smart_exit_engine

        self.mm_strategy = MarketMakingStrategy(
            self.config, self.engine, self.book_reader,
            self.price_feed, self.kelly, self.fee_calc, self.logger,
            self.reward_optimizer, self.vol_tracker, self.churn_manager)
        self.sniper = LateSniper(
            self.config, self.engine, self.book_reader,
            self.price_feed, self.kelly, self.fee_calc, self.logger)
        self.arb = CombinedProbArb(
            self.config, self.engine, self.book_reader, self.fee_calc, self.logger)
        self.contrarian = ContrarianFade(
            self.config, self.engine, self.book_reader,
            self.price_feed, self.fee_calc, self.logger)
        self._loss_stop_until = 0.0
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, signum, frame):
        if not self.running:
            # Second Ctrl+C: force exit immediately
            self.logger.info("\nForce exit (second signal). Goodbye.")
            os._exit(1)
        self.logger.info("\nShutdown signal received. Cancelling all orders...")
        self.logger.info("  (Press Ctrl+C again to force-quit immediately)")
        self.running = False
        # V16: Stop Binance feed
        if hasattr(self, '_binance_feed') and self._binance_feed:
            try:
                self._binance_feed.stop()
                self.logger.info("  V16: Binance feed stopped.")
            except Exception:
                pass
        try:
            self.engine.cancel_all()
        except Exception:
            pass
        self._print_summary("FINAL")
        self._print_claim_summary()
        self._print_v15_1_summary()
        self.logger.info("All orders cancelled. Exiting.")
        os._exit(0)


    def _process_immediate_pair_completions(self):
        if not self.config.immediate_pair_completion:
            return 0
        completed = 0
        for hedge in list(self.engine._pending_hedges):
            wid = hedge["window_id"]
            filled_side = hedge["filled_side"]
            filled_price = hedge["filled_price"]
            filled_size = hedge["filled_size"]
            sides = self.engine.window_fill_sides.get(wid, {})
            other_side = "DOWN" if filled_side == "UP" else "UP"
            if other_side in sides and len(sides[other_side]) > 0:
                self.engine._pending_hedges.remove(hedge)
                continue
            # V15.3-FIX: Resolve other_token from market cache, hedge entry, or metadata
            market = self.engine._market_cache.get(wid)
            if market:
                other_token = market["token_up"] if other_side == "UP" else market["token_down"]
            else:
                other_token = hedge.get("token_up", "") if other_side == "UP" else hedge.get("token_down", "")
                if not other_token:
                    meta = self.engine.window_metadata.get(wid, {})
                    other_token = meta.get("token_up", "") if other_side == "UP" else meta.get("token_down", "")
            if not other_token:
                continue
            spread = self.book_reader.get_spread(other_token)
            if not spread:
                continue
            other_ask = spread["ask"]
            fee_filled = self.fee_calc._interp_fee_per_share(filled_price)
            fee_other = self.fee_calc._interp_fee_per_share(other_ask)
            total_pair_cost = filled_price + other_ask + fee_filled + fee_other
            loss_per_share = total_pair_cost - 1.0
            if loss_per_share > self.config.pair_completion_max_loss:
                self.logger.info(
                    "  PAIR-IMM SKIP | {} | {} @ ${:.2f} | {} ask ${:.2f} | "
                    "Loss ${:.3f}/sh > max ${:.3f} | Defer to hedge".format(
                        wid, filled_side, filled_price, other_side, other_ask,
                        loss_per_share, self.config.pair_completion_max_loss))
                continue
            profit_str = "${:+.3f}/sh".format(-loss_per_share)
            self.logger.info(
                "\n  PAIR-IMM | {} | {} @ ${:.2f} -> Buy {} @ ${:.2f} | "
                "{} | {:.0f} shares".format(
                    wid, filled_side, filled_price, other_side, other_ask,
                    profit_str, filled_size))
            result = self.engine.place_order(
                other_token, "BUY", other_ask, filled_size,
                wid, "PAIR-{}-IMM".format(other_side), "mm", is_taker=True)
            if result:
                self.engine._pending_hedges.remove(hedge)
                self.engine.hedges_completed += 1
                completed += 1
        return completed

    # V15-1: Compute both taker edge and maker edge for each market
    def _compute_market_edges(self, markets):
        for market in markets:
            spread_up = self.book_reader.get_spread(market["token_up"])
            spread_down = self.book_reader.get_spread(market["token_down"])
            if spread_up and spread_down:
                ask_up = spread_up["ask"]
                ask_down = spread_down["ask"]
                fee_up = self.fee_calc._interp_fee_per_share(ask_up)
                fee_down = self.fee_calc._interp_fee_per_share(ask_down)
                edge = 1.0 - (ask_up + ask_down + fee_up + fee_down)
                market["edge"] = edge
                market["ask_sum"] = ask_up + ask_down
                market["spread_data_up"] = spread_up
                market["spread_data_down"] = spread_down

                # Maker edge: estimate using midpoint - base_spread
                mid_up = spread_up["midpoint"]
                d = self.config.mm_base_spread
                buy_up_est = mid_up - d
                buy_down_est = (1.0 - mid_up) - d
                if buy_up_est > 0.02 and buy_down_est > 0.02:
                    mfee_up = self.fee_calc._interp_fee_per_share(buy_up_est)
                    mfee_down = self.fee_calc._interp_fee_per_share(buy_down_est)
                    maker_edge = 1.0 - (buy_up_est + buy_down_est + mfee_up + mfee_down)
                else:
                    maker_edge = edge
                market["maker_edge"] = maker_edge
            else:
                gamma_sum = market.get("gamma_sum", 1.0)
                market["edge"] = max(0, 1.0 - gamma_sum - 0.015)
                market["ask_sum"] = gamma_sum
                market["maker_edge"] = market["edge"]

    # V15-4: Edge map shows both taker and maker edge
    def _print_edge_map(self, all_markets, tradeable_markets):
        if not all_markets:
            return
        self.logger.info("\n  EDGE MAP ({} discovered, {} tradeable):".format(
            len(all_markets), len(tradeable_markets)))
        sorted_m = sorted(all_markets, key=lambda m: m.get("maker_edge", 0), reverse=True)
        for m in sorted_m[:12]:
            edge = m.get("edge", 0)
            maker_edge = m.get("maker_edge", edge)
            ask_sum = m.get("ask_sum", m.get("gamma_sum", 1.0))
            time_left = m.get("time_left", 0)
            wid = m["window_id"]
            is_active = wid in self.engine.known_windows
            if maker_edge >= 0.05:
                tier = "***"
            elif maker_edge >= 0.03:
                tier = "** "
            elif maker_edge >= self.config.pair_min_profit:
                tier = "*  "
            else:
                tier = "   "
            active_tag = " [ACTIVE]" if is_active else ""
            self.logger.info(
                "    {} {:30s} | {:>5.0f}s | taker: {:>5.1%} | maker: {:>5.1%} | sum: {:.3f}{}".format(
                    tier, wid[:30], time_left, edge, maker_edge, ask_sum, active_tag))
        premium = sum(1 for m in all_markets if m.get("maker_edge", 0) >= 0.05)
        good = sum(1 for m in all_markets if 0.03 <= m.get("maker_edge", 0) < 0.05)
        marginal = sum(1 for m in all_markets
                       if self.config.pair_min_profit <= m.get("maker_edge", 0) < 0.03)
        below = sum(1 for m in all_markets if m.get("maker_edge", 0) < self.config.pair_min_profit)
        self.logger.info("    Tiers: ***={} premium  **={} good  *={} marginal  {}=below min".format(
            premium, good, marginal, below))

    def _print_summary(self, label=""):
        stats = self.engine.get_stats()
        sep = "=" * 70
        self.logger.info("\n" + sep)
        self.logger.info("  SUMMARY " + label)
        self.logger.info(sep)
        self.logger.info("  Active Orders:    {}".format(stats["active_orders"]))
        self.logger.info("  Total Placed:     {}".format(stats["total_placed"]))
        self.logger.info("  Active Windows:   {}".format(stats["windows_active"]))
        self.logger.info("  Exposure:         ${:.2f}".format(stats["total_exposure"]))
        self.logger.info("  Capital in Pos:   ${:.2f}".format(stats["capital_in_positions"]))
        self.logger.info("  Available Cap:    ${:.2f}".format(stats["available_capital"]))
        self.logger.info("  Session Spent:    ${:.2f}".format(stats["session_spent"]))
        self.logger.info("  Hedges:           {} completed / {} skipped / {} pending".format(
            stats["hedges_completed"], stats["hedges_skipped"],
            len(self.engine._pending_hedges)))
        self.logger.info("  Paired Windows:   {}".format(stats["paired_windows"]))
        merge_stats = self.auto_merger.get_stats()
        self.logger.info("  Merges:           {} ok / {} fail | ${:.2f} returned".format(
            merge_stats["merges_completed"], merge_stats["merges_failed"],
            merge_stats["total_merged_usd"]))
        wallet_delta = stats.get("wallet_delta")
        if wallet_delta is not None:
            tag = "PROFIT" if wallet_delta >= 0 else "LOSS"
            self.logger.info("  [{}] Wallet \u0394:    ${:+,.2f}".format(tag, wallet_delta))
            held_val = stats.get("held_value", 0)
            total_est = stats.get("total_pnl_est")
            if total_est is not None:
                self.logger.info("  Est Total P&L: ${:+,.2f} (W\u0394 + ${:.0f} deployed + ${:.0f} held)".format(
                    total_est, stats.get("capital_deployed", 0), held_val))
            self.logger.info("  Held Windows:  {}".format(stats.get("held_windows", 0)))
        self.logger.info("  -- Strategies --")
        self.logger.info("  MM: {}  Sniper: {}  Arb: {}  Contrarian: {}".format(
            stats["mm_trades"], stats["sniper_trades"],
            stats["arb_trades"], stats["contrarian_trades"]))
        cs = self.claim_manager.get_claim_stats()
        self.logger.info("  -- Claims --")
        self.logger.info("  Claimed: {} (${:.2f}) | Pending: {} | Blind: {}/{}".format(
            cs["claimed_total"], cs["total_claimed_usd"], cs["pending_claims"],
            cs.get("blind_successes", 0), cs.get("blind_attempts", 0)))
        if self.sim_engine:
            s = self.sim_engine.get_summary()
            tag = "PROFIT" if s["realized_pnl"] >= 0 else "LOSS"
            self.logger.info("  -- Sim P&L --")
            self.logger.info("  [{}] ${:+,.2f} ({:+.2f}%) | WR: {:.0%} ({}W/{}L)".format(
                tag, s["realized_pnl"], s["pnl_pct"],
                s["win_rate"], s["total_wins"], s["total_losses"]))
        self.logger.info(sep)

    def _print_v15_1_summary(self):
        sep = "-" * 50
        self.logger.info("\n" + sep)
        self.logger.info("  V15.1 COMPONENT STATS")
        self.logger.info(sep)
        ro_stats = self.reward_optimizer.get_stats()
        self.logger.info("  Reward Optimizer: {} calcs, {} fallbacks".format(
            ro_stats["calculations"], ro_stats["fallbacks"]))
        vol_stats = self.vol_tracker.get_all_stats()
        if vol_stats:
            for key, info in list(vol_stats.items())[:8]:
                asset_tag = " [{}]".format(info.get("asset", "?"))
                vol_val = info.get("vol_sum")
                vol_str = "{:.4f}".format(vol_val) if vol_val is not None else "N/A"
                self.logger.info("  Vol {}{}: {} ({})".format(
                    key, asset_tag, info["level"], vol_str))
        churn_stats = self.churn_manager.get_stats()
        self.logger.info("  Churn: {} suppressed, {} allowed ({:.0f}% reduction)".format(
            churn_stats["suppressed"], churn_stats["allowed"], churn_stats["reduction_pct"]))
        mm_stats = self.mm_strategy.get_reward_stats()
        self.logger.info("  Est. Maker Rewards: ${:.4f}".format(
            mm_stats["total_estimated_reward"]))
        disc_stats = self.market_discovery.get_cache_stats()
        self.logger.info("  Discovery: {} scans | Cache: +{} -{} entries".format(
            disc_stats["discoveries"], disc_stats["pos_cache"], disc_stats["neg_cache"]))
        merge_stats = self.auto_merger.get_stats()
        self.logger.info("  Merger: {} completed, {} failed, ${:.2f} returned".format(
            merge_stats["merges_completed"], merge_stats["merges_failed"],
            merge_stats["total_merged_usd"]))
        self.logger.info("  Pair Sizing: {} | Min Taker Edge: {:.1%} | MM Min Profit: {:.1%} | Hedge: ${:.3f}".format(
            "EQUAL_SHARES" if self.config.pair_sizing_equal_shares else "EQUAL_DOLLARS",
            self.config.min_pair_edge, self.config.pair_min_profit,
            self.config.hedge_max_loss_per_share))
        self.logger.info("  Circuit Breaker: {:.1%} | Claim Timeout: {:.0f}s | Blind Redeem: {}".format(
            self.config.vol_circuit_breaker, self.config.claim_timeout_seconds,
            "ON" if self.config.blind_redeem_enabled else "OFF"))
        self.logger.info("  Max Exposure: ${:.0f} | Max/Market: ${:.0f} | Horizon: {:.0f}s".format(
            self.config.max_total_exposure, self.config.max_position_per_market,
            self.config.max_order_horizon))
        self.logger.info(sep)

    def _print_claim_summary(self):
        cs = self.claim_manager.get_claim_stats()
        pending = self.engine.expired_windows_pending_claim
        if not cs["claim_results"] and not pending:
            return
        self.logger.info("\n  -- CLAIM DETAILS --")
        for r in cs["claim_results"]:
            self.logger.info("  OK  | {} | {} | ${:.2f}".format(
                r["window_id"], r["method"], r["est_amount"]))
        for wid, info in pending.items():
            self.logger.info("  PENDING | {} | tokens: {} | cost: ${:.2f}".format(
                wid, len(info.get("tokens", [])), info.get("fill_cost", 0)))

    def _resolve_expired_windows(self, active_markets):
        if not self.sim_engine:
            return
        active_ids = {m["window_id"] for m in active_markets}
        for wid in list(self.engine.known_windows):
            if wid in active_ids or wid in self.sim_engine.resolved_windows:
                continue
            parts = wid.split("-")
            if len(parts) < 3:
                continue
            asset = parts[0]
            end_price = None
            if self.price_feed.use_chainlink:
                end_price = self.price_feed.chainlink.get_price(asset)
            if not end_price:
                end_price = self.price_feed.get_current_price(asset)
            start_price = self.sim_engine.window_start_prices.get(wid)
            if end_price and start_price:
                self.sim_engine.resolve_window(wid, asset, end_price, start_price)
                if self.kelly.enabled:
                    self.kelly.update_bankroll(self.sim_engine.current_bankroll)

    def _schedule_live_claims(self):
        for wid in list(self.engine.expired_windows_pending_claim.keys()):
            info = self.engine.expired_windows_pending_claim[wid]
            condition_id = info.get("condition_id", "") or self.window_conditions.get(wid, "")
            if not condition_id:
                if time.time() - info.get("expired_at", 0) > 120:
                    del self.engine.expired_windows_pending_claim[wid]
                continue
            end_ts = info.get("end_time", 0)
            if isinstance(end_ts, float):
                end_ts = int(end_ts)
            self.claim_manager.schedule_claim(
                condition_id=condition_id, window_id=wid, end_time=end_ts,
                slug=info.get("slug", ""), tokens=info.get("tokens", []),
                token_up=info.get("token_up", ""), token_down=info.get("token_down", ""))
            del self.engine.expired_windows_pending_claim[wid]
            # V15.5-FIX2: Mark window as closed when claim is scheduled
            self.engine.closed_windows.add(wid)

    # V15-6 + V15.1-6: Score/sort uses maker_edge for priority
    def _score_and_sort_markets(self, markets):
        """V15.1-19: Enhanced scoring with fill-probability factors.

        Score = edge * vol_penalty * fill_prob_bonus
        fill_prob_bonus considers:
        - Spread symmetry (UP vs DN spread similarity)
        - Momentum (lower = better for pair fill)
        - Time remaining (more time = more chance to fill both sides)
        """
        if not markets:
            return markets
        for market in markets:
            token_up = market["token_up"]
            token_dn = market.get("token_down", "")
            vol_level, vol_sum = self.vol_tracker.get_volatility_level(token_up)
            edge = market.get("maker_edge", market.get("edge", 0))
            vol_penalty = 1.0
            if vol_level == "EXTREME":
                vol_penalty = 0.1
            elif vol_level == "HIGH":
                vol_penalty = 0.5
            elif vol_level == "MEDIUM":
                vol_penalty = 0.8

            # Fill probability bonus (1.0 = neutral, >1 = better fill chance)
            fill_prob = 1.0

            # Factor 1: Spread symmetry — similar spreads = balanced book
            spread_up = self.book_reader.get_spread(token_up)
            spread_dn = self.book_reader.get_spread(token_dn) if token_dn else None
            if spread_up and spread_dn:
                sp_delta = abs(spread_up["spread"] - spread_dn["spread"])
                if sp_delta < 0.005:
                    fill_prob *= 1.15  # Very symmetric — bonus
                elif sp_delta < 0.01:
                    fill_prob *= 1.05
                elif sp_delta > 0.03:
                    fill_prob *= 0.85  # Asymmetric — penalty

            # Factor 2: Momentum — low momentum = balanced fills
            asset = market.get("asset", "")
            mom = self.price_feed.get_momentum(asset, self.config.tf_lookback_minutes)
            if mom is not None:
                abs_mom = abs(mom)
                if abs_mom < 0.001:
                    fill_prob *= 1.10  # Very calm — bonus
                elif abs_mom > 0.005:
                    fill_prob *= 0.90  # Trending — penalty

            # Factor 3: Time remaining — more time = more fill opportunity
            time_left = market.get("time_left", 0)
            if time_left > 600:
                fill_prob *= 1.05  # >10 min left
            elif time_left < 120:
                fill_prob *= 0.90  # <2 min left — risky

            market["_sort_score"] = edge * vol_penalty * fill_prob
        markets.sort(key=lambda m: m.get("_sort_score", 0), reverse=True)
        return markets

    def run(self):
        mode = "DRY RUN" if self.config.dry_run else "LIVE TRADING"
        self.logger.info("=" * 70)
        self.logger.info("  POLYMARKET CRYPTO TRADING BOT v15.1")
        self.logger.info("  Pair Integrity Edition")
        self.logger.info("  Mode: {} | Price: {} | Assets: BTC, ETH, SOL, XRP".format(
            mode, self.price_feed.get_price_source()))
        pk = self.config.private_key
        pw = self.config.proxy_wallet
        self.logger.info("  Key: {} | Proxy: {}".format(
            "{}...{} ({}ch)".format(pk[:6], pk[-4:], len(pk)) if len(pk) > 10 else "MISSING",
            "{}...{}".format(pw[:8], pw[-4:]) if len(pw) > 12 else "MISSING"))
        self.logger.info("  V15.1: Pair integrity | MM min: {:.1%} | Arb min: {:.1%}".format(
            self.config.pair_min_profit, self.config.min_pair_edge))
        self.logger.info("  Merge: {} | Pair-IMM: {} | Blind Redeem: {} | "
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
        self.logger.info("  MM: reward-optimized | Pair validation: {} | Churn reduction: {}".format(
            "ON" if self.config.pair_validation_enabled else "OFF",
            "ON" if self.config.churn_enabled else "OFF"))
        rpc_name = ("polygon-rpc.com (WARNING: rate limited)"
                     if "polygon-rpc.com" in self.config.polygon_rpc
                     else self.config.polygon_rpc[:40])
        self.logger.info("  RPC: {}".format(rpc_name))
        if "polygon-rpc.com" in self.config.polygon_rpc:
            self.logger.warning("  !! Free RPC will rate-limit. Set POLYGON_RPC_URL to Alchemy/Infura.")
        self.logger.info("=" * 70)

        # V15.5-FIX3: ALWAYS detect wallet balance for loss gate,
        # even when auto_detect_bankroll is False (CC is bankroll authority).
        if self.balance_checker and not self.config.dry_run:
            wallet_bal = None
            for _attempt in range(5):
                self.balance_checker._cache_time = 0  # force fresh read
                wallet_bal = self.balance_checker.get_balance()
                if wallet_bal is not None:
                    break
                self.logger.info("  Wallet detect attempt {} failed, retrying...".format(_attempt + 1))
                time.sleep(3)
            if wallet_bal is not None and wallet_bal > 0:
                # Always set starting_wallet_balance for the loss gate
                self.engine.starting_wallet_balance = wallet_bal
                self.logger.info("  Wallet balance: ${:.2f} (loss gate baseline)".format(wallet_bal))
                if self.config.auto_detect_bankroll:
                    # Only override CC bankroll if auto_detect is ON
                    old_bankroll = self.config.kelly_bankroll
                    self.config.kelly_bankroll = wallet_bal
                    self.logger.info("  Bankroll: ${:.2f} (was ${:.2f})".format(
                        wallet_bal, old_bankroll))
            else:
                self.logger.warning("  Wallet detect FAILED. Loss gate will be DISABLED.")

        # V15.1-1: ALWAYS scale exposure with bankroll (whether auto-detect succeeded or not)
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
                    # V15.1-20: Show wallet delta as primary P&L (hard fact)
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

                held_str = ""
                held_count = stats.get("held_windows", 0)
                if held_count > 0:
                    held_str = " | Held:{}".format(held_count)

                self.logger.info(
                    "\n{}\n  C{} | {} | Ord:{} | Exp:${:.0f} | Avail:${:.0f} | MaxExp:${:.0f}"
                    "{}{}{}{}{}{}{}".format(
                        "_" * 60, cycle,
                        dt.now(timezone.utc).strftime("%H:%M:%S"),
                        stats["active_orders"], stats["total_exposure"],
                        stats["available_capital"],
                        self.config.max_total_exposure,
                        wallet_str, pnl_str, claim_str, hedge_str,
                        merge_str, churn_str, held_str,
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
                self.engine.purge_recently_cancelled()

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
                elif not self.config.dry_run and self.engine.starting_wallet_balance:
                    # V15.1-20: LIVE-MODE LOSS STOP using wallet delta
                    wallet_now = None
                    if self.balance_checker:
                        wallet_now = self.balance_checker.get_balance()
                    if wallet_now is not None:
                        wallet_delta = wallet_now - self.engine.starting_wallet_balance
                        loss_limit = -self.config.hard_loss_stop_pct * self.config.kelly_bankroll
                        if wallet_delta < loss_limit:
                            self._loss_stop_until = now + self.config.hard_loss_cooloff
                            self.logger.warning(
                                "  LIVE LOSS STOP | W\u0394: ${:.2f} < limit ${:.2f} "
                                "({}% of ${:.0f} bankroll) | Halting for {}s".format(
                                    wallet_delta, loss_limit,
                                    self.config.hard_loss_stop_pct * 100,
                                    self.config.kelly_bankroll,
                                    self.config.hard_loss_cooloff))
                            trading_halted = True
                        elif cycle % 10 == 1:
                            # Periodic wallet delta status (every 10 cycles)
                            self.logger.info(
                                "  LOSS MONITOR | W\u0394: ${:+.2f} / limit ${:.2f}".format(
                                    wallet_delta, loss_limit))

                # V15.1-19: Session blackout windows
                if self.config.trading_blackout_windows and not trading_halted:
                    utc_now = datetime.datetime.utcnow()
                    current_hour_min = utc_now.hour + utc_now.minute / 60.0
                    for bw in self.config.trading_blackout_windows:
                        if len(bw) == 2:
                            start_h, end_h = float(bw[0]), float(bw[1])
                            if start_h <= current_hour_min < end_h:
                                if cycle % 10 == 1:
                                    self.logger.info(
                                        "  SESSION BLACKOUT | {:.2f}-{:.2f} UTC | "
                                        "Current: {:.2f} | MM paused".format(
                                            start_h, end_h, current_hour_min))
                                trading_halted = True
                                break

                # V15-2 + V15.1-6 + V15.1-10: Dual-path tradeable filter
                # with condition_id deduplication
                tradeable_markets = []
                # V15.1-20: Include held_windows in active count.
                active_window_ids = (set(self.engine.window_exposure.keys())
                                     | self.engine.filled_windows
                                     | self.engine.held_windows)
                # V15.1-10: Track condition_ids with active exposure
                active_condition_ids = set()
                for awid in active_window_ids:
                    meta = self.engine._window_metadata.get(awid, {})
                    cid = meta.get("condition_id", "")
                    if cid:
                        active_condition_ids.add(cid)
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
                    # V15.1-10: Condition_id dedup
                    mkt_cid = market.get("condition_id", "")
                    if mkt_cid and mkt_cid in active_condition_ids:
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
                        # V15.1-7/20: Dynamic max_concurrent_windows enforcement
                        current_active = (set(self.engine.window_exposure.keys())
                                          | self.engine.filled_windows
                                          | self.engine.held_windows)
                        wid = market["window_id"]
                        if (len(current_active) >= self.config.max_concurrent_windows
                                and wid not in current_active):
                            continue
                        # V15.1-10: Dynamic condition_id dedup
                        cur_cids = set()
                        for awid in current_active:
                            meta = self.engine._window_metadata.get(awid, {})
                            c = meta.get("condition_id", "")
                            if c:
                                cur_cids.add(c)
                        mkt_cid = market.get("condition_id", "")
                        if mkt_cid and mkt_cid in cur_cids and wid not in current_active:
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

            except Exception as e:
                self.logger.error("  Cycle error: {}".format(e))

            time.sleep(self.config.cycle_interval)


# -----------------------------------------------------------------
# Entry Point
# -----------------------------------------------------------------

if __name__ == "__main__":
    bot = PolymarketBot()
    bot.run()
