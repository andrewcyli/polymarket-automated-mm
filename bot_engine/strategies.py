"""Trading strategies: Market Making, Late Sniper, Combined Prob Arb, Contrarian Fade."""

import time
import math
import logging


class MarketMakingStrategy:
    def __init__(self, config, engine, book_reader, price_feed, kelly, fee_calc,
                 logger, reward_optimizer, vol_tracker, churn_manager, vpin_tracker=None):
        self.config = config
        self.engine = engine
        self.book_reader = book_reader
        self.price_feed = price_feed
        self.kelly = kelly
        self.fee_calc = fee_calc
        self.logger = logger
        self.reward_optimizer = reward_optimizer
        self.vol_tracker = vol_tracker
        self.churn_manager = churn_manager
        self.vpin_tracker = vpin_tracker  # V15.6: VPIN toxicity tracker
        self.last_refresh = {}
        self._window_first_placed = {}  # {window_id: timestamp} — when orders were first placed
        self._cycle_reward_estimates = []
        self._total_estimated_reward = 0.0
        self._gate_block_count = {}  # {asset: consecutive_block_count} — for momentum gate bypass
        # V15.9: Auto-pause state — tracks consecutive gate blocks/passes globally
        self._autopause_active = False
        self._autopause_consec_blocks = 0  # Global consecutive gate blocks (across all assets)
        self._autopause_consec_passes = 0  # Global consecutive gate passes since last block
        self._autopause_trigger_count = 0  # How many times auto-pause has been triggered this session

    def execute(self, market):
        asset = market["asset"]
        window_id = market["window_id"]
        now = time.time()
        time_remaining = market["end_time"] - now
        # V15.7: Graduated spread replaces binary min_time_remaining cutoff
        # Instead of stopping entirely at 60s, we gradually widen spread from
        # graduated_spread_start_secs down to graduated_spread_stop_secs.
        # Below stop_secs, we still don't quote (too risky).
        if self.config.graduated_spread_enabled:
            if time_remaining < self.config.graduated_spread_stop_secs:
                return
        else:
            # Legacy behavior: binary cutoff
            if time_remaining < self.config.min_time_remaining:
                return
        # V15.1-6: Skip windows too far out
        if time_remaining > self.config.max_order_horizon:
            return
        # V15.1-19: Closed window guard — permanently blocked after momentum exit
        if window_id in self.engine.closed_windows:
            self.logger.debug(
                "  CLOSED GUARD | {} | Window closed (momentum exit) — no re-entry".format(
                    window_id))
            return
        # V15.1-15: PERSISTENT filled window guard (replaces CC-OPT-C).
        # filled_windows survives reconcile_capital_from_wallet() which resets
        # window_fill_cost. This is the PRIMARY re-entry prevention mechanism.
        if window_id in self.engine.filled_windows:
            fill_cost = self.engine.window_fill_cost.get(window_id, 0)
            entries = self.engine.window_entry_count.get(window_id, 0)
            self.logger.debug(
                "  FILL GUARD | {} | Already filled ${:.2f} (entries={}) — no re-entry".format(
                    window_id, fill_cost, entries))
            return
        # CC-OPT-C (backup): Also check window_fill_cost in case filled_windows
        # was somehow missed (belt-and-suspenders).
        fill_cost = self.engine.window_fill_cost.get(window_id, 0)
        if fill_cost > 0:
            self.engine.filled_windows.add(window_id)  # Repair missing entry
            self.engine.held_windows.add(window_id)  # V15.1-20: repair held tracking too
            self.logger.debug(
                "  FILL SKIP | {} | Already filled ${:.2f} — position established".format(
                    window_id, fill_cost))
            return
        # V15.1-P5: NO-CHURN-REFRESH GUARD — once orders are placed for a
        # window, never cancel+replace them. Cancelling is async on Polymarket;
        # if the old order fills before the cancel takes effect, BOTH old and
        # new orders fill, doubling the position size. Let original orders
        # stand until they fill or the window expires.
        if window_id in self._window_first_placed:
            self.logger.debug(
                "  NO-REFRESH | {} | Orders placed at {:.0f}s ago — standing".format(
                    window_id, now - self._window_first_placed[window_id]))
            return
        last = self.last_refresh.get(window_id, 0)
        if now - last < self.config.mm_refresh_interval:
            return
        spread_up = self.book_reader.get_spread(market["token_up"])
        spread_down = self.book_reader.get_spread(market["token_down"])
        if not spread_up:
            return
        midpoint = spread_up["midpoint"]
        spread = spread_up["spread"]
        bid = spread_up["bid"]
        ask = spread_up["ask"]
        if spread < self.config.mm_min_spread:
            return
        if spread > self.config.mm_max_spread:
            midpoint = 0.50
        vol_level, vol_sum = self.vol_tracker.get_volatility_level(market["token_up"])
        if vol_level == "EXTREME":
            self.logger.info("  VOL PAUSE | {} | vol={:.3f} (EXTREME) | Skipping MM".format(
                window_id, vol_sum if vol_sum else 0))
            return
        prediction = self.price_feed.predict_resolution(
            asset, market["timestamp"], market["end_time"])
        momentum = self.price_feed.get_momentum(asset, self.config.tf_lookback_minutes)
        skew = 0.0
        trend_label = "NEUTRAL"
        if prediction and prediction["confidence"] > 0.60:
            if prediction["direction"] == "UP":
                skew = self.config.tf_skew_factor * (prediction["confidence"] - 0.5) * 4
                trend_label = "CL-UP ({:.0%})".format(prediction["confidence"])
            else:
                skew = -self.config.tf_skew_factor * (prediction["confidence"] - 0.5) * 4
                trend_label = "CL-DN ({:.0%})".format(prediction["confidence"])
        current_price = self.price_feed.get_current_price(asset)
        price_str = "${:,.2f}".format(current_price) if current_price else "N/A"
        mom_str = "{:+.3f}%".format(momentum * 100) if momentum is not None else "N/A"
        vol_str = "{} ({:.3f})".format(vol_level, vol_sum) if vol_sum is not None else "UNKNOWN"
        maker_edge = market.get("maker_edge", market.get("edge", 0))
        edge_str = "mE:{:.1%}".format(maker_edge) if maker_edge else "mE:?"
        self.logger.info(
            "\n  {:4s} {:3s} | Price: {:>12s} | Mom: {:>8s} | {:16s} | "
            "Bid: {:.2f} Ask: {:.2f} Sprd: {:.3f} | Vol: {} | {} | {:.0f}s".format(
                asset.upper(), market["timeframe"].upper(), price_str,
                mom_str, trend_label, bid, ask, spread, vol_str, edge_str,
                time_remaining))
        is_high_vol = momentum is not None and abs(momentum) > self.config.vol_circuit_breaker
        if is_high_vol and self.config.mm_enabled:
            self.logger.info("  CIRCUIT BREAKER | {} | Mom: {} | MM PAUSED".format(
                asset.upper(), mom_str))
            return
        if not self.config.mm_enabled:
            return
        # V15.1-19 Filter A: Momentum Gate — skip if short-term momentum too strong
        # V15.1-P54: Bypass timer — after N consecutive blocks per asset, relax threshold
        if (self.config.momentum_gate_threshold > 0 and momentum is not None):
            consec = self._gate_block_count.get(asset, 0)
            max_consec = self.config.momentum_gate_max_consec
            # V15.1-29 Strategy 4: Asset-specific scaling — more volatile assets
            # get a higher threshold so they aren't blocked too aggressively.
            asset_scale = self.config.momentum_gate_asset_scale.get(asset, 1.0)
            effective_threshold = self.config.momentum_gate_threshold * asset_scale
            bypassed = False
            if max_consec > 0 and consec >= max_consec:
                # Relax threshold: double it for each consecutive bypass period
                multiplier = 1 + (consec // max_consec)
                effective_threshold = self.config.momentum_gate_threshold * asset_scale * (1 + multiplier * 0.5)
                bypassed = True
            if abs(momentum) > effective_threshold:
                self._gate_block_count[asset] = consec + 1
                bypass_str = " (bypass={}, eff={:.3f}%)".format(consec + 1, effective_threshold * 100) if consec > 0 else ""
                self.logger.info(
                    "  MOMENTUM GATE | {} | Mom: {:+.3f}% > {:.3f}%{} | Skipping MM".format(
                        window_id, momentum * 100, effective_threshold * 100, bypass_str))
                # V15.1-25: Track gate block analytics
                self.engine.hedge_analytics["gate_blocks"] += 1
                # V15.9: Auto-pause — track global consecutive gate blocks
                if self.config.auto_pause_enabled:
                    self._autopause_consec_blocks += 1
                    self._autopause_consec_passes = 0  # Reset pass counter on any block
                    if (not self._autopause_active
                            and self._autopause_consec_blocks >= self.config.auto_pause_gate_threshold):
                        self._autopause_active = True
                        self._autopause_trigger_count += 1
                        self.logger.warning(
                            "  AUTO-PAUSE TRIGGERED | {} consecutive gate blocks >= threshold {} | "
                            "Trigger #{} | Halting new window creation".format(
                                self._autopause_consec_blocks,
                                self.config.auto_pause_gate_threshold,
                                self._autopause_trigger_count))
                return
            else:
                if bypassed:
                    self.logger.info(
                        "  MOMENTUM GATE BYPASS | {} | Mom: {:+.3f}% <= {:.3f}% (relaxed from {:.3f}%) | "
                        "Consecutive blocks: {} | Allowing MM".format(
                            window_id, momentum * 100, effective_threshold * 100,
                            self.config.momentum_gate_threshold * 100, consec))
                    # V15.1-25: Track gate bypass analytics
                    self.engine.hedge_analytics["gate_bypasses"] += 1
                # Reset counter on pass
                self._gate_block_count[asset] = 0
                # V15.9: Auto-pause — track global consecutive passes
                if self.config.auto_pause_enabled:
                    self._autopause_consec_passes += 1
                    self._autopause_consec_blocks = 0  # Reset block counter on pass
                    if (self._autopause_active
                            and self._autopause_consec_passes >= self.config.auto_pause_resume_threshold):
                        self._autopause_active = False
                        self.logger.info(
                            "  AUTO-PAUSE RESUMED | {} consecutive passes >= resume threshold {} | "
                            "New window creation re-enabled".format(
                                self._autopause_consec_passes,
                                self.config.auto_pause_resume_threshold))
        # V15.1-19 Filter C: Spread Symmetry — skip if UP/DN spreads diverge too much
        if (self.config.max_spread_asymmetry > 0 and spread_down):
            dn_spread = spread_down.get("spread", 0)
            spread_delta = abs(spread - dn_spread)
            if spread_delta > self.config.max_spread_asymmetry:
                self.logger.info(
                    "  SPREAD ASYM | {} | UP sprd: {:.3f} DN sprd: {:.3f} | "
                    "Delta {:.3f} > max {:.3f} | Skipping".format(
                        window_id, spread, dn_spread, spread_delta,
                        self.config.max_spread_asymmetry))
                return
        # V15.1-29 Strategy 2: Bid/ask midpoint directional filter
        # When the UP token midpoint deviates significantly from 0.50, the market
        # is pricing in a directional outcome. If mid > 0.53, UP is favored (DOWN
        # fills easily but likely loses). If mid < 0.47, DOWN is favored (UP fills
        # easily but likely loses). In both cases, skip to avoid one-sided fills
        # that create orphan positions.
        midpoint_skew_limit = getattr(self.config, 'midpoint_skew_limit', 0.03)
        if midpoint_skew_limit > 0 and abs(midpoint - 0.50) > midpoint_skew_limit:
            direction = "UP-favored" if midpoint > 0.50 else "DOWN-favored"
            self.logger.info(
                "  MIDPOINT FILTER | {} | Mid: {:.3f} | {} | "
                "Skew {:.3f} > limit {:.3f} | Skipping to avoid orphan".format(
                    window_id, midpoint, direction,
                    abs(midpoint - 0.50), midpoint_skew_limit))
            if "midpoint_skips" not in self.engine.hedge_analytics:
                self.engine.hedge_analytics["midpoint_skips"] = 0
            self.engine.hedge_analytics["midpoint_skips"] += 1
            return
        # V15.6: VPIN toxicity filter — widen spread or block when order flow is one-sided
        vpin_spread_mult = 1.0
        vpin_level_str = "OFF"
        vpin_value = 0.0
        if self.vpin_tracker:
            vpin_spread_mult, vpin_level_str, vpin_value = self.vpin_tracker.get_spread_multiplier(
                market["token_up"], market["token_down"])
            if vpin_level_str == "TOXIC":
                self.logger.info(
                    "  VPIN TOXIC | {} | VPIN: {:.2f} > {:.2f} | Spread x{:.1f} | "
                    "Order flow too one-sided".format(
                        window_id, vpin_value, self.config.vpin_threshold,
                        vpin_spread_mult))
                # Track VPIN blocks in analytics
                if "vpin_blocks" not in self.engine.hedge_analytics:
                    self.engine.hedge_analytics["vpin_blocks"] = 0
                self.engine.hedge_analytics["vpin_blocks"] += 1
            elif vpin_level_str == "ELEVATED":
                self.logger.info(
                    "  VPIN ELEVATED | {} | VPIN: {:.2f} | Spread x{:.1f}".format(
                        window_id, vpin_value, vpin_spread_mult))
                if "vpin_widens" not in self.engine.hedge_analytics:
                    self.engine.hedge_analytics["vpin_widens"] = 0
                self.engine.hedge_analytics["vpin_widens"] += 1

        # V15.6: Dynamic spread — scale base spread with realized volatility
        dynamic_base_spread = self.config.mm_base_spread
        if self.config.dynamic_spread_enabled:
            if vol_level == "EXTREME":
                dynamic_base_spread = self.config.mm_base_spread * self.config.dynamic_spread_vol_extreme
            elif vol_level == "HIGH":
                dynamic_base_spread = self.config.mm_base_spread * self.config.dynamic_spread_vol_high
            elif vol_level == "MEDIUM":
                dynamic_base_spread = self.config.mm_base_spread * self.config.dynamic_spread_vol_medium
            else:
                dynamic_base_spread = self.config.mm_base_spread * self.config.dynamic_spread_vol_floor
        # Apply VPIN multiplier on top of volatility-adjusted spread
        # V15.7: Cap combined multiplier to prevent over-widening
        vol_mult = dynamic_base_spread / self.config.mm_base_spread if self.config.mm_base_spread > 0 else 1.0
        combined_mult = vol_mult * vpin_spread_mult
        if combined_mult > self.config.spread_multiplier_cap:
            capped_mult = self.config.spread_multiplier_cap
            self.logger.info(
                "  SPREAD CAP | {} | vol={:.1f}x vpin={:.1f}x combined={:.1f}x > cap {:.1f}x | "
                "Using capped {:.1f}x".format(
                    window_id, vol_mult, vpin_spread_mult, combined_mult,
                    self.config.spread_multiplier_cap, capped_mult))
            dynamic_base_spread = self.config.mm_base_spread * capped_mult
        else:
            dynamic_base_spread *= vpin_spread_mult

        # V15.1-12: Relaxed directional filter — CL-DN/CL-UP now only block
        # at STRONG confidence (>80%). At 60-80% (CL- prefix), the skew already
        # adjusts prices to favor the predicted side, which is sufficient protection.
        # This dramatically improves fill rate on 15m crypto windows where CL-UP/CL-DN
        # triggers frequently at 70% confidence, causing most markets to be skipped.
        is_strong_down = trend_label.startswith("STRONG DN")
        is_strong_up = trend_label.startswith("STRONG UP")
        if spread_down:
            result = self.reward_optimizer.optimal_distance_for_pair(
                spread_up, spread_down or {}, midpoint)
            optimal_d, reward_score, pair_cost, pair_profit = result
        else:
            optimal_d = None
        if optimal_d is None:
            optimal_d = dynamic_base_spread  # V15.6: Use dynamic spread instead of fixed
            if vol_level == "HIGH":
                optimal_d = dynamic_base_spread * 1.5
            elif vol_level == "MEDIUM":
                optimal_d = dynamic_base_spread * 1.2
            buy_up = midpoint - optimal_d
            buy_down = (1.0 - midpoint) - optimal_d
            if buy_up > 0.02 and buy_down > 0.02:
                fee_u = self.fee_calc._interp_fee_per_share(buy_up)
                fee_d = self.fee_calc._interp_fee_per_share(buy_down)
                pair_cost = buy_up + buy_down + fee_u + fee_d
                pair_profit = 1.0 - pair_cost
                reward_score = self.reward_optimizer.reward_score(spread, optimal_d)
            else:
                return
        else:
            if vol_level == "HIGH":
                wider_d = optimal_d * 1.5
                buy_up = midpoint - wider_d + skew
                buy_down = (1.0 - midpoint) - wider_d - skew
                if buy_up > 0.02 and buy_down > 0.02:
                    fee_u = self.fee_calc._interp_fee_per_share(buy_up)
                    fee_d = self.fee_calc._interp_fee_per_share(buy_down)
                    new_pair_cost = buy_up + buy_down + fee_u + fee_d
                    if 1.0 - new_pair_cost >= self.config.pair_min_profit:
                        optimal_d = wider_d
                        pair_cost = new_pair_cost
                        pair_profit = 1.0 - new_pair_cost
                        reward_score = self.reward_optimizer.reward_score(spread, optimal_d)
        # Fix 3: Enforce optimal_d >= dynamic_base_spread (V15.6: vol+VPIN adjusted)
        # This ensures the bot never places orders closer to midpoint than the
        # dynamically adjusted minimum spread (base * vol_mult * vpin_mult).
        if optimal_d < dynamic_base_spread:
            self.logger.debug(
                "  SPREAD FLOOR | {} | d={:.3f} < min {:.3f} (base {:.3f} x vol x vpin), using min".format(
                    window_id, optimal_d, dynamic_base_spread, self.config.mm_base_spread))
            optimal_d = dynamic_base_spread

        # V15.8: Comprehensive spread decomposition log
        # Records every spread component for post-session analysis.
        _pre_grad_d = optimal_d  # Save pre-graduated value for logging

        # V15.7: Graduated spread near window close
        # Linearly widen spread as time_remaining decreases from start_secs to stop_secs.
        # This replaces the binary 60s cutoff with a smooth ramp:
        #   At start_secs: multiplier = 1.0 (no change)
        #   At stop_secs:  multiplier = graduated_spread_max_multiplier (e.g., 3.0x)
        #   Below stop_secs: don't quote (handled at top of execute())
        if self.config.graduated_spread_enabled:
            tf = market.get("timeframe", "5m")
            if tf == "15m":
                grad_start = self.config.graduated_spread_start_secs_15m
            else:
                grad_start = self.config.graduated_spread_start_secs_5m
            grad_stop = self.config.graduated_spread_stop_secs
            if time_remaining < grad_start:
                # Linear interpolation: 1.0 at grad_start, max_mult at grad_stop
                progress = (grad_start - time_remaining) / (grad_start - grad_stop)
                progress = min(1.0, max(0.0, progress))  # Clamp to [0, 1]
                grad_mult = 1.0 + progress * (self.config.graduated_spread_max_multiplier - 1.0)
                optimal_d *= grad_mult
                self.logger.info(
                    "  GRAD SPREAD | {} | {:.0f}s rem | progress={:.0%} | "
                    "spread x{:.2f} | d={:.4f}".format(
                        window_id, time_remaining, progress, grad_mult, optimal_d))
                # Track graduated spread activations
                if "graduated_spread_activations" not in self.engine.hedge_analytics:
                    self.engine.hedge_analytics["graduated_spread_activations"] = 0
                self.engine.hedge_analytics["graduated_spread_activations"] += 1
        # V15.8: Log the full spread decomposition for every order placement
        grad_mult_actual = optimal_d / _pre_grad_d if _pre_grad_d > 0 else 1.0
        self.logger.info(
            "  SPREAD DECOMP | {} | base={:.3f} | dynSprd={:.3f} (vol={} x{:.1f}) | "
            "vpin={} x{:.1f} (val={:.2f}) | combined={:.3f} (cap={:.1f}) | "
            "grad=x{:.2f} | final_d={:.4f} | mid={:.3f} | skew={:+.3f} | "
            "levels={}".format(
                window_id,
                self.config.mm_base_spread,
                dynamic_base_spread / vpin_spread_mult if vpin_spread_mult > 0 else dynamic_base_spread,
                vol_level, vol_mult,
                vpin_level_str, vpin_spread_mult, vpin_value,
                dynamic_base_spread,
                self.config.spread_multiplier_cap,
                grad_mult_actual,
                optimal_d,
                midpoint, skew,
                self.config.mm_num_levels))

        buy_up_price = round(midpoint - optimal_d + skew, 2)
        buy_down_price = round((1.0 - midpoint) - optimal_d - skew, 2)
        if buy_up_price <= 0.02 or buy_down_price <= 0.02:
            return
        fee_up = self.fee_calc._interp_fee_per_share(buy_up_price)
        fee_down = self.fee_calc._interp_fee_per_share(buy_down_price)
        final_pair_cost = buy_up_price + buy_down_price + fee_up + fee_down
        final_pair_profit = 1.0 - final_pair_cost
        # V15.1-3: Epsilon to avoid floating point boundary rejection
        if final_pair_profit < self.config.pair_min_profit - 0.0001:
            self.logger.info(
                "  PAIR REJECT | {} | UP:{:.2f} + DN:{:.2f} + fees:{:.3f} = {:.3f} | "
                "Profit: ${:.4f} < min ${:.3f}".format(
                    window_id, buy_up_price, buy_down_price, fee_up + fee_down,
                    final_pair_cost, final_pair_profit, self.config.pair_min_profit))
            return
        if self.config.pair_sizing_equal_shares:
            cost_per_pair = buy_up_price + buy_down_price
            if cost_per_pair <= 0:
                return
            total_budget = self.config.mm_order_size * 2
            if final_pair_profit >= self.config.edge_premium_threshold:
                boosted = total_budget * self.config.edge_premium_size_mult
                # V15.5-FIX: Cap premium edge at max_position_per_market to prevent
                # budget overflow that causes one-sided fills when the second order
                # exceeds the per-market cap.
                max_budget = self.config.max_position_per_market
                total_budget = min(boosted, max_budget)
                self.logger.info(
                    "  PREMIUM EDGE | {} | {:.1%} >= {:.1%} | Budget ${:.0f} -> ${:.0f}{}".format(
                        window_id, final_pair_profit, self.config.edge_premium_threshold,
                        self.config.mm_order_size * 2, total_budget,
                        " (capped at max_position)" if total_budget < boosted else ""))
            num_pairs = total_budget / cost_per_pair
            num_pairs = max(5.0, num_pairs)
            up_size = num_pairs
            down_size = num_pairs
            guaranteed_profit = num_pairs * final_pair_profit
            self.logger.info(
                "  PAIR OK | {} | d={:.3f} | UP:{:.2f} + DN:{:.2f} = {:.3f} | "
                "Profit: ${:.3f}/pair x {:.0f} = ${:.2f} guaranteed | Reward: {:.1%} | Vol: {} | VPIN: {} ({:.2f})".format(
                    window_id, optimal_d, buy_up_price, buy_down_price,
                    final_pair_cost, final_pair_profit, num_pairs, guaranteed_profit,
                    reward_score, vol_level, vpin_level_str, vpin_value))
        else:
            up_size = max(5.0, self.config.mm_order_size / buy_up_price) if buy_up_price > 0.02 else 0
            down_size = max(5.0, self.config.mm_order_size / buy_down_price) if buy_down_price > 0.02 else 0
            self.logger.info(
                "  PAIR OK | {} | d={:.3f} | UP:{:.2f} + DN:{:.2f} = {:.3f} | "
                "Profit: ${:.3f}/pair | Reward: {:.1%} | Vol: {}".format(
                    window_id, optimal_d, buy_up_price, buy_down_price,
                    final_pair_cost, final_pair_profit, reward_score, vol_level))

        # V15.1-19 Filter B: Order Book Depth — check both sides have liquidity
        # We place MAKER (limit) orders, so we measure bid-side depth (activity near our price)
        # plus ask-side depth within 5c of midpoint to gauge overall market activity.
        if self.config.min_book_depth > 0:
            up_book = self.book_reader.get_book(market["token_up"])
            dn_book = self.book_reader.get_book(market["token_down"])
            for side_name, book, target_price in [
                    ("UP", up_book, buy_up_price), ("DN", dn_book, buy_down_price)]:
                if not book:
                    self.logger.info(
                        "  DEPTH GATE | {} | {} book unavailable | Skipping".format(
                            window_id, side_name))
                    return
                # Sum bid-side depth within 5c of our buy price (other buyers = market activity)
                bids = book.get("bids", [])
                asks = book.get("asks", [])
                depth_usd = 0.0
                for order in bids:
                    op = float(order["price"])
                    if abs(op - target_price) <= 0.05:
                        depth_usd += float(order["size"]) * op
                # Also count asks within 5c of midpoint (sellers = potential fills)
                for order in asks:
                    op = float(order["price"])
                    if op <= target_price + 0.05:
                        depth_usd += float(order["size"]) * op
                if depth_usd < self.config.min_book_depth:
                    self.logger.info(
                        "  DEPTH GATE | {} | {} depth ${:.2f} < min ${:.2f} | Skipping".format(
                            window_id, side_name, depth_usd, self.config.min_book_depth))
                    return

        # V15.1-2: Pre-check capital for FULL PAIR before placing either side
        total_pair_dollar_cost = (buy_up_price * up_size) + (buy_down_price * down_size)
        available_cap = self.engine.get_available_capital()
        total_exp_after = self.engine.total_exposure + total_pair_dollar_cost
        if total_pair_dollar_cost > available_cap:
            self.logger.info(
                "  PAIR CAPITAL SKIP | {} | Need ${:.2f} but only ${:.2f} available".format(
                    window_id, total_pair_dollar_cost, available_cap))
            return
        if total_exp_after > self.engine.config.max_total_exposure:
            self.logger.info(
                "  PAIR EXPOSURE SKIP | {} | Would be ${:.2f} exp > max ${:.2f}".format(
                    window_id, total_exp_after, self.engine.config.max_total_exposure))
            return

        # V15.9: Auto-pause guard — skip NEW window creation when auto-paused.
        # Existing windows (already in _window_first_placed) continue to be managed
        # for refreshes, but no new windows are opened.
        if (self.config.auto_pause_enabled and self._autopause_active
                and window_id not in self._window_first_placed):
            self.logger.info(
                "  AUTO-PAUSE SKIP | {} | New window blocked (trigger #{}, {} consec blocks)".format(
                    window_id, self._autopause_trigger_count,
                    self._autopause_consec_blocks))
            return

        for level in range(self.config.mm_num_levels):
            offset = optimal_d + (level * self.config.mm_level_spacing)
            up_price = round(midpoint - offset + skew, 2)
            down_price = round((1.0 - midpoint) - offset - skew, 2)
            if self.config.pair_sizing_equal_shares and level > 0:
                if up_price > 0.02 and down_price > 0.02:
                    lv_cost = up_price + down_price
                    total_budget_lv = self.config.mm_order_size * 2
                    lv_profit = 1.0 - lv_cost - fee_up - fee_down
                    if lv_profit >= self.config.edge_premium_threshold:
                        lv_boosted = total_budget_lv * self.config.edge_premium_size_mult
                        total_budget_lv = min(lv_boosted, self.config.max_position_per_market)
                    lv_pairs = max(5.0, total_budget_lv / lv_cost)
                    up_size = lv_pairs
                    down_size = lv_pairs
                else:
                    continue
            elif not self.config.pair_sizing_equal_shares:
                up_size = max(5.0, self.config.mm_order_size / up_price) if up_price > 0.02 else 0
                down_size = max(5.0, self.config.mm_order_size / down_price) if down_price > 0.02 else 0
            should_update_up = self.churn_manager.should_update_orders(
                window_id, market["token_up"], up_price, up_size) if up_size > 0 else False
            should_update_down = self.churn_manager.should_update_orders(
                window_id, market["token_down"], down_price, down_size) if down_size > 0 else False
            if not should_update_up and not should_update_down:
                self.logger.debug("  CHURN SKIP | {} | No significant change".format(window_id))
                self.last_refresh[window_id] = now
                return
            self.engine.cancel_window_orders(window_id, strategy_filter="mm")
            self.last_refresh[window_id] = now
            # --- PAIR-OR-SKIP GUARD ---
            # Both sides must be eligible; never place one-sided exposure
            can_place_up = (not is_strong_down and 0.02 <= up_price < midpoint and up_size > 0)
            can_place_down = (not is_strong_up and 0.02 <= down_price <= 0.98 and down_size > 0)
            # V15.1-12: Log when directional skew is active but not blocking
            if (trend_label.startswith("CL-DN") or trend_label.startswith("CL-UP")) and can_place_up and can_place_down:
                self.logger.debug(
                    "    SKEW ACTIVE | {} | {} | skew={:+.3f} (not blocking)".format(
                        window_id, trend_label, skew))
            if not can_place_up or not can_place_down:
                self.logger.info(
                    "    PAIR SKIP | {} | Can't fund both sides (up={} dn={})".format(
                        window_id, can_place_up, can_place_down))
                continue
            up_result = self.engine.place_order(
                market["token_up"], "BUY", up_price, up_size,
                window_id, "MM-L{}".format(level), "mm", is_taker=False)
            if not up_result:
                self.logger.info(
                    "    PAIR ABORT | {} | UP side failed, skipping DN".format(window_id))
                continue
            self.churn_manager.record_update(
                window_id, market["token_up"], up_price, up_size)
            dn_result = self.engine.place_order(
                market["token_down"], "BUY", down_price, down_size,
                window_id, "MM-L{}d".format(level), "mm", is_taker=False)
            if not dn_result:
                # DN failed — cancel the orphaned UP to prevent one-sided exposure
                self.logger.warning(
                    "    ORPHAN CANCEL | {} | DN rejected, cancelling UP {}".format(
                        window_id, up_result))
                self.engine.cancel_window_orders(window_id, strategy_filter="mm")
                continue
            self.churn_manager.record_update(
                window_id, market["token_down"], down_price, down_size)
            # V15.1-P5: Record first placement time — blocks churn refresh
            if window_id not in self._window_first_placed:
                self._window_first_placed[window_id] = time.time()
            est_reward = self.reward_optimizer.estimate_reward_per_hour(
                2, reward_score, (up_size + down_size) / 2)
            self._total_estimated_reward += est_reward * (self.config.cycle_interval / 3600)

    def cleanup_window(self, window_id):
        """V15.1-P5: Clean up tracking when a window expires."""
        self._window_first_placed.pop(window_id, None)

    def get_autopause_state(self):
        """V15.9: Return auto-pause state for CC reporting."""
        return {
            "active": self._autopause_active,
            "consec_blocks": self._autopause_consec_blocks,
            "consec_passes": self._autopause_consec_passes,
            "trigger_count": self._autopause_trigger_count,
        }

    def get_reward_stats(self):
        return {"total_estimated_reward": self._total_estimated_reward}


# -----------------------------------------------------------------
# Strategy 2: Late Sniper
# -----------------------------------------------------------------

class LateSniper:
    def __init__(self, config, engine, book_reader, price_feed, kelly, fee_calc, logger):
        self.config = config
        self.engine = engine
        self.book_reader = book_reader
        self.price_feed = price_feed
        self.kelly = kelly
        self.fee_calc = fee_calc
        self.logger = logger
        self.sniped_windows = set()

    def execute(self, market):
        if not self.config.sniper_enabled:
            return
        window_id = market["window_id"]
        # V15.1-19: Skip closed windows
        if window_id in self.engine.closed_windows:
            return
        # V15.1-15: Skip windows with existing fills
        if window_id in self.engine.filled_windows:
            return
        asset = market["asset"]
        now = time.time()
        time_remaining = market["end_time"] - now
        if time_remaining > self.config.sniper_time_window or time_remaining < 30:
            return
        if window_id in self.sniped_windows:
            return
        prediction = self.price_feed.predict_resolution(
            asset, market["timestamp"], market["end_time"])
        if not prediction:
            spread_up = self.book_reader.get_spread(market["token_up"])
            if not spread_up:
                return
            if spread_up["midpoint"] > self.config.sniper_min_probability:
                prediction = {
                    "direction": "UP", "confidence": spread_up["midpoint"],
                    "prob_up": spread_up["midpoint"],
                    "prob_down": 1 - spread_up["midpoint"],
                }
            elif spread_up["midpoint"] < (1 - self.config.sniper_min_probability):
                prediction = {
                    "direction": "DOWN",
                    "confidence": 1 - spread_up["midpoint"],
                    "prob_up": spread_up["midpoint"],
                    "prob_down": 1 - spread_up["midpoint"],
                }
            else:
                return
        if prediction["confidence"] < self.config.sniper_min_probability:
            return
        if prediction["direction"] == "UP":
            token_id = market["token_up"]
            spread = self.book_reader.get_spread(market["token_up"])
        else:
            token_id = market["token_down"]
            spread = self.book_reader.get_spread(market["token_down"])
        if not spread:
            return
        buy_price = spread["ask"]
        fee_per_share = self.fee_calc._interp_fee_per_share(buy_price)
        net_edge = prediction["confidence"] - buy_price - fee_per_share
        if net_edge < self.config.sniper_min_edge:
            return
        if buy_price > self.config.sniper_max_price:
            return
        dollar_size = self.config.sniper_size
        if self.kelly.enabled:
            ks = self.kelly.optimal_size(prediction["confidence"], buy_price, is_taker=True)
            if ks > 0:
                dollar_size = max(ks, self.config.sniper_size)
        size = max(5.0, dollar_size / buy_price)
        liq = self.book_reader.get_available_liquidity(token_id, "BUY", buy_price, size)
        if not liq["available"]:
            return
        self.logger.info(
            "\n  SNIPER | {} {} | {} @ {:.1%} | Buy @ ${:.2f} | "
            "Edge: {:.1%} | ${:.0f} | {:.0f}s left".format(
                asset.upper(), market["timeframe"].upper(),
                prediction["direction"], prediction["confidence"],
                buy_price, net_edge, dollar_size, time_remaining))
        result = self.engine.place_order(
            token_id, "BUY", buy_price, size, window_id,
            "SNIPE-{}".format(prediction["direction"]), "sniper", is_taker=True)
        if result:
            self.sniped_windows.add(window_id)


# -----------------------------------------------------------------
# Strategy 3: Combined Probability Arb
# -----------------------------------------------------------------

class CombinedProbArb:
    def __init__(self, config, engine, book_reader, fee_calc, logger):
        self.config = config
        self.engine = engine
        self.book_reader = book_reader
        self.fee_calc = fee_calc
        self.logger = logger
        self.last_scan = {}
        self.pending_arb_legs = []

    def execute(self, market):
        if not self.config.arb_enabled:
            return
        window_id = market["window_id"]
        # V15.1-19: Skip closed windows
        if window_id in self.engine.closed_windows:
            return
        # V15.1-15: Skip windows with existing fills
        if window_id in self.engine.filled_windows:
            return
        now = time.time()
        last = self.last_scan.get(window_id, 0)
        if now - last < self.config.arb_scan_interval:
            return
        self.last_scan[window_id] = now
        time_remaining = market["end_time"] - now
        if time_remaining < 60:
            return
        self._cleanup_stale_legs(now)
        spread_up = self.book_reader.get_spread(market["token_up"])
        spread_down = self.book_reader.get_spread(market["token_down"])
        if not spread_up or not spread_down:
            return
        ask_up = spread_up["ask"]
        ask_down = spread_down["ask"]
        fee_up = self.fee_calc._interp_fee_per_share(ask_up)
        fee_down = self.fee_calc._interp_fee_per_share(ask_down)
        total_cost = ask_up + ask_down + fee_up + fee_down
        profit_per_share = 1.0 - total_cost
        if profit_per_share < self.config.arb_min_profit:
            return
        if profit_per_share < self.config.min_pair_edge:
            return
        if profit_per_share >= 0.05:
            dollar_size = self.config.arb_max_size
        elif profit_per_share >= 0.03:
            dollar_size = self.config.arb_max_size * 0.7
        else:
            dollar_size = min(self.config.arb_max_size * 0.5, profit_per_share * 500)
        shares = max(5.0, dollar_size / total_cost)
        guaranteed = shares * profit_per_share
        self.logger.info(
            "\n  ARB FOUND | {} {} | Edge: {:.1%} | {:.0f} pairs x ${:.3f} = "
            "${:.2f} guaranteed | {:.0f}s left".format(
                market["asset"].upper(), market["timeframe"].upper(),
                profit_per_share, shares, profit_per_share, guaranteed,
                time_remaining))
        oid_up = self.engine.place_order(
            market["token_up"], "BUY", ask_up, shares,
            window_id, "ARB-UP", "arb", is_taker=True)
        oid_down = self.engine.place_order(
            market["token_down"], "BUY", ask_down, shares,
            window_id, "ARB-DN", "arb", is_taker=True)
        if oid_up and not oid_down:
            self.pending_arb_legs.append({"oid": oid_up, "window_id": window_id, "time": now})
        elif oid_down and not oid_up:
            self.pending_arb_legs.append({"oid": oid_down, "window_id": window_id, "time": now})

    def _cleanup_stale_legs(self, now):
        timeout = self.config.arb_leg_timeout
        stale = [l for l in self.pending_arb_legs if now - l["time"] > timeout]
        for leg in stale:
            self.engine.cancel_window_orders(leg["window_id"], strategy_filter="arb")
        self.pending_arb_legs = [l for l in self.pending_arb_legs if now - l["time"] <= timeout]


# -----------------------------------------------------------------
# Strategy 4: Contrarian Panic Fade
# -----------------------------------------------------------------

class ContrarianFade:
    def __init__(self, config, engine, book_reader, price_feed, fee_calc, logger):
        self.config = config
        self.engine = engine
        self.book_reader = book_reader
        self.price_feed = price_feed
        self.fee_calc = fee_calc
        self.logger = logger
        self.faded_windows = set()

    def execute(self, market):
        if not self.config.contrarian_enabled:
            return
        window_id = market["window_id"]
        # V15.1-19: Skip closed windows
        if window_id in self.engine.closed_windows:
            return
        # V15.1-15: Skip windows with existing fills
        if window_id in self.engine.filled_windows:
            return
        asset = market["asset"]
        now = time.time()
        time_remaining = market["end_time"] - now
        if time_remaining < self.config.contrarian_min_time:
            return
        if window_id in self.faded_windows:
            return
        spread_up = self.book_reader.get_spread(market["token_up"])
        spread_down = self.book_reader.get_spread(market["token_down"])
        if not spread_up or not spread_down:
            return
        mid_up = spread_up["midpoint"]
        mid_down = spread_down["midpoint"]
        panic_side = None
        if mid_up < (0.50 - self.config.contrarian_panic_threshold):
            panic_side = "UP"
        elif mid_down < (0.50 - self.config.contrarian_panic_threshold):
            panic_side = "DOWN"
        if not panic_side:
            return
        prediction = self.price_feed.predict_resolution(
            asset, market["timestamp"], market["end_time"])
        if prediction:
            if panic_side == "UP" and prediction["prob_up"] < 0.35:
                return
            if panic_side == "DOWN" and prediction["prob_down"] < 0.35:
                return
        token_id = market["token_up"] if panic_side == "UP" else market["token_down"]
        spread = spread_up if panic_side == "UP" else spread_down
        buy_price = spread["ask"]
        dollar_size = self.config.contrarian_size
        size = dollar_size / buy_price
        liq = self.book_reader.get_available_liquidity(token_id, "BUY", buy_price, size)
        if not liq["available"]:
            return
        self.logger.info(
            "\n  CONTRARIAN | {} {} | Fading {} panic | Buy @ ${:.2f}".format(
                asset.upper(), market["timeframe"].upper(), panic_side, buy_price))
        result = self.engine.place_order(
            token_id, "BUY", buy_price, size, window_id,
            "FADE-{}".format(panic_side), "contrarian", is_taker=True)
        if result:
            self.faded_windows.add(window_id)


# -----------------------------------------------------------------
# Main Bot (V15.1)
