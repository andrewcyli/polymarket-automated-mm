"""Core trading engine: order placement, fill detection, exposure tracking."""

import os
import time
import math
import json
import logging
import requests
from datetime import datetime, timezone

try:
    from web3 import Web3
    from eth_account import Account
except ImportError:
    pass

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY, SELL
except ImportError:
    pass

from bot_engine.constants import (
    PROXY_EXEC_ABI, ZERO_ADDR, CTF_FULL_ABI,
    CTF_ADDRESS, USDC_E_ADDRESS, _encode_abi,
)
from bot_engine.logging_setup import AuditLogger
from bot_engine.rpc import RPCRateLimiter


class TradingEngine:
    def __init__(self, config, fee_calc, logger, balance_checker=None):
        self.config = config
        self.fee_calc = fee_calc
        self.logger = logger
        self.audit = AuditLogger()
        self.balance_checker = balance_checker
        self.orders_by_window = {}
        self.active_orders = {}
        self.window_exposure = {}
        self.total_exposure = 0.0
        self.asset_exposure = {}
        self.daily_pnl = 0.0
        self.total_orders_placed = 0
        self.total_orders_filled = 0
        self.total_orders_cancelled = 0
        self.sniper_trades = 0
        self.arb_trades = 0
        self.contrarian_trades = 0
        self.mm_trades = 0
        self.trend_trades = 0
        self.known_windows = set()
        self.sim_engine = None
        self._is_up_token_cache = {}
        self._last_reset_date = None
        self.capital_deployed = 0.0
        self.capital_in_positions = 0.0
        self.total_capital_used = 0.0
        self.token_holdings = {}
        self.session_total_spent = 0.0
        self.window_direction_lock = {}
        self.orders_this_cycle = {}
        self.cycle_count = 0
        self.strategy_capital_used = {
            "mm": 0.0, "trend": 0.0, "sniper": 0.0, "arb": 0.0, "contrarian": 0.0
        }
        self._cached_exchange_balance = None
        self._balance_cache_time = 0
        self.window_fill_cost = {}
        self.starting_wallet_balance = None
        self.window_fill_tokens = {}
        self.expired_windows_pending_claim = {}
        self.window_metadata = {}
        self.window_fill_sides = {}
        self._pending_hedges = []
        self._market_cache = {}
        self.hedges_completed = 0
        self.hedges_skipped = 0
        # V15.9: Per-window resolution log for CC Analytics
        # {window_id: {resolution, time, time_to_close_sec, opposing_fill_price, ...}}
        self._resolution_log = {}
        # V15.1-25: Hedge & exit analytics tracker
        self.hedge_analytics = {
            "one_sided_fills": 0,       # Total one-sided fills detected
            "resolved_by_hedge": 0,     # Completed via hedge (any tier)
            "resolved_by_exit": 0,      # Completed via momentum exit sell
            "resolved_by_merge": 0,     # Completed via merge (both sides filled naturally)
            "resolved_abandoned": 0,    # Abandoned (expired without resolution)
            "resolved_by_t4_sell": 0,   # V15.2-T4: Resolved via last-resort sell of filled side
            "tier_counts": {"t1": 0, "t2": 0, "t3": 0, "t4": 0},  # Hedge completions per tier
            "tier_costs": {"t1": [], "t2": [], "t3": [], "t4": []},  # Combined cost per hedge per tier
            "tier_times": {"t1": [], "t2": [], "t3": [], "t4": []},  # Seconds to complete per tier
            "exit_profits": [],          # Profit per momentum exit (sell_price - buy_cost)
            "exit_hold_times": [],       # Hold time in seconds for each momentum exit
            "gate_blocks": 0,           # Total momentum gate blocks
            "gate_bypasses": 0,         # Total momentum gate bypasses
            "orphan_recoveries": 0,     # V15.1-28: Orphans recovered via re-pairing
            "orphan_sells": 0,          # V15.1-28: Orphans sold at market (fallback)
            "per_asset": {},            # Per-asset breakdown {asset: {hedges, exits, merges, abandoned, t4_sells}}
        }
        self.estimated_rewards_total = 0.0
        self.reward_snapshots = []
        self.window_edge = {}
        self.unredeemed_position_value = 0.0
        self.paired_windows = set()
        # V15.1-P5: Realized PnL tracking.
        # Only counts returns from resolved positions (merges + claims).
        # session_realized_returns = total $ returned from merges/claims
        # session_realized_pnl = returns - cost of those positions
        self.session_realized_returns = 0.0  # $ returned from merges + claims
        self.session_realized_cost = 0.0     # $ cost of positions that were merged/claimed
        # V15.1-15: Persistent filled window tracking.
        # Once a window has ANY fill, it's added here and never re-entered.
        # Only cleared on window expiry/claim or momentum exit.
        # This survives reconcile_capital_from_wallet() which resets window_fill_cost.
        self.filled_windows = set()  # {window_id, ...}
        self.window_entry_count = {}  # {window_id: int} — safety counter
        # V15.1-19: Closed windows — permanently block re-entry after momentum exit
        self.closed_windows = set()  # {window_id, ...}
        # V15.1-20: Held windows — windows with tokens still on-chain (not yet merged/claimed/sold).
        # Unlike filled_windows (cleared on expiry), held_windows persists until capital is recovered.
        # Used in concurrent window count to prevent unlimited window accumulation.
        self.held_windows = set()  # {window_id, ...}
        # Phase 3: Recently-cancelled orders buffer.
        # When cancel_window_orders() removes an order from active_orders,
        # the cancel on Polymarket is async. If the order filled before the
        # cancel took effect, the WS fill event arrives for an order_id no
        # longer in active_orders. This buffer preserves the order metadata
        # so the fill can still be processed correctly.
        self._recently_cancelled = {}  # {order_id: {**order_info, "cancelled_at": float}}
        self._bot_cancelled_orders = set()  # V15.4-FIX: Track orders explicitly cancelled by bot
        self._recently_cancelled_ttl = 900  # V15.1-21: 15 min to catch late fills (was 120s)

        if not config.dry_run and HAS_CLOB:
            try:
                temp_client = ClobClient(
                    config.host, key=config.private_key, chain_id=config.chain_id)
                creds = temp_client.create_or_derive_api_creds()
                self.logger.info("  API creds derived (key: {}...{})".format(
                    creds.api_key[:8], creds.api_key[-4:]))
            except Exception as e:
                self.logger.warning(f"  Failed to derive API creds: {e}")
                creds = ApiCreds(
                    api_key=config.api_key, api_secret=config.api_secret,
                    api_passphrase=config.api_passphrase)
            self.client = ClobClient(
                config.host, key=config.private_key, chain_id=config.chain_id,
                creds=creds, signature_type=config.signature_type,
                funder=config.proxy_wallet)
        else:
            self.client = None

    def register_window_metadata(self, market):
        wid = market["window_id"]
        self.window_metadata[wid] = {
            "slug": market.get("slug", ""),
            "condition_id": market.get("condition_id", ""),
            "token_up": market.get("token_up", ""),
            "token_down": market.get("token_down", ""),
            "asset": market.get("asset", ""),
            "end_time": market.get("end_time", 0),
        }
        self._market_cache[wid] = market
        if "edge" in market:
            self.window_edge[wid] = market["edge"]

    def check_daily_reset(self):
        today = datetime.now(timezone.utc).date()
        if self._last_reset_date is None:
            self._last_reset_date = today
        elif today > self._last_reset_date:
            self.logger.info(f"  New day ({today}) -- resetting daily P&L from ${self.daily_pnl:+.2f}")
            self.daily_pnl = 0.0
            self._last_reset_date = today

    def prune_stale_orders(self):
        now = time.time()
        max_age = self.config.stale_order_max_age
        stale = [oid for oid, info in self.active_orders.items()
                 if now - info.get("time", now) > max_age]
        for oid in stale:
            info = self.active_orders.get(oid)
            if info:
                wid = info.get("window_id", "")
                self._recently_cancelled[oid] = {
                    **info, "cancelled_at": time.time()}
                self._bot_cancelled_orders.add(oid)  # V15.4-FIX
                del self.active_orders[oid]
                if wid in self.orders_by_window:
                    self.orders_by_window[wid] = [
                        o for o in self.orders_by_window[wid] if o != oid]
                if self.sim_engine and oid in self.sim_engine.pending_orders:
                    del self.sim_engine.pending_orders[oid]
                self.total_orders_cancelled += 1
        if stale:
            self._recalc_exposure()

    def cleanup_expired_windows(self, active_markets, churn_manager=None):
        active_ids = {m["window_id"] for m in active_markets}
        expired = [w for w in self.known_windows if w not in active_ids]

        # ── V15.5-FIX3: REST check before cancel ──────────────────────────
        # Before cancelling orders on expired windows, check the exchange via
        # REST API to see if any orders have already filled. This catches fills
        # that Polymarket processed but whose WS notification hasn't arrived yet
        # (96% of fills are WS-RECOVERED due to 4-90s WS latency).
        exchange_open_ids = None
        if not self.config.dry_run and self.client and expired:
            try:
                open_orders = self.client.get_orders()
                if isinstance(open_orders, list):
                    exchange_open_ids = {o.get("id", o.get("orderID", "")) for o in open_orders}
            except Exception as e:
                self.logger.warning("  REST PRE-CANCEL CHECK FAILED | {}".format(str(e)[:100]))

        for wid in expired:
            oids = self.orders_by_window.pop(wid, [])

            # V15.5-FIX3: Check each order against REST before cancelling.
            # If an order is NOT on the exchange AND was NOT already processed,
            # it was filled — process it as a fill instead of cancelling.
            if exchange_open_ids is not None:
                for oid in list(oids):
                    if oid.startswith("DRY-"):
                        continue
                    if oid not in self.active_orders:
                        continue
                    if oid in exchange_open_ids:
                        continue  # Still open on exchange, will be cancelled below
                    # Order NOT on exchange = filled! Process it before cancel.
                    info = self.active_orders.pop(oid, None)
                    if not info:
                        continue
                    fill_price = info.get("price", 0)
                    fill_size = info.get("size", 0)
                    fill_side = info.get("side", "BUY")
                    fill_token = info.get("token_id", "")
                    self.record_fill(fill_token, fill_side, fill_price, fill_size)
                    if fill_side == "BUY":
                        cost = fill_price * fill_size
                        self.window_fill_cost[wid] = self.window_fill_cost.get(wid, 0) + cost
                        self.filled_windows.add(wid)
                        self.window_entry_count[wid] = self.window_entry_count.get(wid, 0) + 1
                        if wid not in self.window_fill_tokens:
                            self.window_fill_tokens[wid] = []
                        self.window_fill_tokens[wid].append({
                            "token_id": fill_token, "size": fill_size,
                            "price": fill_price,
                            "is_up": self._is_up_token_cache.get(fill_token),
                            "time": time.time(),
                        })
                        is_up = self._is_up_token_cache.get(fill_token)
                        side_label = "UP" if is_up else "DOWN"
                        if wid not in self.window_fill_sides:
                            self.window_fill_sides[wid] = {}
                        if side_label not in self.window_fill_sides[wid]:
                            self.window_fill_sides[wid][side_label] = []
                        self.window_fill_sides[wid][side_label].append({
                            "token_id": fill_token, "price": fill_price,
                            "size": fill_size, "time": time.time(),
                        })
                        sides = self.window_fill_sides.get(wid, {})
                        if "UP" in sides and "DOWN" in sides:
                            self.paired_windows.add(wid)
                        self.logger.info(
                            "  FILL [REST-PRE-CANCEL] | {} {} {:.1f} @ ${:.2f} | {}".format(
                                side_label, fill_token[:12] + "...",
                                fill_size, fill_price, wid))
                    oids.remove(oid)  # Don't cancel this order — it's already filled

            for oid in oids:
                if oid in self.active_orders:
                    self._recently_cancelled[oid] = {
                        **self.active_orders[oid], "cancelled_at": time.time()}
                    self._bot_cancelled_orders.add(oid)  # V15.4-FIX
                    del self.active_orders[oid]
                    self.total_orders_cancelled += 1
                if self.sim_engine and oid in self.sim_engine.pending_orders:
                    del self.sim_engine.pending_orders[oid]
            self.window_exposure.pop(wid, None)
            self.known_windows.discard(wid)
            self.window_edge.pop(wid, None)
            self.paired_windows.discard(wid)
            if churn_manager:
                churn_manager.cleanup_window(wid)
            fill_cost = self.window_fill_cost.pop(wid, 0)
            if fill_cost > 0 and not self.config.dry_run:
                self.capital_in_positions = max(0, self.capital_in_positions - fill_cost)
                self.unredeemed_position_value += fill_cost
                self._update_total_capital()
                self.logger.info("  Released ${:.2f} from expired {}".format(fill_cost, wid))
            fill_tokens = self.window_fill_tokens.pop(wid, [])
            meta = self.window_metadata.get(wid, {})
            if fill_tokens and not self.config.dry_run:
                self.expired_windows_pending_claim[wid] = {
                    "tokens": fill_tokens,
                    "condition_id": meta.get("condition_id", ""),
                    "slug": meta.get("slug", ""),
                    "token_up": meta.get("token_up", ""),
                    "token_down": meta.get("token_down", ""),
                    "end_time": meta.get("end_time", time.time()),
                    "expired_at": time.time(), "fill_cost": fill_cost,
                }
                sides = self.window_fill_sides.get(wid, {})
                self.logger.info("  CLAIM QUEUED | {} | sides: {} | cost ${:.2f}".format(
                    wid, "+".join(sorted(sides.keys())) if sides else "?", fill_cost))
            # V15.1-21/25: Only preserve window_fill_sides for momentum exit if:
            # 1. The window is truly one-sided (only 1 side filled, not paired)
            # 2. The window has NOT been merged (tokens still exist)
            # 3. The window is NOT pending claim (merge already consumed tokens)
            sides = self.window_fill_sides.get(wid, {})
            is_one_sided = len(sides) == 1 and wid not in self.paired_windows
            was_merged = wid in self.expired_windows_pending_claim
            if is_one_sided and not was_merged:
                # Check actual token holdings — only preserve if tokens exist
                side_fills = list(sides.values())[0] if sides else []
                token_id = side_fills[0].get("token_id", "") if side_fills else ""
                actual_held = self.token_holdings.get(token_id, {}).get("size", 0)
                if actual_held >= 1.0:
                    self.logger.info("  PRESERVING fill_sides for momentum exit | {} | sides: {} | held: {:.1f}".format(
                        wid, "+".join(sorted(sides.keys())), actual_held))
                else:
                    self.logger.info("  CLEANUP fill_sides | {} | sides: {} | tokens already consumed (held={:.1f})".format(
                        wid, "+".join(sorted(sides.keys())), actual_held))
                    self.window_fill_sides.pop(wid, None)
                    is_one_sided = False  # Allow cleanup below
            else:
                if was_merged and is_one_sided:
                    self.logger.info("  CLEANUP fill_sides | {} | window already merged".format(wid))
                self.window_fill_sides.pop(wid, None)
            self._market_cache.pop(wid, None)
            # V15.1-15/21: Only release filled_windows if NOT one-sided
            # (one-sided fills need to stay locked for momentum exit)
            if not is_one_sided:
                self.filled_windows.discard(wid)
                self.window_entry_count.pop(wid, None)
        self.known_windows = active_ids
        self._recalc_exposure()

    def get_available_capital(self):
        """V15.1-18: Bankroll = total capital the bot can invest this session.
        available = bankroll - capital_deployed (open orders) - capital_in_positions (filled, held)
        After merge/claim, capital_in_positions decreases, freeing up available capital.
        Also capped by actual wallet balance if available.
        """
        bankroll = self.config.kelly_bankroll
        available = max(0, bankroll - self.total_capital_used)
        if self.balance_checker and self.config.check_wallet_balance:
            wallet_bal = self.balance_checker.get_balance()
            if wallet_bal is not None:
                return min(wallet_bal, available)
        return available

    def get_strategy_budget(self, strategy):
        if not self.config.strategy_budget_enabled:
            return self.get_available_capital()
        bankroll = self.config.kelly_bankroll
        pct = self.config.strategy_budget_pct.get(strategy, 0.10)
        return bankroll * pct

    def get_strategy_available(self, strategy):
        if not self.config.strategy_budget_enabled:
            return self.get_available_capital()
        strategy_budget = self.get_strategy_budget(strategy)
        strategy_used = self.strategy_capital_used.get(strategy, 0)
        strategy_remaining = max(0, strategy_budget - strategy_used)
        global_remaining = self.get_available_capital()
        if strategy != "sniper" and self.config.sniper_reserved_min > 0:
            sniper_used = self.strategy_capital_used.get("sniper", 0)
            sniper_reserve_remaining = max(0, self.config.sniper_reserved_min - sniper_used)
            global_remaining = max(0, global_remaining - sniper_reserve_remaining)
        return min(strategy_remaining, global_remaining)

    def sync_exchange_balance(self):
        if not self.config.sync_balance_per_cycle:
            return
        now = time.time()
        if now - self._balance_cache_time < self.config.wallet_balance_cache_ttl:
            return
        if self.balance_checker:
            bal = self.balance_checker.get_balance()
            if bal is not None:
                self._cached_exchange_balance = bal
                self._balance_cache_time = now

    def reset_cycle_counters(self):
        self.orders_this_cycle = {}
        self.cycle_count += 1

    def reconcile_capital_from_wallet(self):
        if (len(self.window_exposure) == 0 and len(self.active_orders) == 0
                and self.capital_in_positions > 1.0):
            # V15.1-16: Do NOT zero capital_in_positions if we still hold tokens.
            # The old code zeroed it, which made the P&L calculation think we had
            # $0 in positions, triggering false loss stops (wallet down but tokens held).
            # Only release capital for windows that have been fully claimed/sold.
            filled_capital = sum(self.window_fill_cost.get(wid, 0)
                                 for wid in self.filled_windows)
            unfilled_capital = self.capital_in_positions - filled_capital
            if unfilled_capital > 1.0:
                self.logger.info("  RECONCILE: Releasing ${:.2f} unfilled capital "
                    "(keeping ${:.2f} in {} filled windows)".format(
                        unfilled_capital, filled_capital, len(self.filled_windows)))
                self.capital_in_positions = max(0, filled_capital)
            elif len(self.filled_windows) == 0:
                # No filled windows, safe to release all
                self.logger.info("  RECONCILE: No orders/fills, releasing ${:.2f}".format(
                    self.capital_in_positions))
                self.capital_in_positions = 0
            else:
                self.logger.debug("  RECONCILE: Keeping ${:.2f} in {} filled windows".format(
                    self.capital_in_positions, len(self.filled_windows)))
            # V15.1-15: Only clear window_fill_cost for windows NOT in filled_windows.
            surviving_fill_cost = {}
            for wid in self.filled_windows:
                if wid in self.window_fill_cost:
                    surviving_fill_cost[wid] = self.window_fill_cost[wid]
            self.window_fill_cost = surviving_fill_cost
            self._update_total_capital()
            return
        if self.balance_checker and self.config.check_wallet_balance:
            wallet_bal = self.balance_checker.get_balance()
            if wallet_bal is not None and self.capital_in_positions > 5.0:
                bankroll = self.config.kelly_bankroll
                expected_free = bankroll - self.capital_deployed - self.capital_in_positions
                surplus = wallet_bal - max(expected_free, 0)
                if surplus > 10.0:
                    release = min(surplus * 0.8, self.capital_in_positions)
                    if release > 5.0:
                        self.capital_in_positions = max(0, self.capital_in_positions - release)
                        self._update_total_capital()

    def get_position_value(self):
        """V15.8: Compute live position value with expired-market awareness.
        For ACTIVE markets: uses market price (bid/ask from market_cache).
        For EXPIRED markets: values at $0 (conservative). Winning tokens are
        tracked separately as unclaimed_winnings. The wallet already reflects
        claimed winnings, so counting expired tokens at market price double-counts.
        Returns total value of ACTIVE positions only.
        """
        return self.get_position_value_breakdown()["active_value"]

    def get_position_value_breakdown(self):
        """V15.8: Detailed position value with active vs expired breakdown.
        Returns dict with:
          active_value: value of tokens in live/tradeable markets
          expired_token_count: number of tokens in expired markets (worth $0 or claimable)
          expired_shares: total shares in expired markets
          unclaimed_est: estimated value of unclaimed winning tokens
          total_legacy: old-style total (for backward compat logging)
        """
        import time as _time
        now = _time.time()
        active_value = 0.0
        total_legacy = 0.0
        expired_token_count = 0
        expired_shares = 0.0
        unclaimed_est = 0.0

        # Build reverse lookup: token_id -> (window_id, end_time, side)
        token_window_map = {}
        # Check market_cache (active windows)
        for wid, market in self._market_cache.items():
            end_time = market.get("end_time", 0)
            for key, side in (("token_up", "UP"), ("token_down", "DOWN")):
                tid = market.get(key, "")
                if tid:
                    token_window_map[tid] = {"wid": wid, "end_time": end_time, "side": side}
        # Check expired_windows_pending_claim
        for wid, info in self.expired_windows_pending_claim.items():
            end_time = info.get("end_time", 0)
            winning_token = info.get("winning_token", "")
            for key, side in (("token_up", "UP"), ("token_down", "DOWN")):
                tid = info.get(key, "")
                if tid:
                    token_window_map[tid] = {
                        "wid": wid, "end_time": end_time, "side": side,
                        "winning_token": winning_token,
                        "resolved": info.get("resolved", False),
                    }
        # Check window_metadata for any remaining
        for wid, meta in self.window_metadata.items():
            end_time = meta.get("end_time", 0)
            for key, side in (("token_up", "UP"), ("token_down", "DOWN")):
                tid = meta.get(key, "")
                if tid and tid not in token_window_map:
                    token_window_map[tid] = {"wid": wid, "end_time": end_time, "side": side}

        for token_id, holding in self.token_holdings.items():
            size = holding.get("size", 0)
            if size <= 0:
                continue

            # Determine market price for legacy/active valuation
            price = None
            for wid, market in self._market_cache.items():
                if token_id == market.get("token_up"):
                    price = market.get("up_price", market.get("prob_up"))
                    break
                elif token_id == market.get("token_down"):
                    price = market.get("down_price", market.get("prob_down"))
                    break
            if price is None or price <= 0:
                cost = holding.get("cost", 0)
                price = (cost / size) if size > 0 and cost > 0 else 0.50
            total_legacy += size * price

            # Check if this token belongs to an expired market
            win_info = token_window_map.get(token_id)
            if win_info:
                end_time = win_info.get("end_time", 0)
                # 5-minute markets: expired if end_time + 5min < now
                # Use generous buffer (10 min) to avoid false positives
                if end_time > 0 and (end_time + 600) < now:
                    expired_token_count += 1
                    expired_shares += size
                    # If we know the winning token, estimate unclaimed value
                    winning_token = win_info.get("winning_token", "")
                    if winning_token == token_id:
                        unclaimed_est += size * 1.0  # Winner: $1.00/share
                    elif winning_token:
                        pass  # Loser: $0.00/share
                    else:
                        # Unknown resolution: estimate conservatively at $0
                        pass
                    continue  # Don't add to active_value

            # Active market: use market price
            active_value += size * price

        return {
            "active_value": round(active_value, 4),
            "expired_token_count": expired_token_count,
            "expired_shares": round(expired_shares, 1),
            "unclaimed_est": round(unclaimed_est, 4),
            "total_legacy": round(total_legacy, 4),
        }

    def get_live_pnl(self):
        """V15.8: Return dict with wallet_delta, portfolio_pnl, and total_pnl.
        wallet_delta = current_wallet - starting_wallet (hard fact, no estimation)
        portfolio_pnl = wallet_delta + active_held_value + unclaimed_est
        V15.8 FIX: Expired tokens are no longer counted at stale market price.
        Active positions use live market price. Unclaimed winnings estimated separately.
        """
        if self.starting_wallet_balance is None or not self.balance_checker:
            return None
        current = self.balance_checker.get_balance()
        if current is None:
            return None
        wallet_delta = current - self.starting_wallet_balance
        # V15.8: Use breakdown to separate active vs expired positions
        breakdown = self.get_position_value_breakdown()
        active_value = breakdown["active_value"]
        unclaimed_est = breakdown["unclaimed_est"]
        expired_count = breakdown["expired_token_count"]
        total_legacy = breakdown["total_legacy"]
        # V15.8: Corrected held_value = active positions + estimated unclaimed winnings
        held_value = active_value + unclaimed_est
        # V15.8: Portfolio P&L = wallet change + active positions + unclaimed winnings
        portfolio_pnl = wallet_delta + held_value
        # Total P&L also includes capital in open orders (not yet filled)
        total_pnl = wallet_delta + self.capital_deployed + held_value
        return {
            "wallet_delta": wallet_delta,
            "portfolio_pnl": portfolio_pnl,  # V15.8: CORRECTED P&L metric
            "total_pnl": total_pnl,
            "held_value": held_value,
            "capital_deployed": self.capital_deployed,
            "wallet_now": current,
            "wallet_start": self.starting_wallet_balance,
            "capital_in_positions": self.capital_in_positions,
            "session_realized_pnl": self.session_realized_returns - self.session_realized_cost,
            # V15.8: Position breakdown for CC dashboard
            "active_position_value": active_value,
            "unclaimed_est": unclaimed_est,
            "expired_token_count": expired_count,
            "expired_shares": breakdown["expired_shares"],
            "held_value_legacy": total_legacy,  # Old-style total for comparison
        }

    # V15.1-4: Verbose order rejection logging
    def place_order(self, token_id, side, price, size, window_id, label="",
                    strategy="mm", is_taker=False):
        if self.daily_pnl < -self.config.max_daily_loss:
            self.logger.info("    REJECT {} | {} | daily loss limit".format(label, window_id))
            return None
        # V15.7: Portfolio-based loss stop — check portfolio P&L (wallet + positions)
        if self.config.portfolio_loss_stop_enabled and side == "BUY":
            pnl_data = self.get_live_pnl()
            if pnl_data and isinstance(pnl_data, dict):
                port_pnl = pnl_data.get("portfolio_pnl", 0)
                bankroll = self.config.kelly_bankroll
                loss_limit = bankroll * self.config.portfolio_loss_stop_pct
                if port_pnl < -loss_limit:
                    self.logger.info(
                        "    REJECT {} | {} | PORTFOLIO LOSS STOP: ${:.2f} < -${:.2f} ({:.0f}% of bankroll)".format(
                            label, window_id, port_pnl, loss_limit,
                            self.config.portfolio_loss_stop_pct * 100))
                    return None
        price = max(0.01, min(0.99, round(price, 2)))
        size = max(1, round(size, 1))
        strat_window_key = "{}|{}".format(strategy, window_id)
        cycle_count = self.orders_this_cycle.get(strat_window_key, 0)
        if cycle_count >= self.config.max_orders_per_market:
            return None
        if side == "BUY":
            order_cost = price * size
            if is_taker:
                order_cost += self.fee_calc.fee_amount(price, size)
            if self.config.strategy_budget_enabled:
                strat_avail = self.get_strategy_available(strategy)
                if order_cost > strat_avail:
                    self.logger.info(
                        "    REJECT {} | {} | strategy budget ${:.2f} > avail ${:.2f}".format(
                            label, window_id, order_cost, strat_avail))
                    return None
            available = self.get_available_capital()
            if order_cost > available:
                self.logger.info(
                    "    REJECT {} | {} | cost ${:.2f} > available ${:.2f}".format(
                        label, window_id, order_cost, available))
                self.audit.order_rejected("capital_exceeded", token_id, side, price, size,
                                          window_id, strategy,
                                          {"cost": order_cost, "available": available})
                return None
            # V15.9-FIX: Hedge buys are recovery operations, not new exposure.
            # They MUST bypass the window spend cap, total exposure, and asset
            # exposure checks. The window already has one-sided risk; the hedge
            # reduces net risk by completing the pair. Without this bypass,
            # hedges are rejected whenever filled_cost + hedge_cost > cap
            # (which is always true when the opposing ask > $0.50).
            is_hedge_order = label.startswith("HEDGE-")
            wexp = self.window_exposure.get(window_id, 0)
            # V15.1-16: Include fill costs in per-market budget check.
            # window_exposure only tracks OPEN orders; after a fill the order is
            # removed, so hedge/re-entry buys bypass the cap. Adding
            # window_fill_cost gives the TRUE total spend on this window.
            wfill = self.window_fill_cost.get(window_id, 0)
            total_window_spend = wexp + wfill
            # Allow 2% tolerance on per-market cap to handle rounding in equal-shares sizing
            # This scales automatically when max_position_per_market changes
            market_cap_with_tolerance = self.config.max_position_per_market * 1.02
            if total_window_spend + order_cost > market_cap_with_tolerance:
                if is_hedge_order:
                    self.logger.info(
                        "    HEDGE BYPASS | {} | window spend ${:.2f}(open)+${:.2f}(filled)+${:.2f}(new) > max ${:.2f} — allowed (recovery)".format(
                            window_id, wexp, wfill, order_cost,
                            market_cap_with_tolerance))
                else:
                    self.logger.info(
                        "    REJECT {} | {} | window spend ${:.2f}(open)+${:.2f}(filled)+${:.2f}(new) > max ${:.2f}".format(
                            label, window_id, wexp, wfill, order_cost,
                            market_cap_with_tolerance))
                    return None
            if self.total_exposure + order_cost > self.config.max_total_exposure:
                if is_hedge_order:
                    self.logger.info(
                        "    HEDGE BYPASS | {} | total exp ${:.2f}+${:.2f} > max ${:.2f} — allowed (recovery)".format(
                            window_id, self.total_exposure, order_cost,
                            self.config.max_total_exposure))
                else:
                    self.logger.info(
                        "    REJECT {} | {} | total exp ${:.2f}+${:.2f} > max ${:.2f}".format(
                            label, window_id, self.total_exposure, order_cost,
                            self.config.max_total_exposure))
                    return None
            asset = window_id.split("-")[0] if "-" in window_id else ""
            aexp = self.asset_exposure.get(asset, 0)
            max_asset = self.config.max_total_exposure * self.config.max_asset_exposure_pct
            if aexp + order_cost > max_asset:
                if is_hedge_order:
                    self.logger.info(
                        "    HEDGE BYPASS | {} | asset {} exp ${:.2f}+${:.2f} > max ${:.2f} — allowed (recovery)".format(
                            window_id, asset, aexp, order_cost, max_asset))
                else:
                    self.logger.info(
                        "    REJECT {} | {} | asset {} exp ${:.2f}+${:.2f} > max ${:.2f}".format(
                            label, window_id, asset, aexp, order_cost, max_asset))
                    return None
        elif side == "SELL":
            held = self.token_holdings.get(token_id, {}).get("size", 0)
            if held < size * 0.5 and not self.config.dry_run:
                return None
        if self.config.dry_run:
            order_id = "DRY-{}-{}".format(int(time.time() * 1000), self.total_orders_placed)
            fee_str = ""
            if is_taker:
                fee = self.fee_calc.fee_amount(price, size)
                fee_str = " (fee: ${:.3f})".format(fee)
            self.logger.info(
                "    [{:8s}] {:12s} {:4s} {:6.1f} @ ${:.2f} = ${:6.2f}{}".format(
                    strategy.upper(), label, side, size, price, price * size, fee_str))
            self._track_order(order_id, window_id, side, price, size, token_id, strategy)
            self.audit.order_placed(order_id, token_id, side, price, size,
                                    window_id, strategy, is_taker, dry_run=True)
            if self.sim_engine:
                self.sim_engine.record_order(order_id, {
                    "window_id": window_id, "side": side, "price": price,
                    "size": size, "token_id": token_id, "strategy": strategy,
                    "is_up_token": self._is_up_token_cache.get(token_id),
                    "is_taker": is_taker,
                })
            return order_id
        try:
            order_side = BUY if side == "BUY" else SELL
            order_args = OrderArgs(price=price, size=size, side=order_side, token_id=token_id)
            signed = self.client.create_order(order_args)
            result = self.client.post_order(signed, OrderType.GTC)
            if isinstance(result, dict) and result.get("success"):
                oid = result["orderID"]
                self.logger.info(
                    "    [{:8s}] {:12s} {:4s} {:6.1f} @ ${:.2f} = ${:6.2f} | {}...".format(
                        strategy.upper(), label, side, size, price, price * size, oid[:16]))
                self._track_order(oid, window_id, side, price, size, token_id, strategy)
                self.audit.order_placed(oid, token_id, side, price, size,
                                        window_id, strategy, is_taker, dry_run=False)
                # V15.1-8: Taker orders fill immediately on Polymarket (FOK).
                # Record the fill right away so token_holdings is up-to-date
                # for merge/claim logic within the same cycle.
                if is_taker and side == "BUY":
                    fee = self.fee_calc.fee_amount(price, size)
                    self.record_fill(token_id, side, price, size, fee)
                    # Also update fill tracking for merge/claim pipeline
                    if window_id not in self.window_fill_tokens:
                        self.window_fill_tokens[window_id] = []
                    self.window_fill_tokens[window_id].append({
                        "token_id": token_id, "size": size,
                        "price": price,
                        "is_up": self._is_up_token_cache.get(token_id),
                        "time": time.time(),
                    })
                    self.window_fill_cost[window_id] = (
                        self.window_fill_cost.get(window_id, 0) + price * size)
                    # V15.1-15: Mark window as filled — prevents re-entry
                    self.filled_windows.add(window_id)
                    self.held_windows.add(window_id)  # V15.1-20: track until capital recovered
                    self.window_entry_count[window_id] = self.window_entry_count.get(window_id, 0) + 1
                    is_up = self._is_up_token_cache.get(token_id)
                    side_label = "UP" if is_up else "DOWN"
                    if window_id not in self.window_fill_sides:
                        self.window_fill_sides[window_id] = {}
                    if side_label not in self.window_fill_sides[window_id]:
                        self.window_fill_sides[window_id][side_label] = []
                    self.window_fill_sides[window_id][side_label].append({
                        "token_id": token_id, "price": price,
                        "size": size, "time": time.time(),
                    })
                    sides = self.window_fill_sides.get(window_id, {})
                    if "UP" in sides and "DOWN" in sides:
                        self.paired_windows.add(window_id)
                    # Remove from active_orders so check_fills doesn't double-count
                    if oid in self.active_orders:
                        del self.active_orders[oid]
                    if window_id in self.orders_by_window:
                        self.orders_by_window[window_id] = [
                            o for o in self.orders_by_window[window_id] if o != oid]
                    self._recalc_exposure()
                    self.logger.info("    TAKER FILL (immediate) | {} {} {:.1f} @ ${:.2f} | {}".format(
                        side, token_id[:12] + "...", size, price, window_id))
                return oid
            else:
                err_msg = (result.get("errorMsg", str(result))
                           if isinstance(result, dict) else str(result))
                self.logger.warning(f"    Order rejected: {err_msg}")
                self.audit.order_rejected("exchange_rejected", token_id, side, price, size,
                                          window_id, strategy, {"error": err_msg})
        except Exception as e:
            import traceback
            self.logger.error(f"    Order failed: {e}")
            self.logger.error(f"    Traceback: {traceback.format_exc()}")
        return None

    def record_fill(self, token_id, side, price, size, fee=0):
        self.total_orders_filled += 1
        if side == "BUY":
            if token_id not in self.token_holdings:
                self.token_holdings[token_id] = {"size": 0, "cost": 0}
            self.token_holdings[token_id]["size"] += size
            self.token_holdings[token_id]["cost"] += price * size + fee
            self.capital_in_positions += price * size + fee
            self.session_total_spent += price * size + fee
        elif side == "SELL":
            if token_id in self.token_holdings:
                self.token_holdings[token_id]["size"] = max(
                    0, self.token_holdings[token_id]["size"] - size)
            self.capital_in_positions = max(0, self.capital_in_positions - price * size)
        self.audit.fill_recorded(token_id, side, price, size, fee)
        self._update_total_capital()

    def record_claim(self, amount):
        self.capital_in_positions = max(0, self.capital_in_positions - amount)
        self.session_total_spent = max(0, self.session_total_spent - amount)
        self.unredeemed_position_value = max(0, self.unredeemed_position_value - amount)
        self._update_total_capital()

    def _update_total_capital(self):
        self.total_capital_used = self.capital_deployed + self.capital_in_positions

    def check_fills(self):
        if self.config.dry_run or not self.client:
            return 0
        filled = 0
        try:
            open_orders = self.client.get_orders()
            open_ids = set()
            if isinstance(open_orders, list):
                open_ids = {o.get("id", o.get("orderID", "")) for o in open_orders}
            # Check active orders + recently cancelled orders
            all_tracked = list(self.active_orders.keys()) + list(self._recently_cancelled.keys())
            for oid in all_tracked:
                if oid.startswith("DRY-"):
                    continue
                if oid not in open_ids:
                    info = self.active_orders.pop(oid, None)
                    recovered = False
                    if not info:
                        info = self._recently_cancelled.pop(oid, None)
                        if info:
                            recovered = True
                    if info:
                        wid = info.get("window_id", "")
                        if wid in self.orders_by_window:
                            self.orders_by_window[wid] = [
                                o for o in self.orders_by_window[wid] if o != oid]
                        fill_price = info.get("price", 0)
                        fill_size = info.get("size", 0)
                        fill_side = info.get("side", "BUY")
                        fill_token = info.get("token_id", "")
                        self.record_fill(fill_token, fill_side, fill_price, fill_size)
                        if fill_side == "BUY":
                            cost = fill_price * fill_size
                            self.window_fill_cost[wid] = (
                                self.window_fill_cost.get(wid, 0) + cost)
                            # V15.1-15: Mark window as filled — prevents re-entry
                            self.filled_windows.add(wid)
                            self.held_windows.add(wid)  # V15.1-20: track until capital recovered
                            self.window_entry_count[wid] = self.window_entry_count.get(wid, 0) + 1
                            if wid not in self.window_fill_tokens:
                                self.window_fill_tokens[wid] = []
                            self.window_fill_tokens[wid].append({
                                "token_id": fill_token, "size": fill_size,
                                "price": fill_price,
                                "is_up": self._is_up_token_cache.get(fill_token),
                                "time": time.time(),
                            })
                            is_up = self._is_up_token_cache.get(fill_token)
                            side_label = "UP" if is_up else "DOWN"
                            if wid not in self.window_fill_sides:
                                self.window_fill_sides[wid] = {}
                            if side_label not in self.window_fill_sides[wid]:
                                self.window_fill_sides[wid][side_label] = []
                            self.window_fill_sides[wid][side_label].append({
                                "token_id": fill_token, "price": fill_price,
                                "size": fill_size, "time": time.time(),
                            })
                            sides = self.window_fill_sides.get(wid, {})
                            if "UP" in sides and "DOWN" in sides:
                                self.paired_windows.add(wid)
                            elif self.config.hedge_completion_enabled:
                                # V15.5-FIX: Guard against phantom hedges on already-merged windows.
                                # If a late fill arrives after cleanup_expired_windows already merged
                                # and claimed the window, do NOT create a pending hedge — the tokens
                                # are already resolved. Creating a hedge here would buy NEW tokens
                                # that become orphans themselves.
                                if wid in self.expired_windows_pending_claim or wid in self.closed_windows:
                                    self.logger.info(
                                        "  HEDGE SKIP (ALREADY RESOLVED) | {} | {} | "
                                        "Window already merged/claimed, skipping phantom hedge".format(
                                            wid, side_label))
                                else:
                                    # V15.3-FIX: Store token_up/token_down + end_time in hedge entry
                                    # so hedge tiers can work even after cleanup_expired_windows
                                    # removes the window from _market_cache.
                                    _hedge_market = self._market_cache.get(wid, {})
                                    _hedge_meta = self.window_metadata.get(wid, {})
                                    self._pending_hedges.append({
                                        "window_id": wid, "filled_side": side_label,
                                        "filled_price": fill_price,
                                        "filled_size": fill_size,
                                        "filled_token": fill_token,
                                        "time": time.time(),
                                        "token_up": _hedge_market.get("token_up", _hedge_meta.get("token_up", "")),
                                        "token_down": _hedge_market.get("token_down", _hedge_meta.get("token_down", "")),
                                        "end_time": _hedge_market.get("end_time", _hedge_meta.get("end_time", 0)),
                                        "interval": _hedge_market.get("interval", 300 if "-5m-" in wid else 900),
                                    })
                                    # V15.1-25: Track one-sided fill for analytics
                                    self.hedge_analytics["one_sided_fills"] += 1
                                    asset = wid.split("-")[0] if "-" in wid else "unknown"
                                    if asset not in self.hedge_analytics["per_asset"]:
                                        self.hedge_analytics["per_asset"][asset] = {
                                            "one_sided": 0, "hedges": 0, "exits": 0,
                                            "merges": 0, "abandoned": 0, "t4_sells": 0
                                        }
                                    self.hedge_analytics["per_asset"][asset]["one_sided"] += 1
                        filled += 1
                        tag = "REST-RECOVERED" if recovered else "REST"
                        self.logger.info("  FILL [{}] | {} {} {:.1f} @ ${:.2f} | {}".format(
                            tag, fill_side, fill_token[:12] + "...",
                            fill_size, fill_price, wid))
            if filled:
                self._recalc_exposure()
        except Exception as e:
            self.logger.debug(f"  Fill check error: {e}")
        return filled

    def process_hedge_completions(self, book_reader, vol_tracker=None):
        if not self.config.hedge_completion_enabled:
            return 0
        now = time.time()
        completed = 0
        # V15.2: Percentage-based tiered hedge pricing.
        # config.hedge_tiers is a list of (pct_remaining_threshold, max_combined_cost)
        # V15.8: hedge_tiers format is (pct_remaining_threshold, max_ask).
        # max_ask = the maximum opposing ask price the bot will pay to complete the hedge.
        # This is a direct price check — no fee calculation needed at the gate.
        # sorted by pct descending (T1=75% triggers first, T3=40% triggers last).
        # Tier triggers when % of window time remaining < threshold.
        hedge_tiers = getattr(self.config, 'hedge_tiers', [
            (75, 0.52), (55, 0.69), (40, 0.79)
        ])
        # Sort tiers by pct descending (highest pct = earliest trigger)
        hedge_tiers = sorted(hedge_tiers, key=lambda t: t[0], reverse=True)

        for hedge in list(self._pending_hedges):
            wid = hedge["window_id"]
            filled_side = hedge["filled_side"]
            filled_price = hedge["filled_price"]
            filled_size = hedge["filled_size"]
            elapsed = now - hedge["time"]

            # V15.5-FIX: Guard against phantom hedges on already-resolved windows.
            # If the window was already merged/claimed (e.g., both fills arrived but
            # fill_sides was cleaned between them), remove the hedge immediately.
            if wid in self.expired_windows_pending_claim or wid in self.closed_windows:
                self.logger.info(
                    "  HEDGE CANCEL (ALREADY RESOLVED) | {} | {} | "
                    "Window merged/claimed, removing phantom hedge".format(
                        wid, filled_side))
                self._pending_hedges.remove(hedge)
                continue

            # V15.3-FIX: Use hedge entry's stored data as PRIMARY source for timing
            # and token info. Falls back to _market_cache then window_metadata.
            # This ensures hedge tiers work even after cleanup_expired_windows
            # removes the window from _market_cache.
            market = self._market_cache.get(wid)
            # Timing data: prefer hedge entry (always available), then market, then metadata
            window_end = hedge.get("end_time", 0)
            window_duration = hedge.get("interval", 0)
            if not window_end:
                if market:
                    window_end = market.get("end_time", 0)
                    window_duration = market.get("interval", 900)
                else:
                    meta = self.window_metadata.get(wid, {})
                    window_end = meta.get("end_time", 0)
                    window_duration = 300 if "-5m-" in wid else 900
            if not window_duration:
                window_duration = 300 if "-5m-" in wid else 900

            # V15.2-FIX: window_end is the observation START (slug timestamp),
            # not the market close. Actual market close = window_end + window_duration.
            actual_market_close = window_end + window_duration
            time_remaining = max(0, actual_market_close - now)
            pct_remaining = (time_remaining / window_duration * 100) if window_duration > 0 else 0

            # Determine which tier applies based on % remaining
            active_tier = None
            active_tier_idx = -1
            for idx, (tier_pct, tier_cost) in enumerate(hedge_tiers):
                if pct_remaining < tier_pct:
                    active_tier = (tier_pct, tier_cost)
                    active_tier_idx = idx

            # Not yet reached first tier — skip for now
            if active_tier is None:
                continue

            # Check if already paired
            sides = self.window_fill_sides.get(wid, {})
            other_side = "DOWN" if filled_side == "UP" else "UP"
            if other_side in sides and len(sides[other_side]) > 0:
                self._pending_hedges.remove(hedge)
                continue

            # V15.3-FIX: Resolve other_token from hedge entry, market cache, or metadata
            # (no longer gated on 'if not market' which caused all hedges to silently fail)
            other_token = ""
            if market:
                other_token = market["token_up"] if other_side == "UP" else market["token_down"]
            else:
                # Use hedge entry's stored tokens (V15.3)
                other_token = hedge.get("token_up", "") if other_side == "UP" else hedge.get("token_down", "")
                if not other_token:
                    # Last resort: window_metadata
                    meta = self.window_metadata.get(wid, {})
                    other_token = meta.get("token_up", "") if other_side == "UP" else meta.get("token_down", "")
            if not other_token:
                # No token data available — abandon if expired
                if time_remaining <= 0:
                    self._pending_hedges.remove(hedge)
                    self.hedges_skipped += 1
                    self.hedge_analytics["resolved_abandoned"] += 1
                    asset = wid.split("-")[0] if "-" in wid else "unknown"
                    if asset not in self.hedge_analytics["per_asset"]:
                        self.hedge_analytics["per_asset"][asset] = {
                            "one_sided": 0, "hedges": 0, "exits": 0,
                            "merges": 0, "abandoned": 0, "t4_sells": 0
                        }
                    self.hedge_analytics["per_asset"][asset]["abandoned"] += 1
                    # V15.9: Log abandoned resolution for CC Analytics
                    self._resolution_log[wid] = {
                        "resolution": "abandoned",
                        "time": time.time(),
                        "time_to_close_sec": time.time() - hedge["time"],
                    }
                    self.logger.warning(
                        "  HEDGE ABANDON (NO TOKEN) | {} | No token data for {} side".format(
                            wid, other_side))
                continue
            spread = book_reader.get_spread(other_token)
            if not spread:
                if time_remaining <= 0:
                    self._pending_hedges.remove(hedge)
                    self.hedges_skipped += 1
                    # V15.1-25: Track abandoned hedge
                    self.hedge_analytics["resolved_abandoned"] += 1
                    asset = wid.split("-")[0] if "-" in wid else "unknown"
                    if asset not in self.hedge_analytics["per_asset"]:
                        self.hedge_analytics["per_asset"][asset] = {
                            "one_sided": 0, "hedges": 0, "exits": 0,
                            "merges": 0, "abandoned": 0, "t4_sells": 0
                        }
                    self.hedge_analytics["per_asset"][asset]["abandoned"] += 1
                    # V15.9: Log abandoned resolution for CC Analytics
                    self._resolution_log[wid] = {
                        "resolution": "abandoned",
                        "time": time.time(),
                        "time_to_close_sec": time.time() - hedge["time"],
                    }
                continue

            other_ask = spread["ask"]
            other_bid = spread.get("bid", 0.0)  # V16: Need bid for sell value estimation
            fee_filled = self.fee_calc._interp_fee_per_share(filled_price)
            fee_other = self.fee_calc._interp_fee_per_share(other_ask)
            total_cost = filled_price + other_ask + fee_filled + fee_other
            profit_per_share = 1.0 - total_cost

            # ── V16: Smart Exit Engine ──────────────────────────────────
            # When exit_mode="smart", use the scoring model instead of tier waterfall.
            # The smart engine evaluates ask level, velocity, time pressure, and CEX
            # momentum to decide SELL vs HEDGE vs WAIT.
            if getattr(self.config, 'exit_mode', 'tiers') == 'smart' and hasattr(self, '_smart_exit_engine') and self._smart_exit_engine:
                asset = wid.split("-")[0] if "-" in wid else ""
                filled_token = hedge.get("filled_token", "")
                decision = self._smart_exit_engine.evaluate(
                    window_id=wid,
                    filled_side=filled_side,
                    filled_price=filled_price,
                    opposing_ask=other_ask,
                    opposing_bid=other_bid,
                    pct_remaining=pct_remaining,
                    window_duration=window_duration,
                    asset=asset,
                )
                action = decision["action"]
                score = decision["score"]
                reason = decision["reason"]

                if action == "WAIT":
                    self.logger.info(
                        "  SMART WAIT | {} | {} | score={:.3f} | {}".format(
                            wid, filled_side, score, reason))
                    continue

                elif action == "SELL":
                    # Smart sell — sell the held token (like T4 but score-driven)
                    if filled_token:
                        filled_spread = book_reader.get_spread(filled_token)
                        if filled_spread:
                            sell_bid = filled_spread["bid"]
                            actual_held = self.token_holdings.get(filled_token, {}).get("size", 0)
                            sell_size = min(filled_size, actual_held) if actual_held >= 1.0 else 0
                            if sell_size >= 1.0 and sell_bid >= 0.01:  # Accept any non-zero bid
                                fee_sell = self.fee_calc._interp_fee_per_share(sell_bid)
                                loss_per_share = filled_price - sell_bid + fee_sell
                                self.logger.info(
                                    "\n  SMART-SELL | {} | {} {:.0f} @ ${:.2f} -> bid ${:.2f} | "
                                    "Loss ${:.3f}/sh | score={:.3f} | {}".format(
                                        wid, filled_side, sell_size, filled_price, sell_bid,
                                        loss_per_share, score, reason))
                                cancelled = self.cancel_window_orders(wid)
                                if cancelled:
                                    self.logger.info(
                                        "  SMART-SELL CANCEL | {} | Cancelled {} orders".format(wid, cancelled))
                                result = self.place_order(
                                    filled_token, "SELL", sell_bid, sell_size,
                                    wid, "SMART-SELL", "mm", is_taker=True)
                                # Retry with approval if rejected
                                if not result and not self.config.dry_run and self.client:
                                    try:
                                        self.logger.info(
                                            "  SMART-SELL RETRY | {} | Attempting CLOB approval + retry".format(wid))
                                        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
                                        self.client.update_balance_allowance(
                                            BalanceAllowanceParams(
                                                asset_type=AssetType.CONDITIONAL,
                                                token_id=filled_token,
                                            )
                                        )
                                        import time as _time
                                        _time.sleep(2)
                                        result = self.place_order(
                                            filled_token, "SELL", sell_bid, sell_size,
                                            wid, "SMART-SELL-RETRY", "mm", is_taker=True)
                                    except Exception as e:
                                        self.logger.warning(
                                            "  SMART-SELL APPROVAL FAIL | {} | {}".format(wid, str(e)[:100]))
                                if result:
                                    self._pending_hedges.remove(hedge)
                                    completed += 1
                                    self.window_fill_sides.pop(wid, None)
                                    self.window_fill_cost.pop(wid, None)
                                    self.window_fill_tokens.pop(wid, None)
                                    self.closed_windows.add(wid)
                                    self.held_windows.discard(wid)
                                    self.window_entry_count.pop(wid, None)
                                    cost = filled_price * sell_size
                                    self.capital_in_positions = max(0, self.capital_in_positions - cost)
                                    self._update_total_capital()
                                    self.hedge_analytics["resolved_by_t4_sell"] += 1
                                    elapsed_se = time.time() - hedge["time"]
                                    self.hedge_analytics["tier_counts"]["smart_sell"] = self.hedge_analytics["tier_counts"].get("smart_sell", 0) + 1
                                    self._resolution_log[wid] = {
                                        "resolution": "smart_sell",
                                        "time": time.time(),
                                        "time_to_close_sec": elapsed_se,
                                        "opposing_fill_price": sell_bid,
                                        "smart_exit_score": score,
                                        "smart_exit_trigger": decision.get("trigger", ""),
                                        "window_pnl": -(loss_per_share * sell_size),
                                    }
                                    self._smart_exit_engine.on_window_resolved(wid)
                                    self.logger.info(
                                        "  SMART-SELL OK | {} | {} {:.0f} @ ${:.2f}".format(
                                            wid, filled_side, sell_size, sell_bid))
                                    continue
                                else:
                                    self.logger.warning(
                                        "  SMART-SELL FAILED | {} | Sell rejected".format(wid))
                    # If sell failed or no token, fall through to abandon check
                    if time_remaining <= 0:
                        self._pending_hedges.remove(hedge)
                        self.hedges_skipped += 1
                        self.hedge_analytics["resolved_abandoned"] += 1
                        asset = wid.split("-")[0] if "-" in wid else "unknown"
                        if asset not in self.hedge_analytics["per_asset"]:
                            self.hedge_analytics["per_asset"][asset] = {
                                "one_sided": 0, "hedges": 0, "exits": 0,
                                "merges": 0, "abandoned": 0, "t4_sells": 0
                            }
                        self.hedge_analytics["per_asset"][asset]["abandoned"] += 1
                        self._resolution_log[wid] = {
                            "resolution": "abandoned",
                            "time": time.time(),
                            "time_to_close_sec": time.time() - hedge["time"],
                            "smart_exit_score": score,
                        }
                        self._smart_exit_engine.on_window_resolved(wid)
                    continue

                elif action == "HEDGE":
                    # Smart hedge — buy the opposing side (like T1/T2 but score-driven)
                    size = filled_size
                    self.logger.info(
                        "\n  SMART-HEDGE | {} | {} @ ${:.2f} | Buy {} @ ${:.2f} | "
                        "Pair: ${:.3f} | ${:+.3f}/sh | score={:.3f} | {}".format(
                            wid, filled_side, filled_price, other_side, other_ask,
                            total_cost, profit_per_share, score, reason))
                    result = self.place_order(
                        other_token, "BUY", other_ask, size,
                        wid, "SMART-HEDGE-{}".format(other_side), "mm", is_taker=True)
                    if result:
                        completed += 1
                        self.hedges_completed += 1
                        self.hedge_analytics["resolved_by_hedge"] += 1
                        self.hedge_analytics["tier_counts"]["smart_hedge"] = self.hedge_analytics["tier_counts"].get("smart_hedge", 0) + 1
                        elapsed_sh = time.time() - hedge["time"]
                        asset = wid.split("-")[0] if "-" in wid else "unknown"
                        if asset not in self.hedge_analytics["per_asset"]:
                            self.hedge_analytics["per_asset"][asset] = {
                                "one_sided": 0, "hedges": 0, "exits": 0,
                                "merges": 0, "abandoned": 0, "t4_sells": 0
                            }
                        self.hedge_analytics["per_asset"][asset]["hedges"] += 1
                        self._resolution_log[wid] = {
                            "resolution": "smart_hedge",
                            "time": time.time(),
                            "time_to_close_sec": elapsed_sh,
                            "opposing_fill_price": other_ask,
                            "smart_exit_score": score,
                            "smart_exit_trigger": decision.get("trigger", ""),
                            "window_pnl": profit_per_share * size,
                        }
                        self._smart_exit_engine.on_window_resolved(wid)
                    self._pending_hedges.remove(hedge)
                    continue
            # ── End V16 Smart Exit ──────────────────────────────────────

            tier_pct, tier_max_ask = active_tier
            is_last_tier = (active_tier_idx == len(hedge_tiers) - 1)

            # V15.8: Direct max_ask comparison — no fee calculation needed at gate
            if other_ask > tier_max_ask:
                # If we haven't exhausted all tiers, keep waiting for next tier
                if not is_last_tier and time_remaining > 0:
                    self.logger.info(
                        "  HEDGE WAIT T{} | {} | {} ask ${:.2f} > maxAsk ${:.2f} | "
                        "{:.0f}% remaining ({}s left) | src={}".format(
                            active_tier_idx + 1, wid,
                            other_side, other_ask, tier_max_ask,
                            pct_remaining, int(time_remaining),
                            "cache" if market else "hedge-entry"))
                    continue
                # ── V15.2-T4: Last Resort Sell ──────────────────────────────
                # All buy-tiers exhausted. Instead of abandoning, try to SELL
                # the filled side at market bid to recover capital with minimal loss.
                t4_pct = getattr(self.config, 'hedge_t4_sell_pct', 33.0)  # V15.5: sell-at-33%
                t4_min_bid = getattr(self.config, 'hedge_t4_min_bid', 0.15)  # V15.8: min bid to accept sell
                t4_enabled = getattr(self.config, 'hedge_t4_enabled', True)
                # V15.4-FIX: Removed time_remaining > 0 condition.
                # T4 must fire even after window expires because we still hold tokens.
                if t4_enabled and (pct_remaining < t4_pct or time_remaining <= 0):
                    filled_token = hedge.get("filled_token", "")
                    if filled_token:
                        filled_spread = book_reader.get_spread(filled_token)
                        if filled_spread:
                            sell_bid = filled_spread["bid"]
                            loss_per_share = filled_price - sell_bid
                            # Check actual token holdings before selling
                            actual_held = self.token_holdings.get(filled_token, {}).get("size", 0)
                            sell_size = min(filled_size, actual_held) if actual_held >= 1.0 else 0
                            # V15.8: Use min_bid instead of max_loss
                            if sell_size >= 1.0 and sell_bid >= t4_min_bid:
                                fee_sell = self.fee_calc._interp_fee_per_share(sell_bid)
                                net_loss = loss_per_share + fee_sell
                                self.logger.info(
                                    "\n  T4 LAST RESORT SELL | {} | {} {:.0f} @ ${:.2f} -> bid ${:.2f} | "
                                    "Loss ${:.3f}/sh (fee ${:.3f}) | Net ${:.3f}/sh | {:.0f}% rem ({:.0f}s left)".format(
                                        wid, filled_side, sell_size, filled_price, sell_bid,
                                        loss_per_share, fee_sell, net_loss, pct_remaining, time_remaining))
                                # Cancel any remaining orders on this window first
                                cancelled = self.cancel_window_orders(wid)
                                if cancelled:
                                    self.logger.info(
                                        "  T4 CANCEL | {} | Cancelled {} orders".format(wid, cancelled))
                                result = self.place_order(
                                    filled_token, "SELL", sell_bid, sell_size,
                                    wid, "T4-SELL", "mm", is_taker=True)
                                # Retry with approval if rejected (same pattern as momentum exit)
                                if not result and not self.config.dry_run and self.client:
                                    try:
                                        self.logger.info(
                                            "  T4 RETRY | {} | Attempting CLOB approval + retry".format(wid))
                                        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
                                        self.client.update_balance_allowance(
                                            BalanceAllowanceParams(
                                                asset_type=AssetType.CONDITIONAL,
                                                token_id=filled_token,
                                            )
                                        )
                                        import time as _time
                                        _time.sleep(2)
                                        result = self.place_order(
                                            filled_token, "SELL", sell_bid, sell_size,
                                            wid, "T4-SELL-RETRY", "mm", is_taker=True)
                                    except Exception as e:
                                        self.logger.warning(
                                            "  T4 APPROVAL FAIL | {} | {}".format(wid, str(e)[:100]))
                                if result:
                                    self._pending_hedges.remove(hedge)
                                    completed += 1
                                    # Clean up window state (same as momentum exit)
                                    self.window_fill_sides.pop(wid, None)
                                    self.window_fill_cost.pop(wid, None)
                                    self.window_fill_tokens.pop(wid, None)
                                    self.closed_windows.add(wid)
                                    self.held_windows.discard(wid)
                                    self.window_entry_count.pop(wid, None)
                                    cost = filled_price * sell_size
                                    self.capital_in_positions = max(0, self.capital_in_positions - cost)
                                    self._update_total_capital()
                                    # Track T4 analytics
                                    elapsed_t4 = time.time() - hedge["time"]
                                    self.hedge_analytics["resolved_by_t4_sell"] += 1
                                    self.hedge_analytics["tier_counts"]["t4"] += 1
                                    self.hedge_analytics["tier_costs"]["t4"].append(loss_per_share)
                                    self.hedge_analytics["tier_times"]["t4"].append(elapsed_t4)
                                    asset = wid.split("-")[0] if "-" in wid else "unknown"
                                    if asset not in self.hedge_analytics["per_asset"]:
                                        self.hedge_analytics["per_asset"][asset] = {
                                            "one_sided": 0, "hedges": 0, "exits": 0,
                                            "merges": 0, "abandoned": 0, "t4_sells": 0
                                        }
                                    pa = self.hedge_analytics["per_asset"][asset]
                                    pa["t4_sells"] = pa.get("t4_sells", 0) + 1
                                    # V15.9: Log T4 sell resolution for CC Analytics
                                    self._resolution_log[wid] = {
                                        "resolution": "t4_sell",
                                        "time": time.time(),
                                        "time_to_close_sec": elapsed_t4,
                                        "opposing_fill_price": sell_bid,
                                        "hedge_tier_reached": 4,
                                        "window_pnl": -(net_loss * sell_size),
                                    }
                                    self.logger.info(
                                        "  T4 SOLD | {} | {} {:.0f} @ ${:.2f} | Loss ${:.3f}/sh".format(
                                            wid, filled_side, sell_size, sell_bid, net_loss))
                                    continue
                                else:
                                    self.logger.warning(
                                        "  T4 SELL FAILED | {} | Sell rejected, scheduling claim fallback".format(wid))
                                    # V15.7-BUG1: Schedule claim for stranded tokens
                                    meta = self.window_metadata.get(wid, {})
                                    cid = meta.get("condition_id", "")
                                    if cid and hasattr(self, '_claim_manager_ref'):
                                        self._claim_manager_ref.schedule_claim(
                                            cid, wid, meta.get("end_time", 0),
                                            slug=meta.get("slug", ""),
                                            sides="BOTH",
                                            cost=self.window_fill_cost.get(wid, 0))
                                        self.logger.info(
                                            "  T4 -> CLAIM | {} | Scheduled CTF-DIRECT fallback".format(wid))
                            elif sell_size < 1.0:
                                self.logger.info(
                                    "  T4 SKIP | {} | No tokens held (held={:.1f})".format(
                                        wid, actual_held))
                            else:
                                self.logger.info(
                                    "  T4 BID LOW | {} | bid ${:.2f} < minBid ${:.2f} (loss ${:.3f}/sh) | Abandoning".format(
                                        wid, sell_bid, t4_min_bid, loss_per_share))
                # ── End T4 ──────────────────────────────────────────────────
                # V15.5-FIX2: If T4 didn't fire because pct_remaining is still
                # above threshold but time remains, keep waiting for T4 trigger.
                # Only abandon if T4 was attempted and failed, or time is up.
                t4_enabled_check = getattr(self.config, 'hedge_t4_enabled', True)
                t4_pct_check = getattr(self.config, 'hedge_t4_sell_pct', 33.0)
                if t4_enabled_check and is_last_tier and pct_remaining >= t4_pct_check and time_remaining > 0:
                    self.logger.info(
                        "  HEDGE WAIT T4 | {} | {} ask ${:.2f} > maxAsk ${:.2f} | "
                        "{:.0f}% rem > {:.0f}% T4 trigger | Waiting for T4 ({:.0f}s left)".format(
                            wid, other_side, other_ask, tier_max_ask,
                            pct_remaining, t4_pct_check, time_remaining))
                    continue
                # If T4 didn't fire or failed, fall through to abandon
                self.logger.info(
                    "  HEDGE ASK CAP (ALL TIERS) | {} | {} @ ${:.2f} | {} ask ${:.2f} > maxAsk ${:.2f} | "
                    "{:.0f}% rem".format(
                        wid, filled_side, filled_price, other_side, other_ask,
                        tier_max_ask, pct_remaining))
                self._pending_hedges.remove(hedge)
                self.hedges_skipped += 1
                self.hedge_analytics["resolved_abandoned"] += 1
                asset = wid.split("-")[0] if "-" in wid else "unknown"
                if asset not in self.hedge_analytics["per_asset"]:
                    self.hedge_analytics["per_asset"][asset] = {
                        "one_sided": 0, "hedges": 0, "exits": 0,
                        "merges": 0, "abandoned": 0, "t4_sells": 0
                    }
                self.hedge_analytics["per_asset"][asset]["abandoned"] += 1
                # V15.9: Log abandoned resolution for CC Analytics
                self._resolution_log[wid] = {
                    "resolution": "abandoned",
                    "time": time.time(),
                    "time_to_close_sec": time.time() - hedge["time"],
                    "opposing_fill_price": other_ask,
                    "hedge_tier_reached": active_tier_idx + 1,
                }
                # V15.7-BUG1: Schedule claim for abandoned hedge windows
                meta = self.window_metadata.get(wid, {})
                cid = meta.get("condition_id", "")
                if cid and hasattr(self, '_claim_manager_ref'):
                    self._claim_manager_ref.schedule_claim(
                        cid, wid, meta.get("end_time", 0),
                        slug=meta.get("slug", ""),
                        sides="BOTH",
                        cost=self.window_fill_cost.get(wid, 0))
                    self.logger.info(
                        "  HEDGE ABANDONED -> CLAIM | {} | Scheduled CTF-DIRECT fallback".format(wid))
                continue

            # V15.8: min_profit_per_share gate REMOVED.
            # The max_ask per tier already controls profitability directly.
            # If you set T1 max_ask=$0.52, that implies profit. If T3 max_ask=$0.79,
            # you're explicitly accepting a loss to complete the pair.
            # The old min_profit_per_share was blocking T2/T3 from ever completing
            # hedges, making them dead code.

            size = filled_size
            tier_label = "T{}".format(active_tier_idx + 1)
            profit_str = "${:+.3f}/sh".format(profit_per_share)
            self.logger.info(
                "\n  HEDGE COMPLETE [{}] | {} | {} @ ${:.2f} | Buy {} @ ${:.2f} | "
                "Pair: ${:.3f} | {} | {:.0f} shares | {:.0f}% rem ({:.0f}s left)".format(
                    tier_label, wid, filled_side, filled_price, other_side, other_ask,
                    total_cost, profit_str, size, pct_remaining, time_remaining))
            result = self.place_order(
                other_token, "BUY", other_ask, size,
                wid, "HEDGE-{}".format(other_side), "mm", is_taker=True)
            if result:
                completed += 1
                self.hedges_completed += 1
                # V15.1-25: Track hedge tier analytics
                tier_key = "t{}".format(active_tier_idx + 1)
                self.hedge_analytics["resolved_by_hedge"] += 1
                self.hedge_analytics["tier_counts"][tier_key] += 1
                self.hedge_analytics["tier_costs"][tier_key].append(total_cost)
                elapsed = time.time() - hedge["time"]
                self.hedge_analytics["tier_times"][tier_key].append(elapsed)
                asset = wid.split("-")[0] if "-" in wid else "unknown"
                if asset not in self.hedge_analytics["per_asset"]:
                    self.hedge_analytics["per_asset"][asset] = {
                        "one_sided": 0, "hedges": 0, "exits": 0,
                        "merges": 0, "abandoned": 0, "t4_sells": 0
                    }
                self.hedge_analytics["per_asset"][asset]["hedges"] += 1
                # V15.9: Log resolution for CC Analytics
                self._resolution_log[wid] = {
                    "resolution": "hedge_buy",
                    "time": time.time(),
                    "time_to_close_sec": elapsed,
                    "opposing_fill_price": other_ask,
                    "hedge_tier_reached": active_tier_idx + 1,
                    "window_pnl": profit_per_share * size,
                }
            self._pending_hedges.remove(hedge)
        return completed

    def process_momentum_exits(self, book_reader):
        """V15.1-14: Momentum exit — sell one-sided fills if price rises >X%.

        When one side fills but the hedge hasn't completed after max_wait_secs,
        check if the filled token's current bid has risen above the fill price
        by momentum_exit_threshold. If so, sell the position for profit and
        cancel any remaining orders on that window.

        This captures directional moves when the market trends in our favor
        instead of waiting indefinitely for the other side to fill.

        V15.1-25 fixes:
        - Skip windows that have been merged (tokens no longer exist)
        - Check actual token_holdings before selling
        - Adjust sell size to min(fill_size, actual_held)
        - Retry with approval if CLOB rejects with 'not enough balance'
        """
        if not self.config.momentum_exit_enabled:
            return 0
        now = time.time()
        exits = 0
        stale_wids = []  # Track windows to clean up
        for wid, sides in list(self.window_fill_sides.items()):
            # Only check windows with exactly one side filled (not paired)
            if wid in self.paired_windows:
                continue
            if len(sides) != 1:
                continue
            # V15.1-25: Skip windows that have already been merged or claimed.
            # After merge, the tokens are consumed — nothing left to sell.
            if wid in self.expired_windows_pending_claim:
                # Check if the merge already happened for this window
                pending = self.expired_windows_pending_claim.get(wid, {})
                tokens = pending.get("tokens", [])
                # If there are tokens pending claim, the window was merged
                if tokens:
                    self.logger.info(
                        "  MOM-EXIT SKIP | {} | Window already merged/pending claim".format(wid))
                    stale_wids.append(wid)
                    continue
            filled_side = list(sides.keys())[0]
            fills = sides[filled_side]
            if not fills:
                continue
            # Use the earliest fill for timing
            earliest_fill = min(fills, key=lambda f: f.get("time", now))
            fill_time = earliest_fill.get("time", now)
            fill_price = earliest_fill.get("price", 0)
            fill_size = sum(f.get("size", 0) for f in fills)
            fill_token = earliest_fill.get("token_id", "")
            # V15.1-25: Check actual token holdings before attempting sell.
            # Tokens may have been consumed by merge, claimed, or partially sold.
            actual_held = self.token_holdings.get(fill_token, {}).get("size", 0)
            if actual_held < 1.0:
                # No tokens left — the position was already resolved (merged/claimed)
                self.logger.info(
                    "  MOM-EXIT SKIP | {} | {} | No tokens held (held={:.1f}, expected={:.1f})".format(
                        wid, filled_side, actual_held, fill_size))
                stale_wids.append(wid)
                continue
            # V15.1-25: Use actual held amount, not fill amount (may differ after partial merge)
            sell_size = min(fill_size, actual_held)
            hold_secs = now - fill_time
            # Must hold for minimum time
            if hold_secs < self.config.momentum_exit_min_hold_secs:
                continue
            # Only check momentum after waiting long enough for hedge
            if hold_secs < self.config.momentum_exit_max_wait_secs:
                continue
            # Check current bid price for the filled token
            spread = book_reader.get_spread(fill_token)
            if not spread:
                continue
            current_bid = spread["bid"]
            price_change = (current_bid - fill_price) / fill_price if fill_price > 0 else 0
            if price_change >= self.config.momentum_exit_threshold:
                # Price has risen enough — sell for profit
                sell_profit = (current_bid - fill_price) * sell_size
                fee_est = self.fee_calc._interp_fee_per_share(current_bid) * sell_size
                net_profit = sell_profit - fee_est
                self.logger.info(
                    "\n  MOMENTUM EXIT | {} | {} {:.0f} @ ${:.2f} -> bid ${:.2f} | "
                    "Change: {:+.1%} | Gross ${:+.2f} | Net ${:+.2f} (after ~${:.2f} fees) | "
                    "Held {:.0f}s{}".format(
                        wid, filled_side, sell_size, fill_price, current_bid,
                        price_change, sell_profit, net_profit, fee_est, hold_secs,
                        " [adj from {:.0f}]".format(fill_size) if sell_size != fill_size else ""))
                # V15.1-22: Cancel ALL remaining orders on this window.
                opposite_tokens = set()
                for oid, oinfo in list(self.active_orders.items()):
                    if oinfo.get("window_id") == wid:
                        tid = oinfo.get("token_id", "")
                        if tid and tid != fill_token:
                            opposite_tokens.add(tid)
                cancelled = self.cancel_window_orders(wid)
                self.logger.info(
                    "  MOM-EXIT CANCEL | {} | Cancelled {} orders | "
                    "Opposite tokens: {}".format(
                        wid, cancelled, list(opposite_tokens)))
                if not self.config.dry_run and self.client and opposite_tokens:
                    for tid in opposite_tokens:
                        try:
                            self.client.cancel_market_orders(asset_id=str(tid))
                            self.logger.info(
                                "  MOM-EXIT BATCH CANCEL | {} | token {}".format(
                                    wid, tid[:16] + "..."))
                        except Exception as e:
                            self.logger.warning(
                                "  MOM-EXIT BATCH CANCEL FAIL | {} | {}".format(
                                    wid, str(e)[:100]))
                # V15.1-25: Place sell order — retry once with approval if rejected
                result = self.place_order(
                    fill_token, "SELL", current_bid, sell_size,
                    wid, "MOM-EXIT", "mm", is_taker=True)
                if not result and not self.config.dry_run and self.client:
                    # V15.1-29 Strategy 3: Fix approval retry — ClobClient has
                    # update_balance_allowance(), not set_allowances().
                    # Use CONDITIONAL asset type for CT token sells.
                    try:
                        self.logger.info(
                            "  MOM-EXIT RETRY | {} | Attempting CLOB approval + retry".format(wid))
                        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
                        self.client.update_balance_allowance(
                            BalanceAllowanceParams(
                                asset_type=AssetType.CONDITIONAL,
                                token_id=fill_token,
                            )
                        )
                        import time as _time
                        _time.sleep(2)  # Brief wait for approval to propagate
                        result = self.place_order(
                            fill_token, "SELL", current_bid, sell_size,
                            wid, "MOM-EXIT-RETRY", "mm", is_taker=True)
                    except Exception as e:
                        self.logger.warning(
                            "  MOM-EXIT APPROVAL FAIL | {} | {}".format(
                                wid, str(e)[:100]))
                if result:
                    exits += 1
                    self.window_fill_sides.pop(wid, None)
                    self.window_fill_cost.pop(wid, None)
                    self.window_fill_tokens.pop(wid, None)
                    self.closed_windows.add(wid)
                    self.held_windows.discard(wid)
                    self.window_entry_count.pop(wid, None)
                    cost = fill_price * sell_size
                    self.capital_in_positions = max(0, self.capital_in_positions - cost)
                    self._update_total_capital()
                    # V15.1-25: Track momentum exit analytics
                    exit_profit = (current_bid - fill_price) * sell_size
                    self.hedge_analytics["resolved_by_exit"] += 1
                    self.hedge_analytics["exit_profits"].append(exit_profit)
                    self.hedge_analytics["exit_hold_times"].append(hold_secs)
                    asset = wid.split("-")[0] if "-" in wid else "unknown"
                    if asset not in self.hedge_analytics["per_asset"]:
                        self.hedge_analytics["per_asset"][asset] = {
                            "one_sided": 0, "hedges": 0, "exits": 0,
                            "merges": 0, "abandoned": 0, "t4_sells": 0
                        }
                    self.hedge_analytics["per_asset"][asset]["exits"] += 1
                    # V15.9: Log resolution for CC Analytics
                    self._resolution_log[wid] = {
                        "resolution": "momentum_exit",
                        "time": time.time(),
                        "time_to_close_sec": hold_secs,
                        "window_pnl": exit_profit,
                    }
                    # V15.1-28: Orphan recovery — check if opposite side has tokens
                    # from a late fill (order filled after cancel). Instead of selling
                    # at a loss, try to BUY the missing side to re-pair for merge.
                    meta = self.window_metadata.get(wid, {})
                    opp_key = "token_down" if filled_side == "UP" else "token_up"
                    opp_token = meta.get(opp_key, "")
                    # The missing side is the one we just sold via momentum exit
                    missing_key = "token_up" if filled_side == "UP" else "token_down"
                    missing_token = meta.get(missing_key, "")
                    if opp_token:
                        opp_held = self.token_holdings.get(opp_token, {}).get("size", 0)
                        if opp_held >= 1.0:
                            # Orphan detected — decide: re-pair or sell
                            opp_side_label = opp_key.replace("token_", "").upper()
                            missing_side_label = missing_key.replace("token_", "").upper()
                            # Check remaining time in window
                            market_data = self._market_cache.get(wid)
                            if market_data:
                                window_end = market_data.get("end_time", 0)
                                window_interval = market_data.get("interval", 900)
                            else:
                                window_end = meta.get("end_time", 0)
                                window_interval = 300 if "-5m-" in wid else 900
                            # V15.2-FIX: Use actual market close (end_time + interval)
                            time_left = max(0, (window_end + window_interval) - now)
                            # Estimate orphan cost (use ask price as proxy if fill price unknown)
                            opp_spread = book_reader.get_spread(opp_token)
                            opp_bid = opp_spread["bid"] if opp_spread else 0
                            # Check if we can re-pair profitably
                            missing_spread = book_reader.get_spread(missing_token) if missing_token else None
                            missing_ask = missing_spread["ask"] if missing_spread else 0
                            # Max price for missing side: $1.00 - orphan_cost - min_margin
                            # Use opp_bid as proxy for orphan cost (conservative)
                            min_margin = 0.02  # $0.02 minimum profit per pair
                            max_recovery_price = round(1.0 - opp_bid - min_margin, 2) if opp_bid > 0 else 0
                            can_repair = (
                                time_left >= 60  # At least 60s remaining
                                and missing_token  # We know the missing token
                                and missing_ask > 0  # There's liquidity
                                and missing_ask <= max_recovery_price  # Profitable pair
                            )
                            if can_repair:
                                # RE-PAIR: Buy the missing side to complete the pair
                                recovery_price = min(missing_ask, max_recovery_price)
                                self.logger.warning(
                                    "  ORPHAN RECOVERY | {} | {} has {:.1f} orphan tokens | "
                                    "Buying {} @ ${:.2f} (ask ${:.2f}, max ${:.2f}) | "
                                    "{:.0f}s remaining".format(
                                        wid, opp_side_label, opp_held,
                                        missing_side_label, recovery_price,
                                        missing_ask, max_recovery_price, time_left))
                                recovery_result = self.place_order(
                                    missing_token, "BUY", recovery_price, opp_held,
                                    wid, "ORPHAN-RECOVER", "mm", is_taker=True)
                                if recovery_result:
                                    # Re-open the window for merge pipeline
                                    self.closed_windows.discard(wid)
                                    self.held_windows.add(wid)
                                    # Re-register fill sides so merge can find it
                                    if wid not in self.window_fill_sides:
                                        self.window_fill_sides[wid] = {}
                                    self.window_fill_sides[wid][opp_side_label] = [{
                                        "token_id": opp_token,
                                        "price": opp_bid,  # best estimate
                                        "size": opp_held,
                                        "time": now,
                                    }]
                                    # The recovery buy will be tracked via normal fill detection
                                    # and will add the missing side to window_fill_sides,
                                    # triggering paired_windows and merge.
                                    recovery_cost = recovery_price * opp_held
                                    self.capital_in_positions += recovery_cost
                                    self._update_total_capital()
                                    # Track analytics
                                    self.hedge_analytics["orphan_recoveries"] += 1
                                    self.logger.info(
                                        "  ORPHAN RECOVERY PLACED | {} | BUY {} {:.1f} @ ${:.2f} "
                                        "= ${:.2f} | Window re-opened for merge".format(
                                            wid, missing_side_label, opp_held,
                                            recovery_price, recovery_cost))
                                else:
                                    self.logger.warning(
                                        "  ORPHAN RECOVERY FAILED | {} | BUY {} rejected | "
                                        "Falling back to sell {} at bid ${:.2f}".format(
                                            wid, missing_side_label,
                                            opp_side_label, opp_bid))
                                    # Fall back to sell
                                    can_repair = False
                            if not can_repair:
                                # SELL: Not enough time or can't re-pair profitably
                                if opp_bid > 0:
                                    reason = "no time" if time_left < 60 else (
                                        "ask ${:.2f} > max ${:.2f}".format(missing_ask, max_recovery_price)
                                        if missing_ask > max_recovery_price else "no liquidity")
                                    self.logger.warning(
                                        "  ORPHAN SELL | {} | {} {:.1f} tokens | "
                                        "Selling @ bid ${:.2f} ({}) | {:.0f}s left".format(
                                            wid, opp_side_label, opp_held,
                                            opp_bid, reason, time_left))
                                    opp_result = self.place_order(
                                        opp_token, "SELL", opp_bid, opp_held,
                                        wid, "ORPHAN-SELL", "mm", is_taker=True)
                                    if opp_result:
                                        opp_cost = opp_held * opp_bid
                                        self.capital_in_positions = max(0,
                                            self.capital_in_positions - opp_cost)
                                        self._update_total_capital()
                                        self.hedge_analytics["orphan_sells"] += 1
                                        # V15.9: Log orphan sell resolution for CC Analytics
                                        self._resolution_log[wid + "_orphan"] = {
                                            "resolution": "orphan_sell",
                                            "time": time.time(),
                                            "opposing_fill_price": opp_bid,
                                            "window_pnl": -(opp_held * (1.0 - opp_bid)),  # loss from selling below $1
                                        }
                                        self.logger.info(
                                            "  ORPHAN SOLD | {} | {} {:.1f} @ ${:.2f} "
                                            "= ${:.2f}".format(
                                                wid, opp_side_label,
                                                opp_held, opp_bid, opp_cost))
                                    else:
                                        self.logger.warning(
                                            "  ORPHAN SELL FAILED | {} | {} {:.1f} "
                                            "still held".format(
                                                wid, opp_side_label, opp_held))
                                else:
                                    self.logger.warning(
                                        "  ORPHAN DETECTED | {} | {} {:.1f} tokens | "
                                        "No bid available, holding".format(
                                            wid, opp_side_label, opp_held))
                else:
                    self.logger.warning(
                        "  MOM-EXIT SELL FAILED | {} | Sell order rejected, "
                        "position still held (held={:.1f})".format(wid, actual_held))
                    # V15.1-25: Don't permanently close on first failure — allow retry
                    # next cycle. Only close after 3 consecutive failures.
                    fail_key = "_mom_exit_fail_count"
                    if not hasattr(self, fail_key):
                        setattr(self, fail_key, {})
                    fail_counts = getattr(self, fail_key)
                    fail_counts[wid] = fail_counts.get(wid, 0) + 1
                    if fail_counts[wid] >= 3:
                        self.logger.warning(
                            "  MOM-EXIT ABANDONED | {} | {} consecutive failures, "
                            "scheduling claim fallback".format(wid, fail_counts[wid]))
                        self.closed_windows.add(wid)
                        stale_wids.append(wid)
                        fail_counts.pop(wid, None)
                        # V15.7-BUG1: Schedule claim so ClaimManager can try
                        # CTF-DIRECT redeem after market resolution, instead of
                        # just abandoning the tokens in the wallet.
                        meta = self.window_metadata.get(wid, {})
                        condition_id = meta.get("condition_id", "")
                        end_time = meta.get("end_time", 0)
                        if condition_id and hasattr(self, '_claim_manager_ref'):
                            self._claim_manager_ref.schedule_claim(
                                condition_id, wid, end_time,
                                slug=meta.get("slug", ""),
                                sides="BOTH",
                                cost=self.window_fill_cost.get(wid, 0))
                            self.logger.info(
                                "  MOM-EXIT -> CLAIM | {} | Scheduled for CTF-DIRECT "
                                "redeem after resolution".format(wid))
            else:
                self.logger.debug(
                    "  MOM CHECK | {} | {} @ ${:.2f} | bid ${:.2f} | {:+.1%} < {:.1%} | {:.0f}s".format(
                        wid, filled_side, fill_price, current_bid,
                        price_change, self.config.momentum_exit_threshold, hold_secs))
        # V15.1-25: Clean up stale window_fill_sides for merged/claimed windows
        for wid in stale_wids:
            self.window_fill_sides.pop(wid, None)
            self.window_fill_cost.pop(wid, None)
            self.held_windows.discard(wid)
            self.filled_windows.discard(wid)
        return exits

    def cancel_window_orders(self, window_id, strategy_filter=None):
        oids = self.orders_by_window.get(window_id, [])
        cancelled = 0
        remaining = []
        # V15.4-FIX: Initialize _bot_cancelled_orders if not present
        if not hasattr(self, '_bot_cancelled_orders'):
            self._bot_cancelled_orders = set()
        for oid in list(oids):
            info = self.active_orders.get(oid)
            if info and strategy_filter and info.get("strategy") != strategy_filter:
                remaining.append(oid)
                continue
            if self.config.dry_run:
                if oid in self.active_orders:
                    self._recently_cancelled[oid] = {
                        **self.active_orders[oid], "cancelled_at": time.time()}
                    del self.active_orders[oid]
                    self._bot_cancelled_orders.add(oid)  # V15.4-FIX
                    cancelled += 1
                if self.sim_engine and oid in self.sim_engine.pending_orders:
                    del self.sim_engine.pending_orders[oid]
            else:
                try:
                    # Save to recently_cancelled BEFORE cancelling on exchange.
                    # If the order filled before the cancel takes effect, the
                    # WS fill event can still be processed using this metadata.
                    if oid in self.active_orders:
                        self._recently_cancelled[oid] = {
                            **self.active_orders[oid], "cancelled_at": time.time()}
                    self.client.cancel(oid)
                    if oid in self.active_orders:
                        del self.active_orders[oid]
                    self._bot_cancelled_orders.add(oid)  # V15.4-FIX
                    cancelled += 1
                except Exception:
                    # Cancel failed — order may still be live; remove from buffer
                    self._recently_cancelled.pop(oid, None)
                    remaining.append(oid)
        self.orders_by_window[window_id] = remaining
        self.total_orders_cancelled += cancelled
        # V15.1-P4: Batch cancel safety net — ensure exchange-side cleanup.
        # Individual cancel(oid) can succeed locally but the exchange may
        # still show the order briefly. cancel_market_orders(asset_id) is a
        # server-side batch cancel that catches any stragglers.
        if not self.config.dry_run and self.client and cancelled > 0:
            cancelled_tokens = set()
            for oid, info in self._recently_cancelled.items():
                tid = info.get("token_id", "")
                if tid and info.get("window_id") == window_id:
                    cancelled_tokens.add(tid)
            for tid in cancelled_tokens:
                try:
                    self.client.cancel_market_orders(asset_id=str(tid))
                except Exception:
                    pass  # Best-effort; individual cancels already succeeded
        self._recalc_exposure()
        return cancelled

    def cancel_all(self):
        for wid in list(self.orders_by_window.keys()):
            self.cancel_window_orders(wid)

    def _track_order(self, oid, window_id, side, price, size, token_id, strategy="mm"):
        self.active_orders[oid] = {
            "window_id": window_id, "side": side, "price": price,
            "size": size, "token_id": token_id, "strategy": strategy,
            "time": time.time(),
        }
        if window_id not in self.orders_by_window:
            self.orders_by_window[window_id] = []
        self.orders_by_window[window_id].append(oid)
        if side == "BUY":
            cost = price * size
            self.window_exposure[window_id] = (
                self.window_exposure.get(window_id, 0) + cost)
            self.total_exposure += cost
            asset = window_id.split("-")[0] if "-" in window_id else ""
            self.asset_exposure[asset] = self.asset_exposure.get(asset, 0) + cost
            self.capital_deployed += cost
            self._update_total_capital()
            self.strategy_capital_used[strategy] = (
                self.strategy_capital_used.get(strategy, 0) + cost)
        self.total_orders_placed += 1
        strat_window_key = "{}|{}".format(strategy, window_id)
        self.orders_this_cycle[strat_window_key] = (
            self.orders_this_cycle.get(strat_window_key, 0) + 1)
        counter_map = {
            "sniper": "sniper_trades", "arb": "arb_trades",
            "contrarian": "contrarian_trades", "trend": "trend_trades", "mm": "mm_trades",
        }
        attr = counter_map.get(strategy, "mm_trades")
        setattr(self, attr, getattr(self, attr) + 1)

    def purge_recently_cancelled(self):
        """Remove entries older than TTL from the recently-cancelled buffer."""
        now = time.time()
        expired = [oid for oid, info in self._recently_cancelled.items()
                   if now - info.get("cancelled_at", 0) > self._recently_cancelled_ttl]
        for oid in expired:
            del self._recently_cancelled[oid]

    def _recalc_exposure(self):
        self.window_exposure = {}
        self.asset_exposure = {}
        self.capital_deployed = 0.0
        strat_deployed = {
            "mm": 0.0, "trend": 0.0, "sniper": 0.0, "arb": 0.0, "contrarian": 0.0
        }
        for oid, info in self.active_orders.items():
            if info["side"] != "BUY":
                continue
            wid = info["window_id"]
            cost = info["price"] * info["size"]
            self.window_exposure[wid] = self.window_exposure.get(wid, 0) + cost
            asset = wid.split("-")[0] if "-" in wid else ""
            self.asset_exposure[asset] = self.asset_exposure.get(asset, 0) + cost
            self.capital_deployed += cost
            strat = info.get("strategy", "mm")
            strat_deployed[strat] = strat_deployed.get(strat, 0) + cost
        for strat in strat_deployed:
            self.strategy_capital_used[strat] = strat_deployed[strat]
        self.total_exposure = sum(self.window_exposure.values())
        self._update_total_capital()

    def _get_hedge_analytics_summary(self):
        """V15.1-25: Build a summary of hedge/exit analytics for CC reporting."""
        ha = self.hedge_analytics
        # Compute averages for tier costs and times
        summary = {
            "one_sided_fills": ha["one_sided_fills"],
            "resolved_by_hedge": ha["resolved_by_hedge"],
            "resolved_by_exit": ha["resolved_by_exit"],
            "resolved_by_merge": ha["resolved_by_merge"],
            "resolved_abandoned": ha["resolved_abandoned"],
            "resolved_by_t4_sell": ha["resolved_by_t4_sell"],
            "gate_blocks": ha["gate_blocks"],
            "gate_bypasses": ha["gate_bypasses"],
            "tier_counts": ha["tier_counts"],
            "tier_avg_cost": {},
            "tier_avg_time": {},
            "exit_count": len(ha["exit_profits"]),
            "exit_total_profit": sum(ha["exit_profits"]) if ha["exit_profits"] else 0,
            "exit_avg_profit": (sum(ha["exit_profits"]) / len(ha["exit_profits"])) if ha["exit_profits"] else 0,
            "exit_avg_hold_time": (sum(ha["exit_hold_times"]) / len(ha["exit_hold_times"])) if ha["exit_hold_times"] else 0,
            "per_asset": ha["per_asset"],
            # V15.6: VPIN toxicity stats
            "vpin_blocks": ha.get("vpin_blocks", 0),
            "vpin_widens": ha.get("vpin_widens", 0),
            "midpoint_skips": ha.get("midpoint_skips", 0),
        }
        for tier in ["t1", "t2", "t3", "t4"]:
            costs = ha["tier_costs"][tier]
            times = ha["tier_times"][tier]
            summary["tier_avg_cost"][tier] = (sum(costs) / len(costs)) if costs else 0
            summary["tier_avg_time"][tier] = (sum(times) / len(times)) if times else 0
        # Resolution rate (T4 sells count as resolved — capital recovered)
        total_resolved = ha["resolved_by_hedge"] + ha["resolved_by_exit"] + ha["resolved_by_merge"] + ha["resolved_by_t4_sell"]
        total_one_sided = ha["one_sided_fills"] or 1
        summary["resolution_rate"] = total_resolved / total_one_sided
        summary["hedge_vs_exit_ratio"] = (
            ha["resolved_by_hedge"] / max(1, ha["resolved_by_exit"])
        ) if ha["resolved_by_exit"] > 0 else float('inf') if ha["resolved_by_hedge"] > 0 else 0
        return summary

    def get_stats(self):
        pnl_data = self.get_live_pnl()  # V15.7: now returns dict with portfolio_pnl
        # Backward-compat: extract scalar live_pnl for existing consumers
        if pnl_data and isinstance(pnl_data, dict):
            live_pnl = pnl_data.get("portfolio_pnl")  # V15.7: PRIMARY metric is portfolio P&L
            wallet_delta = pnl_data.get("wallet_delta")
            portfolio_pnl = pnl_data.get("portfolio_pnl")
            total_pnl = pnl_data.get("total_pnl")
            held_value = pnl_data.get("held_value", 0)
            wallet_now = pnl_data.get("wallet_now")
            wallet_start = pnl_data.get("wallet_start")
        else:
            live_pnl = pnl_data  # Legacy: None
            wallet_delta = None
            portfolio_pnl = None
            total_pnl = None
            held_value = 0
            wallet_now = None
            wallet_start = None
        return {
            "active_orders": len(self.active_orders),
            "total_placed": self.total_orders_placed,
            "total_filled": self.total_orders_filled,
            "total_cancelled": self.total_orders_cancelled,
            "total_exposure": self.total_exposure,
            "windows_active": len(self.window_exposure),
            "daily_pnl": self.daily_pnl,
            "sniper_trades": self.sniper_trades, "arb_trades": self.arb_trades,
            "contrarian_trades": self.contrarian_trades,
            "mm_trades": self.mm_trades, "trend_trades": self.trend_trades,
            "asset_exposure": dict(self.asset_exposure),
            "capital_deployed": self.capital_deployed,
            "capital_in_positions": self.capital_in_positions,
            "total_capital_used": self.total_capital_used,
            "available_capital": self.get_available_capital(),
            "session_spent": self.session_total_spent,
            "token_holdings": len(self.token_holdings),
            "live_pnl": live_pnl,  # V15.7: now portfolio_pnl (wallet + positions)
            "wallet_delta": wallet_delta,
            "portfolio_pnl": portfolio_pnl,  # V15.7: wallet_delta + held_value
            "total_pnl_est": total_pnl,
            "held_value": held_value,
            "wallet_now": wallet_now,
            "wallet_start": wallet_start,
            "pending_claims": len(self.expired_windows_pending_claim),
            "hedges_completed": self.hedges_completed,
            "hedges_skipped": self.hedges_skipped,
            "estimated_rewards": self.estimated_rewards_total,
            "unredeemed_value": self.unredeemed_position_value,
            "paired_windows": len(self.paired_windows),
            "filled_windows": len(self.filled_windows),
            "closed_windows": len(self.closed_windows),
            "held_windows": len(self.held_windows),
            "session_realized_returns": self.session_realized_returns,
            "session_realized_cost": self.session_realized_cost,
            "session_realized_pnl": self.session_realized_returns - self.session_realized_cost,
            "hedge_analytics": self._get_hedge_analytics_summary(),
        }


# -----------------------------------------------------------------
# Simulated Fill Engine
