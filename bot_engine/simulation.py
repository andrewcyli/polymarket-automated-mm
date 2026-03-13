"""Simulated fill engine for dry-run mode."""

import time
import random
import logging


class SimulatedFillEngine:
    def __init__(self, config, fee_calc, logger, engine=None):
        self.config = config
        self.fee_calc = fee_calc
        self.logger = logger
        self.engine = engine
        self.bankroll = config.kelly_bankroll
        self.current_bankroll = config.kelly_bankroll
        self.available_cash = config.kelly_bankroll
        self.filled_positions = {}
        self.pending_orders = {}
        self.realized_pnl = 0.0
        self.total_fees_paid = 0.0
        self.strategy_pnl = {"mm": 0, "trend": 0, "sniper": 0, "arb": 0, "contrarian": 0}
        self.strategy_wins = {"mm": 0, "trend": 0, "sniper": 0, "arb": 0, "contrarian": 0}
        self.strategy_losses = {"mm": 0, "trend": 0, "sniper": 0, "arb": 0, "contrarian": 0}
        self.strategy_trades = {"mm": 0, "trend": 0, "sniper": 0, "arb": 0, "contrarian": 0}
        self.strategy_fees = {"mm": 0, "trend": 0, "sniper": 0, "arb": 0, "contrarian": 0}
        self.resolved_windows = {}
        self.window_start_prices = {}

    def record_order(self, order_id, order_info):
        self.pending_orders[order_id] = {**order_info, "placed_time": time.time()}

    def record_window_start_price(self, window_id, asset, price):
        if window_id not in self.window_start_prices:
            self.window_start_prices[window_id] = price

    def simulate_fills(self, book_reader, markets):
        filled_this_cycle = 0
        spread_cache = {}
        fill_rate = self.config.sim_fill_rate
        max_per_window = self.config.max_fills_per_window
        ws_fills = {}
        for pos in self.filled_positions.values():
            wid = pos.get("window_id", "")
            strat = pos.get("strategy", "mm")
            key = wid + "|" + strat
            ws_fills[key] = ws_fills.get(key, 0) + 1
        window_sides = {}
        for pos in self.filled_positions.values():
            wid = pos.get("window_id", "")
            is_up = pos.get("is_up_token")
            if wid not in window_sides:
                window_sides[wid] = set()
            window_sides[wid].add("UP" if is_up else "DOWN")
        for oid, info in list(self.pending_orders.items()):
            token_id = info.get("token_id", "")
            side = info.get("side", "")
            price = info.get("price", 0)
            size = info.get("size", 0)
            strategy = info.get("strategy", "mm")
            window_id = info.get("window_id", "")
            is_taker = info.get("is_taker", False)
            is_up = info.get("is_up_token")
            key = window_id + "|" + strategy
            if ws_fills.get(key, 0) >= max_per_window:
                continue
            if self.config.mm_block_opposing_fills and window_id in window_sides:
                token_dir = "UP" if is_up else "DOWN"
                existing_sides = window_sides[window_id]
                opposite = "DOWN" if token_dir == "UP" else "UP"
                if opposite in existing_sides and token_dir not in existing_sides:
                    continue
            if token_id not in spread_cache:
                spread_cache[token_id] = book_reader.get_spread(token_id)
            spread = spread_cache[token_id]
            if not spread:
                continue
            filled = False
            if side == "BUY":
                if price >= spread["ask"]:
                    filled = True
                elif price >= spread["bid"] and spread["ask"] > spread["bid"]:
                    spread_width = spread["ask"] - spread["bid"]
                    position_in_spread = (price - spread["bid"]) / spread_width
                    fp = position_in_spread * fill_rate
                    age = time.time() - info.get("placed_time", time.time())
                    age_bonus = min(age / 60.0, 1.0) * 0.05
                    fp = min(fp + age_bonus, 0.95)
                    if np.random.random() < fp:
                        filled = True
            elif side == "SELL":
                if price <= spread["bid"]:
                    filled = True
                elif price <= spread["ask"] and spread["ask"] > spread["bid"]:
                    spread_width = spread["ask"] - spread["bid"]
                    position_in_spread = (spread["ask"] - price) / spread_width
                    fp = position_in_spread * fill_rate
                    age = time.time() - info.get("placed_time", time.time())
                    age_bonus = min(age / 60.0, 1.0) * 0.05
                    fp = min(fp + age_bonus, 0.95)
                    if np.random.random() < fp:
                        filled = True
            if filled:
                fill_frac = self.config.sim_partial_fill_min + (
                    np.random.random() ** 0.7 * (1.0 - self.config.sim_partial_fill_min))
                fill_size = size * fill_frac
                fill_price = price
                if is_taker:
                    slip = np.random.random() * self.config.sim_slippage_max
                    if side == "BUY":
                        fill_price = min(price * (1 + slip), 0.99)
                    else:
                        fill_price = max(price * (1 - slip), 0.01)
                fee = 0.0
                if is_taker:
                    fee = self.fee_calc.fee_amount(fill_price, fill_size)
                if side == "BUY":
                    total_cost = fill_price * fill_size + fee
                    if total_cost > self.available_cash:
                        continue
                    self.available_cash -= total_cost
                else:
                    self.available_cash += fill_price * fill_size - fee
                self.total_fees_paid += fee
                self.strategy_fees[strategy] = self.strategy_fees.get(strategy, 0) + fee
                self.filled_positions[oid] = {
                    **info, "fill_time": time.time(), "fill_price": fill_price,
                    "fill_size": fill_size, "fee": fee,
                }
                del self.pending_orders[oid]
                filled_this_cycle += 1
                self.strategy_trades[strategy] = self.strategy_trades.get(strategy, 0) + 1
                ws_fills[key] = ws_fills.get(key, 0) + 1
                if self.engine:
                    self.engine.record_fill(token_id, side, fill_price, fill_size, fee)
                if window_id not in window_sides:
                    window_sides[window_id] = set()
                window_sides[window_id].add("UP" if is_up else "DOWN")
        return filled_this_cycle

    def resolve_window(self, window_id, asset, end_price, start_price):
        if window_id in self.resolved_windows:
            return 0.0
        winner = "UP" if end_price >= start_price else "DOWN"
        window_pnl = 0.0
        window_fees = 0.0
        positions_resolved = 0
        wins = 0
        losses = 0
        resolved_positions = []
        for oid, pos in list(self.filled_positions.items()):
            if pos.get("window_id") != window_id:
                continue
            strategy = pos.get("strategy", "mm")
            side = pos.get("side", "")
            fill_price = pos.get("fill_price", pos.get("price", 0))
            fill_size = pos.get("fill_size", pos.get("size", 0))
            fee = pos.get("fee", 0)
            is_up = pos.get("is_up_token")
            cost = fill_price * fill_size + fee
            resolved_positions.append(pos)
            if side == "BUY":
                token_won = (is_up and winner == "UP") or (not is_up and winner == "DOWN")
                if token_won:
                    pnl = (1.0 * fill_size) - cost
                    wins += 1
                    self.available_cash += 1.0 * fill_size
                else:
                    pnl = -cost
                    losses += 1
            elif side == "SELL":
                token_won = (is_up and winner == "UP") or (not is_up and winner == "DOWN")
                if token_won:
                    pnl = cost - fee - (1.0 * fill_size)
                    losses += 1
                else:
                    pnl = (fill_price * fill_size) - fee
                    wins += 1
            else:
                pnl = 0
            window_pnl += pnl
            window_fees += fee
            self.strategy_pnl[strategy] = self.strategy_pnl.get(strategy, 0) + pnl
            if pnl > 0:
                self.strategy_wins[strategy] = self.strategy_wins.get(strategy, 0) + 1
            elif pnl < 0:
                self.strategy_losses[strategy] = self.strategy_losses.get(strategy, 0) + 1
            positions_resolved += 1
            token_id = pos.get("token_id", "")
            if self.engine and token_id in self.engine.token_holdings:
                self.engine.token_holdings[token_id]["size"] = max(
                    0, self.engine.token_holdings[token_id]["size"] - fill_size)
            del self.filled_positions[oid]
        for oid in list(self.pending_orders.keys()):
            if self.pending_orders[oid].get("window_id") == window_id:
                del self.pending_orders[oid]
        self.realized_pnl += window_pnl
        self.current_bankroll += window_pnl
        if self.engine and positions_resolved > 0:
            total_resolved_cost = sum(
                (p.get("fill_price", 0) * p.get("fill_size", 0) + p.get("fee", 0))
                for p in resolved_positions)
            self.engine.capital_in_positions = max(
                0, self.engine.capital_in_positions - total_resolved_cost)
            self.engine._update_total_capital()
        price_change = (
            ((end_price - start_price) / start_price * 100) if start_price else 0)
        self.resolved_windows[window_id] = {
            "winner": winner, "pnl": window_pnl, "fees": window_fees,
            "positions": positions_resolved, "wins": wins, "losses": losses,
            "start_price": start_price, "end_price": end_price,
            "price_change": price_change,
        }
        if positions_resolved > 0:
            tag = "[WIN]" if window_pnl >= 0 else "[LOSS]"
            self.logger.info(
                "\n  {} RESOLVED | {} | Winner: {} | P&L: ${:+.2f} | "
                "Pos: {} (W:{} L:{}) | Bankroll: ${:.2f}".format(
                    tag, window_id, winner, window_pnl,
                    positions_resolved, wins, losses, self.current_bankroll))
        return window_pnl

    def get_summary(self):
        total_fills = sum(self.strategy_trades.values())
        total_wins = sum(self.strategy_wins.values())
        total_losses = sum(self.strategy_losses.values())
        wr = total_wins / (total_wins + total_losses) if (total_wins + total_losses) > 0 else 0
        return {
            "starting_bankroll": self.bankroll,
            "current_bankroll": self.current_bankroll,
            "available_cash": self.available_cash,
            "realized_pnl": self.realized_pnl,
            "pnl_pct": (self.realized_pnl / self.bankroll * 100) if self.bankroll > 0 else 0,
            "total_fees": self.total_fees_paid,
            "total_fills": total_fills,
            "total_wins": total_wins, "total_losses": total_losses,
            "win_rate": wr,
            "windows_resolved": len(self.resolved_windows),
            "pending_orders": len(self.pending_orders),
            "open_positions": len(self.filled_positions),
            "strategy_pnl": dict(self.strategy_pnl),
            "strategy_wins": dict(self.strategy_wins),
            "strategy_losses": dict(self.strategy_losses),
            "strategy_trades": dict(self.strategy_trades),
            "strategy_fees": dict(self.strategy_fees),
        }


# -----------------------------------------------------------------
# Strategy 1: Market Making (V15.1-2: pair pre-check, V15.1-3: epsilon)
