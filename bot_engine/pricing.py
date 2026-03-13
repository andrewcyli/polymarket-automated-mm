"""Fee calculation, reward optimization, and Kelly sizing."""

import math


class RewardOptimizer:
    def __init__(self, config, fee_calc, logger):
        self.config = config
        self.fee_calc = fee_calc
        self.logger = logger
        self._stats = {"calculations": 0, "fallbacks": 0}

    def reward_score(self, max_spread, distance):
        if max_spread <= 0 or distance < 0:
            return 0.0
        if distance >= max_spread:
            return 0.0
        ratio = (max_spread - distance) / max_spread
        return ratio ** 2

    def optimal_distance(self, max_spread, target_score=None):
        if target_score is None:
            target_score = self.config.reward_target_pct
        if max_spread <= 0 or target_score <= 0 or target_score >= 1:
            return self.config.mm_base_spread
        target_score = min(target_score, 1.0)  # Guard against math domain error in sqrt
        s = max_spread * (1.0 - math.sqrt(target_score))
        s = max(self.config.reward_min_distance, min(s, self.config.reward_max_distance))
        self._stats["calculations"] += 1
        return s

    def optimal_distance_for_pair(self, spread_up, spread_down, mid_up):
        max_spread = max(spread_up.get("spread", 0), spread_down.get("spread", 0))
        if max_spread < self.config.mm_min_spread:
            return None, 0, 0, 0
        d = self.optimal_distance(max_spread)
        buy_up_price = mid_up - d
        buy_down_price = (1.0 - mid_up) - d
        if buy_up_price <= 0.01 or buy_down_price <= 0.01:
            return None, 0, 0, 0
        fee_up = self.fee_calc._interp_fee_per_share(buy_up_price)
        fee_down = self.fee_calc._interp_fee_per_share(buy_down_price)
        pair_cost = buy_up_price + buy_down_price + fee_up + fee_down
        pair_profit = 1.0 - pair_cost
        if self.config.pair_validation_enabled:
            attempts = 0
            while pair_profit < self.config.pair_min_profit and attempts < 20:
                d += 0.002
                buy_up_price = mid_up - d
                buy_down_price = (1.0 - mid_up) - d
                if buy_up_price <= 0.02 or buy_down_price <= 0.02:
                    return None, 0, 0, 0
                fee_up = self.fee_calc._interp_fee_per_share(buy_up_price)
                fee_down = self.fee_calc._interp_fee_per_share(buy_down_price)
                pair_cost = buy_up_price + buy_down_price + fee_up + fee_down
                pair_profit = 1.0 - pair_cost
                attempts += 1
        if pair_profit < self.config.pair_min_profit:
            self._stats["fallbacks"] += 1
            return None, 0, pair_cost, pair_profit
        score = self.reward_score(max_spread, d)
        return d, score, pair_cost, pair_profit

    def estimate_reward_per_hour(self, orders_active, avg_score, avg_size):
        if orders_active <= 0 or avg_score <= 0:
            return 0.0
        return orders_active * avg_score * avg_size * 0.001

    def get_stats(self):
        return dict(self._stats)


# -----------------------------------------------------------------
# V13.1-1: Volatility Tracker


class FeeCalculator:
    FEE_TABLE = {
        0.01: 0.0000, 0.05: 0.0000, 0.10: 0.0006, 0.15: 0.0006,
        0.20: 0.0013, 0.25: 0.0022, 0.30: 0.0033, 0.35: 0.0045,
        0.40: 0.0058, 0.45: 0.0069, 0.50: 0.0078, 0.55: 0.0084,
        0.60: 0.0086, 0.65: 0.0084, 0.70: 0.0077, 0.75: 0.0066,
        0.80: 0.0051, 0.85: 0.0035, 0.90: 0.0018, 0.95: 0.0005,
        0.99: 0.0000, 1.00: 0.0000,
    }

    def __init__(self):
        self._prices = sorted(self.FEE_TABLE.keys())

    def _interp_fee_per_share(self, price):
        if price <= self._prices[0]:
            return self.FEE_TABLE[self._prices[0]]
        if price >= self._prices[-1]:
            return self.FEE_TABLE[self._prices[-1]]
        for i in range(len(self._prices) - 1):
            p1, p2 = self._prices[i], self._prices[i + 1]
            if p1 <= price <= p2:
                f1 = self.FEE_TABLE[p1]
                f2 = self.FEE_TABLE[p2]
                t = (price - p1) / (p2 - p1)
                return f1 + t * (f2 - f1)
        return 0.0

    def fee_amount(self, price, shares):
        return self._interp_fee_per_share(price) * shares

    def pair_fee(self, price_a, price_b, shares):
        return self.fee_amount(price_a, shares) + self.fee_amount(price_b, shares)

    def net_cost_taker_buy(self, price, shares):
        return (price * shares) + self.fee_amount(price, shares)


# -----------------------------------------------------------------
# API Retry / RPC Rate Limiter
# -----------------------------------------------------------------

def api_retry(func, max_retries=3, base_delay=1.0, logger=None):
    for attempt in range(max_retries):
        try:
            return func()
        except requests.exceptions.Timeout:
            time.sleep(base_delay * (2 ** attempt))
        except requests.exceptions.ConnectionError:
            time.sleep(base_delay * (2 ** attempt))
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else 0
            if status == 429:
                time.sleep(base_delay * (4 ** attempt))
            elif 500 <= status < 600:
                time.sleep(base_delay * (2 ** attempt))
            else:
                raise
        except Exception:
            if attempt == max_retries - 1:
                raise
            time.sleep(base_delay * (2 ** attempt))
    return None




class KellySizer:
    def __init__(self, config, fee_calc):
        self.fraction = config.kelly_fraction
        self.bankroll = config.kelly_bankroll
        self.enabled = config.kelly_enabled
        self.fee_calc = fee_calc

    def update_bankroll(self, new_bankroll):
        self.bankroll = max(new_bankroll, 10.0)

    def optimal_size(self, prob_win, price, is_taker=False):
        if not self.enabled or prob_win <= 0 or prob_win >= 1 or price <= 0 or price >= 1:
            return 0
        fee_per_share = self.fee_calc._interp_fee_per_share(price) if is_taker else 0
        effective_cost = price + fee_per_share
        if effective_cost >= 1.0:
            return 0
        b = (1.0 / effective_cost) - 1.0
        q = 1 - prob_win
        kelly_f = (prob_win * b - q) / b
        if kelly_f <= 0:
            return 0
        dollar_size = kelly_f * self.fraction * self.bankroll
        return max(2.0, min(dollar_size, self.bankroll * 0.15))


# -----------------------------------------------------------------
# Wallet Balance Checker
