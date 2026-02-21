"""
Reward Tracker - Estimates maker rewards for each market (logging disabled in Command Center mode)
"""

import time
from datetime import datetime
import poly_data.global_state as global_state
import traceback

_last_snapshot_time = {}


def estimate_order_reward(price, size, mid_price, max_spread, daily_rate):
    """Estimate maker rewards for a single order based on Polymarket's formula."""
    try:
        s = abs(price - mid_price)
        v = max_spread / 100
        if v == 0:
            return 0
        S = ((v - s) / v) ** 2
        Q = S * size
        hourly_rate = daily_rate / 24
        estimated_reward = (Q / (Q + 1000)) * hourly_rate
        return max(0, estimated_reward)
    except Exception as e:
        print(f"Error calculating reward: {e}")
        return 0


def log_market_snapshot(market_id, market_name):
    """
    Log a snapshot of current orders and estimate rewards for a market.
    
    NOTE: In Command Center mode, reward tracking is handled by the dashboard's
    P&L analysis. This function is kept for compatibility but does not log to sheets.
    """
    global _last_snapshot_time

    try:
        current_time = time.time()
        last_snapshot = _last_snapshot_time.get(market_id, 0)
        
        # Rate limit: only log every 5 minutes
        if current_time - last_snapshot < 300:
            return
        _last_snapshot_time[market_id] = current_time

        if global_state.df is None:
            return

        market_row = global_state.df[global_state.df['condition_id'] == market_id]
        if market_row.empty:
            return
        market_row = market_row.iloc[0]

        # Calculate mid price from order book
        if market_id in global_state.all_data:
            bids = global_state.all_data[market_id]['bids']
            asks = global_state.all_data[market_id]['asks']
            if len(bids) > 0 and len(asks) > 0:
                best_bid = list(bids.keys())[-1]
                best_ask = list(asks.keys())[-1]
                mid_price = (best_bid + best_ask) / 2
            else:
                mid_price = 0.5
        else:
            mid_price = 0.5

        # Estimate rewards for current orders (print to console only)
        for token_name in ['token1', 'token2']:
            token_id = str(market_row[token_name])
            answer = market_row['answer1'] if token_name == 'token1' else market_row['answer2']
            orders = global_state.orders.get(token_id, {'buy': {'price': 0, 'size': 0}, 'sell': {'price': 0, 'size': 0}})

            if orders['buy']['size'] > 0:
                buy_reward = estimate_order_reward(
                    orders['buy']['price'], orders['buy']['size'], mid_price,
                    market_row['max_spread'], market_row['rewards_daily_rate']
                )
                print(f"  Estimated BUY reward for {answer}: ${buy_reward:.4f}/hr")

            if orders['sell']['size'] > 0:
                sell_reward = estimate_order_reward(
                    orders['sell']['price'], orders['sell']['size'], mid_price,
                    market_row['max_spread'], market_row['rewards_daily_rate']
                )
                print(f"  Estimated SELL reward for {answer}: ${sell_reward:.4f}/hr")

        return True

    except Exception as e:
        print(f"Failed to calculate reward snapshot: {e}")
        traceback.print_exc()
        return False


def reset_reward_cache():
    """Legacy function - no-op for Command Center mode"""
    pass
