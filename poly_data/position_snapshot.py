"""
Position Snapshot Logger - Logs position snapshots (Command Center mode: console only)
"""
from datetime import datetime
import poly_data.global_state as global_state
import traceback
import pandas as pd

_last_snapshot_time = 0

def log_position_snapshot():
    """
    Log a snapshot of all current positions to console.
    
    NOTE: In Command Center mode, position tracking is handled by the dashboard's
    real-time monitoring. This function is kept for compatibility but only prints to console.
    """
    global _last_snapshot_time
    
    import time
    current_time = time.time()
    
    # Rate limit: only log every 5 minutes
    if current_time - _last_snapshot_time < 300:
        return
    _last_snapshot_time = current_time
    
    try:
        if global_state.client is None:
            return
        
        # Get balances
        try:
            usdc_balance = global_state.client.get_usdc_balance()
            pos_balance = global_state.client.get_pos_balance()
            total_balance = global_state.client.get_total_balance()
        except Exception as e:
            print(f"⚠️  Warning: Could not get balances: {e}")
            usdc_balance = pos_balance = total_balance = 0

        # Get positions
        try:
            positions_df = global_state.client.get_all_positions()
            positions_df = positions_df[positions_df['size'].astype(float) > 0] if not positions_df.empty else pd.DataFrame()
        except Exception as e:
            print(f"⚠️  Warning: Could not get positions: {e}")
            positions_df = pd.DataFrame()

        # Get active orders count
        try:
            orders_df = global_state.client.get_all_orders()
            order_count = len(orders_df) if not orders_df.empty else 0
        except:
            order_count = 0

        # Get wallet address
        wallet_address = global_state.client.browser_address if hasattr(global_state.client, 'browser_address') else 'N/A'

        # Print snapshot to console
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"\n{'='*80}")
        print(f"📊 POSITION SNAPSHOT - {timestamp}")
        print(f"{'='*80}")
        print(f"Wallet: {wallet_address}")
        print(f"USDC Balance: ${usdc_balance:.2f}")
        print(f"Position Value: ${pos_balance:.2f}")
        print(f"Total Balance: ${total_balance:.2f}")
        print(f"Active Orders: {order_count}")
        print(f"{'-'*80}")

        if positions_df.empty:
            print("No open positions")
        else:
            print(f"Open Positions ({len(positions_df)}):")
            for idx, pos in positions_df.iterrows():
                size = float(pos.get('size', 0))
                avg_price = float(pos.get('averagePrice', 0))
                market_price = float(pos.get('marketPrice', 0))

                # Calculate P&L
                pnl_per_share = market_price - avg_price
                total_pnl = pnl_per_share * size
                pnl_percent = (pnl_per_share / avg_price * 100) if avg_price > 0 else 0

                outcome = pos.get('outcome', 'Unknown')
                market = pos.get('market', 'Unknown')
                position_value = size * market_price

                print(f"  • {market[:60]}")
                print(f"    Outcome: {outcome} | Size: {size:.2f} @ ${avg_price:.4f}")
                print(f"    Market Price: ${market_price:.4f} | Value: ${position_value:.2f}")
                print(f"    P&L: ${total_pnl:.2f} ({pnl_percent:+.2f}%)")

        print(f"{'='*80}\n")

        return True

    except Exception as e:
        print(f"⚠️  Failed to log position snapshot: {e}")
        # Don't crash the bot if logging fails
        traceback.print_exc()
        return False


def reset_snapshot_cache():
    """Legacy function - no-op for Command Center mode"""
    pass
