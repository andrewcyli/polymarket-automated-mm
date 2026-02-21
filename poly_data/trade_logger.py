"""
Trade Logger - Logs all trades to Command Center in real-time
"""
from datetime import datetime
import traceback

def log_trade_to_sheets(trade_data):
    """
    Log a trade to Command Center via polymaker_client.
    
    Args:
        trade_data (dict): Trade information with keys:
            - timestamp: Trade timestamp
            - action: 'BUY' or 'SELL'
            - token_id: Token ID
            - market: Market name/question
            - price: Order price
            - size: Order size in USDC
            - order_id: Order ID (if available)
            - status: 'PLACED', 'FILLED', 'CANCELED', etc.
            - neg_risk: Whether it's a neg_risk market
    """
    try:
        # Import here to avoid circular dependency
        from polymaker_client import cc
        
        # Convert to native Python types
        def to_native_type(val):
            """Convert numpy/pandas types to native Python types"""
            try:
                import numpy as np
                if isinstance(val, (np.integer, np.int64, np.int32)):
                    return int(val)
                elif isinstance(val, (np.floating, np.float64, np.float32)):
                    return float(val)
            except ImportError:
                pass
            return val
        
        # Log to Command Center
        result = cc.log_trade(
            token_id=str(trade_data.get('token_id', 'N/A')),
            side=trade_data.get('action', 'BUY').upper(),
            price=float(trade_data.get('price', 0)),
            size=float(trade_data.get('size', 0)),
            order_id=str(trade_data.get('order_id', '')),
            status=trade_data.get('status', 'PLACED').upper(),
            market_question=str(trade_data.get('market', 'Unknown'))[:200],
            position_before=to_native_type(trade_data.get('position_before', 0)),
            position_after=to_native_type(trade_data.get('position_after', 0)),
            notes=str(trade_data.get('notes', ''))
        )
        
        if result:
            print(f"✓ Trade logged to Command Center: {trade_data.get('action')} {trade_data.get('size')} @ ${trade_data.get('price'):.4f}")
        else:
            print(f"⚠️  Failed to log trade to Command Center (API returned False)")
        
        return result
    
    except Exception as e:
        print(f"⚠️  Failed to log trade to Command Center: {e}")
        # Don't crash the bot if logging fails
        traceback.print_exc()
        return False


def reset_worksheet_cache():
    """Legacy function - no-op for Command Center mode"""
    pass
