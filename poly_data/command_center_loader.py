"""
Command Center Config Loader
Fetches market data and trading parameters from the PolyMaker Command Center instead of Google Sheets.
"""
import pandas as pd
from polymaker_client import cc

def load_from_command_center():
    """
    Fetch market configuration from Command Center.
    Returns (df, params) tuple matching the Google Sheets format.
    
    Returns:
        df (pd.DataFrame): Market list with columns [question, token1, token2, condition_id, max_size, trade_size, param_type]
        params (dict): Global trading parameters
    """
    if not cc:
        print("⚠️  Command Center client not available. Using empty config.")
        return pd.DataFrame(columns=['question', 'token1', 'token2', 'condition_id', 'max_size', 'trade_size', 'param_type']), {}
    
    try:
        config = cc.get_config()
        if not config:
            print("⚠️  No active config found in Command Center. Using empty config.")
            return pd.DataFrame(columns=['question', 'token1', 'token2', 'condition_id', 'max_size', 'trade_size', 'param_type']), {}
        
        # Markets will be auto-discovered based on targetAssets and windowDurations
        # The bot's market discovery logic will populate this later
        df = pd.DataFrame(columns=['question', 'token1', 'token2', 'condition_id', 'max_size', 'trade_size', 'param_type'])
        print(f"✅ Using auto-discovery mode for markets (targetAssets: {config.get('targetAssets', 'BTC,ETH')})")
        
        # Extract global parameters from config
        params = {
            'bankroll': config.get('bankroll', 100),
            'targetSpread': config.get('targetSpread', 0.02),
            'orderSize': config.get('orderSize', 10),
            'maxLossPercent': config.get('maxLossPercent', 0.05),
            'maxMarkets': config.get('maxMarkets', 5),
            'fillTimeout': config.get('fillTimeout', 30),
            'simulatedFillRate': config.get('simulatedFillRate', 0.7),
            'targetAssets': [a.strip() for a in config.get('targetAssets', 'BTC,ETH').split(',') if a.strip()],
            'windowDurations': [w.strip() for w in config.get('windowDurations', '5m,15m').split(',') if w.strip()],
            'maxConcurrentWindows': config.get('maxConcurrentWindows', 4),
            'tradeAdvanceWindows': config.get('tradeAdvanceWindows', False),
        }
        
        print(f"✅ Loaded parameters from Command Center: bankroll=${params['bankroll']}, spread={params['targetSpread']}, orderSize=${params['orderSize']}")
        
        return df, params
        
    except Exception as e:
        print(f"❌ Error loading config from Command Center: {str(e)}")
        return pd.DataFrame(columns=['question', 'token1', 'token2', 'condition_id', 'max_size', 'trade_size', 'param_type']), {}


def get_sheet_df_with_fallback():
    """
    Try Command Center first, fall back to Google Sheets if Command Center is unavailable.
    """
    # Try Command Center first
    df, params = load_from_command_center()
    
    # If Command Center returned data, use it
    if not df.empty or params:
        return df, params
    
    # Otherwise, try Google Sheets as fallback
    try:
        from poly_data.utils import get_sheet_df as get_sheet_df_original
        print("⚠️  Falling back to Google Sheets...")
        return get_sheet_df_original()
    except Exception as e:
        print(f"⚠️  Google Sheets fallback also failed: {str(e)}")
        return pd.DataFrame(columns=['question', 'token1', 'token2', 'condition_id', 'max_size', 'trade_size', 'param_type']), {}
