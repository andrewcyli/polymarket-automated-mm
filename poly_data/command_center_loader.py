"""
Command Center Config Loader with Market Auto-Discovery
Fetches market data and trading parameters from the PolyMaker Command Center.
"""
import pandas as pd
from polymaker_client import cc
import poly_data.global_state as global_state

def _parse_list_field(value):
    """
    Parse a field that can be either a string ("BTC,ETH") or an array (["BTC", "ETH"]).
    Returns a list of stripped non-empty strings.
    """
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    elif isinstance(value, str):
        return [item.strip() for item in value.split(',') if item.strip()]
    else:
        return []

def discover_markets(target_assets, window_durations, max_markets=10):
    """
    Auto-discover active Polymarket markets based on target assets and window durations.
    
    Args:
        target_assets: List of crypto assets (e.g., ['BTC', 'ETH', 'XRP'])
        window_durations: List of time windows (e.g., ['5m', '15m'])
        max_markets: Maximum number of markets to return
        
    Returns:
        pd.DataFrame with columns: question, token1, token2, condition_id, answer1, answer2, 
                                   max_spread, rewards_daily_rate, neg_risk, min_size
    """
    if not global_state.client:
        print("⚠️  Polymarket client not available for market discovery")
        return pd.DataFrame()
    
    try:
        print(f"🔍 Discovering markets for {target_assets} with windows {window_durations}...")
        
        # Get sampling markets from Polymarket (active markets with rewards)
        markets_response = global_state.client.get_sampling_markets(next_cursor="")
        if not markets_response or 'data' not in markets_response:
            print("⚠️  No markets returned from Polymarket API")
            return pd.DataFrame()
        
        markets_data = markets_response['data']
        print(f"  Found {len(markets_data)} total markets from Polymarket")
        
        # Filter markets by target assets and window durations
        matched_markets = []
        for market in markets_data:
            question = market.get('question', '').lower()
            
            # Check if question contains any target asset
            has_asset = any(asset.lower() in question for asset in target_assets)
            if not has_asset:
                continue
            
            # Check if question contains any window duration
            has_window = any(window.lower() in question for window in window_durations)
            if not has_window:
                continue
            
            # Extract market data
            tokens = market.get('tokens', [])
            if len(tokens) < 2:
                continue
            
            rewards = market.get('rewards', {})
            
            # Find USDC reward rate
            reward_rate = 0
            for rate_info in rewards.get('rates', []):
                # USDC.e on Polygon
                if rate_info.get('asset_address', '').lower() == '0x2791bca1f2de4661ed88a30c99a7a9449aa84174'.lower():
                    reward_rate = rate_info.get('rewards_daily_rate', 0)
                    break
            
            market_data = {
                'question': market.get('question', ''),
                'token1': tokens[0].get('token_id', ''),
                'token2': tokens[1].get('token_id', ''),
                'condition_id': market.get('condition_id', ''),
                'answer1': tokens[0].get('outcome', ''),
                'answer2': tokens[1].get('outcome', ''),
                'max_spread': rewards.get('max_spread', 5),
                'rewards_daily_rate': reward_rate,
                'neg_risk': market.get('neg_risk', False),
                'min_size': rewards.get('min_size', 10),
                'market_slug': market.get('market_slug', ''),
                'end_date_iso': market.get('end_date_iso', ''),
            }
            
            matched_markets.append(market_data)
            
            if len(matched_markets) >= max_markets:
                break
        
        if not matched_markets:
            print(f"⚠️  No markets found matching {target_assets} + {window_durations}")
            return pd.DataFrame()
        
        df = pd.DataFrame(matched_markets)
        print(f"✅ Discovered {len(df)} markets: {df['question'].tolist()[:3]}...")
        
        return df
        
    except Exception as e:
        print(f"❌ Error discovering markets: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

def load_from_command_center():
    """
    Fetch market configuration from Command Center and auto-discover markets.
    Returns (df, params) tuple matching the expected format.
    
    Returns:
        df (pd.DataFrame): Market list with discovered markets
        params (dict): Global trading parameters
    """
    if not cc:
        print("⚠️  Command Center client not available. Bot will run with empty config.")
        return pd.DataFrame(columns=['question', 'token1', 'token2', 'condition_id', 'max_size', 'trade_size', 'param_type']), {}
    
    try:
        config = cc.get_config()
        if not config:
            print("⚠️  No active config found in Command Center. Bot will run with empty config.")
            return pd.DataFrame(columns=['question', 'token1', 'token2', 'condition_id', 'max_size', 'trade_size', 'param_type']), {}
        
        # Extract global parameters from config
        target_assets = _parse_list_field(config.get('targetAssets', 'BTC,ETH'))
        window_durations = _parse_list_field(config.get('windowDurations', '5m,15m'))
        max_concurrent = config.get('maxConcurrentWindows', 5)
        
        params = {
            'bankroll': config.get('kellyBankroll', 100),
            'targetSpread': config.get('mmSpreadMin', 0.02),
            'orderSize': config.get('mmOrderSize', 10),
            'maxLossPercent': config.get('maxLossPct', 0.05),
            'maxMarkets': max_concurrent,
            'fillTimeout': 30,  # Not in config yet, use default
            'simulatedFillRate': config.get('simFillRate', 0.7),
            'targetAssets': target_assets,
            'windowDurations': window_durations,
            'maxConcurrentWindows': max_concurrent,
            'tradeAdvanceWindows': config.get('tradeAdvanceWindows', False),
        }
        
        print(f"✅ Using auto-discovery mode for markets (targetAssets: {target_assets})")
        print(f"✅ Loaded parameters from Command Center: bankroll=${params['bankroll']}, spread={params['targetSpread']}, orderSize=${params['orderSize']}")
        
        # Auto-discover markets based on config
        df = discover_markets(target_assets, window_durations, max_markets=max_concurrent * 2)
        
        if df.empty:
            print("⚠️  No markets discovered. Bot will run with empty config.")
        
        return df, params
        
    except Exception as e:
        print(f"❌ Error loading config from Command Center: {str(e)}")
        print("Bot will run with empty config.")
        import traceback
        traceback.print_exc()
        return pd.DataFrame(columns=['question', 'token1', 'token2', 'condition_id', 'max_size', 'trade_size', 'param_type']), {}
