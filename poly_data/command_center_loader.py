"""
Command Center Config Loader with Market Auto-Discovery
Fetches market data and trading parameters from the PolyMaker Command Center.
"""
import pandas as pd
from polymaker_client import cc
import requests
import time

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

def _get_market_timestamp(window):
    """
    Calculate the market timestamp for a given window.
    Markets are created at rounded intervals (5m = 300s, 15m = 900s).
    """
    current_time = int(time.time())
    
    if window == '5m':
        interval = 300  # 5 minutes
    elif window == '15m':
        interval = 900  # 15 minutes
    else:
        return None
    
    # Round to nearest interval
    return (current_time // interval) * interval

def discover_markets(target_assets, window_durations, max_markets=10):
    """
    Auto-discover active Polymarket crypto up/down markets.
    
    Markets follow the pattern: {asset}-updown-{window}-{unix_timestamp}
    Example: btc-updown-5m-1771692000
    
    Args:
        target_assets: List of crypto assets (e.g., ['BTC', 'ETH', 'XRP', 'SOL'])
        window_durations: List of time windows (e.g., ['5m', '15m'])
        max_markets: Maximum number of markets to return
        
    Returns:
        pd.DataFrame with columns: question, token1, token2, condition_id, answer1, answer2, 
                                   max_spread, rewards_daily_rate, neg_risk, min_size
    """
    try:
        print(f"🔍 Discovering markets for {target_assets} with windows {window_durations}...")
        
        matched_markets = []
        
        # For each asset and window, calculate the current market timestamp
        for asset in target_assets:
            for window in window_durations:
                timestamp = _get_market_timestamp(window)
                if not timestamp:
                    continue
                
                slug = f"{asset.lower()}-updown-{window}-{timestamp}"
                url = f"https://gamma-api.polymarket.com/events?slug={slug}"
                
                try:
                    resp = requests.get(url, timeout=10)
                    if resp.status_code != 200:
                        print(f"  ⚠️  API returned {resp.status_code} for {slug}")
                        continue
                    
                    data = resp.json()
                    if not data:
                        print(f"  ⚠️  No market found for {slug}")
                        continue
                    
                    event = data[0]
                    market = event.get('markets', [{}])[0]
                    
                    if not market:
                        continue
                    
                    # Extract token IDs from clobTokenIds string
                    token_ids_str = market.get('clobTokenIds', '')
                    # Format: "[\"token1\", \"token2\"]" -> ['token1', 'token2']
                    token_ids = token_ids_str.strip('[]"').split('", "') if token_ids_str else []
                    
                    if len(token_ids) < 2:
                        print(f"  ⚠️  Invalid token IDs for {slug}")
                        continue
                    
                    # Extract reward info
                    rewards = market.get('rewards', {})
                    reward_rate = rewards.get('rewardsMaxSpread', 0) if isinstance(rewards, dict) else 0
                    
                    # Get tick size (default 0.01 for crypto markets)
                    tick_size = market.get('minimumTickSize', 0.01)
                    
                    market_data = {
                        'question': event.get('title', f"{asset.upper()} Up or Down {window}"),
                        'token1': token_ids[0],
                        'token2': token_ids[1],
                        'condition_id': market.get('conditionId', ''),
                        'answer1': 'Up',
                        'answer2': 'Down',
                        'max_spread': market.get('rewardsMaxSpread', 5),
                        'rewards_daily_rate': reward_rate,
                        'neg_risk': market.get('negRisk', False),
                        'min_size': market.get('rewardsMinSize', 10),
                        'market_slug': slug,
                        'end_date_iso': market.get('endDate', ''),
                        'tick_size': tick_size,
                        'param_type': 'default',  # Will be set by data_utils.py
                        'trade_size': 10,  # Default trade size, can be overridden by params
                        'max_size': 100,  # Default max size, can be overridden by params
                        'best_bid': 0.5,  # Will be updated by WebSocket
                        'best_ask': 0.5,  # Will be updated by WebSocket
                        '3_hour': 0.0,  # Volatility metric, not used for crypto updown
                    }
                    
                    matched_markets.append(market_data)
                    print(f"  ✓ Found {asset.upper()} {window}: {slug}")
                    
                    if len(matched_markets) >= max_markets:
                        break
                        
                except requests.Timeout:
                    print(f"  ⚠️  Timeout fetching {slug}")
                except Exception as e:
                    print(f"  ⚠️  Error fetching {slug}: {e}")
            
            if len(matched_markets) >= max_markets:
                break
        
        if not matched_markets:
            print(f"⚠️  No markets found matching {target_assets} + {window_durations}")
            return pd.DataFrame()
        
        df = pd.DataFrame(matched_markets)
        print(f"✅ Discovered {len(df)} active markets")
        
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
        target_assets = _parse_list_field(config.get('targetAssets', 'BTC,ETH,SOL'))
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
