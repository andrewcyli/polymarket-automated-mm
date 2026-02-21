import gc  # Garbage collection
import time  # Time functions
import asyncio  # Asynchronous I/O
import traceback  # Exception handling
import logging  # Logging for debugging
import signal  # Signal handling for graceful shutdown
import pandas as pd  # For data processing
from poly_data.polymarket_client import PolymarketClient
from poly_data.data_utils import update_markets, update_positions, update_orders
from poly_data.websocket_handlers import connect_market_websocket, connect_user_websocket
import poly_data.global_state as global_state
from poly_data.data_processing import remove_from_performing
from poly_data.position_snapshot import log_position_snapshot
from dotenv import load_dotenv

# Command Center integration (non-blocking, fault-tolerant)
from polymaker_client import cc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('main.log'),  # Log to file
        logging.StreamHandler()  # Log to console
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

def update_once():
    """
    Initialize the application state by fetching market data, positions, and orders.
    """
    update_markets()  # Get market information from Command Center
    update_positions()  # Get current positions from Polymarket
    update_orders()  # Get current orders from Polymarket
    logger.info(f"Loaded {len(global_state.df)} markets from Command Center")

def remove_from_pending():
    """
    Clean up stale trades that have been pending for too long (>15 seconds).
    """
    try:
        current_time = time.time()
        for col in list(global_state.performing.keys()):
            for trade_id in list(global_state.performing[col]):
                try:
                    if current_time - global_state.performing_timestamps[col].get(trade_id, current_time) > 15:
                        logger.info(f"Removing stale entry {trade_id} from {col} after 15 seconds")
                        remove_from_performing(col, trade_id)
                except:
                    logger.error(f"Error removing trade {trade_id} from {col}: {traceback.format_exc()}")
    except:
        logger.error(f"Error in remove_from_pending: {traceback.format_exc()}")

def _get_current_pnl() -> float:
    """Calculate approximate current P&L from positions."""
    try:
        total_pnl = 0.0
        for token_id, pos in global_state.positions.items():
            size = pos.get('size', 0)
            avg_price = pos.get('avgPrice', 0)
            if size > 0 and avg_price > 0:
                # Approximate P&L based on position (actual P&L requires market price)
                # This is a rough estimate; real P&L comes from fills
                pass
        return total_pnl
    except:
        return 0.0

def _get_current_bankroll() -> float:
    """Get current USDC balance if available."""
    try:
        if global_state.client:
            return float(global_state.client.get_usdc_balance())
    except:
        pass
    return 0.0

def _report_active_markets():
    """Report currently active markets to the Command Center."""
    try:
        if global_state.df is None or global_state.df.empty:
            return
        markets = []
        for _, row in global_state.df.iterrows():
            # Extract asset name from the question (e.g., "Will BTC go up..." -> "BTC")
            question = str(row.get('question', ''))
            asset = "UNKNOWN"
            for crypto in ["BTC", "ETH", "SOL", "DOGE", "XRP", "ADA", "MATIC", "AVAX", "LINK", "DOT"]:
                if crypto in question.upper():
                    asset = crypto
                    break
            
            # Detect window from question
            window = "15m"
            if "5 min" in question.lower() or "5m" in question.lower():
                window = "5m"
            elif "1 hour" in question.lower() or "1h" in question.lower():
                window = "1h"
            
            token1 = str(row.get('token1', ''))
            if token1:
                markets.append({
                    "tokenId": token1,
                    "asset": asset,
                    "window": window,
                })
        
        if markets:
            cc.report_markets(markets[:20])  # Report up to 20 markets
    except Exception as e:
        logger.debug(f"Could not report markets to Command Center: {e}")

async def update_periodically():
    """
    Asynchronous function that periodically updates market data, positions, and orders.
    - Positions and orders every 10 seconds
    - Market data every 60 seconds (every 6 cycles)
    - Position snapshots every 5 minutes (every 30 cycles)
    - Command Center metrics push every 30 seconds (auto-throttled)
    - Stale pending trades removed each cycle
    """
    i = 1
    while True:
        await asyncio.sleep(10)  # Update every 10 seconds
        try:
            remove_from_pending()
            update_positions(avgOnly=True)
            update_orders()
            
            # Increment Command Center cycle counter
            cc.increment_cycle()
            
            # Push metrics to Command Center (auto-throttled to every 30s)
            cc.push_metrics_if_due(
                total_pnl=_get_current_pnl(),
                ending_bankroll=_get_current_bankroll(),
            )
            
            if i % 6 == 0:  # Every 60 seconds
                update_markets()
                # Report active markets to Command Center
                _report_active_markets()
            if i % 30 == 0:  # Every 5 minutes (300 seconds)
                log_position_snapshot()
            i += 1
            if i > 30:
                i = 1
        except Exception as e:
            logger.error(f"Error in update_periodically: {e}")

async def main():
    """
    Main application entry point. Initializes client, data, and manages websocket connections.
    """
    # Initialize client with error handling
    try:
        logger.info("=" * 80)
        logger.info("STARTING POLYMARKET TRADING BOT")
        logger.info("=" * 80)
        global_state.client = PolymarketClient()
        logger.info("✓ Polymarket client initialized successfully")
    except ValueError as e:
        logger.error(f"❌ Configuration error: {e}")
        logger.error("Please check your .env file and ensure PK and BROWSER_ADDRESS are set correctly.")
        return
    except RuntimeError as e:
        logger.error(f"❌ Authentication error: {e}")
        logger.error("Please verify your private key and API credentials are valid.")
        return
    except Exception as e:
        logger.error(f"❌ Unexpected error during client initialization: {e}")
        logger.error(traceback.format_exc())
        return

    # Initialize state and fetch initial data
    try:
        global_state.all_tokens = []
        update_once()
        logger.info(f"After initial updates: orders={global_state.orders}, positions={global_state.positions}")
    except Exception as e:
        logger.error(f"❌ Failed to load initial market data: {e}")
        logger.error("Please check your Command Center connection and network.")
        return

    # ========== Command Center: Start Run ==========
    # Try to fetch config from Command Center and start a run session
    cc_config = cc.get_config()
    config_id = 1  # Default config ID
    run_mode = "live"
    starting_bankroll = _get_current_bankroll()
    
    if cc_config:
        config_id = cc_config.get("id", 1)
        logger.info(f"✓ Fetched config #{config_id} from Command Center")
        # You can optionally apply config values here:
        # e.g., override trading parameters from cc_config
    else:
        logger.info("ℹ️  No Command Center config available, using local settings")
    
    run_id = cc.start_run(
        config_id=config_id,
        mode=run_mode,
        bankroll=starting_bankroll,
    )
    if run_id:
        logger.info(f"✓ Command Center run #{run_id} started")
    
    # Report initial active markets
    _report_active_markets()

    # ========== Graceful Shutdown Handler ==========
    shutdown_event = asyncio.Event()
    
    def _handle_shutdown(signum, frame):
        logger.info(f"\n{'=' * 80}")
        logger.info("SHUTTING DOWN TRADING BOT")
        logger.info(f"{'=' * 80}")
        
        # Stop the run in the Command Center
        if cc.current_run_id:
            cc.stop_run(
                status="completed",
                total_cycles=cc.cycle_count,
                total_orders=cc.total_orders,
                total_fills=cc.total_fills,
                total_pnl=_get_current_pnl(),
                ending_bankroll=_get_current_bankroll(),
            )
        
        shutdown_event.set()
    
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    # Subscribe to WebSocket using subscribed_assets (token IDs), not condition_ids
    # subscribed_assets is populated by update_markets() in data_utils.py
    logger.info(f"Subscribing to WebSocket for {len(global_state.subscribed_assets)} token IDs: {global_state.subscribed_assets}")

    logger.info(
        f"There are {len(global_state.df)} markets, {len(global_state.positions)} positions, and {len(global_state.orders)} orders. Starting positions: {global_state.positions}")

    # Start periodic updates as an async task
    asyncio.create_task(update_periodically())

    # Main loop - maintain websocket connections with backoff
    backoff_time = 5
    while not shutdown_event.is_set():
        try:
            # Connect to market and user websockets simultaneously
            # Subscribe using token IDs from subscribed_assets, not condition_ids
            await asyncio.gather(
                connect_market_websocket(list(global_state.subscribed_assets)),
                connect_user_websocket()
            )
            logger.info("Reconnecting to the websocket")
            backoff_time = 5  # Reset backoff on success
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            await asyncio.sleep(backoff_time)
            backoff_time = min(backoff_time * 2, 60)  # Exponential backoff up to 60 seconds

if __name__ == "__main__":
    asyncio.run(main())
