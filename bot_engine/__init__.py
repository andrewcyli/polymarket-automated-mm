"""bot_engine — modular trading bot package extracted from trading_bot_v15.py"""

from bot_engine.config import BotConfig
from bot_engine.logging_setup import setup_logging, AuditLogger, LOG_DIR
from bot_engine.constants import (
    PROXY_EXEC_ABI, ZERO_ADDR, CTF_FULL_ABI, CTF_ADDRESS, USDC_E_ADDRESS,
    HAS_WEB3, HAS_CLOB, _encode_abi,
)
from bot_engine.pricing import RewardOptimizer, FeeCalculator, KellySizer
from bot_engine.market_data import (
    VolatilityTracker, VPINTracker, OrderChurnManager, MergeDetector,
    ChainlinkFeed, PriceFeed, MarketDiscovery, OrderBookReader,
)
from bot_engine.rpc import RPCRateLimiter, WalletBalanceChecker
from bot_engine.merge_claim import AutoMerger, AutoClaimManager
from bot_engine.engine import TradingEngine
from bot_engine.simulation import SimulatedFillEngine
from bot_engine.strategies import (
    MarketMakingStrategy, LateSniper, CombinedProbArb, ContrarianFade,
)
