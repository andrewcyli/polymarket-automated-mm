"""
Polymarket Crypto Trading Bot v15.1
Pair Integrity Edition

Building from v15, fixing critical issue where DOWN orders were silently
rejected due to max_total_exposure not scaling with kelly_bankroll when
wallet auto-detect fails.

V15.1 Changes from V15:
  V15.1-1. EXPOSURE SCALES WITH BANKROLL: max_total_exposure is always set
           to kelly_bankroll * 0.80 at startup, not hardcoded $80. Previously
           only updated when wallet auto-detect succeeded, leaving $80 default
           when it failed — causing SOL DOWN orders to be silently rejected
           after $75 of exposure.
  V15.1-2. PAIR CAPITAL PRE-CHECK: Before placing UP order, verify capital
           exists for BOTH sides. If total pair cost exceeds available capital
           or would breach exposure limits, skip the entire window. Prevents
           one-sided deployments that can't hedge.
  V15.1-3. FLOATING POINT EPSILON: Pair profit check uses 0.0001 epsilon to
           avoid rejecting pairs at exactly the boundary. Fixes "Profit:
           $0.005 < min $0.005" caused by 0.004999 rounding.
  V15.1-4. VERBOSE ORDER REJECTION: place_order() now logs the specific
           reason when returning None (exposure limit, budget limit, capital
           limit). Previously failed silently, making debugging impossible.
  V15.1-5. BANKROLL AUTO-DETECT RELIABILITY: 5 attempts with 3s delay (was
           3 attempts, 2s). Invalidates balance cache between retries.
  V15.1-6. CURRENT-WINDOW FOCUS: Only place orders on windows expiring within
           max_order_horizon (default 2700s = 45min). Prevents burning capital
           and API calls on windows 90+ minutes away.

  V15.1-18. BANKROLL MANAGEMENT REDESIGN:
            get_available_capital() now returns bankroll - total_capital_used
            (no deploy_reserve_pct deduction). Bankroll = what user wants to
            deploy. max_total_exposure defaults to bankroll (not 80%).
            After merge/claim reduces capital_in_positions, available capital
            increases, allowing new trades. Example: $100 bankroll, 3x$30
            trades = $90 used, $10 available. After merge returns $40,
            capital_in_positions drops by $30, available = $40 for next trade.

  V15.1-17. FIX MERGE + REDEEM CASHFLOW:
            MERGE: _find_mergeable now builds combined lookup from _market_cache
            + expired_windows_pending_claim + window_metadata + window_fill_tokens.
            Previously, cleanup_expired_windows removed windows from _market_cache
            BEFORE merge ran, so merge could never find paired UP+DOWN holdings.
            Now merge runs BEFORE cleanup in the main loop.
            REDEEM: process_claims now checks resolution FIRST before any redeem
            attempt. Previously tried blind redeem every 3rd attempt regardless
            of resolution status, wasting RPC calls. New priority:
            1) CLOB-SELL (fastest, no gas) 2) CTF-DIRECT 3) CTF-PROXY 4) BLIND-REDEEM.
            V15.1-17b: After successful merge, reduce window_fill_cost by merged
            amount to keep reconcile accounting accurate. filled_windows guard
            remains (window stays blocked from re-entry even after merge).

  V15.1-16. PER-MARKET BUDGET INCLUDES FILLS: place_order() per-market check
            now uses window_exposure (open orders) + window_fill_cost (filled orders)
            instead of just window_exposure. Previously, after a fill removed the
            order from active_orders, window_exposure dropped, allowing hedge buys
            to pass the cap. Total spend could reach 2-3x budget_per_market.
            Also fixes reconcile_capital_from_wallet() to NOT zero
            capital_in_positions when filled_windows exist — tokens are still held
            and must be counted in P&L. The old code caused wallet-only P&L to
            trigger false loss stops (wallet down $57 but tokens worth $57 = $0
            actual loss), halting the bot for 6+ hours overnight.

  V15.1-15. PERSISTENT FILLED WINDOW GUARD: Adds filled_windows set that
            tracks windows with ANY fill. Survives reconcile_capital_from_wallet()
            which previously wiped window_fill_cost={}, allowing re-entry into
            windows that already had fills. Root cause of 5x $30 DOWN orders on
            same BTC 15m window ($150 on $100 bankroll). Guard is checked in:
            (1) MM execute, (2) Sniper/Arb/Contrarian execute, (3) tradeable
            filter, (4) strategy execution loop. Only released on window expiry
            or momentum exit. Also preserves window_fill_cost for filled windows
            during reconciliation instead of blanket reset.
  Carried from V15:
  V15-1 through V15-6, V14.1-1 through V14.1-8, V14-1 through V14-10,
  V13.1-1 through V13.1-2, V13-1 through V13-12, V8-1 through V8-13,
  V7-1 through V7-15, V5-1 through V5-24, V6-FIX
"""

import os
import sys
import json
import time
import math
import signal
import logging
import requests
import numpy as np
from datetime import datetime, timezone, date
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv

load_dotenv()

try:
    from web3 import Web3
    HAS_WEB3 = True
except ImportError:
    HAS_WEB3 = False

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY, SELL
    HAS_CLOB = True
except ImportError:
    HAS_CLOB = False


# -----------------------------------------------------------------
# Web3 v6/v7 compatibility helper
# -----------------------------------------------------------------

def _encode_abi(contract, fn_name, args):
    """Encode ABI data for a contract function call.
    Compatible with both web3 v6 (encodeABI) and v7+ (encode_abi).
    v6 uses encodeABI(fn_name=...), v7 renamed to encode_abi(abi_element_identifier=...).
    We use positional args to avoid keyword mismatch.
    """
    if hasattr(contract, 'encodeABI'):
        return contract.encodeABI(fn_name, args=args)
    elif hasattr(contract, 'encode_abi'):
        return contract.encode_abi(fn_name, args=args)  # positional: abi_element_identifier
    else:
        # Fallback: try via functions object
        fn = getattr(contract.functions, fn_name)
        if hasattr(fn(*args), 'build_transaction'):
            # web3 v7 style: use the function object directly
            return fn(*args)._encode_transaction_data()
        raise AttributeError(
            "Contract has neither encodeABI nor encode_abi. "
            "Check web3.py version compatibility.")


# -----------------------------------------------------------------
# Config
# -----------------------------------------------------------------

@dataclass
class BotConfig:
    host: str = os.getenv("POLYMARKET_HOST", os.getenv("HOST", "https://clob.polymarket.com"))
    private_key: str = os.getenv("POLY_PRIVATE_KEY", os.getenv("PK", os.getenv("PRIVATE_KEY", "")))
    api_key: str = os.getenv("POLY_API_KEY", os.getenv("API_KEY", os.getenv("CLOB_API_KEY", "")))
    api_secret: str = os.getenv("POLY_API_SECRET", os.getenv("API_SECRET", os.getenv("CLOB_SECRET", "")))
    api_passphrase: str = os.getenv("POLY_API_PASSPHRASE", os.getenv("API_PASSPHRASE", os.getenv("CLOB_PASS_PHRASE", "")))
    proxy_wallet: str = os.getenv("POLY_PROXY_WALLET", os.getenv("POLYMARKET_PROXY_ADDRESS", os.getenv("PROXY_WALLET", "")))
    chain_id: int = int(os.getenv("POLY_CHAIN_ID", os.getenv("CHAIN_ID", "137")))
    signature_type: int = int(os.getenv("POLY_SIG_TYPE", os.getenv("SIGNATURE_TYPE", "2")))
    polygon_rpc: str = os.getenv("POLYGON_RPC_URL", os.getenv("POLYGON_RPC", "https://polygon.drpc.org"))

    timeframes: list = field(default_factory=lambda: ["15m", "5m"])
    assets_15m: list = field(default_factory=lambda: ["btc", "eth", "sol", "xrp"])
    assets_5m: list = field(default_factory=lambda: ["btc", "eth", "sol"])

    # Market Making
    mm_enabled: bool = True
    mm_order_size: float = 15.0
    mm_base_spread: float = 0.030
    mm_level_spacing: float = 0.02
    mm_num_levels: int = 1
    mm_min_spread: float = 0.005
    mm_max_spread: float = 0.40
    mm_refresh_interval: float = 60.0

    # V14-1: Equal shares pair sizing
    pair_sizing_equal_shares: bool = True

    # V14-2: Multi-window discovery
    scan_windows_ahead: int = 8
    discovery_cache_ttl: float = 45.0
    negative_cache_ttl: float = 120.0

    # V14-3/4: Edge thresholds
    min_pair_edge: float = 0.02
    edge_premium_threshold: float = 0.05
    edge_premium_size_mult: float = 1.5

    # V14-9: Edge map logging interval
    edge_map_interval: int = 5

    # V13-1: Reward-optimized pricing
    reward_target_pct: float = 0.72
    reward_min_distance: float = 0.005
    reward_max_distance: float = 0.05

    # V13-4: Guaranteed-profit pair validation
    pair_min_profit: float = 0.005
    pair_validation_enabled: bool = True

    # V13.1-1: Volatility tracking
    volatility_lookback: float = 600.0
    volatility_low_threshold: float = 0.08
    volatility_high_threshold: float = 0.25
    volatility_pause_threshold: float = 0.40
    volatility_min_points: int = 5

    # V13-3: Order churn reduction
    churn_cooldown: float = 30.0
    churn_price_threshold: float = 0.015
    churn_size_threshold: float = 0.25
    churn_enabled: bool = True

    # V13-6: Profitability scoring
    profitability_scoring_enabled: bool = True
    profitability_min_score: float = 0.5

    # V13-8 / V14.1-1: Position merge
    merge_detection_enabled: bool = True
    merge_min_amount: float = 20.0
    auto_merge_enabled: bool = True
    merge_min_shares: float = 1.0
    merge_position_decimals: int = 6

    # V14.1-3: Immediate pair completion
    immediate_pair_completion: bool = True
    pair_completion_max_loss: float = 0.030

    # Trend Following (DISABLED)
    tf_enabled: bool = False
    tf_directional_size: float = 5.0
    tf_momentum_threshold: float = 0.001
    tf_strong_threshold: float = 0.003
    tf_skew_factor: float = 0.03
    tf_lookback_minutes: int = 5

    # Late Sniper
    sniper_enabled: bool = True
    sniper_time_window: float = 180.0
    sniper_min_probability: float = 0.85
    sniper_max_price: float = 0.82
    sniper_min_edge: float = 0.01
    sniper_size: float = 10.0

    # Combined Probability Arb
    arb_enabled: bool = True
    arb_min_profit: float = 0.015
    arb_max_size: float = 20.0
    arb_scan_interval: float = 15.0
    arb_leg_timeout: float = 30.0

    # Contrarian Panic Fade
    contrarian_enabled: bool = True
    contrarian_panic_threshold: float = 0.15
    contrarian_size: float = 8.0
    contrarian_min_time: float = 120.0

    # Kelly Criterion
    kelly_enabled: bool = True
    kelly_fraction: float = 0.25
    kelly_bankroll: float = float(os.getenv("KELLY_BANKROLL", "100"))

    # Risk
    max_total_exposure: float = 80.0
    max_position_per_market: float = 35.0
    max_daily_loss: float = 20.0
    min_time_remaining: float = 60.0
    max_asset_exposure_pct: float = 0.45
    max_concurrent_windows: int = 6
    deploy_reserve_pct: float = 0.20

    vol_circuit_breaker: float = 0.015

    hard_loss_stop_pct: float = 0.20
    hard_loss_cooloff: int = 600

    stale_order_max_age: float = 120.0

    # V15.1-6: Don't place orders on windows too far out
    max_order_horizon: float = 2700.0  # 45 minutes

    # V15.1-22: Max time sessions to consider per timeframe (closest N)
    # e.g. 3 means only look at the 3 closest 5m sessions and 3 closest 15m sessions
    max_order_sessions: int = 3
    # V15.1-22: Enable dynamic re-prioritization (cancel+re-place when better markets appear)
    dynamic_reprioritize: bool = True

    # Simulation
    sim_fill_rate: float = 0.25
    sim_slippage_max: float = 0.002
    sim_partial_fill_min: float = 0.40
    max_fills_per_window: int = 12

    trade_advance_windows: bool = True
    advance_window_min_time: float = 45.0
    sequential_deployment: bool = True

    direction_lock_enabled: bool = True
    mm_block_opposing_fills: bool = True
    sniper_reserved_min: float = 10.0
    max_chainlink_confidence: float = 0.70
    sync_balance_per_cycle: bool = True
    max_orders_per_market: int = 4
    auto_detect_bankroll: bool = True
    rotate_asset_priority: bool = True

    strategy_budget_pct: dict = field(default_factory=lambda: {
        "mm": 0.80, "trend": 0.00, "sniper": 0.15, "arb": 0.03, "contrarian": 0.02
    })
    strategy_budget_enabled: bool = True

    budget_current_window_pct: float = 0.60
    budget_next_window_pct: float = 0.15
    min_next_window_time: float = 120.0

    hedge_completion_enabled: bool = True
    hedge_max_loss_per_share: float = 0.020
    hedge_completion_delay: float = 3.0       # V15.1-13: Reduced from 2.0 — hedge faster
    hedge_vol_multiplier: float = 2.0
    hedge_min_loss_threshold: float = 0.005
    hedge_max_loss_threshold: float = 0.025
    hedge_max_combined_cost: float = 0.98     # V15.1-13: Max UP+DOWN+fees before rejecting hedge
    hedge_min_profit_per_share: float = 0.005 # V15.1-13: Min profit/share after fees to accept hedge

    auto_claim_enabled: bool = True
    claim_delay_seconds: float = 30.0
    claim_check_interval: float = 15.0
    claim_max_attempts: int = 60
    claim_timeout_seconds: float = 3600.0
    claim_fallback_sell: bool = True
    claim_sell_min_price: float = 0.95
    blind_redeem_enabled: bool = True

    pre_exit_enabled: bool = True
    pre_exit_time_seconds: float = 30.0
    pre_exit_min_confidence: float = 0.75
    pre_exit_min_price: float = 0.80

    # V15.1-14: Momentum exit — sell one-sided fill if price rises >X%
    momentum_exit_enabled: bool = True
    momentum_exit_threshold: float = 0.03    # 3% price increase triggers sell
    momentum_exit_min_hold_secs: float = 10.0  # Min hold time before checking
    momentum_exit_max_wait_secs: float = 120.0 # Max wait for hedge before checking momentum

    # V15.1-19: Pre-entry filters for orphan reduction
    momentum_gate_threshold: float = 0.003   # Skip MM if |momentum| > 0.3%
    momentum_gate_lookback: int = 5          # Lookback minutes for momentum gate
    min_book_depth: float = 5.0              # Min $ depth within 2c of target price
    max_spread_asymmetry: float = 0.02       # Max spread difference between UP/DN
    # V15.1-19: Session blackout windows (list of [start_hour_utc, end_hour_utc] pairs)
    trading_blackout_windows: list = field(default_factory=list)

    rpc_gas_cache_ttl: float = 30.0
    rpc_min_call_interval: float = 1.5

    check_wallet_balance: bool = True
    wallet_balance_cache_ttl: float = 10.0

    track_unredeemed_value: bool = True

    dry_run: bool = os.getenv("DRY_RUN", "false").lower() != "false"
    log_level: str = "INFO"
    cycle_interval: int = 10
    summary_interval: int = 30

    def validate(self):
        errors = []
        sniper_edge = self.sniper_min_probability - self.sniper_max_price
        if sniper_edge < self.sniper_min_edge:
            errors.append(
                "sniper edge ({:.3f}) < min_edge ({:.3f})".format(sniper_edge, self.sniper_min_edge))
        if self.kelly_fraction <= 0 or self.kelly_fraction > 1:
            errors.append("kelly_fraction must be in (0, 1]")
        if self.kelly_bankroll <= 0:
            errors.append("kelly_bankroll must be > 0")
        if self.mm_base_spread >= self.mm_max_spread:
            errors.append("mm_base_spread >= mm_max_spread")
        if self.max_asset_exposure_pct <= 0 or self.max_asset_exposure_pct > 1:
            errors.append("max_asset_exposure_pct must be in (0, 1]")
        if self.arb_min_profit <= 0:
            errors.append("arb_min_profit must be > 0")
        if self.pair_min_profit < 0:
            errors.append("pair_min_profit must be >= 0")
        if self.min_pair_edge < 0:
            errors.append("min_pair_edge must be >= 0")
        return errors


# -----------------------------------------------------------------
# Logging
# -----------------------------------------------------------------

LOG_DIR = "logs"


def setup_logging(level="INFO"):
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = logging.getLogger("polybot_v15_1")
    logger.setLevel(getattr(logging, level, logging.INFO))
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    fh = RotatingFileHandler("bot_v15_1.log", maxBytes=10 * 1024 * 1024, backupCount=5)
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    session_path = os.path.join(LOG_DIR, "bot_v15_1_{}".format(ts) + ".log")
    sh = logging.FileHandler(session_path, mode="w")
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.info("  Session log: {}".format(session_path))
    return logger


# -----------------------------------------------------------------
# Shared constants and ABIs
# -----------------------------------------------------------------

PROXY_EXEC_ABI = json.loads("""[
    {"inputs":[{"name":"to","type":"address"},{"name":"value","type":"uint256"},{"name":"data","type":"bytes"}],"name":"execute","outputs":[{"name":"","type":"bool"},{"name":"","type":"bytes"}],"stateMutability":"nonpayable","type":"function"},
    {"inputs":[{"name":"to","type":"address"},{"name":"data","type":"bytes"}],"name":"exec","outputs":[{"name":"","type":"bool"},{"name":"","type":"bytes"}],"stateMutability":"nonpayable","type":"function"}
]""")

CTF_FULL_ABI = json.loads("""[
    {"inputs":[{"internalType":"address","name":"collateralToken","type":"address"},{"internalType":"bytes32","name":"parentCollectionId","type":"bytes32"},{"internalType":"bytes32","name":"conditionId","type":"bytes32"},{"internalType":"uint256[]","name":"indexSets","type":"uint256[]"}],"name":"redeemPositions","outputs":[],"stateMutability":"nonpayable","type":"function"},
    {"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"name":"payoutDenominator","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},
    {"inputs":[{"internalType":"address","name":"collateralToken","type":"address"},{"internalType":"bytes32","name":"parentCollectionId","type":"bytes32"},{"internalType":"bytes32","name":"conditionId","type":"bytes32"},{"internalType":"uint256[]","name":"indexSets","type":"uint256[]"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"mergePositions","outputs":[],"stateMutability":"nonpayable","type":"function"},
    {"inputs":[{"internalType":"address","name":"account","type":"address"},{"internalType":"uint256","name":"id","type":"uint256"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},
    {"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"operator","type":"address"}],"name":"isApprovedForAll","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},
    {"inputs":[{"internalType":"address","name":"operator","type":"address"},{"internalType":"bool","name":"approved","type":"bool"}],"name":"setApprovalForAll","outputs":[],"stateMutability":"nonpayable","type":"function"}
]""")

CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC_E_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"


# -----------------------------------------------------------------
# V13-1: Reward Optimizer
# -----------------------------------------------------------------

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
# -----------------------------------------------------------------

class VolatilityTracker:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.thresholds = (
            config.volatility_low_threshold,
            config.volatility_high_threshold,
            config.volatility_pause_threshold,
        )
        self.window_sec = config.volatility_lookback
        self.min_points = getattr(config, "volatility_min_points", 5)
        self.condition_asset_map = {}
        self.token_asset_map = {}
        self.asset_prices = {}
        self._cache = {}
        self._cache_ttl = 10.0

    def register_condition(self, condition_id, asset):
        if condition_id and condition_id not in self.condition_asset_map:
            self.condition_asset_map[condition_id] = asset.upper()

    def register_token(self, token_id, asset):
        if token_id and token_id not in self.token_asset_map:
            self.token_asset_map[token_id] = asset.upper()

    def update_price(self, asset, price, ts=None):
        a = asset.upper()
        ts = ts or time.time()
        if a not in self.asset_prices:
            self.asset_prices[a] = []
        self.asset_prices[a].append((ts, price))
        cutoff = ts - self.window_sec * 2
        self.asset_prices[a] = [(t, p) for t, p in self.asset_prices[a] if t >= cutoff]

    def _resolve_asset(self, identifier):
        if not identifier:
            return None
        upper = identifier.upper()
        if upper in self.asset_prices or upper in ("BTC", "ETH", "SOL", "XRP"):
            return upper
        if identifier in self.token_asset_map:
            return self.token_asset_map[identifier]
        if identifier in self.condition_asset_map:
            return self.condition_asset_map[identifier]
        return None

    def _compute_vol(self, asset):
        now = time.time()
        if asset in self._cache:
            vc, vv, ct = self._cache[asset]
            if now - ct < self._cache_ttl:
                return vc, vv
        hist = self.asset_prices.get(asset)
        if not hist:
            return "UNKNOWN", None
        cutoff = now - self.window_sec
        prices = [p for t, p in hist if t >= cutoff]
        if len(prices) < self.min_points:
            return "UNKNOWN", None
        arr = np.array(prices, dtype=np.float64)
        if np.any(arr <= 0):
            return "UNKNOWN", None
        log_returns = np.diff(np.log(arr))
        if len(log_returns) == 0:
            return "UNKNOWN", None
        vol = float(np.sum(np.abs(log_returns)))
        vc = self._classify(vol)
        self._cache[asset] = (vc, vol, now)
        return vc, vol

    def _classify(self, vol):
        if vol < self.thresholds[0]:
            return "LOW"
        elif vol < self.thresholds[1]:
            return "MEDIUM"
        elif vol < self.thresholds[2]:
            return "HIGH"
        return "EXTREME"

    def get_volatility_level(self, identifier):
        asset = self._resolve_asset(identifier)
        if not asset:
            return "UNKNOWN", None
        return self._compute_vol(asset)

    def get_volatility_sum(self, identifier, lookback=None):
        _, vol = self.get_volatility_level(identifier)
        return vol

    def should_trade(self, identifier):
        level, _ = self.get_volatility_level(identifier)
        return level in ("LOW", "MEDIUM", "UNKNOWN")

    def get_dynamic_hedge_threshold(self, identifier, base_threshold):
        level, vol = self.get_volatility_level(identifier)
        if vol is None:
            return base_threshold
        cfg = self.config
        if level == "LOW":
            return max(cfg.hedge_min_loss_threshold, base_threshold * 0.5)
        elif level == "MEDIUM":
            return base_threshold
        else:
            return min(cfg.hedge_max_loss_threshold,
                       base_threshold * cfg.hedge_vol_multiplier)

    def get_profitability_score(self, identifier, estimated_reward):
        _, vol = self.get_volatility_level(identifier)
        if vol is None:
            vol = 0.01
        return estimated_reward / (vol + 0.001)

    def get_all_stats(self):
        stats = {}
        for cid, asset in self.condition_asset_map.items():
            level, vol = self._compute_vol(asset)
            key = str(cid)[:20] + "..."
            stats[key] = {"level": level, "vol_sum": vol, "asset": asset}
        if not stats:
            for asset in self.asset_prices:
                level, vol = self._compute_vol(asset)
                stats[asset] = {"level": level, "vol_sum": vol, "asset": asset}
        return stats


# -----------------------------------------------------------------
# V13-3: Order Churn Manager
# -----------------------------------------------------------------

class OrderChurnManager:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self._last_action_time = {}
        self._last_order_params = {}
        self._suppressed_count = 0
        self._allowed_count = 0

    def should_update_orders(self, window_id, token_id, new_price, new_size):
        if not self.config.churn_enabled:
            return True
        now = time.time()
        last_time = self._last_action_time.get(window_id, 0)
        if now - last_time < self.config.churn_cooldown:
            self._suppressed_count += 1
            return False
        key = "{}|{}".format(window_id, token_id)
        prev = self._last_order_params.get(key)
        if prev:
            price_diff = abs(new_price - prev["price"]) / prev["price"] if prev["price"] > 0 else 1.0
            size_diff = abs(new_size - prev["size"]) / prev["size"] if prev["size"] > 0 else 1.0
            if price_diff < self.config.churn_price_threshold and size_diff < self.config.churn_size_threshold:
                self._suppressed_count += 1
                return False
        self._allowed_count += 1
        return True

    def record_update(self, window_id, token_id, price, size):
        self._last_action_time[window_id] = time.time()
        key = "{}|{}".format(window_id, token_id)
        self._last_order_params[key] = {"price": price, "size": size}

    def force_allow(self, window_id):
        self._last_action_time.pop(window_id, None)

    def cleanup_window(self, window_id):
        self._last_action_time.pop(window_id, None)
        keys_to_remove = [k for k in self._last_order_params if k.startswith(window_id + "|")]
        for k in keys_to_remove:
            del self._last_order_params[k]

    def get_stats(self):
        total = self._suppressed_count + self._allowed_count
        reduction_pct = (self._suppressed_count / total * 100) if total > 0 else 0
        return {
            "suppressed": self._suppressed_count,
            "allowed": self._allowed_count,
            "reduction_pct": reduction_pct,
        }


# -----------------------------------------------------------------
# V13-8: Position Merge Detector (alert-only)
# -----------------------------------------------------------------

class MergeDetector:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self._alerted = set()

    def check_merges(self, token_holdings, market_cache):
        if not self.config.merge_detection_enabled:
            return []
        mergeable = []
        window_tokens = {}
        for token_id, holding in token_holdings.items():
            size = holding.get("size", 0)
            if size < 1.0:
                continue
            for wid, market in market_cache.items():
                if token_id == market.get("token_up") or token_id == market.get("token_down"):
                    if wid not in window_tokens:
                        window_tokens[wid] = {"up": 0, "down": 0, "market": market}
                    if token_id == market.get("token_up"):
                        window_tokens[wid]["up"] = size
                    else:
                        window_tokens[wid]["down"] = size
        for wid, info in window_tokens.items():
            up_size = info["up"]
            down_size = info["down"]
            mergeable_amount = min(up_size, down_size)
            if mergeable_amount >= self.config.merge_min_amount and wid not in self._alerted:
                self._alerted.add(wid)
                mergeable.append({
                    "window_id": wid, "mergeable_shares": mergeable_amount,
                    "up_held": up_size, "down_held": down_size,
                    "freed_capital_est": mergeable_amount,
                })
                self.logger.info(
                    "  MERGE OPPORTUNITY | {} | {:.0f} shares mergeable | "
                    "UP:{:.0f} DOWN:{:.0f} | ~${:.0f} locked".format(
                        wid, mergeable_amount, up_size, down_size, mergeable_amount))
        return mergeable


# -----------------------------------------------------------------
# Fee Calculator
# -----------------------------------------------------------------

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


class RPCRateLimiter:
    def __init__(self, min_interval=1.5):
        self.min_interval = min_interval
        self._last_call = 0
        self._backoff_until = 0
        self._gas_cache = None
        self._gas_cache_time = 0
        self._gas_cache_ttl = 30.0

    def wait(self):
        now = time.time()
        if now < self._backoff_until:
            time.sleep(self._backoff_until - now)
        elapsed = time.time() - self._last_call
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last_call = time.time()

    def report_rate_limit(self, retry_after=10):
        self._backoff_until = time.time() + retry_after + 2

    def get_gas_price(self, w3):
        now = time.time()
        if self._gas_cache and now - self._gas_cache_time < self._gas_cache_ttl:
            return self._gas_cache
        self.wait()
        try:
            price = w3.eth.gas_price
            self._gas_cache = price
            self._gas_cache_time = now
            return price
        except Exception:
            return self._gas_cache or 50_000_000_000

    @property
    def is_backed_off(self):
        return time.time() < self._backoff_until


# -----------------------------------------------------------------
# Chainlink Direct Price Feed
# -----------------------------------------------------------------

class ChainlinkFeed:
    ABI = json.loads('[{"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"latestRoundData","outputs":[{"internalType":"uint80","name":"roundId","type":"uint80"},{"internalType":"int256","name":"answer","type":"int256"},{"internalType":"uint256","name":"startedAt","type":"uint256"},{"internalType":"uint256","name":"updatedAt","type":"uint256"},{"internalType":"uint80","name":"answeredInRound","type":"uint80"}],"stateMutability":"view","type":"function"}]')
    FEEDS = {
        "btc": "0xc907E116054Ad103354f2D350FD2514433D57F6f",
        "eth": "0xF9680D99D6C9589e2a93a78A04A279e509205945",
        "xrp": "0x785ba89291f676b5386652eB12b30cF361020694",
        "sol": "0x4FFD6aE0DC14AEA55966903817BC3deA47b924CF",
    }

    def __init__(self, rpc_url, logger):
        self.logger = logger
        self.w3 = None
        self.contracts = {}
        self.cache = {}
        self.cache_ttl = 5
        if HAS_WEB3:
            try:
                self.w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 10}))
                for asset, addr in self.FEEDS.items():
                    self.contracts[asset] = self.w3.eth.contract(
                        address=Web3.to_checksum_address(addr), abi=self.ABI)
                self.logger.info("  Chainlink feed initialized (Polygon Mainnet)")
            except Exception as e:
                self.logger.warning(f"  Chainlink init failed: {e}")
                self.w3 = None

    def get_price(self, asset):
        if not self.w3 or asset not in self.contracts:
            return None
        now = time.time()
        cached = self.cache.get(asset)
        if cached and now - cached["time"] < self.cache_ttl:
            return cached["price"]
        try:
            data = self.contracts[asset].functions.latestRoundData().call()
            decimals = self.contracts[asset].functions.decimals().call()
            price = data[1] / (10 ** decimals)
            self.cache[asset] = {"price": price, "time": now, "updated": data[3]}
            return price
        except Exception:
            return None

    def predict_resolution(self, asset, window_start_ts, window_end_ts):
        current = self.get_price(asset)
        if not current:
            return None
        start_price = self.cache.get(f"{asset}_start_{window_start_ts}")
        if not start_price:
            self.cache[f"{asset}_start_{window_start_ts}"] = current
            return None
        pct_change = (current - start_price) / start_price
        now = time.time()
        elapsed = now - window_start_ts
        total = window_end_ts - window_start_ts
        progress = max(0.01, min(elapsed / total, 1.0)) if total > 0 else 0.5
        k = 200 + (1800 * progress)
        prob_up = 1.0 / (1.0 + np.exp(-k * pct_change))
        prob_up = max(0.01, min(0.99, prob_up))
        direction = "UP" if prob_up >= 0.5 else "DOWN"
        raw_confidence = prob_up if direction == "UP" else (1 - prob_up)
        confidence = min(raw_confidence, 0.70)
        if direction == "UP":
            prob_up_capped = min(prob_up, 0.70)
            prob_down_capped = 1 - prob_up_capped
        else:
            prob_down_capped = min(1 - prob_up, 0.70)
            prob_up_capped = 1 - prob_down_capped
        return {
            "direction": direction, "confidence": confidence,
            "prob_up": prob_up_capped, "prob_down": prob_down_capped,
            "pct_change": pct_change, "k_factor": k,
            "raw_confidence": raw_confidence,
        }


class PriceFeed:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.prices = {}
        self.price_history = {}
        self.use_chainlink = False
        self.chainlink = None
        if HAS_WEB3:
            self.chainlink = ChainlinkFeed(config.polygon_rpc, logger)
            if self.chainlink.w3:
                self.use_chainlink = True

    def update(self):
        all_assets = list(set(self.config.assets_15m + self.config.assets_5m))
        for asset in all_assets:
            price = None
            if self.use_chainlink:
                price = self.chainlink.get_price(asset)
            if not price:
                price = self._coingecko_price(asset)
            if price:
                self.prices[asset] = price
                if asset not in self.price_history:
                    self.price_history[asset] = []
                self.price_history[asset].append({"time": time.time(), "price": price})
                max_hist = 120
                if len(self.price_history[asset]) > max_hist:
                    self.price_history[asset] = self.price_history[asset][-max_hist:]

    def _coingecko_price(self, asset):
        cg_map = {"btc": "bitcoin", "eth": "ethereum", "sol": "solana", "xrp": "ripple"}
        cg_id = cg_map.get(asset)
        if not cg_id:
            return None
        try:
            resp = api_retry(lambda: requests.get(
                "https://api.coingecko.com/api/v3/simple/price",
                params={"ids": cg_id, "vs_currencies": "usd"}, timeout=10,
            ), logger=self.logger)
            if resp and resp.status_code == 200:
                data = resp.json()
                return data.get(cg_id, {}).get("usd")
        except Exception:
            pass
        return None

    def get_current_price(self, asset):
        return self.prices.get(asset)

    def get_momentum(self, asset, lookback_minutes=5):
        hist = self.price_history.get(asset, [])
        if len(hist) < 2:
            return None
        cutoff = time.time() - (lookback_minutes * 60)
        recent = [h for h in hist if h["time"] >= cutoff]
        if len(recent) < 2:
            if len(hist) >= 2:
                recent = hist[-min(10, len(hist)):]
            else:
                return None
        first = recent[0]["price"]
        last = recent[-1]["price"]
        if first <= 0:
            return None
        return (last - first) / first

    def predict_resolution(self, asset, window_start_ts, window_end_ts):
        if self.use_chainlink:
            return self.chainlink.predict_resolution(asset, window_start_ts, window_end_ts)
        return None

    def get_price_source(self):
        return "Chainlink (Polygon)" if self.use_chainlink else "CoinGecko"


# -----------------------------------------------------------------
# Market Discovery
# -----------------------------------------------------------------

class MarketDiscovery:
    GAMMA_BASE = "https://gamma-api.polymarket.com"

    def __init__(self, config, logger=None):
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self._pos_cache = {}
        self._neg_cache = {}
        self._discovery_count = 0

    def discover(self):
        markets = []
        now = int(time.time())
        now_f = time.time()
        self._discovery_count += 1
        for tf in self.config.timeframes:
            assets = self.config.assets_15m if tf == "15m" else self.config.assets_5m
            interval = 900 if tf == "15m" else 300
            current_end = ((now // interval) + 1) * interval
            for i in range(self.config.scan_windows_ahead):
                ts = current_end + (i * interval)
                time_left = ts - now
                if time_left < 30:
                    continue
                is_advance = (i > 0)
                for asset in assets:
                    slug = f"{asset}-updown-{tf}-{ts}"
                    if slug in self._neg_cache:
                        if now_f - self._neg_cache[slug] < self.config.negative_cache_ttl:
                            continue
                        else:
                            del self._neg_cache[slug]
                    if slug in self._pos_cache:
                        entry = self._pos_cache[slug]
                        if now_f - entry["time"] < self.config.discovery_cache_ttl:
                            m = entry["data"].copy()
                            m["time_left"] = time_left
                            m["is_advance"] = is_advance
                            m["window_id"] = f"{asset}-{tf}-{ts}"
                            markets.append(m)
                            continue
                        else:
                            del self._pos_cache[slug]
                    market_data = self._fetch_slug(slug, asset, tf, ts, time_left, interval, is_advance)
                    if market_data:
                        self._pos_cache[slug] = {"data": market_data, "time": now_f}
                        markets.append(market_data)
                    else:
                        self._neg_cache[slug] = now_f
        if self._discovery_count % 50 == 0:
            self._prune_caches(now_f)
        return markets

    def _fetch_slug(self, slug, asset, tf, ts, time_left, interval, is_advance):
        try:
            resp = api_retry(lambda s=slug: requests.get(
                f"{self.GAMMA_BASE}/events", params={"slug": s}, timeout=10,
            ), logger=self.logger)
            if not resp or resp.status_code != 200:
                return None
            events = resp.json()
            if not events:
                return None
            event = events[0]
            event_markets = event.get("markets", [])
            if not event_markets:
                return None
            m = event_markets[0]
            clob_ids_raw = m.get("clobTokenIds", "[]")
            if isinstance(clob_ids_raw, str):
                clob_ids = json.loads(clob_ids_raw)
            else:
                clob_ids = clob_ids_raw
            if len(clob_ids) < 2:
                return None
            prices_raw = m.get("outcomePrices", "[]")
            if isinstance(prices_raw, str):
                prices = json.loads(prices_raw)
            else:
                prices = prices_raw
            price_up = float(prices[0]) if prices else 0.5
            price_down = float(prices[1]) if len(prices) > 1 else 0.5
            gamma_sum = price_up + price_down
            gamma_edge_est = max(0, 1.0 - gamma_sum - 0.015)
            return {
                "slug": slug, "asset": asset, "timeframe": tf,
                "timestamp": ts, "token_up": clob_ids[0],
                "token_down": clob_ids[1], "price_up": price_up,
                "price_down": price_down,
                "condition_id": m.get("conditionId", ""),
                "end_time": ts, "window_id": f"{asset}-{tf}-{ts}",
                "interval": interval, "time_left": time_left,
                "is_advance": is_advance,
                "gamma_sum": gamma_sum,
                "gamma_edge_est": gamma_edge_est,
            }
        except Exception as e:
            self.logger.debug(f"  Failed to discover {slug}: {e}")
            return None

    def _prune_caches(self, now_f):
        stale_pos = [k for k, v in self._pos_cache.items()
                     if now_f - v["time"] > self.config.discovery_cache_ttl * 3]
        for k in stale_pos:
            del self._pos_cache[k]
        stale_neg = [k for k, v in self._neg_cache.items()
                     if now_f - v > self.config.negative_cache_ttl * 3]
        for k in stale_neg:
            del self._neg_cache[k]

    def get_cache_stats(self):
        return {
            "pos_cache": len(self._pos_cache),
            "neg_cache": len(self._neg_cache),
            "discoveries": self._discovery_count,
        }


# -----------------------------------------------------------------
# Order Book Reader
# -----------------------------------------------------------------

class OrderBookReader:
    CLOB_BASE = "https://clob.polymarket.com"

    def __init__(self, logger=None):
        self.logger = logger
        self._spread_cache = {}
        self._book_cache = {}
        self._cache_ttl = 5.0

    def get_book(self, token_id):
        now = time.time()
        if token_id in self._book_cache:
            cached, ts = self._book_cache[token_id]
            if now - ts < self._cache_ttl:
                return cached
        try:
            resp = api_retry(lambda: requests.get(
                f"{self.CLOB_BASE}/book",
                params={"token_id": token_id}, timeout=10,
            ), logger=self.logger)
            if resp and resp.status_code == 200:
                result = resp.json()
                self._book_cache[token_id] = (result, now)
                return result
        except Exception:
            pass
        return None

    def get_spread(self, token_id):
        now = time.time()
        if token_id in self._spread_cache:
            cached, ts = self._spread_cache[token_id]
            if now - ts < self._cache_ttl:
                return cached
        book = self.get_book(token_id)
        if not book:
            return None
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        if not bids or not asks:
            return None
        sorted_bids = sorted(bids, key=lambda x: float(x["price"]), reverse=True)
        sorted_asks = sorted(asks, key=lambda x: float(x["price"]))
        real_bids = [b for b in sorted_bids if float(b["price"]) > 0.05]
        real_asks = [a for a in sorted_asks if float(a["price"]) < 0.95]
        if not real_bids or not real_asks:
            real_bids = sorted_bids
            real_asks = sorted_asks
        best_bid = float(real_bids[0]["price"])
        best_ask = float(real_asks[0]["price"])
        spread = best_ask - best_bid
        midpoint = (best_bid + best_ask) / 2
        total_bid_size = sum(float(b["size"]) for b in bids)
        total_ask_size = sum(float(a["size"]) for a in asks)
        imbalance = 0
        if (total_bid_size + total_ask_size) > 0:
            imbalance = (total_bid_size - total_ask_size) / (total_bid_size + total_ask_size)
        result = {
            "bid": best_bid, "ask": best_ask, "spread": spread, "midpoint": midpoint,
            "total_bid_size": total_bid_size, "total_ask_size": total_ask_size,
            "imbalance": imbalance,
        }
        self._spread_cache[token_id] = (result, now)
        return result

    def invalidate_cache(self, token_id=None):
        if token_id:
            self._spread_cache.pop(token_id, None)
            self._book_cache.pop(token_id, None)
        else:
            self._spread_cache.clear()
            self._book_cache.clear()

    def get_available_liquidity(self, token_id, side, price, size_needed):
        book = self.get_book(token_id)
        if not book:
            return {"available": False, "fillable_size": 0, "avg_price": price, "slippage": 1.0}
        if side == "BUY":
            orders = sorted(book.get("asks", []), key=lambda x: float(x["price"]))
        else:
            orders = sorted(book.get("bids", []), key=lambda x: float(x["price"]), reverse=True)
        filled = 0.0
        total_cost = 0.0
        for order in orders:
            op = float(order["price"])
            os_val = float(order["size"])
            can_fill = min(os_val, size_needed - filled)
            filled += can_fill
            total_cost += can_fill * op
            if filled >= size_needed:
                break
        avg_price = total_cost / filled if filled > 0 else price
        slippage = abs(avg_price - price) / price if price > 0 else 0
        return {
            "available": filled >= size_needed * 0.5,
            "fillable_size": filled, "avg_price": avg_price, "slippage": slippage,
        }


# -----------------------------------------------------------------
# Kelly Criterion
# -----------------------------------------------------------------

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
# -----------------------------------------------------------------

class WalletBalanceChecker:
    USDC_E = USDC_E_ADDRESS
    ERC20_ABI = json.loads('[{"constant":true,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"type":"function"}]')
    # V15.1-7: Fallback RPCs for wallet balance reads
    FALLBACK_RPCS = [
        "https://polygon-bor-rpc.publicnode.com",
        "https://polygon.llamarpc.com",
        "https://rpc.ankr.com/polygon",
        "https://polygon-rpc.com",
    ]

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.w3 = None
        self.contract = None
        self.decimals = 6
        self._cache = None
        self._cache_time = 0
        self._fallback_providers = []
        self._init_web3()

    def _init_web3(self):
        if not HAS_WEB3:
            return
        try:
            self.w3 = Web3(Web3.HTTPProvider(
                self.config.polygon_rpc, request_kwargs={"timeout": 10}))
            self.contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(self.USDC_E), abi=self.ERC20_ABI)
            self.logger.info("  Wallet balance checker initialized (primary: {})".format(
                self.config.polygon_rpc[:40]))
        except Exception as e:
            self.logger.warning(f"  Wallet balance checker init failed: {e}")
            self.w3 = None
        # Pre-init fallback providers
        for rpc_url in self.FALLBACK_RPCS:
            if rpc_url == self.config.polygon_rpc:
                continue
            try:
                fb_w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 8}))
                fb_contract = fb_w3.eth.contract(
                    address=Web3.to_checksum_address(self.USDC_E), abi=self.ERC20_ABI)
                self._fallback_providers.append((rpc_url, fb_w3, fb_contract))
            except Exception:
                pass
        if self._fallback_providers:
            self.logger.info("  Wallet balance: {} fallback RPCs ready".format(
                len(self._fallback_providers)))

    def _read_balance_from(self, contract, wallet):
        raw = contract.functions.balanceOf(
            Web3.to_checksum_address(wallet)).call()
        return raw / (10 ** self.decimals)

    def get_balance(self):
        wallet = self.config.proxy_wallet
        if not wallet:
            return None
        now = time.time()
        if self._cache is not None and now - self._cache_time < self.config.wallet_balance_cache_ttl:
            return self._cache
        # Try primary RPC
        if self.w3 and self.contract:
            try:
                balance = self._read_balance_from(self.contract, wallet)
                self._cache = balance
                self._cache_time = now
                return balance
            except Exception as e:
                self.logger.info("  Wallet read failed (primary): {}".format(
                    str(e)[:80]))
        # Try fallback RPCs
        for rpc_url, fb_w3, fb_contract in self._fallback_providers:
            try:
                balance = self._read_balance_from(fb_contract, wallet)
                self._cache = balance
                self._cache_time = now
                self.logger.info("  Wallet read OK via fallback: {}".format(
                    rpc_url[:40]))
                return balance
            except Exception as e:
                self.logger.debug("  Wallet fallback {} failed: {}".format(
                    rpc_url[:30], str(e)[:60]))
                continue
        self.logger.warning("  Wallet read failed on all RPCs (primary + {} fallbacks)".format(
            len(self._fallback_providers)))
        return self._cache


# -----------------------------------------------------------------
# V14.1-1: Auto Merger
# -----------------------------------------------------------------

class AutoMerger:
    def __init__(self, config, logger, engine=None):
        self.config = config
        self.logger = logger
        self.engine = engine
        self.w3 = None
        self.ctf_contract = None
        self.account = None
        self.proxy_contract = None
        self.rpc_limiter = RPCRateLimiter(min_interval=config.rpc_min_call_interval)
        self.merges_completed = 0
        self.merges_failed = 0
        self.total_merged_usd = 0.0
        self._merged_windows = set()
        self._init_web3()

    def set_engine(self, engine):
        self.engine = engine

    def _init_web3(self):
        if not HAS_WEB3 or not self.config.private_key:
            return
        try:
            self.w3 = Web3(Web3.HTTPProvider(
                self.config.polygon_rpc, request_kwargs={"timeout": 15}))
            self.ctf_contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(CTF_ADDRESS), abi=CTF_FULL_ABI)
            self.account = self.w3.eth.account.from_key(self.config.private_key)
            if self.config.proxy_wallet:
                try:
                    self.proxy_contract = self.w3.eth.contract(
                        address=Web3.to_checksum_address(self.config.proxy_wallet),
                        abi=PROXY_EXEC_ABI)
                except Exception:
                    self.proxy_contract = None
            self.logger.info("  AutoMerger initialized | CTF: {}...".format(CTF_ADDRESS[:10]))
        except Exception as e:
            self.logger.warning(f"  AutoMerger init failed: {e}")
            self.w3 = None

    def _to_bytes32(self, hex_str):
        clean = hex_str.replace("0x", "")
        raw = bytes.fromhex(clean)
        if len(raw) < 32:
            raw = raw.rjust(32, b'\x00')
        return raw[:32]

    def query_live_positions(self, market_cache):
        """Query on-chain CTF balanceOf for all tokens in market_cache.
        Returns dict {token_id: {"size": float, "cost": 0}} matching
        the token_holdings format so _find_mergeable can use it directly.
        Falls back to engine.token_holdings if RPC is unavailable."""
        if not self.w3 or not self.ctf_contract:
            if self.engine:
                return dict(self.engine.token_holdings)
            return {}
        wallet = self.config.proxy_wallet
        if not wallet:
            if self.engine:
                return dict(self.engine.token_holdings)
            return {}
        wallet_addr = Web3.to_checksum_address(wallet)
        live = {}
        token_ids = set()
        # Also check expired_windows_pending_claim and window_metadata
        combined_cache = dict(market_cache)
        if self.engine:
            for wid, info in self.engine.expired_windows_pending_claim.items():
                if wid not in combined_cache:
                    combined_cache[wid] = info
            for wid, meta in self.engine.window_metadata.items():
                if wid not in combined_cache:
                    combined_cache[wid] = meta
        for wid, market in combined_cache.items():
            for key in ("token_up", "token_down"):
                tid = market.get(key, "")
                if tid:
                    token_ids.add(tid)
        queried = 0
        errors = 0
        for tid in token_ids:
            try:
                self.rpc_limiter.wait()
                raw = self.ctf_contract.functions.balanceOf(
                    wallet_addr, int(tid, 16) if tid.startswith("0x") else int(tid)
                ).call()
                shares = float(raw) / 1e6
                queried += 1
                if shares >= 1.0:  # Ignore dust
                    live[tid] = {"size": shares, "cost": 0}
            except Exception as e:
                errors += 1
                err = str(e).lower()
                if "rate limit" in err or "-32090" in err:
                    self.rpc_limiter.report_rate_limit(10)
                    self.logger.info("  LIVE POS | RPC rate limited after {} queries".format(queried))
                    break
        if queried > 0:
            self.logger.info("  LIVE POS | Queried {} tokens | {} with balance | {} errors".format(
                queried, len(live), errors))
        # Sync back to engine.token_holdings so PnL calc uses live data
        if self.engine and live:
            for tid, pos in live.items():
                if tid in self.engine.token_holdings:
                    self.engine.token_holdings[tid]["size"] = pos["size"]
                else:
                    self.engine.token_holdings[tid] = {"size": pos["size"], "cost": 0}
            # Also clean up stale holdings that are no longer on-chain
            for tid in list(self.engine.token_holdings.keys()):
                if tid in token_ids and tid not in live:
                    self.engine.token_holdings[tid]["size"] = 0
        if errors > 0 and queried == 0:
            # Total RPC failure — fall back to engine data
            if self.engine:
                return dict(self.engine.token_holdings)
        return live

    def check_and_merge_all(self, market_cache, token_holdings):
        if not self.config.auto_merge_enabled or not self.w3:
            return 0
        if self.config.dry_run:
            return self._simulate_merges(market_cache, token_holdings)
        # Use live on-chain positions instead of fill-recorded token_holdings
        live_holdings = self.query_live_positions(market_cache)
        merged_count = 0
        window_holdings = self._find_mergeable(market_cache, live_holdings)
        for wid, info in window_holdings.items():
            mergeable = min(info["up_size"], info["down_size"])
            if mergeable < self.config.merge_min_shares:
                continue
            merge_key = "{}:{:.1f}".format(wid, mergeable)
            if merge_key in self._merged_windows:
                continue
            market = info["market"]
            condition_id = market.get("condition_id", "")
            if not condition_id:
                continue
            self.logger.info(
                "  MERGE ATTEMPT | {} | {:.1f} shares (UP:{:.1f} DN:{:.1f}) | cond: {}...".format(
                    wid, mergeable, info["up_size"], info["down_size"], condition_id[:16]))
            success = self._execute_merge(condition_id, mergeable, wid)
            if success:
                self._merged_windows.add(merge_key)
                merged_count += 1
                self.merges_completed += 1
                self.total_merged_usd += mergeable
                token_up = market.get("token_up", "")
                token_down = market.get("token_down", "")
                # Update both the live_holdings and the passed-in token_holdings
                for holdings in (live_holdings, token_holdings):
                    if token_up in holdings:
                        holdings[token_up]["size"] = max(
                            0, holdings[token_up]["size"] - mergeable)
                    if token_down in holdings:
                        holdings[token_down]["size"] = max(
                            0, holdings[token_down]["size"] - mergeable)
                if self.engine:
                    self.engine.capital_in_positions = max(
                        0, self.engine.capital_in_positions - mergeable)
                    self.engine.session_total_spent = max(
                        0, self.engine.session_total_spent - mergeable)
                    self.engine._update_total_capital()
                    # V15.1-17b: After merge returns USDC, clean up accounting.
                    # window_fill_cost must be reduced so reconcile doesn't
                    # inflate capital_in_positions back up. filled_windows
                    # guard remains until cleanup_expired_windows releases it
                    # (the window is still blocked from re-entry, which is correct
                    # since the position is gone and we don't want to re-enter).
                    old_fill = self.engine.window_fill_cost.get(wid, 0)
                    if old_fill > 0:
                        new_fill = max(0, old_fill - mergeable)
                        if new_fill > 0:
                            self.engine.window_fill_cost[wid] = new_fill
                        else:
                            self.engine.window_fill_cost.pop(wid, None)
                # V15.1-P5: Track realized PnL from merge.
                # Merge returns $1 per pair of shares. The cost was what we paid
                # for those shares (up_price + down_price per pair).
                if self.engine:
                    # Estimate cost: use window_fill_cost if available, else use
                    # the merged amount as a rough cost estimate (conservative)
                    merge_cost = min(old_fill, mergeable) if old_fill > 0 else mergeable
                    self.engine.session_realized_returns += mergeable
                    self.engine.session_realized_cost += merge_cost
                    # V15.1-20: Release held_windows when capital is recovered.
                    # If window_fill_cost is now 0, all capital from this window
                    # has been returned via merge — safe to release the slot.
                    remaining_fill = self.engine.window_fill_cost.get(wid, 0)
                    if remaining_fill <= 0:
                        self.engine.held_windows.discard(wid)
                self.logger.info(
                    "  MERGED OK | {} | {:.1f} shares -> ~${:.2f} USDC returned".format(
                        wid, mergeable, mergeable))
            else:
                self.merges_failed += 1
                self.logger.warning("  MERGE FAILED | {} | Will retry".format(wid))
        return merged_count

    def _simulate_merges(self, market_cache, token_holdings):
        merged = 0
        window_holdings = self._find_mergeable(market_cache, token_holdings)
        for wid, info in window_holdings.items():
            mergeable = min(info["up_size"], info["down_size"])
            if mergeable < self.config.merge_min_shares:
                continue
            merge_key = "{}:{:.1f}".format(wid, mergeable)
            if merge_key in self._merged_windows:
                continue
            self._merged_windows.add(merge_key)
            self.merges_completed += 1
            self.total_merged_usd += mergeable
            market = info["market"]
            token_up = market.get("token_up", "")
            token_down = market.get("token_down", "")
            if token_up in token_holdings:
                token_holdings[token_up]["size"] = max(
                    0, token_holdings[token_up]["size"] - mergeable)
            if token_down in token_holdings:
                token_holdings[token_down]["size"] = max(
                    0, token_holdings[token_down]["size"] - mergeable)
            if self.engine:
                self.engine.capital_in_positions = max(
                    0, self.engine.capital_in_positions - mergeable)
                self.engine.session_total_spent = max(
                    0, self.engine.session_total_spent - mergeable)
                self.engine._update_total_capital()
                # V15.1-17b: Clean up window_fill_cost after simulated merge
                old_fill = self.engine.window_fill_cost.get(wid, 0)
                if old_fill > 0:
                    new_fill = max(0, old_fill - mergeable)
                    if new_fill > 0:
                        self.engine.window_fill_cost[wid] = new_fill
                    else:
                        self.engine.window_fill_cost.pop(wid, None)
            self.logger.info(
                "  MERGED (sim) | {} | {:.1f} shares -> ~${:.2f} returned".format(
                    wid, mergeable, mergeable))
            merged += 1
        return merged

    def _find_mergeable(self, market_cache, token_holdings):
        window_holdings = {}
        # V15.1-17: Build combined lookup from market_cache + expired_windows_pending_claim
        # so we can still match tokens after windows expire from market_cache.
        combined_cache = dict(market_cache)  # start with active windows
        if self.engine:
            for wid, info in self.engine.expired_windows_pending_claim.items():
                if wid not in combined_cache:
                    combined_cache[wid] = {
                        "token_up": info.get("token_up", ""),
                        "token_down": info.get("token_down", ""),
                        "condition_id": info.get("condition_id", ""),
                        "slug": info.get("slug", ""),
                    }
            # Also check window_metadata for windows that have been registered
            for wid, meta in self.engine.window_metadata.items():
                if wid not in combined_cache:
                    combined_cache[wid] = {
                        "token_up": meta.get("token_up", ""),
                        "token_down": meta.get("token_down", ""),
                        "condition_id": meta.get("condition_id", ""),
                        "slug": meta.get("slug", ""),
                    }
            # Also check window_fill_tokens for token mapping
            for wid, tokens in self.engine.window_fill_tokens.items():
                if wid not in combined_cache:
                    # Reconstruct from fill data + _is_up_token_cache
                    token_up = token_down = ""
                    for t in tokens:
                        tid = t.get("token_id", "")
                        if t.get("is_up"):
                            token_up = tid
                        elif t.get("is_up") is False:
                            token_down = tid
                    if token_up or token_down:
                        combined_cache[wid] = {
                            "token_up": token_up, "token_down": token_down,
                            "condition_id": "", "slug": "",
                        }
        for token_id, holding in token_holdings.items():
            size = holding.get("size", 0)
            if size < self.config.merge_min_shares:
                continue
            for wid, market in combined_cache.items():
                if token_id == market.get("token_up"):
                    if wid not in window_holdings:
                        window_holdings[wid] = {"up_size": 0, "down_size": 0, "market": market}
                    window_holdings[wid]["up_size"] = size
                elif token_id == market.get("token_down"):
                    if wid not in window_holdings:
                        window_holdings[wid] = {"up_size": 0, "down_size": 0, "market": market}
                    window_holdings[wid]["down_size"] = size
        # V15.1-17: Log merge scan results for debugging
        if window_holdings:
            for wid, info in window_holdings.items():
                self.logger.info(
                    "  MERGE SCAN | {} | UP:{:.1f} DN:{:.1f} | mergeable:{:.1f}".format(
                        wid, info["up_size"], info["down_size"],
                        min(info["up_size"], info["down_size"])))
        return window_holdings

    def _execute_merge(self, condition_id, shares, window_id):
        if self.rpc_limiter.is_backed_off:
            return False
        try:
            usdc_addr = Web3.to_checksum_address(USDC_E_ADDRESS)
            parent = bytes(32)
            cid_bytes = self._to_bytes32(condition_id)
            decimals = self.config.merge_position_decimals
            amount_raw = int(shares * (10 ** decimals))
            merge_data = _encode_abi(
                self.ctf_contract, "mergePositions",
                [usdc_addr, parent, cid_bytes, [1, 2], amount_raw])
            ctf_addr = Web3.to_checksum_address(CTF_ADDRESS)
            if self.proxy_contract:
                return self._merge_via_proxy(ctf_addr, merge_data, window_id)
            else:
                return self._merge_direct(ctf_addr, merge_data, window_id)
        except Exception as e:
            err = str(e).lower()
            if "rate limit" in err or "-32090" in err:
                self.rpc_limiter.report_rate_limit(15)
            self.logger.warning("  MERGE ERROR | {} | {}".format(window_id, e))
            return False

    def _merge_via_proxy(self, ctf_addr, merge_data, window_id):
        if not self.proxy_contract or not self.account:
            self.logger.warning("  MERGE SKIP | {} | No proxy_contract or account".format(window_id))
            return False
        # V15.1-21: Check CTF approval status for proxy wallet before merge.
        # mergePositions requires the caller (proxy) to have approval to move
        # the CTF tokens. If proxy isn't approved, the tx will revert.
        proxy_addr = Web3.to_checksum_address(self.config.proxy_wallet)
        try:
            self.rpc_limiter.wait()
            is_approved = self.ctf_contract.functions.isApprovedForAll(
                proxy_addr, ctf_addr).call()
            self.logger.info("  MERGE APPROVAL CHECK | {} | proxy={} approved_for_ctf={}".format(
                window_id, proxy_addr[:10] + "...", is_approved))
            if not is_approved:
                # Try to set approval: proxy must approve CTF to manage its tokens
                # This is done via the proxy's execute() function
                self.logger.info("  MERGE | Setting CTF approval for proxy...")
                approval_data = _encode_abi(
                    self.ctf_contract, "setApprovalForAll",
                    [ctf_addr, True])
                success = self._proxy_exec_tx(
                    ctf_addr, approval_data, window_id, "setApprovalForAll")
                if not success:
                    self.logger.warning("  MERGE | Failed to set CTF approval — merge will likely fail")
                else:
                    self.logger.info("  MERGE | CTF approval set successfully")
        except Exception as e_check:
            self.logger.info("  MERGE APPROVAL CHECK FAILED | {} | {}".format(
                window_id, str(e_check)[:100]))
        self.rpc_limiter.wait()
        nonce = self.w3.eth.get_transaction_count(self.account.address)
        gas_price = self.rpc_limiter.get_gas_price(self.w3)
        for fn_name, fn_args_builder in [
            ("execute", lambda: (ctf_addr, 0, bytes.fromhex(merge_data[2:]))),
            ("exec", lambda: (ctf_addr, bytes.fromhex(merge_data[2:]))),
        ]:
            try:
                fn = getattr(self.proxy_contract.functions, fn_name)
                txn = fn(*fn_args_builder()).build_transaction({
                    "from": self.account.address, "nonce": nonce,
                    "gas": 350000, "gasPrice": int(gas_price * 1.3),
                    "chainId": self.config.chain_id,
                })
                signed = self.w3.eth.account.sign_transaction(txn, self.config.private_key)
                self.rpc_limiter.wait()
                tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
                receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=90)
                if receipt.status == 1:
                    self.logger.info("  MERGE TX OK | {} | {} | Tx: {}...".format(
                        window_id, fn_name, self.w3.to_hex(tx_hash)[:20]))
                    return True
                else:
                    # V15.1-21: Verbose revert logging — capture tx hash and gas used
                    self.logger.warning(
                        "  MERGE TX REVERTED | {} | {} | Tx: {} | Gas used: {} | "
                        "Signer: {} | Proxy: {}".format(
                            window_id, fn_name,
                            self.w3.to_hex(tx_hash),
                            receipt.gasUsed,
                            self.account.address[:10] + "...",
                            proxy_addr[:10] + "..."))
            except Exception as e1:
                err = str(e1)
                err_lower = err.lower()
                # V15.1-21: Verbose error logging for merge failures
                self.logger.warning(
                    "  MERGE TX ERROR | {} | {} | Signer: {} | Proxy: {} | Error: {}".format(
                        window_id, fn_name,
                        self.account.address[:10] + "...",
                        proxy_addr[:10] + "...",
                        err[:200]))
                if "rate limit" in err_lower or "-32090" in err_lower:
                    self.rpc_limiter.report_rate_limit(15)
                    return False
                self.rpc_limiter.wait()
                try:
                    nonce = self.w3.eth.get_transaction_count(self.account.address)
                except Exception:
                    return False
        return False

    def _proxy_exec_tx(self, target_addr, call_data, window_id, label):
        """Execute a transaction through the proxy wallet.
        Used for approval and other setup transactions."""
        if not self.proxy_contract or not self.account:
            return False
        try:
            self.rpc_limiter.wait()
            nonce = self.w3.eth.get_transaction_count(self.account.address)
            gas_price = self.rpc_limiter.get_gas_price(self.w3)
            for fn_name, fn_args_builder in [
                ("execute", lambda: (target_addr, 0, bytes.fromhex(call_data[2:]))),
                ("exec", lambda: (target_addr, bytes.fromhex(call_data[2:]))),
            ]:
                try:
                    fn = getattr(self.proxy_contract.functions, fn_name)
                    txn = fn(*fn_args_builder()).build_transaction({
                        "from": self.account.address, "nonce": nonce,
                        "gas": 200000, "gasPrice": int(gas_price * 1.3),
                        "chainId": self.config.chain_id,
                    })
                    signed = self.w3.eth.account.sign_transaction(txn, self.config.private_key)
                    self.rpc_limiter.wait()
                    tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
                    receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
                    if receipt.status == 1:
                        self.logger.info("  PROXY TX OK | {} | {} | {}".format(
                            label, fn_name, self.w3.to_hex(tx_hash)[:20]))
                        return True
                    else:
                        self.logger.warning("  PROXY TX REVERTED | {} | {} | {}".format(
                            label, fn_name, self.w3.to_hex(tx_hash)))
                except Exception as e1:
                    err = str(e1).lower()
                    if "rate limit" in err or "-32090" in err:
                        self.rpc_limiter.report_rate_limit(15)
                        return False
                    self.rpc_limiter.wait()
                    try:
                        nonce = self.w3.eth.get_transaction_count(self.account.address)
                    except Exception:
                        return False
        except Exception as e:
            self.logger.warning("  PROXY TX ERROR | {} | {}".format(label, str(e)[:100]))
        return False

    def _merge_direct(self, ctf_addr, merge_data, window_id):
        if not self.account:
            self.logger.warning("  MERGE SKIP (direct) | {} | No account".format(window_id))
            return False
        try:
            self.rpc_limiter.wait()
            nonce = self.w3.eth.get_transaction_count(self.account.address)
            gas_price = self.rpc_limiter.get_gas_price(self.w3)
            txn = {
                "to": ctf_addr, "data": merge_data,
                "from": self.account.address, "nonce": nonce,
                "gas": 300000, "gasPrice": int(gas_price * 1.3),
                "chainId": self.config.chain_id, "value": 0,
            }
            signed = self.w3.eth.account.sign_transaction(txn, self.config.private_key)
            self.rpc_limiter.wait()
            tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=90)
            if receipt.status == 1:
                self.logger.info("  MERGE TX OK (direct) | {} | Tx: {}...".format(
                    window_id, self.w3.to_hex(tx_hash)[:20]))
                return True
            else:
                # V15.1-21: Verbose revert logging for direct merge
                self.logger.warning(
                    "  MERGE TX REVERTED (direct) | {} | Tx: {} | Gas used: {} | "
                    "Signer: {}".format(
                        window_id, self.w3.to_hex(tx_hash),
                        receipt.gasUsed, self.account.address[:10] + "..."))
        except Exception as e:
            err = str(e)
            err_lower = err.lower()
            # V15.1-21: Verbose error logging
            self.logger.warning(
                "  MERGE TX ERROR (direct) | {} | Signer: {} | Error: {}".format(
                    window_id, self.account.address[:10] + "...", err[:200]))
            if "rate limit" in err_lower or "-32090" in err_lower:
                self.rpc_limiter.report_rate_limit(15)
        return False

    def get_stats(self):
        return {
            "merges_completed": self.merges_completed,
            "merges_failed": self.merges_failed,
            "total_merged_usd": self.total_merged_usd,
        }


# -----------------------------------------------------------------
# Auto-Claim Manager
# -----------------------------------------------------------------

class AutoClaimManager:
    GAMMA_BASE = "https://gamma-api.polymarket.com"

    def __init__(self, config, logger, engine=None):
        self.config = config
        self.logger = logger
        self.engine = engine
        self.w3 = None
        self.ctf_contract = None
        self.account = None
        self.proxy_contract = None
        self.claimed_conditions = set()
        self._pending_claims = {}
        self._claim_attempts = {}
        self._claim_results = []
        self.total_claimed_usd = 0.0
        self.blind_redeem_attempts = 0
        self.blind_redeem_successes = 0
        self.rpc_limiter = RPCRateLimiter(min_interval=config.rpc_min_call_interval)
        self._init_web3()

    def set_engine(self, engine):
        self.engine = engine

    def _init_web3(self):
        if not HAS_WEB3 or not self.config.private_key:
            self.logger.info("  Auto-claim: web3 not available or no private key")
            return
        try:
            self.w3 = Web3(Web3.HTTPProvider(
                self.config.polygon_rpc, request_kwargs={"timeout": 15}))
            self.ctf_contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(CTF_ADDRESS), abi=CTF_FULL_ABI)
            self.account = self.w3.eth.account.from_key(self.config.private_key)
            if self.config.proxy_wallet:
                try:
                    self.proxy_contract = self.w3.eth.contract(
                        address=Web3.to_checksum_address(self.config.proxy_wallet),
                        abi=PROXY_EXEC_ABI)
                except Exception:
                    self.proxy_contract = None
            self.logger.info("  Auto-claim initialized | Signer: {}...{} | Proxy: {}".format(
                self.account.address[:8], self.account.address[-4:],
                "{}...{}".format(self.config.proxy_wallet[:8], self.config.proxy_wallet[-4:])
                if self.config.proxy_wallet else "NONE"))
        except Exception as e:
            self.logger.warning(f"  Auto-claim init failed: {e}")
            self.w3 = None

    def _to_bytes32(self, hex_str):
        clean = hex_str.replace("0x", "")
        raw = bytes.fromhex(clean)
        if len(raw) < 32:
            raw = raw.rjust(32, b'\x00')
        return raw[:32]

    def _parse_rate_limit_delay(self, error_msg):
        msg = str(error_msg).lower()
        if "retry in" in msg:
            try:
                parts = msg.split("retry in")[1].strip().split("s")[0].strip()
                return int(parts) + 2
            except (ValueError, IndexError):
                pass
        return 15

    def check_resolution_gamma(self, slug):
        try:
            resp = api_retry(lambda: requests.get(
                f"{self.GAMMA_BASE}/events", params={"slug": slug}, timeout=10,
            ), logger=self.logger)
            if resp and resp.status_code == 200:
                events = resp.json()
                if events:
                    event = events[0]
                    event_markets = event.get("markets", [])
                    if event_markets:
                        m = event_markets[0]
                        resolved = m.get("resolved", False)
                        if resolved:
                            outcomes_raw = m.get("outcomes", "[]")
                            if isinstance(outcomes_raw, str):
                                outcomes = json.loads(outcomes_raw)
                            else:
                                outcomes = outcomes_raw
                            outcome = m.get("outcome", "")
                            clob_ids_raw = m.get("clobTokenIds", "[]")
                            if isinstance(clob_ids_raw, str):
                                clob_ids = json.loads(clob_ids_raw)
                            else:
                                clob_ids = clob_ids_raw
                            winning_token = ""
                            if outcome and clob_ids and outcomes:
                                try:
                                    idx = outcomes.index(outcome)
                                    winning_token = clob_ids[idx] if idx < len(clob_ids) else ""
                                except (ValueError, IndexError):
                                    pass
                            return {
                                "resolved": True, "outcome": outcome,
                                "winning_token": winning_token,
                                "condition_id": m.get("conditionId", ""),
                            }
            return {"resolved": False}
        except Exception as e:
            self.logger.debug(f"  Gamma resolution check failed: {e}")
            return {"resolved": False}

    def check_resolution_onchain(self, condition_id):
        if not self.w3 or not self.ctf_contract or not condition_id:
            return False
        if self.rpc_limiter.is_backed_off:
            return False
        try:
            self.rpc_limiter.wait()
            cid_bytes = self._to_bytes32(condition_id)
            denominator = self.ctf_contract.functions.payoutDenominator(cid_bytes).call()
            return denominator > 0
        except Exception as e:
            err_msg = str(e)
            if "rate limit" in err_msg.lower() or "-32090" in err_msg:
                self.rpc_limiter.report_rate_limit(self._parse_rate_limit_delay(err_msg))
            return False

    def schedule_claim(self, condition_id, window_id, end_time, slug="",
                       tokens=None, token_up="", token_down=""):
        if not condition_id or condition_id in self.claimed_conditions:
            return
        if window_id in self._pending_claims:
            return
        self._pending_claims[window_id] = {
            "condition_id": condition_id, "end_time": end_time,
            "scheduled": time.time(), "slug": slug,
            "tokens": tokens or [], "token_up": token_up,
            "token_down": token_down, "last_check": 0,
            "resolved": False, "winning_token": "", "outcome": "",
        }
        self._claim_attempts[window_id] = 0
        self.logger.info("  CLAIM SCHEDULED | {} | cond: {}...".format(
            window_id, condition_id[:16]))

    def process_claims(self):
        if not self.config.auto_claim_enabled:
            return 0
        now = time.time()
        claimed = 0
        for wid in list(self._pending_claims.keys()):
            info = self._pending_claims[wid]
            condition_id = info.get("condition_id", "")
            if condition_id in self.claimed_conditions:
                del self._pending_claims[wid]
                continue
            if now < info["end_time"] + self.config.claim_delay_seconds:
                continue
            attempts = self._claim_attempts.get(wid, 0)
            backoff_interval = self.config.claim_check_interval * (1.2 ** min(attempts, 10))
            if now - info.get("last_check", 0) < backoff_interval:
                continue
            info["last_check"] = now
            if attempts >= self.config.claim_max_attempts:
                self.logger.warning("  CLAIM MAX ATTEMPTS | {} | {} tries".format(wid, attempts))
                self._log_manual_claim_instructions(wid, info)
                del self._pending_claims[wid]
                continue
            if now - info["end_time"] > self.config.claim_timeout_seconds:
                self.logger.warning("  CLAIM TIMEOUT | {}".format(wid))
                self._log_manual_claim_instructions(wid, info)
                del self._pending_claims[wid]
                continue
            self._claim_attempts[wid] = attempts + 1
            # V15.1-17: Check resolution FIRST before any redeem attempt.
            # Previous logic tried blind redeem before checking resolution,
            # wasting RPC calls on unresolved markets.
            if not info.get("resolved", False):
                resolved_info = self._check_if_resolved(wid, info)
                if not resolved_info:
                    # Not resolved yet — skip all redeem attempts
                    if attempts % 10 == 0:
                        self.logger.info("  CLAIM WAIT | {} | Not resolved yet ({} checks)".format(
                            wid, attempts + 1))
                    continue
                info["resolved"] = True
                info["winning_token"] = resolved_info.get("winning_token", "")
                info["outcome"] = resolved_info.get("outcome", "")
                self.logger.info("  RESOLVED | {} | Winner: {}".format(wid, info["outcome"]))
            # Market is resolved — now try to claim/redeem
            # Priority: 1) CLOB-SELL (fastest, no gas) 2) CTF-DIRECT 3) CTF-PROXY 4) BLIND-REDEEM
            if self.config.claim_fallback_sell and info.get("winning_token"):
                success = self._fallback_sell(wid, info)
                if success:
                    self._mark_claimed(wid, condition_id, "CLOB-SELL")
                    claimed += 1
                    continue
            if not self.rpc_limiter.is_backed_off:
                success = self._redeem_direct(condition_id, wid)
                if success:
                    self._mark_claimed(wid, condition_id, "CTF-DIRECT")
                    claimed += 1
                    continue
            if self.proxy_contract and self.config.proxy_wallet:
                if not self.rpc_limiter.is_backed_off:
                    success = self._redeem_via_proxy(condition_id, wid)
                    if success:
                        self._mark_claimed(wid, condition_id, "CTF-PROXY")
                        claimed += 1
                        continue
            # V15.1-17: Blind redeem as last resort (market already confirmed resolved)
            if self.config.blind_redeem_enabled and not self.rpc_limiter.is_backed_off:
                success = self._try_blind_redeem(condition_id, wid)
                if success:
                    self._mark_claimed(wid, condition_id, "BLIND-REDEEM")
                    claimed += 1
                    continue
            next_interval = self.config.claim_check_interval * (1.2 ** min(attempts + 1, 10))
            if attempts % 10 == 0:
                self.logger.info("  CLAIM RETRY | {} | {}/{} | Next {:.0f}s".format(
                    wid, attempts + 1, self.config.claim_max_attempts, next_interval))
        return claimed

    def _try_blind_redeem(self, condition_id, window_id):
        if not self.w3 or not self.ctf_contract or not self.account:
            self.logger.debug("  BLIND REDEEM SKIP | {} | No web3/contract/account".format(window_id))
            return False
        self.blind_redeem_attempts += 1
        try:
            usdc_addr = Web3.to_checksum_address(USDC_E_ADDRESS)
            parent = bytes(32)
            cid_bytes = self._to_bytes32(condition_id)
            # V15.1-16: Check payoutDenominator first to avoid wasting gas on
            # unresolved markets. If denom==0, market hasn't resolved yet.
            try:
                self.rpc_limiter.wait()
                denom = self.ctf_contract.functions.payoutDenominator(cid_bytes).call()
                if denom == 0:
                    self.logger.info("  BLIND REDEEM SKIP | {} | Not resolved (denom=0)".format(window_id))
                    return False
            except Exception as e_denom:
                err_d = str(e_denom).lower()
                if "rate limit" in err_d or "-32090" in err_d:
                    self.rpc_limiter.report_rate_limit(self._parse_rate_limit_delay(e_denom))
                    return False
                self.logger.info("  BLIND REDEEM | {} | payoutDenominator check failed: {}".format(
                    window_id, str(e_denom)[:80]))
                # Continue anyway — blind redeem is meant to try even without confirmation
            # Try proxy wallet first, then direct
            if self.proxy_contract and self.config.proxy_wallet:
                redeem_data = _encode_abi(
                    self.ctf_contract, "redeemPositions",
                    [usdc_addr, parent, cid_bytes, [1, 2]])
                ctf_addr = Web3.to_checksum_address(CTF_ADDRESS)
                self.rpc_limiter.wait()
                nonce = self.w3.eth.get_transaction_count(self.account.address)
                gas_price = self.rpc_limiter.get_gas_price(self.w3)
                for fn_name, fn_args_builder in [
                    ("execute", lambda: (ctf_addr, 0, bytes.fromhex(redeem_data[2:]))),
                    ("exec", lambda: (ctf_addr, bytes.fromhex(redeem_data[2:]))),
                ]:
                    try:
                        fn = getattr(self.proxy_contract.functions, fn_name)
                        txn = fn(*fn_args_builder()).build_transaction({
                            "from": self.account.address, "nonce": nonce,
                            "gas": 300000, "gasPrice": int(gas_price * 1.3),
                            "chainId": self.config.chain_id,
                        })
                        signed = self.w3.eth.account.sign_transaction(txn, self.config.private_key)
                        self.rpc_limiter.wait()
                        tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
                        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
                        if receipt.status == 1:
                            self.blind_redeem_successes += 1
                            self.logger.info("  BLIND REDEEM OK | {} | {} | Tx: {}...".format(
                                window_id, fn_name, self.w3.to_hex(tx_hash)[:20]))
                            return True
                        else:
                            self.logger.info("  BLIND REDEEM REVERTED | {} | {} | Tx: {}...".format(
                                window_id, fn_name, self.w3.to_hex(tx_hash)[:20]))
                            return False
                    except Exception as e1:
                        err = str(e1).lower()
                        self.logger.info("  BLIND REDEEM FAIL | {} | {} | {}".format(
                            window_id, fn_name, str(e1)[:100]))
                        if "rate limit" in err or "-32090" in err:
                            self.rpc_limiter.report_rate_limit(self._parse_rate_limit_delay(e1))
                            return False
                        if "revert" in err or "execution reverted" in err:
                            # Try next fn_name variant
                            try:
                                self.rpc_limiter.wait()
                                nonce = self.w3.eth.get_transaction_count(self.account.address)
                            except Exception:
                                return False
                            continue
                        try:
                            self.rpc_limiter.wait()
                            nonce = self.w3.eth.get_transaction_count(self.account.address)
                        except Exception:
                            return False
            # V15.1-16: Fall back to direct redeem if no proxy or proxy failed
            return self._redeem_direct(condition_id, window_id)
        except Exception as e:
            err = str(e).lower()
            self.logger.info("  BLIND REDEEM ERROR | {} | {}".format(window_id, str(e)[:100]))
            if "rate limit" in err or "-32090" in err:
                self.rpc_limiter.report_rate_limit(self._parse_rate_limit_delay(e))
            return False

    def _check_if_resolved(self, wid, info):
        slug = info.get("slug", "")
        if slug:
            res = self.check_resolution_gamma(slug)
            if res.get("resolved"):
                return res
        condition_id = info.get("condition_id", "")
        if condition_id and self.check_resolution_onchain(condition_id):
            token_up = info.get("token_up", "")
            token_down = info.get("token_down", "")
            winning_token = ""
            outcome = "UNKNOWN"
            if self.engine:
                held_up = self.engine.token_holdings.get(token_up, {}).get("size", 0)
                held_down = self.engine.token_holdings.get(token_down, {}).get("size", 0)
                if held_up > held_down:
                    winning_token = token_up
                    outcome = "Up (inferred)"
                elif held_down > 0:
                    winning_token = token_down
                    outcome = "Down (inferred)"
            return {"resolved": True, "outcome": outcome, "winning_token": winning_token}
        return None

    def _redeem_direct(self, condition_id, window_id):
        if not self.w3 or not self.ctf_contract or not self.account:
            return False
        if self.rpc_limiter.is_backed_off:
            return False
        try:
            usdc_addr = Web3.to_checksum_address(USDC_E_ADDRESS)
            parent = bytes(32)
            cid_bytes = self._to_bytes32(condition_id)
            self.rpc_limiter.wait()
            try:
                denom = self.ctf_contract.functions.payoutDenominator(cid_bytes).call()
                if denom == 0:
                    return False
            except Exception as e:
                if "rate limit" in str(e).lower() or "-32090" in str(e):
                    self.rpc_limiter.report_rate_limit(self._parse_rate_limit_delay(e))
                    return False
            self.rpc_limiter.wait()
            nonce = self.w3.eth.get_transaction_count(self.account.address)
            gas_price = self.rpc_limiter.get_gas_price(self.w3)
            txn = self.ctf_contract.functions.redeemPositions(
                usdc_addr, parent, cid_bytes, [1, 2],
            ).build_transaction({
                "from": self.account.address, "nonce": nonce,
                "gas": 250000, "gasPrice": int(gas_price * 1.3),
                "chainId": self.config.chain_id,
            })
            signed = self.w3.eth.account.sign_transaction(txn, self.config.private_key)
            self.rpc_limiter.wait()
            tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=90)
            if receipt.status == 1:
                self.logger.info("  CLAIMED (direct) | {} | Tx: {}".format(
                    window_id, self.w3.to_hex(tx_hash)[:20]))
                return True
            return False
        except Exception as e:
            err = str(e).lower()
            if "rate limit" in err or "-32090" in err:
                self.rpc_limiter.report_rate_limit(self._parse_rate_limit_delay(e))
            return False

    def _redeem_via_proxy(self, condition_id, window_id):
        if not self.w3 or not self.proxy_contract or not self.account:
            return False
        if self.rpc_limiter.is_backed_off:
            return False
        try:
            usdc_addr = Web3.to_checksum_address(USDC_E_ADDRESS)
            parent = bytes(32)
            cid_bytes = self._to_bytes32(condition_id)
            redeem_data = _encode_abi(
                self.ctf_contract, "redeemPositions",
                [usdc_addr, parent, cid_bytes, [1, 2]])
            ctf_addr = Web3.to_checksum_address(CTF_ADDRESS)
            self.rpc_limiter.wait()
            nonce = self.w3.eth.get_transaction_count(self.account.address)
            gas_price = self.rpc_limiter.get_gas_price(self.w3)
            for fn_name, fn_args_builder in [
                ("execute", lambda: (ctf_addr, 0, bytes.fromhex(redeem_data[2:]))),
                ("exec", lambda: (ctf_addr, bytes.fromhex(redeem_data[2:]))),
            ]:
                try:
                    fn = getattr(self.proxy_contract.functions, fn_name)
                    txn = fn(*fn_args_builder()).build_transaction({
                        "from": self.account.address, "nonce": nonce,
                        "gas": 350000, "gasPrice": int(gas_price * 1.3),
                        "chainId": self.config.chain_id,
                    })
                    signed = self.w3.eth.account.sign_transaction(txn, self.config.private_key)
                    self.rpc_limiter.wait()
                    tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
                    receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=90)
                    if receipt.status == 1:
                        self.logger.info("  CLAIMED (proxy/{}) | {}".format(fn_name, window_id))
                        return True
                except Exception as e1:
                    err = str(e1).lower()
                    if "rate limit" in err or "-32090" in err:
                        self.rpc_limiter.report_rate_limit(self._parse_rate_limit_delay(e1))
                        return False
                    self.rpc_limiter.wait()
                    try:
                        nonce = self.w3.eth.get_transaction_count(self.account.address)
                    except Exception:
                        return False
            return False
        except Exception as e:
            err = str(e).lower()
            if "rate limit" in err or "-32090" in err:
                self.rpc_limiter.report_rate_limit(self._parse_rate_limit_delay(e))
            return False

    def _fallback_sell(self, wid, info):
        if not self.engine or not self.engine.client:
            return False
        winning_token = info.get("winning_token", "")
        if not winning_token:
            token_up = info.get("token_up", "")
            token_down = info.get("token_down", "")
            outcome = info.get("outcome", "").lower()
            if "up" in outcome:
                winning_token = token_up
            elif "down" in outcome:
                winning_token = token_down
            else:
                for tok in [token_up, token_down]:
                    if tok and tok in self.engine.token_holdings:
                        held = self.engine.token_holdings[tok].get("size", 0)
                        if held > 0:
                            winning_token = tok
                            break
        if not winning_token:
            return False
        held = self.engine.token_holdings.get(winning_token, {}).get("size", 0)
        if held < 1.0:
            return False
        try:
            result = self.engine.place_order(
                winning_token, "SELL", self.config.claim_sell_min_price, held,
                wid, "CLAIM-SELL", "mm", is_taker=True)
            if result:
                self.logger.info("  CLAIM-SELL | {} | {:.1f} @ ${:.2f}".format(
                    wid, held, self.config.claim_sell_min_price))
                return True
        except Exception as e:
            self.logger.debug(f"  CLOB sell failed for {wid}: {e}")
        return False

    def _mark_claimed(self, wid, condition_id, method):
        self.claimed_conditions.add(condition_id)
        info = self._pending_claims.pop(wid, {})
        tokens = info.get("tokens", [])
        total_size = sum(t.get("size", 0) for t in tokens)
        self.total_claimed_usd += total_size
        self._claim_results.append({
            "window_id": wid, "method": method, "time": time.time(),
            "condition_id": condition_id, "est_amount": total_size,
        })
        if self.engine:
            self.engine.record_claim(total_size)
            # V15.1-P5: Track realized PnL from claim.
            # Claim returns the winning side's shares as USDC.
            # Cost was what we paid for those shares.
            fill_cost = info.get("fill_cost", 0)
            self.engine.session_realized_returns += total_size
            self.engine.session_realized_cost += fill_cost
            # V15.1-20: Release held_windows — capital recovered via claim
            self.engine.held_windows.discard(wid)
        self.logger.info("  CLAIMED | {} | {} | Est: ${:.2f}".format(wid, method, total_size))

    def _log_manual_claim_instructions(self, wid, info):
        cid = info.get("condition_id", "")
        self.logger.warning("  ---- MANUAL CLAIM REQUIRED ----")
        self.logger.warning("  Window: {} | Condition: {}".format(wid, cid))
        self.logger.warning("  Go to polymarket.com -> Portfolio -> Claim")
        self.logger.warning("  ---------------------------------")

    def execute_pre_exits(self, markets, price_feed, book_reader):
        if not self.config.pre_exit_enabled or not self.engine:
            return 0
        exits = 0
        now = time.time()
        for market in markets:
            wid = market["window_id"]
            time_left = market["end_time"] - now
            if time_left > self.config.pre_exit_time_seconds or time_left < 5:
                continue
            token_up = market["token_up"]
            token_down = market["token_down"]
            held_up = self.engine.token_holdings.get(token_up, {}).get("size", 0)
            held_down = self.engine.token_holdings.get(token_down, {}).get("size", 0)
            if held_up < 1.0 and held_down < 1.0:
                continue
            prediction = price_feed.predict_resolution(
                market["asset"], market["timestamp"], market["end_time"])
            if not prediction or prediction["confidence"] < self.config.pre_exit_min_confidence:
                continue
            if prediction["direction"] == "UP" and held_up >= 1.0:
                spread = book_reader.get_spread(token_up)
                if spread and spread["bid"] >= self.config.pre_exit_min_price:
                    result = self.engine.place_order(
                        token_up, "SELL", spread["bid"], held_up,
                        wid, "PRE-EXIT-UP", "mm", is_taker=True)
                    if result:
                        exits += 1
            elif prediction["direction"] == "DOWN" and held_down >= 1.0:
                spread = book_reader.get_spread(token_down)
                if spread and spread["bid"] >= self.config.pre_exit_min_price:
                    result = self.engine.place_order(
                        token_down, "SELL", spread["bid"], held_down,
                        wid, "PRE-EXIT-DN", "mm", is_taker=True)
                    if result:
                        exits += 1
        return exits

    def get_claim_stats(self):
        return {
            "pending_claims": len(self._pending_claims),
            "claimed_total": len(self.claimed_conditions),
            "total_claimed_usd": self.total_claimed_usd,
            "claim_results": list(self._claim_results[-10:]),
            "rpc_backed_off": self.rpc_limiter.is_backed_off,
            "blind_attempts": self.blind_redeem_attempts,
            "blind_successes": self.blind_redeem_successes,
        }


# -----------------------------------------------------------------
# Trading Engine (V15.1-4: verbose order rejection)
# -----------------------------------------------------------------

class TradingEngine:
    def __init__(self, config, fee_calc, logger, balance_checker=None):
        self.config = config
        self.fee_calc = fee_calc
        self.logger = logger
        self.balance_checker = balance_checker
        self.orders_by_window = {}
        self.active_orders = {}
        self.window_exposure = {}
        self.total_exposure = 0.0
        self.asset_exposure = {}
        self.daily_pnl = 0.0
        self.total_orders_placed = 0
        self.total_orders_cancelled = 0
        self.sniper_trades = 0
        self.arb_trades = 0
        self.contrarian_trades = 0
        self.mm_trades = 0
        self.trend_trades = 0
        self.known_windows = set()
        self.sim_engine = None
        self._is_up_token_cache = {}
        self._last_reset_date = None
        self.capital_deployed = 0.0
        self.capital_in_positions = 0.0
        self.total_capital_used = 0.0
        self.token_holdings = {}
        self.session_total_spent = 0.0
        self.window_direction_lock = {}
        self.orders_this_cycle = {}
        self.cycle_count = 0
        self.strategy_capital_used = {
            "mm": 0.0, "trend": 0.0, "sniper": 0.0, "arb": 0.0, "contrarian": 0.0
        }
        self._cached_exchange_balance = None
        self._balance_cache_time = 0
        self.window_fill_cost = {}
        self.starting_wallet_balance = None
        self.window_fill_tokens = {}
        self.expired_windows_pending_claim = {}
        self.window_metadata = {}
        self.window_fill_sides = {}
        self._pending_hedges = []
        self._market_cache = {}
        self.hedges_completed = 0
        self.hedges_skipped = 0
        self.estimated_rewards_total = 0.0
        self.reward_snapshots = []
        self.window_edge = {}
        self.unredeemed_position_value = 0.0
        self.paired_windows = set()
        # V15.1-P5: Realized PnL tracking.
        # Only counts returns from resolved positions (merges + claims).
        # session_realized_returns = total $ returned from merges/claims
        # session_realized_pnl = returns - cost of those positions
        self.session_realized_returns = 0.0  # $ returned from merges + claims
        self.session_realized_cost = 0.0     # $ cost of positions that were merged/claimed
        # V15.1-15: Persistent filled window tracking.
        # Once a window has ANY fill, it's added here and never re-entered.
        # Only cleared on window expiry/claim or momentum exit.
        # This survives reconcile_capital_from_wallet() which resets window_fill_cost.
        self.filled_windows = set()  # {window_id, ...}
        self.window_entry_count = {}  # {window_id: int} — safety counter
        # V15.1-19: Closed windows — permanently block re-entry after momentum exit
        self.closed_windows = set()  # {window_id, ...}
        # V15.1-20: Held windows — windows with tokens still on-chain (not yet merged/claimed/sold).
        # Unlike filled_windows (cleared on expiry), held_windows persists until capital is recovered.
        # Used in concurrent window count to prevent unlimited window accumulation.
        self.held_windows = set()  # {window_id, ...}
        # Phase 3: Recently-cancelled orders buffer.
        # When cancel_window_orders() removes an order from active_orders,
        # the cancel on Polymarket is async. If the order filled before the
        # cancel took effect, the WS fill event arrives for an order_id no
        # longer in active_orders. This buffer preserves the order metadata
        # so the fill can still be processed correctly.
        self._recently_cancelled = {}  # {order_id: {**order_info, "cancelled_at": float}}
        self._recently_cancelled_ttl = 900  # V15.1-21: 15 min to catch late fills (was 120s)

        if not config.dry_run and HAS_CLOB:
            try:
                temp_client = ClobClient(
                    config.host, key=config.private_key, chain_id=config.chain_id)
                creds = temp_client.create_or_derive_api_creds()
                self.logger.info("  API creds derived (key: {}...{})".format(
                    creds.api_key[:8], creds.api_key[-4:]))
            except Exception as e:
                self.logger.warning(f"  Failed to derive API creds: {e}")
                creds = ApiCreds(
                    api_key=config.api_key, api_secret=config.api_secret,
                    api_passphrase=config.api_passphrase)
            self.client = ClobClient(
                config.host, key=config.private_key, chain_id=config.chain_id,
                creds=creds, signature_type=config.signature_type,
                funder=config.proxy_wallet)
        else:
            self.client = None

    def register_window_metadata(self, market):
        wid = market["window_id"]
        self.window_metadata[wid] = {
            "slug": market.get("slug", ""),
            "condition_id": market.get("condition_id", ""),
            "token_up": market.get("token_up", ""),
            "token_down": market.get("token_down", ""),
            "asset": market.get("asset", ""),
            "end_time": market.get("end_time", 0),
        }
        self._market_cache[wid] = market
        if "edge" in market:
            self.window_edge[wid] = market["edge"]

    def check_daily_reset(self):
        today = datetime.now(timezone.utc).date()
        if self._last_reset_date is None:
            self._last_reset_date = today
        elif today > self._last_reset_date:
            self.logger.info(f"  New day ({today}) -- resetting daily P&L from ${self.daily_pnl:+.2f}")
            self.daily_pnl = 0.0
            self._last_reset_date = today

    def prune_stale_orders(self):
        now = time.time()
        max_age = self.config.stale_order_max_age
        stale = [oid for oid, info in self.active_orders.items()
                 if now - info.get("time", now) > max_age]
        for oid in stale:
            info = self.active_orders.get(oid)
            if info:
                wid = info.get("window_id", "")
                self._recently_cancelled[oid] = {
                    **info, "cancelled_at": time.time()}
                del self.active_orders[oid]
                if wid in self.orders_by_window:
                    self.orders_by_window[wid] = [
                        o for o in self.orders_by_window[wid] if o != oid]
                if self.sim_engine and oid in self.sim_engine.pending_orders:
                    del self.sim_engine.pending_orders[oid]
                self.total_orders_cancelled += 1
        if stale:
            self._recalc_exposure()

    def cleanup_expired_windows(self, active_markets, churn_manager=None):
        active_ids = {m["window_id"] for m in active_markets}
        expired = [w for w in self.known_windows if w not in active_ids]
        for wid in expired:
            oids = self.orders_by_window.pop(wid, [])
            for oid in oids:
                if oid in self.active_orders:
                    self._recently_cancelled[oid] = {
                        **self.active_orders[oid], "cancelled_at": time.time()}
                    del self.active_orders[oid]
                    self.total_orders_cancelled += 1
                if self.sim_engine and oid in self.sim_engine.pending_orders:
                    del self.sim_engine.pending_orders[oid]
            self.window_exposure.pop(wid, None)
            self.known_windows.discard(wid)
            self.window_edge.pop(wid, None)
            self.paired_windows.discard(wid)
            if churn_manager:
                churn_manager.cleanup_window(wid)
            fill_cost = self.window_fill_cost.pop(wid, 0)
            if fill_cost > 0 and not self.config.dry_run:
                self.capital_in_positions = max(0, self.capital_in_positions - fill_cost)
                self.unredeemed_position_value += fill_cost
                self._update_total_capital()
                self.logger.info("  Released ${:.2f} from expired {}".format(fill_cost, wid))
            fill_tokens = self.window_fill_tokens.pop(wid, [])
            meta = self.window_metadata.get(wid, {})
            if fill_tokens and not self.config.dry_run:
                self.expired_windows_pending_claim[wid] = {
                    "tokens": fill_tokens,
                    "condition_id": meta.get("condition_id", ""),
                    "slug": meta.get("slug", ""),
                    "token_up": meta.get("token_up", ""),
                    "token_down": meta.get("token_down", ""),
                    "end_time": meta.get("end_time", time.time()),
                    "expired_at": time.time(), "fill_cost": fill_cost,
                }
                sides = self.window_fill_sides.get(wid, {})
                self.logger.info("  CLAIM QUEUED | {} | sides: {} | cost ${:.2f}".format(
                    wid, "+".join(sorted(sides.keys())) if sides else "?", fill_cost))
            # V15.1-21: Only pop window_fill_sides if the window is paired
            # (both sides filled) or has no fills. One-sided fills must be
            # preserved for process_momentum_exits to act on them.
            sides = self.window_fill_sides.get(wid, {})
            is_one_sided = len(sides) == 1 and wid not in self.paired_windows
            if not is_one_sided:
                self.window_fill_sides.pop(wid, None)
            else:
                self.logger.info("  PRESERVING fill_sides for momentum exit | {} | sides: {}".format(
                    wid, "+".join(sorted(sides.keys()))))
            self._market_cache.pop(wid, None)
            # V15.1-15/21: Only release filled_windows if NOT one-sided
            # (one-sided fills need to stay locked for momentum exit)
            if not is_one_sided:
                self.filled_windows.discard(wid)
                self.window_entry_count.pop(wid, None)
        self.known_windows = active_ids
        self._recalc_exposure()

    def get_available_capital(self):
        """V15.1-18: Bankroll = total capital the bot can invest this session.
        available = bankroll - capital_deployed (open orders) - capital_in_positions (filled, held)
        After merge/claim, capital_in_positions decreases, freeing up available capital.
        Also capped by actual wallet balance if available.
        """
        bankroll = self.config.kelly_bankroll
        available = max(0, bankroll - self.total_capital_used)
        if self.balance_checker and self.config.check_wallet_balance:
            wallet_bal = self.balance_checker.get_balance()
            if wallet_bal is not None:
                return min(wallet_bal, available)
        return available

    def get_strategy_budget(self, strategy):
        if not self.config.strategy_budget_enabled:
            return self.get_available_capital()
        bankroll = self.config.kelly_bankroll
        pct = self.config.strategy_budget_pct.get(strategy, 0.10)
        return bankroll * pct

    def get_strategy_available(self, strategy):
        if not self.config.strategy_budget_enabled:
            return self.get_available_capital()
        strategy_budget = self.get_strategy_budget(strategy)
        strategy_used = self.strategy_capital_used.get(strategy, 0)
        strategy_remaining = max(0, strategy_budget - strategy_used)
        global_remaining = self.get_available_capital()
        if strategy != "sniper" and self.config.sniper_reserved_min > 0:
            sniper_used = self.strategy_capital_used.get("sniper", 0)
            sniper_reserve_remaining = max(0, self.config.sniper_reserved_min - sniper_used)
            global_remaining = max(0, global_remaining - sniper_reserve_remaining)
        return min(strategy_remaining, global_remaining)

    def sync_exchange_balance(self):
        if not self.config.sync_balance_per_cycle:
            return
        now = time.time()
        if now - self._balance_cache_time < self.config.wallet_balance_cache_ttl:
            return
        if self.balance_checker:
            bal = self.balance_checker.get_balance()
            if bal is not None:
                self._cached_exchange_balance = bal
                self._balance_cache_time = now

    def reset_cycle_counters(self):
        self.orders_this_cycle = {}
        self.cycle_count += 1

    def reconcile_capital_from_wallet(self):
        if (len(self.window_exposure) == 0 and len(self.active_orders) == 0
                and self.capital_in_positions > 1.0):
            # V15.1-16: Do NOT zero capital_in_positions if we still hold tokens.
            # The old code zeroed it, which made the P&L calculation think we had
            # $0 in positions, triggering false loss stops (wallet down but tokens held).
            # Only release capital for windows that have been fully claimed/sold.
            filled_capital = sum(self.window_fill_cost.get(wid, 0)
                                 for wid in self.filled_windows)
            unfilled_capital = self.capital_in_positions - filled_capital
            if unfilled_capital > 1.0:
                self.logger.info("  RECONCILE: Releasing ${:.2f} unfilled capital "
                    "(keeping ${:.2f} in {} filled windows)".format(
                        unfilled_capital, filled_capital, len(self.filled_windows)))
                self.capital_in_positions = max(0, filled_capital)
            elif len(self.filled_windows) == 0:
                # No filled windows, safe to release all
                self.logger.info("  RECONCILE: No orders/fills, releasing ${:.2f}".format(
                    self.capital_in_positions))
                self.capital_in_positions = 0
            else:
                self.logger.debug("  RECONCILE: Keeping ${:.2f} in {} filled windows".format(
                    self.capital_in_positions, len(self.filled_windows)))
            # V15.1-15: Only clear window_fill_cost for windows NOT in filled_windows.
            surviving_fill_cost = {}
            for wid in self.filled_windows:
                if wid in self.window_fill_cost:
                    surviving_fill_cost[wid] = self.window_fill_cost[wid]
            self.window_fill_cost = surviving_fill_cost
            self._update_total_capital()
            return
        if self.balance_checker and self.config.check_wallet_balance:
            wallet_bal = self.balance_checker.get_balance()
            if wallet_bal is not None and self.capital_in_positions > 5.0:
                bankroll = self.config.kelly_bankroll
                expected_free = bankroll - self.capital_deployed - self.capital_in_positions
                surplus = wallet_bal - max(expected_free, 0)
                if surplus > 10.0:
                    release = min(surplus * 0.8, self.capital_in_positions)
                    if release > 5.0:
                        self.capital_in_positions = max(0, self.capital_in_positions - release)
                        self._update_total_capital()

    def get_position_value(self):
        """V15.1-16: Compute live position value from token_holdings and market_cache.
        Uses the best available price for each token:
        1. Market's last known price from market_cache
        2. Cost basis per share from token_holdings
        3. Fallback: 0.50 (binary market midpoint)
        """
        total = 0.0
        for token_id, holding in self.token_holdings.items():
            size = holding.get("size", 0)
            if size <= 0:
                continue
            # Try to find the token's current price from market_cache
            price = None
            for wid, market in self._market_cache.items():
                if token_id == market.get("token_up"):
                    # UP token: price is the UP probability
                    price = market.get("up_price", market.get("prob_up"))
                    break
                elif token_id == market.get("token_down"):
                    # DOWN token: price is the DOWN probability
                    price = market.get("down_price", market.get("prob_down"))
                    break
            if price is None or price <= 0:
                # Fallback: use cost basis per share, or 0.50
                cost = holding.get("cost", 0)
                price = (cost / size) if size > 0 and cost > 0 else 0.50
            total += size * price
        return total

    def get_live_pnl(self):
        """V15.1-20: Return dict with wallet_delta (primary) and total_pnl (estimated).
        wallet_delta = current_wallet - starting_wallet (hard fact, no estimation)
        total_pnl = wallet_delta + position_value + capital_deployed (includes estimates)
        """
        if self.starting_wallet_balance is None or not self.balance_checker:
            return None
        current = self.balance_checker.get_balance()
        if current is None:
            return None
        wallet_delta = current - self.starting_wallet_balance
        # V15.1-20: Position value is an estimate — orphan tokens may be worth $0
        # but get_position_value marks them at market price. Use conservatively.
        held_value = self.get_position_value()
        total_pnl = wallet_delta + self.capital_deployed + held_value
        return {
            "wallet_delta": wallet_delta,
            "total_pnl": total_pnl,
            "held_value": held_value,
            "capital_deployed": self.capital_deployed,
            "wallet_now": current,
        }

    # V15.1-4: Verbose order rejection logging
    def place_order(self, token_id, side, price, size, window_id, label="",
                    strategy="mm", is_taker=False):
        if self.daily_pnl < -self.config.max_daily_loss:
            self.logger.info("    REJECT {} | {} | daily loss limit".format(label, window_id))
            return None
        price = max(0.01, min(0.99, round(price, 2)))
        size = max(1, round(size, 1))
        strat_window_key = "{}|{}".format(strategy, window_id)
        cycle_count = self.orders_this_cycle.get(strat_window_key, 0)
        if cycle_count >= self.config.max_orders_per_market:
            return None
        if side == "BUY":
            order_cost = price * size
            if is_taker:
                order_cost += self.fee_calc.fee_amount(price, size)
            if self.config.strategy_budget_enabled:
                strat_avail = self.get_strategy_available(strategy)
                if order_cost > strat_avail:
                    self.logger.info(
                        "    REJECT {} | {} | strategy budget ${:.2f} > avail ${:.2f}".format(
                            label, window_id, order_cost, strat_avail))
                    return None
            available = self.get_available_capital()
            if order_cost > available:
                self.logger.info(
                    "    REJECT {} | {} | cost ${:.2f} > available ${:.2f}".format(
                        label, window_id, order_cost, available))
                return None
            wexp = self.window_exposure.get(window_id, 0)
            # V15.1-16: Include fill costs in per-market budget check.
            # window_exposure only tracks OPEN orders; after a fill the order is
            # removed, so hedge/re-entry buys bypass the cap. Adding
            # window_fill_cost gives the TRUE total spend on this window.
            wfill = self.window_fill_cost.get(window_id, 0)
            total_window_spend = wexp + wfill
            # Allow 2% tolerance on per-market cap to handle rounding in equal-shares sizing
            # This scales automatically when max_position_per_market changes
            market_cap_with_tolerance = self.config.max_position_per_market * 1.02
            if total_window_spend + order_cost > market_cap_with_tolerance:
                self.logger.info(
                    "    REJECT {} | {} | window spend ${:.2f}(open)+${:.2f}(filled)+${:.2f}(new) > max ${:.2f}".format(
                        label, window_id, wexp, wfill, order_cost,
                        market_cap_with_tolerance))
                return None
            if self.total_exposure + order_cost > self.config.max_total_exposure:
                self.logger.info(
                    "    REJECT {} | {} | total exp ${:.2f}+${:.2f} > max ${:.2f}".format(
                        label, window_id, self.total_exposure, order_cost,
                        self.config.max_total_exposure))
                return None
            asset = window_id.split("-")[0] if "-" in window_id else ""
            aexp = self.asset_exposure.get(asset, 0)
            max_asset = self.config.max_total_exposure * self.config.max_asset_exposure_pct
            if aexp + order_cost > max_asset:
                self.logger.info(
                    "    REJECT {} | {} | asset {} exp ${:.2f}+${:.2f} > max ${:.2f}".format(
                        label, window_id, asset, aexp, order_cost, max_asset))
                return None
        elif side == "SELL":
            held = self.token_holdings.get(token_id, {}).get("size", 0)
            if held < size * 0.5 and not self.config.dry_run:
                return None
        if self.config.dry_run:
            order_id = "DRY-{}-{}".format(int(time.time() * 1000), self.total_orders_placed)
            fee_str = ""
            if is_taker:
                fee = self.fee_calc.fee_amount(price, size)
                fee_str = " (fee: ${:.3f})".format(fee)
            self.logger.info(
                "    [{:8s}] {:12s} {:4s} {:6.1f} @ ${:.2f} = ${:6.2f}{}".format(
                    strategy.upper(), label, side, size, price, price * size, fee_str))
            self._track_order(order_id, window_id, side, price, size, token_id, strategy)
            if self.sim_engine:
                self.sim_engine.record_order(order_id, {
                    "window_id": window_id, "side": side, "price": price,
                    "size": size, "token_id": token_id, "strategy": strategy,
                    "is_up_token": self._is_up_token_cache.get(token_id),
                    "is_taker": is_taker,
                })
            return order_id
        try:
            order_side = BUY if side == "BUY" else SELL
            order_args = OrderArgs(price=price, size=size, side=order_side, token_id=token_id)
            signed = self.client.create_order(order_args)
            result = self.client.post_order(signed, OrderType.GTC)
            if isinstance(result, dict) and result.get("success"):
                oid = result["orderID"]
                self.logger.info(
                    "    [{:8s}] {:12s} {:4s} {:6.1f} @ ${:.2f} = ${:6.2f} | {}...".format(
                        strategy.upper(), label, side, size, price, price * size, oid[:16]))
                self._track_order(oid, window_id, side, price, size, token_id, strategy)
                # V15.1-8: Taker orders fill immediately on Polymarket (FOK).
                # Record the fill right away so token_holdings is up-to-date
                # for merge/claim logic within the same cycle.
                if is_taker and side == "BUY":
                    fee = self.fee_calc.fee_amount(price, size)
                    self.record_fill(token_id, side, price, size, fee)
                    # Also update fill tracking for merge/claim pipeline
                    if window_id not in self.window_fill_tokens:
                        self.window_fill_tokens[window_id] = []
                    self.window_fill_tokens[window_id].append({
                        "token_id": token_id, "size": size,
                        "price": price,
                        "is_up": self._is_up_token_cache.get(token_id),
                        "time": time.time(),
                    })
                    self.window_fill_cost[window_id] = (
                        self.window_fill_cost.get(window_id, 0) + price * size)
                    # V15.1-15: Mark window as filled — prevents re-entry
                    self.filled_windows.add(window_id)
                    self.held_windows.add(window_id)  # V15.1-20: track until capital recovered
                    self.window_entry_count[window_id] = self.window_entry_count.get(window_id, 0) + 1
                    is_up = self._is_up_token_cache.get(token_id)
                    side_label = "UP" if is_up else "DOWN"
                    if window_id not in self.window_fill_sides:
                        self.window_fill_sides[window_id] = {}
                    if side_label not in self.window_fill_sides[window_id]:
                        self.window_fill_sides[window_id][side_label] = []
                    self.window_fill_sides[window_id][side_label].append({
                        "token_id": token_id, "price": price,
                        "size": size, "time": time.time(),
                    })
                    sides = self.window_fill_sides.get(window_id, {})
                    if "UP" in sides and "DOWN" in sides:
                        self.paired_windows.add(window_id)
                    # Remove from active_orders so check_fills doesn't double-count
                    if oid in self.active_orders:
                        del self.active_orders[oid]
                    if window_id in self.orders_by_window:
                        self.orders_by_window[window_id] = [
                            o for o in self.orders_by_window[window_id] if o != oid]
                    self._recalc_exposure()
                    self.logger.info("    TAKER FILL (immediate) | {} {} {:.1f} @ ${:.2f} | {}".format(
                        side, token_id[:12] + "...", size, price, window_id))
                return oid
            else:
                err_msg = (result.get("errorMsg", str(result))
                           if isinstance(result, dict) else str(result))
                self.logger.warning(f"    Order rejected: {err_msg}")
        except Exception as e:
            import traceback
            self.logger.error(f"    Order failed: {e}")
            self.logger.error(f"    Traceback: {traceback.format_exc()}")
        return None

    def record_fill(self, token_id, side, price, size, fee=0):
        if side == "BUY":
            if token_id not in self.token_holdings:
                self.token_holdings[token_id] = {"size": 0, "cost": 0}
            self.token_holdings[token_id]["size"] += size
            self.token_holdings[token_id]["cost"] += price * size + fee
            self.capital_in_positions += price * size + fee
            self.session_total_spent += price * size + fee
        elif side == "SELL":
            if token_id in self.token_holdings:
                self.token_holdings[token_id]["size"] = max(
                    0, self.token_holdings[token_id]["size"] - size)
            self.capital_in_positions = max(0, self.capital_in_positions - price * size)
        self._update_total_capital()

    def record_claim(self, amount):
        self.capital_in_positions = max(0, self.capital_in_positions - amount)
        self.session_total_spent = max(0, self.session_total_spent - amount)
        self.unredeemed_position_value = max(0, self.unredeemed_position_value - amount)
        self._update_total_capital()

    def _update_total_capital(self):
        self.total_capital_used = self.capital_deployed + self.capital_in_positions

    def check_fills(self):
        if self.config.dry_run or not self.client:
            return 0
        filled = 0
        try:
            open_orders = self.client.get_orders()
            open_ids = set()
            if isinstance(open_orders, list):
                open_ids = {o.get("id", o.get("orderID", "")) for o in open_orders}
            # Check active orders + recently cancelled orders
            all_tracked = list(self.active_orders.keys()) + list(self._recently_cancelled.keys())
            for oid in all_tracked:
                if oid.startswith("DRY-"):
                    continue
                if oid not in open_ids:
                    info = self.active_orders.pop(oid, None)
                    recovered = False
                    if not info:
                        info = self._recently_cancelled.pop(oid, None)
                        if info:
                            recovered = True
                    if info:
                        wid = info.get("window_id", "")
                        if wid in self.orders_by_window:
                            self.orders_by_window[wid] = [
                                o for o in self.orders_by_window[wid] if o != oid]
                        fill_price = info.get("price", 0)
                        fill_size = info.get("size", 0)
                        fill_side = info.get("side", "BUY")
                        fill_token = info.get("token_id", "")
                        self.record_fill(fill_token, fill_side, fill_price, fill_size)
                        if fill_side == "BUY":
                            cost = fill_price * fill_size
                            self.window_fill_cost[wid] = (
                                self.window_fill_cost.get(wid, 0) + cost)
                            # V15.1-15: Mark window as filled — prevents re-entry
                            self.filled_windows.add(wid)
                            self.held_windows.add(wid)  # V15.1-20: track until capital recovered
                            self.window_entry_count[wid] = self.window_entry_count.get(wid, 0) + 1
                            if wid not in self.window_fill_tokens:
                                self.window_fill_tokens[wid] = []
                            self.window_fill_tokens[wid].append({
                                "token_id": fill_token, "size": fill_size,
                                "price": fill_price,
                                "is_up": self._is_up_token_cache.get(fill_token),
                                "time": time.time(),
                            })
                            is_up = self._is_up_token_cache.get(fill_token)
                            side_label = "UP" if is_up else "DOWN"
                            if wid not in self.window_fill_sides:
                                self.window_fill_sides[wid] = {}
                            if side_label not in self.window_fill_sides[wid]:
                                self.window_fill_sides[wid][side_label] = []
                            self.window_fill_sides[wid][side_label].append({
                                "token_id": fill_token, "price": fill_price,
                                "size": fill_size, "time": time.time(),
                            })
                            sides = self.window_fill_sides.get(wid, {})
                            if "UP" in sides and "DOWN" in sides:
                                self.paired_windows.add(wid)
                            elif self.config.hedge_completion_enabled:
                                self._pending_hedges.append({
                                    "window_id": wid, "filled_side": side_label,
                                    "filled_price": fill_price,
                                    "filled_size": fill_size,
                                    "filled_token": fill_token,
                                    "time": time.time(),
                                })
                        filled += 1
                        tag = "REST-RECOVERED" if recovered else "REST"
                        self.logger.info("  FILL [{}] | {} {} {:.1f} @ ${:.2f} | {}".format(
                            tag, fill_side, fill_token[:12] + "...",
                            fill_size, fill_price, wid))
            if filled:
                self._recalc_exposure()
        except Exception as e:
            self.logger.debug(f"  Fill check error: {e}")
        return filled

    def process_hedge_completions(self, book_reader, vol_tracker=None):
        if not self.config.hedge_completion_enabled:
            return 0
        now = time.time()
        completed = 0
        for hedge in list(self._pending_hedges):
            wid = hedge["window_id"]
            filled_side = hedge["filled_side"]
            filled_price = hedge["filled_price"]
            filled_size = hedge["filled_size"]
            if now - hedge["time"] < self.config.hedge_completion_delay:
                continue
            self._pending_hedges.remove(hedge)
            sides = self.window_fill_sides.get(wid, {})
            other_side = "DOWN" if filled_side == "UP" else "UP"
            if other_side in sides and len(sides[other_side]) > 0:
                continue
            market = self._market_cache.get(wid)
            if not market:
                continue
            other_token = market["token_up"] if other_side == "UP" else market["token_down"]
            spread = book_reader.get_spread(other_token)
            if not spread:
                self.hedges_skipped += 1
                continue
            other_ask = spread["ask"]
            fee_filled = self.fee_calc._interp_fee_per_share(filled_price)
            fee_other = self.fee_calc._interp_fee_per_share(other_ask)
            total_cost = filled_price + other_ask + fee_filled + fee_other
            profit_per_share = 1.0 - total_cost

            # V15.1-13: Two-tier hedge price check:
            # 1) Hard cap: combined cost must not exceed hedge_max_combined_cost
            # 2) Profit floor: must earn at least hedge_min_profit_per_share
            # This replaces the old dynamic_threshold approach with a clearer model:
            # "If I filled UP at 47c, max I'll pay for DOWN is (98c - 47c - fees) = ~49c"
            max_other_price = self.config.hedge_max_combined_cost - filled_price - fee_filled - fee_other
            if other_ask > max_other_price:
                self.logger.info(
                    "  HEDGE PRICE CAP | {} | {} @ ${:.2f} | {} ask ${:.2f} > max ${:.2f} | "
                    "Combined ${:.3f} > cap ${:.3f}".format(
                        wid, filled_side, filled_price, other_side, other_ask,
                        max_other_price, total_cost, self.config.hedge_max_combined_cost))
                self.hedges_skipped += 1
                continue
            if profit_per_share < self.config.hedge_min_profit_per_share:
                self.logger.info(
                    "  HEDGE LOW PROFIT | {} | {} @ ${:.2f} | {} ask ${:.2f} | "
                    "Pair ${:.3f} | Profit ${:.4f} < min ${:.3f}".format(
                        wid, filled_side, filled_price, other_side, other_ask,
                        total_cost, profit_per_share, self.config.hedge_min_profit_per_share))
                self.hedges_skipped += 1
                continue
            size = filled_size
            profit_str = "${:+.3f}/sh".format(profit_per_share)
            self.logger.info(
                "\n  HEDGE COMPLETE | {} | {} @ ${:.2f} | Buy {} @ ${:.2f} | "
                "Pair: ${:.3f} | {} | {:.0f} shares".format(
                    wid, filled_side, filled_price, other_side, other_ask,
                    total_cost, profit_str, size))
            result = self.place_order(
                other_token, "BUY", other_ask, size,
                wid, "HEDGE-{}".format(other_side), "mm", is_taker=True)
            if result:
                completed += 1
                self.hedges_completed += 1
        return completed

    def process_momentum_exits(self, book_reader):
        """V15.1-14: Momentum exit — sell one-sided fills if price rises >X%.

        When one side fills but the hedge hasn't completed after max_wait_secs,
        check if the filled token's current bid has risen above the fill price
        by momentum_exit_threshold. If so, sell the position for profit and
        cancel any remaining orders on that window.

        This captures directional moves when the market trends in our favor
        instead of waiting indefinitely for the other side to fill.
        """
        if not self.config.momentum_exit_enabled:
            return 0
        now = time.time()
        exits = 0
        for wid, sides in list(self.window_fill_sides.items()):
            # Only check windows with exactly one side filled (not paired)
            if wid in self.paired_windows:
                continue
            if len(sides) != 1:
                continue
            filled_side = list(sides.keys())[0]
            fills = sides[filled_side]
            if not fills:
                continue
            # Use the earliest fill for timing
            earliest_fill = min(fills, key=lambda f: f.get("time", now))
            fill_time = earliest_fill.get("time", now)
            fill_price = earliest_fill.get("price", 0)
            fill_size = sum(f.get("size", 0) for f in fills)
            fill_token = earliest_fill.get("token_id", "")
            hold_secs = now - fill_time
            # Must hold for minimum time
            if hold_secs < self.config.momentum_exit_min_hold_secs:
                continue
            # Only check momentum after waiting long enough for hedge
            if hold_secs < self.config.momentum_exit_max_wait_secs:
                continue
            # Check current bid price for the filled token
            spread = book_reader.get_spread(fill_token)
            if not spread:
                continue
            current_bid = spread["bid"]
            price_change = (current_bid - fill_price) / fill_price if fill_price > 0 else 0
            if price_change >= self.config.momentum_exit_threshold:
                # Price has risen enough — sell for profit
                sell_profit = (current_bid - fill_price) * fill_size
                fee_est = self.fee_calc._interp_fee_per_share(current_bid) * fill_size
                net_profit = sell_profit - fee_est
                self.logger.info(
                    "\n  MOMENTUM EXIT | {} | {} {:.0f} @ ${:.2f} -> bid ${:.2f} | "
                    "Change: {:+.1%} | Gross ${:+.2f} | Net ${:+.2f} (after ~${:.2f} fees) | "
                    "Held {:.0f}s".format(
                        wid, filled_side, fill_size, fill_price, current_bid,
                        price_change, sell_profit, net_profit, fee_est, hold_secs))
                # V15.1-22: Cancel ALL remaining orders on this window.
                # Collect opposite-side token IDs BEFORE cancelling so we can
                # issue a batch cancel_market_orders as a safety net.
                opposite_tokens = set()
                for oid, oinfo in list(self.active_orders.items()):
                    if oinfo.get("window_id") == wid:
                        tid = oinfo.get("token_id", "")
                        if tid and tid != fill_token:
                            opposite_tokens.add(tid)
                cancelled = self.cancel_window_orders(wid)
                self.logger.info(
                    "  MOM-EXIT CANCEL | {} | Cancelled {} orders | "
                    "Opposite tokens: {}".format(
                        wid, cancelled, list(opposite_tokens)))
                # V15.1-22: Safety net — batch cancel on opposite-side token_ids.
                # cancel_window_orders does individual cancel(oid) + batch cancel
                # for the same window, but the opposite-side order may have been
                # tracked under a slightly different state. Belt-and-suspenders:
                if not self.config.dry_run and self.client and opposite_tokens:
                    for tid in opposite_tokens:
                        try:
                            self.client.cancel_market_orders(asset_id=str(tid))
                            self.logger.info(
                                "  MOM-EXIT BATCH CANCEL | {} | token {}".format(
                                    wid, tid[:16] + "..."))
                        except Exception as e:
                            self.logger.warning(
                                "  MOM-EXIT BATCH CANCEL FAIL | {} | {}".format(
                                    wid, str(e)[:100]))
                # Place sell order as taker
                result = self.place_order(
                    fill_token, "SELL", current_bid, fill_size,
                    wid, "MOM-EXIT", "mm", is_taker=True)
                if result:
                    exits += 1
                    # Remove from fill tracking since we've exited
                    self.window_fill_sides.pop(wid, None)
                    self.window_fill_cost.pop(wid, None)
                    self.window_fill_tokens.pop(wid, None)
                    # V15.1-19: Mark window as permanently closed after momentum exit
                    # Keep in filled_windows AND add to closed_windows to block re-entry
                    self.closed_windows.add(wid)
                    # V15.1-20: Release held_windows — capital recovered via CLOB sell
                    self.held_windows.discard(wid)
                    self.window_entry_count.pop(wid, None)
                    # Update capital tracking
                    cost = fill_price * fill_size
                    self.capital_in_positions = max(0, self.capital_in_positions - cost)
                    self._update_total_capital()
                else:
                    # Sell failed — log but still mark window as closed to prevent
                    # re-entry. The position remains but no new orders will be placed.
                    self.logger.warning(
                        "  MOM-EXIT SELL FAILED | {} | Sell order rejected, "
                        "position still held".format(wid))
                    self.closed_windows.add(wid)
            else:
                self.logger.debug(
                    "  MOM CHECK | {} | {} @ ${:.2f} | bid ${:.2f} | {:+.1%} < {:.1%} | {:.0f}s".format(
                        wid, filled_side, fill_price, current_bid,
                        price_change, self.config.momentum_exit_threshold, hold_secs))
        return exits

    def cancel_window_orders(self, window_id, strategy_filter=None):
        oids = self.orders_by_window.get(window_id, [])
        cancelled = 0
        remaining = []
        for oid in list(oids):
            info = self.active_orders.get(oid)
            if info and strategy_filter and info.get("strategy") != strategy_filter:
                remaining.append(oid)
                continue
            if self.config.dry_run:
                if oid in self.active_orders:
                    self._recently_cancelled[oid] = {
                        **self.active_orders[oid], "cancelled_at": time.time()}
                    del self.active_orders[oid]
                    cancelled += 1
                if self.sim_engine and oid in self.sim_engine.pending_orders:
                    del self.sim_engine.pending_orders[oid]
            else:
                try:
                    # Save to recently_cancelled BEFORE cancelling on exchange.
                    # If the order filled before the cancel takes effect, the
                    # WS fill event can still be processed using this metadata.
                    if oid in self.active_orders:
                        self._recently_cancelled[oid] = {
                            **self.active_orders[oid], "cancelled_at": time.time()}
                    self.client.cancel(oid)
                    if oid in self.active_orders:
                        del self.active_orders[oid]
                    cancelled += 1
                except Exception:
                    # Cancel failed — order may still be live; remove from buffer
                    self._recently_cancelled.pop(oid, None)
                    remaining.append(oid)
        self.orders_by_window[window_id] = remaining
        self.total_orders_cancelled += cancelled
        # V15.1-P4: Batch cancel safety net — ensure exchange-side cleanup.
        # Individual cancel(oid) can succeed locally but the exchange may
        # still show the order briefly. cancel_market_orders(asset_id) is a
        # server-side batch cancel that catches any stragglers.
        if not self.config.dry_run and self.client and cancelled > 0:
            cancelled_tokens = set()
            for oid, info in self._recently_cancelled.items():
                tid = info.get("token_id", "")
                if tid and info.get("window_id") == window_id:
                    cancelled_tokens.add(tid)
            for tid in cancelled_tokens:
                try:
                    self.client.cancel_market_orders(asset_id=str(tid))
                except Exception:
                    pass  # Best-effort; individual cancels already succeeded
        self._recalc_exposure()
        return cancelled

    def cancel_all(self):
        for wid in list(self.orders_by_window.keys()):
            self.cancel_window_orders(wid)

    def _track_order(self, oid, window_id, side, price, size, token_id, strategy="mm"):
        self.active_orders[oid] = {
            "window_id": window_id, "side": side, "price": price,
            "size": size, "token_id": token_id, "strategy": strategy,
            "time": time.time(),
        }
        if window_id not in self.orders_by_window:
            self.orders_by_window[window_id] = []
        self.orders_by_window[window_id].append(oid)
        if side == "BUY":
            cost = price * size
            self.window_exposure[window_id] = (
                self.window_exposure.get(window_id, 0) + cost)
            self.total_exposure += cost
            asset = window_id.split("-")[0] if "-" in window_id else ""
            self.asset_exposure[asset] = self.asset_exposure.get(asset, 0) + cost
            self.capital_deployed += cost
            self._update_total_capital()
            self.strategy_capital_used[strategy] = (
                self.strategy_capital_used.get(strategy, 0) + cost)
        self.total_orders_placed += 1
        strat_window_key = "{}|{}".format(strategy, window_id)
        self.orders_this_cycle[strat_window_key] = (
            self.orders_this_cycle.get(strat_window_key, 0) + 1)
        counter_map = {
            "sniper": "sniper_trades", "arb": "arb_trades",
            "contrarian": "contrarian_trades", "trend": "trend_trades", "mm": "mm_trades",
        }
        attr = counter_map.get(strategy, "mm_trades")
        setattr(self, attr, getattr(self, attr) + 1)

    def purge_recently_cancelled(self):
        """Remove entries older than TTL from the recently-cancelled buffer."""
        now = time.time()
        expired = [oid for oid, info in self._recently_cancelled.items()
                   if now - info.get("cancelled_at", 0) > self._recently_cancelled_ttl]
        for oid in expired:
            del self._recently_cancelled[oid]

    def _recalc_exposure(self):
        self.window_exposure = {}
        self.asset_exposure = {}
        self.capital_deployed = 0.0
        strat_deployed = {
            "mm": 0.0, "trend": 0.0, "sniper": 0.0, "arb": 0.0, "contrarian": 0.0
        }
        for oid, info in self.active_orders.items():
            if info["side"] != "BUY":
                continue
            wid = info["window_id"]
            cost = info["price"] * info["size"]
            self.window_exposure[wid] = self.window_exposure.get(wid, 0) + cost
            asset = wid.split("-")[0] if "-" in wid else ""
            self.asset_exposure[asset] = self.asset_exposure.get(asset, 0) + cost
            self.capital_deployed += cost
            strat = info.get("strategy", "mm")
            strat_deployed[strat] = strat_deployed.get(strat, 0) + cost
        for strat in strat_deployed:
            self.strategy_capital_used[strat] = strat_deployed[strat]
        self.total_exposure = sum(self.window_exposure.values())
        self._update_total_capital()

    def get_stats(self):
        pnl_data = self.get_live_pnl()  # V15.1-20: now returns dict or None
        # Backward-compat: extract scalar live_pnl for existing consumers
        if pnl_data and isinstance(pnl_data, dict):
            live_pnl = pnl_data.get("wallet_delta")  # Primary metric: hard wallet change
            wallet_delta = pnl_data.get("wallet_delta")
            total_pnl = pnl_data.get("total_pnl")
            held_value = pnl_data.get("held_value", 0)
            wallet_now = pnl_data.get("wallet_now")
        else:
            live_pnl = pnl_data  # Legacy: None
            wallet_delta = None
            total_pnl = None
            held_value = 0
            wallet_now = None
        return {
            "active_orders": len(self.active_orders),
            "total_placed": self.total_orders_placed,
            "total_cancelled": self.total_orders_cancelled,
            "total_exposure": self.total_exposure,
            "windows_active": len(self.window_exposure),
            "daily_pnl": self.daily_pnl,
            "sniper_trades": self.sniper_trades, "arb_trades": self.arb_trades,
            "contrarian_trades": self.contrarian_trades,
            "mm_trades": self.mm_trades, "trend_trades": self.trend_trades,
            "asset_exposure": dict(self.asset_exposure),
            "capital_deployed": self.capital_deployed,
            "capital_in_positions": self.capital_in_positions,
            "total_capital_used": self.total_capital_used,
            "available_capital": self.get_available_capital(),
            "session_spent": self.session_total_spent,
            "token_holdings": len(self.token_holdings),
            "live_pnl": live_pnl,  # V15.1-20: now wallet_delta (hard fact)
            "wallet_delta": wallet_delta,
            "total_pnl_est": total_pnl,
            "held_value": held_value,
            "wallet_now": wallet_now,
            "pending_claims": len(self.expired_windows_pending_claim),
            "hedges_completed": self.hedges_completed,
            "hedges_skipped": self.hedges_skipped,
            "estimated_rewards": self.estimated_rewards_total,
            "unredeemed_value": self.unredeemed_position_value,
            "paired_windows": len(self.paired_windows),
            "filled_windows": len(self.filled_windows),
            "closed_windows": len(self.closed_windows),
            "held_windows": len(self.held_windows),
            "session_realized_returns": self.session_realized_returns,
            "session_realized_cost": self.session_realized_cost,
            "session_realized_pnl": self.session_realized_returns - self.session_realized_cost,
        }


# -----------------------------------------------------------------
# Simulated Fill Engine
# -----------------------------------------------------------------

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
# -----------------------------------------------------------------

class MarketMakingStrategy:
    def __init__(self, config, engine, book_reader, price_feed, kelly, fee_calc,
                 logger, reward_optimizer, vol_tracker, churn_manager):
        self.config = config
        self.engine = engine
        self.book_reader = book_reader
        self.price_feed = price_feed
        self.kelly = kelly
        self.fee_calc = fee_calc
        self.logger = logger
        self.reward_optimizer = reward_optimizer
        self.vol_tracker = vol_tracker
        self.churn_manager = churn_manager
        self.last_refresh = {}
        self._window_first_placed = {}  # {window_id: timestamp} — when orders were first placed
        self._cycle_reward_estimates = []
        self._total_estimated_reward = 0.0

    def execute(self, market):
        asset = market["asset"]
        window_id = market["window_id"]
        now = time.time()
        time_remaining = market["end_time"] - now
        if time_remaining < self.config.min_time_remaining:
            return
        # V15.1-6: Skip windows too far out
        if time_remaining > self.config.max_order_horizon:
            return
        # V15.1-19: Closed window guard — permanently blocked after momentum exit
        if window_id in self.engine.closed_windows:
            self.logger.debug(
                "  CLOSED GUARD | {} | Window closed (momentum exit) — no re-entry".format(
                    window_id))
            return
        # V15.1-15: PERSISTENT filled window guard (replaces CC-OPT-C).
        # filled_windows survives reconcile_capital_from_wallet() which resets
        # window_fill_cost. This is the PRIMARY re-entry prevention mechanism.
        if window_id in self.engine.filled_windows:
            fill_cost = self.engine.window_fill_cost.get(window_id, 0)
            entries = self.engine.window_entry_count.get(window_id, 0)
            self.logger.debug(
                "  FILL GUARD | {} | Already filled ${:.2f} (entries={}) — no re-entry".format(
                    window_id, fill_cost, entries))
            return
        # CC-OPT-C (backup): Also check window_fill_cost in case filled_windows
        # was somehow missed (belt-and-suspenders).
        fill_cost = self.engine.window_fill_cost.get(window_id, 0)
        if fill_cost > 0:
            self.engine.filled_windows.add(window_id)  # Repair missing entry
            self.engine.held_windows.add(window_id)  # V15.1-20: repair held tracking too
            self.logger.debug(
                "  FILL SKIP | {} | Already filled ${:.2f} — position established".format(
                    window_id, fill_cost))
            return
        # V15.1-P5: NO-CHURN-REFRESH GUARD — once orders are placed for a
        # window, never cancel+replace them. Cancelling is async on Polymarket;
        # if the old order fills before the cancel takes effect, BOTH old and
        # new orders fill, doubling the position size. Let original orders
        # stand until they fill or the window expires.
        if window_id in self._window_first_placed:
            self.logger.debug(
                "  NO-REFRESH | {} | Orders placed at {:.0f}s ago — standing".format(
                    window_id, now - self._window_first_placed[window_id]))
            return
        last = self.last_refresh.get(window_id, 0)
        if now - last < self.config.mm_refresh_interval:
            return
        spread_up = self.book_reader.get_spread(market["token_up"])
        spread_down = self.book_reader.get_spread(market["token_down"])
        if not spread_up:
            return
        midpoint = spread_up["midpoint"]
        spread = spread_up["spread"]
        bid = spread_up["bid"]
        ask = spread_up["ask"]
        if spread < self.config.mm_min_spread:
            return
        if spread > self.config.mm_max_spread:
            midpoint = 0.50
        vol_level, vol_sum = self.vol_tracker.get_volatility_level(market["token_up"])
        if vol_level == "EXTREME":
            self.logger.info("  VOL PAUSE | {} | vol={:.3f} (EXTREME) | Skipping MM".format(
                window_id, vol_sum if vol_sum else 0))
            return
        prediction = self.price_feed.predict_resolution(
            asset, market["timestamp"], market["end_time"])
        momentum = self.price_feed.get_momentum(asset, self.config.tf_lookback_minutes)
        skew = 0.0
        trend_label = "NEUTRAL"
        if prediction and prediction["confidence"] > 0.60:
            if prediction["direction"] == "UP":
                skew = self.config.tf_skew_factor * (prediction["confidence"] - 0.5) * 4
                trend_label = "CL-UP ({:.0%})".format(prediction["confidence"])
            else:
                skew = -self.config.tf_skew_factor * (prediction["confidence"] - 0.5) * 4
                trend_label = "CL-DN ({:.0%})".format(prediction["confidence"])
        current_price = self.price_feed.get_current_price(asset)
        price_str = "${:,.2f}".format(current_price) if current_price else "N/A"
        mom_str = "{:+.3f}%".format(momentum * 100) if momentum is not None else "N/A"
        vol_str = "{} ({:.3f})".format(vol_level, vol_sum) if vol_sum is not None else "UNKNOWN"
        maker_edge = market.get("maker_edge", market.get("edge", 0))
        edge_str = "mE:{:.1%}".format(maker_edge) if maker_edge else "mE:?"
        self.logger.info(
            "\n  {:4s} {:3s} | Price: {:>12s} | Mom: {:>8s} | {:16s} | "
            "Bid: {:.2f} Ask: {:.2f} Sprd: {:.3f} | Vol: {} | {} | {:.0f}s".format(
                asset.upper(), market["timeframe"].upper(), price_str,
                mom_str, trend_label, bid, ask, spread, vol_str, edge_str,
                time_remaining))
        is_high_vol = momentum is not None and abs(momentum) > self.config.vol_circuit_breaker
        if is_high_vol and self.config.mm_enabled:
            self.logger.info("  CIRCUIT BREAKER | {} | Mom: {} | MM PAUSED".format(
                asset.upper(), mom_str))
            return
        if not self.config.mm_enabled:
            return
        # V15.1-19 Filter A: Momentum Gate — skip if short-term momentum too strong
        if (self.config.momentum_gate_threshold > 0 and momentum is not None
                and abs(momentum) > self.config.momentum_gate_threshold):
            self.logger.info(
                "  MOMENTUM GATE | {} | Mom: {:+.3f}% > {:.3f}% | Skipping MM".format(
                    window_id, momentum * 100, self.config.momentum_gate_threshold * 100))
            return
        # V15.1-19 Filter C: Spread Symmetry — skip if UP/DN spreads diverge too much
        if (self.config.max_spread_asymmetry > 0 and spread_down):
            dn_spread = spread_down.get("spread", 0)
            spread_delta = abs(spread - dn_spread)
            if spread_delta > self.config.max_spread_asymmetry:
                self.logger.info(
                    "  SPREAD ASYM | {} | UP sprd: {:.3f} DN sprd: {:.3f} | "
                    "Delta {:.3f} > max {:.3f} | Skipping".format(
                        window_id, spread, dn_spread, spread_delta,
                        self.config.max_spread_asymmetry))
                return
        # V15.1-12: Relaxed directional filter — CL-DN/CL-UP now only block
        # at STRONG confidence (>80%). At 60-80% (CL- prefix), the skew already
        # adjusts prices to favor the predicted side, which is sufficient protection.
        # This dramatically improves fill rate on 15m crypto windows where CL-UP/CL-DN
        # triggers frequently at 70% confidence, causing most markets to be skipped.
        is_strong_down = trend_label.startswith("STRONG DN")
        is_strong_up = trend_label.startswith("STRONG UP")
        if spread_down:
            result = self.reward_optimizer.optimal_distance_for_pair(
                spread_up, spread_down or {}, midpoint)
            optimal_d, reward_score, pair_cost, pair_profit = result
        else:
            optimal_d = None
        if optimal_d is None:
            optimal_d = self.config.mm_base_spread
            if vol_level == "HIGH":
                optimal_d = self.config.mm_base_spread * 1.5
            elif vol_level == "MEDIUM":
                optimal_d = self.config.mm_base_spread * 1.2
            buy_up = midpoint - optimal_d
            buy_down = (1.0 - midpoint) - optimal_d
            if buy_up > 0.02 and buy_down > 0.02:
                fee_u = self.fee_calc._interp_fee_per_share(buy_up)
                fee_d = self.fee_calc._interp_fee_per_share(buy_down)
                pair_cost = buy_up + buy_down + fee_u + fee_d
                pair_profit = 1.0 - pair_cost
                reward_score = self.reward_optimizer.reward_score(spread, optimal_d)
            else:
                return
        else:
            if vol_level == "HIGH":
                wider_d = optimal_d * 1.5
                buy_up = midpoint - wider_d + skew
                buy_down = (1.0 - midpoint) - wider_d - skew
                if buy_up > 0.02 and buy_down > 0.02:
                    fee_u = self.fee_calc._interp_fee_per_share(buy_up)
                    fee_d = self.fee_calc._interp_fee_per_share(buy_down)
                    new_pair_cost = buy_up + buy_down + fee_u + fee_d
                    if 1.0 - new_pair_cost >= self.config.pair_min_profit:
                        optimal_d = wider_d
                        pair_cost = new_pair_cost
                        pair_profit = 1.0 - new_pair_cost
                        reward_score = self.reward_optimizer.reward_score(spread, optimal_d)
        # Fix 3: Enforce optimal_d >= mm_base_spread (CC mmSpreadMin)
        # This ensures the bot never places orders closer to midpoint than the
        # configured minimum spread, regardless of what the reward optimizer suggests.
        # Scales automatically when mmSpreadMin is changed in CC.
        if optimal_d < self.config.mm_base_spread:
            self.logger.debug(
                "  SPREAD FLOOR | {} | d={:.3f} < min {:.3f}, using min".format(
                    window_id, optimal_d, self.config.mm_base_spread))
            optimal_d = self.config.mm_base_spread
        buy_up_price = round(midpoint - optimal_d + skew, 2)
        buy_down_price = round((1.0 - midpoint) - optimal_d - skew, 2)
        if buy_up_price <= 0.02 or buy_down_price <= 0.02:
            return
        fee_up = self.fee_calc._interp_fee_per_share(buy_up_price)
        fee_down = self.fee_calc._interp_fee_per_share(buy_down_price)
        final_pair_cost = buy_up_price + buy_down_price + fee_up + fee_down
        final_pair_profit = 1.0 - final_pair_cost
        # V15.1-3: Epsilon to avoid floating point boundary rejection
        if final_pair_profit < self.config.pair_min_profit - 0.0001:
            self.logger.info(
                "  PAIR REJECT | {} | UP:{:.2f} + DN:{:.2f} + fees:{:.3f} = {:.3f} | "
                "Profit: ${:.4f} < min ${:.3f}".format(
                    window_id, buy_up_price, buy_down_price, fee_up + fee_down,
                    final_pair_cost, final_pair_profit, self.config.pair_min_profit))
            return
        if self.config.pair_sizing_equal_shares:
            cost_per_pair = buy_up_price + buy_down_price
            if cost_per_pair <= 0:
                return
            total_budget = self.config.mm_order_size * 2
            if final_pair_profit >= self.config.edge_premium_threshold:
                total_budget *= self.config.edge_premium_size_mult
                self.logger.info(
                    "  PREMIUM EDGE | {} | {:.1%} >= {:.1%} | Budget ${:.0f} -> ${:.0f}".format(
                        window_id, final_pair_profit, self.config.edge_premium_threshold,
                        self.config.mm_order_size * 2, total_budget))
            num_pairs = total_budget / cost_per_pair
            num_pairs = max(5.0, num_pairs)
            up_size = num_pairs
            down_size = num_pairs
            guaranteed_profit = num_pairs * final_pair_profit
            self.logger.info(
                "  PAIR OK | {} | d={:.3f} | UP:{:.2f} + DN:{:.2f} = {:.3f} | "
                "Profit: ${:.3f}/pair x {:.0f} = ${:.2f} guaranteed | Reward: {:.1%} | Vol: {}".format(
                    window_id, optimal_d, buy_up_price, buy_down_price,
                    final_pair_cost, final_pair_profit, num_pairs, guaranteed_profit,
                    reward_score, vol_level))
        else:
            up_size = max(5.0, self.config.mm_order_size / buy_up_price) if buy_up_price > 0.02 else 0
            down_size = max(5.0, self.config.mm_order_size / buy_down_price) if buy_down_price > 0.02 else 0
            self.logger.info(
                "  PAIR OK | {} | d={:.3f} | UP:{:.2f} + DN:{:.2f} = {:.3f} | "
                "Profit: ${:.3f}/pair | Reward: {:.1%} | Vol: {}".format(
                    window_id, optimal_d, buy_up_price, buy_down_price,
                    final_pair_cost, final_pair_profit, reward_score, vol_level))

        # V15.1-19 Filter B: Order Book Depth — check both sides have liquidity
        # We place MAKER (limit) orders, so we measure bid-side depth (activity near our price)
        # plus ask-side depth within 5c of midpoint to gauge overall market activity.
        if self.config.min_book_depth > 0:
            up_book = self.book_reader.get_book(market["token_up"])
            dn_book = self.book_reader.get_book(market["token_down"])
            for side_name, book, target_price in [
                    ("UP", up_book, buy_up_price), ("DN", dn_book, buy_down_price)]:
                if not book:
                    self.logger.info(
                        "  DEPTH GATE | {} | {} book unavailable | Skipping".format(
                            window_id, side_name))
                    return
                # Sum bid-side depth within 5c of our buy price (other buyers = market activity)
                bids = book.get("bids", [])
                asks = book.get("asks", [])
                depth_usd = 0.0
                for order in bids:
                    op = float(order["price"])
                    if abs(op - target_price) <= 0.05:
                        depth_usd += float(order["size"]) * op
                # Also count asks within 5c of midpoint (sellers = potential fills)
                for order in asks:
                    op = float(order["price"])
                    if op <= target_price + 0.05:
                        depth_usd += float(order["size"]) * op
                if depth_usd < self.config.min_book_depth:
                    self.logger.info(
                        "  DEPTH GATE | {} | {} depth ${:.2f} < min ${:.2f} | Skipping".format(
                            window_id, side_name, depth_usd, self.config.min_book_depth))
                    return

        # V15.1-2: Pre-check capital for FULL PAIR before placing either side
        total_pair_dollar_cost = (buy_up_price * up_size) + (buy_down_price * down_size)
        available_cap = self.engine.get_available_capital()
        total_exp_after = self.engine.total_exposure + total_pair_dollar_cost
        if total_pair_dollar_cost > available_cap:
            self.logger.info(
                "  PAIR CAPITAL SKIP | {} | Need ${:.2f} but only ${:.2f} available".format(
                    window_id, total_pair_dollar_cost, available_cap))
            return
        if total_exp_after > self.engine.config.max_total_exposure:
            self.logger.info(
                "  PAIR EXPOSURE SKIP | {} | Would be ${:.2f} exp > max ${:.2f}".format(
                    window_id, total_exp_after, self.engine.config.max_total_exposure))
            return

        for level in range(self.config.mm_num_levels):
            offset = optimal_d + (level * self.config.mm_level_spacing)
            up_price = round(midpoint - offset + skew, 2)
            down_price = round((1.0 - midpoint) - offset - skew, 2)
            if self.config.pair_sizing_equal_shares and level > 0:
                if up_price > 0.02 and down_price > 0.02:
                    lv_cost = up_price + down_price
                    total_budget_lv = self.config.mm_order_size * 2
                    lv_profit = 1.0 - lv_cost - fee_up - fee_down
                    if lv_profit >= self.config.edge_premium_threshold:
                        total_budget_lv *= self.config.edge_premium_size_mult
                    lv_pairs = max(5.0, total_budget_lv / lv_cost)
                    up_size = lv_pairs
                    down_size = lv_pairs
                else:
                    continue
            elif not self.config.pair_sizing_equal_shares:
                up_size = max(5.0, self.config.mm_order_size / up_price) if up_price > 0.02 else 0
                down_size = max(5.0, self.config.mm_order_size / down_price) if down_price > 0.02 else 0
            should_update_up = self.churn_manager.should_update_orders(
                window_id, market["token_up"], up_price, up_size) if up_size > 0 else False
            should_update_down = self.churn_manager.should_update_orders(
                window_id, market["token_down"], down_price, down_size) if down_size > 0 else False
            if not should_update_up and not should_update_down:
                self.logger.debug("  CHURN SKIP | {} | No significant change".format(window_id))
                self.last_refresh[window_id] = now
                return
            self.engine.cancel_window_orders(window_id, strategy_filter="mm")
            self.last_refresh[window_id] = now
            # --- PAIR-OR-SKIP GUARD ---
            # Both sides must be eligible; never place one-sided exposure
            can_place_up = (not is_strong_down and 0.02 <= up_price < midpoint and up_size > 0)
            can_place_down = (not is_strong_up and 0.02 <= down_price <= 0.98 and down_size > 0)
            # V15.1-12: Log when directional skew is active but not blocking
            if (trend_label.startswith("CL-DN") or trend_label.startswith("CL-UP")) and can_place_up and can_place_down:
                self.logger.debug(
                    "    SKEW ACTIVE | {} | {} | skew={:+.3f} (not blocking)".format(
                        window_id, trend_label, skew))
            if not can_place_up or not can_place_down:
                self.logger.info(
                    "    PAIR SKIP | {} | Can't fund both sides (up={} dn={})".format(
                        window_id, can_place_up, can_place_down))
                continue
            up_result = self.engine.place_order(
                market["token_up"], "BUY", up_price, up_size,
                window_id, "MM-L{}".format(level), "mm", is_taker=False)
            if not up_result:
                self.logger.info(
                    "    PAIR ABORT | {} | UP side failed, skipping DN".format(window_id))
                continue
            self.churn_manager.record_update(
                window_id, market["token_up"], up_price, up_size)
            dn_result = self.engine.place_order(
                market["token_down"], "BUY", down_price, down_size,
                window_id, "MM-L{}d".format(level), "mm", is_taker=False)
            if not dn_result:
                # DN failed — cancel the orphaned UP to prevent one-sided exposure
                self.logger.warning(
                    "    ORPHAN CANCEL | {} | DN rejected, cancelling UP {}".format(
                        window_id, up_result))
                self.engine.cancel_window_orders(window_id, strategy_filter="mm")
                continue
            self.churn_manager.record_update(
                window_id, market["token_down"], down_price, down_size)
            # V15.1-P5: Record first placement time — blocks churn refresh
            if window_id not in self._window_first_placed:
                self._window_first_placed[window_id] = time.time()
            est_reward = self.reward_optimizer.estimate_reward_per_hour(
                2, reward_score, (up_size + down_size) / 2)
            self._total_estimated_reward += est_reward * (self.config.cycle_interval / 3600)

    def cleanup_window(self, window_id):
        """V15.1-P5: Clean up tracking when a window expires."""
        self._window_first_placed.pop(window_id, None)

    def get_reward_stats(self):
        return {"total_estimated_reward": self._total_estimated_reward}


# -----------------------------------------------------------------
# Strategy 2: Late Sniper
# -----------------------------------------------------------------

class LateSniper:
    def __init__(self, config, engine, book_reader, price_feed, kelly, fee_calc, logger):
        self.config = config
        self.engine = engine
        self.book_reader = book_reader
        self.price_feed = price_feed
        self.kelly = kelly
        self.fee_calc = fee_calc
        self.logger = logger
        self.sniped_windows = set()

    def execute(self, market):
        if not self.config.sniper_enabled:
            return
        window_id = market["window_id"]
        # V15.1-19: Skip closed windows
        if window_id in self.engine.closed_windows:
            return
        # V15.1-15: Skip windows with existing fills
        if window_id in self.engine.filled_windows:
            return
        asset = market["asset"]
        now = time.time()
        time_remaining = market["end_time"] - now
        if time_remaining > self.config.sniper_time_window or time_remaining < 30:
            return
        if window_id in self.sniped_windows:
            return
        prediction = self.price_feed.predict_resolution(
            asset, market["timestamp"], market["end_time"])
        if not prediction:
            spread_up = self.book_reader.get_spread(market["token_up"])
            if not spread_up:
                return
            if spread_up["midpoint"] > self.config.sniper_min_probability:
                prediction = {
                    "direction": "UP", "confidence": spread_up["midpoint"],
                    "prob_up": spread_up["midpoint"],
                    "prob_down": 1 - spread_up["midpoint"],
                }
            elif spread_up["midpoint"] < (1 - self.config.sniper_min_probability):
                prediction = {
                    "direction": "DOWN",
                    "confidence": 1 - spread_up["midpoint"],
                    "prob_up": spread_up["midpoint"],
                    "prob_down": 1 - spread_up["midpoint"],
                }
            else:
                return
        if prediction["confidence"] < self.config.sniper_min_probability:
            return
        if prediction["direction"] == "UP":
            token_id = market["token_up"]
            spread = self.book_reader.get_spread(market["token_up"])
        else:
            token_id = market["token_down"]
            spread = self.book_reader.get_spread(market["token_down"])
        if not spread:
            return
        buy_price = spread["ask"]
        fee_per_share = self.fee_calc._interp_fee_per_share(buy_price)
        net_edge = prediction["confidence"] - buy_price - fee_per_share
        if net_edge < self.config.sniper_min_edge:
            return
        if buy_price > self.config.sniper_max_price:
            return
        dollar_size = self.config.sniper_size
        if self.kelly.enabled:
            ks = self.kelly.optimal_size(prediction["confidence"], buy_price, is_taker=True)
            if ks > 0:
                dollar_size = max(ks, self.config.sniper_size)
        size = max(5.0, dollar_size / buy_price)
        liq = self.book_reader.get_available_liquidity(token_id, "BUY", buy_price, size)
        if not liq["available"]:
            return
        self.logger.info(
            "\n  SNIPER | {} {} | {} @ {:.1%} | Buy @ ${:.2f} | "
            "Edge: {:.1%} | ${:.0f} | {:.0f}s left".format(
                asset.upper(), market["timeframe"].upper(),
                prediction["direction"], prediction["confidence"],
                buy_price, net_edge, dollar_size, time_remaining))
        result = self.engine.place_order(
            token_id, "BUY", buy_price, size, window_id,
            "SNIPE-{}".format(prediction["direction"]), "sniper", is_taker=True)
        if result:
            self.sniped_windows.add(window_id)


# -----------------------------------------------------------------
# Strategy 3: Combined Probability Arb
# -----------------------------------------------------------------

class CombinedProbArb:
    def __init__(self, config, engine, book_reader, fee_calc, logger):
        self.config = config
        self.engine = engine
        self.book_reader = book_reader
        self.fee_calc = fee_calc
        self.logger = logger
        self.last_scan = {}
        self.pending_arb_legs = []

    def execute(self, market):
        if not self.config.arb_enabled:
            return
        window_id = market["window_id"]
        # V15.1-19: Skip closed windows
        if window_id in self.engine.closed_windows:
            return
        # V15.1-15: Skip windows with existing fills
        if window_id in self.engine.filled_windows:
            return
        now = time.time()
        last = self.last_scan.get(window_id, 0)
        if now - last < self.config.arb_scan_interval:
            return
        self.last_scan[window_id] = now
        time_remaining = market["end_time"] - now
        if time_remaining < 60:
            return
        self._cleanup_stale_legs(now)
        spread_up = self.book_reader.get_spread(market["token_up"])
        spread_down = self.book_reader.get_spread(market["token_down"])
        if not spread_up or not spread_down:
            return
        ask_up = spread_up["ask"]
        ask_down = spread_down["ask"]
        fee_up = self.fee_calc._interp_fee_per_share(ask_up)
        fee_down = self.fee_calc._interp_fee_per_share(ask_down)
        total_cost = ask_up + ask_down + fee_up + fee_down
        profit_per_share = 1.0 - total_cost
        if profit_per_share < self.config.arb_min_profit:
            return
        if profit_per_share < self.config.min_pair_edge:
            return
        if profit_per_share >= 0.05:
            dollar_size = self.config.arb_max_size
        elif profit_per_share >= 0.03:
            dollar_size = self.config.arb_max_size * 0.7
        else:
            dollar_size = min(self.config.arb_max_size * 0.5, profit_per_share * 500)
        shares = max(5.0, dollar_size / total_cost)
        guaranteed = shares * profit_per_share
        self.logger.info(
            "\n  ARB FOUND | {} {} | Edge: {:.1%} | {:.0f} pairs x ${:.3f} = "
            "${:.2f} guaranteed | {:.0f}s left".format(
                market["asset"].upper(), market["timeframe"].upper(),
                profit_per_share, shares, profit_per_share, guaranteed,
                time_remaining))
        oid_up = self.engine.place_order(
            market["token_up"], "BUY", ask_up, shares,
            window_id, "ARB-UP", "arb", is_taker=True)
        oid_down = self.engine.place_order(
            market["token_down"], "BUY", ask_down, shares,
            window_id, "ARB-DN", "arb", is_taker=True)
        if oid_up and not oid_down:
            self.pending_arb_legs.append({"oid": oid_up, "window_id": window_id, "time": now})
        elif oid_down and not oid_up:
            self.pending_arb_legs.append({"oid": oid_down, "window_id": window_id, "time": now})

    def _cleanup_stale_legs(self, now):
        timeout = self.config.arb_leg_timeout
        stale = [l for l in self.pending_arb_legs if now - l["time"] > timeout]
        for leg in stale:
            self.engine.cancel_window_orders(leg["window_id"], strategy_filter="arb")
        self.pending_arb_legs = [l for l in self.pending_arb_legs if now - l["time"] <= timeout]


# -----------------------------------------------------------------
# Strategy 4: Contrarian Panic Fade
# -----------------------------------------------------------------

class ContrarianFade:
    def __init__(self, config, engine, book_reader, price_feed, fee_calc, logger):
        self.config = config
        self.engine = engine
        self.book_reader = book_reader
        self.price_feed = price_feed
        self.fee_calc = fee_calc
        self.logger = logger
        self.faded_windows = set()

    def execute(self, market):
        if not self.config.contrarian_enabled:
            return
        window_id = market["window_id"]
        # V15.1-19: Skip closed windows
        if window_id in self.engine.closed_windows:
            return
        # V15.1-15: Skip windows with existing fills
        if window_id in self.engine.filled_windows:
            return
        asset = market["asset"]
        now = time.time()
        time_remaining = market["end_time"] - now
        if time_remaining < self.config.contrarian_min_time:
            return
        if window_id in self.faded_windows:
            return
        spread_up = self.book_reader.get_spread(market["token_up"])
        spread_down = self.book_reader.get_spread(market["token_down"])
        if not spread_up or not spread_down:
            return
        mid_up = spread_up["midpoint"]
        mid_down = spread_down["midpoint"]
        panic_side = None
        if mid_up < (0.50 - self.config.contrarian_panic_threshold):
            panic_side = "UP"
        elif mid_down < (0.50 - self.config.contrarian_panic_threshold):
            panic_side = "DOWN"
        if not panic_side:
            return
        prediction = self.price_feed.predict_resolution(
            asset, market["timestamp"], market["end_time"])
        if prediction:
            if panic_side == "UP" and prediction["prob_up"] < 0.35:
                return
            if panic_side == "DOWN" and prediction["prob_down"] < 0.35:
                return
        token_id = market["token_up"] if panic_side == "UP" else market["token_down"]
        spread = spread_up if panic_side == "UP" else spread_down
        buy_price = spread["ask"]
        dollar_size = self.config.contrarian_size
        size = dollar_size / buy_price
        liq = self.book_reader.get_available_liquidity(token_id, "BUY", buy_price, size)
        if not liq["available"]:
            return
        self.logger.info(
            "\n  CONTRARIAN | {} {} | Fading {} panic | Buy @ ${:.2f}".format(
                asset.upper(), market["timeframe"].upper(), panic_side, buy_price))
        result = self.engine.place_order(
            token_id, "BUY", buy_price, size, window_id,
            "FADE-{}".format(panic_side), "contrarian", is_taker=True)
        if result:
            self.faded_windows.add(window_id)


# -----------------------------------------------------------------
# Main Bot (V15.1)
# -----------------------------------------------------------------

class PolymarketBot:
    def __init__(self):
        self.config = BotConfig()
        errors = self.config.validate()
        if errors:
            print("CONFIG ERRORS:")
            for e in errors:
                print("  - " + e)
            sys.exit(1)

        self.logger = setup_logging(self.config.log_level)
        self.running = False
        self.fee_calc = FeeCalculator()
        self.price_feed = PriceFeed(self.config, self.logger)
        self.market_discovery = MarketDiscovery(self.config, self.logger)
        self.book_reader = OrderBookReader(self.logger)
        self.balance_checker = WalletBalanceChecker(self.config, self.logger)
        self.claim_manager = AutoClaimManager(self.config, self.logger)
        self.engine = TradingEngine(
            self.config, self.fee_calc, self.logger, self.balance_checker)
        self.claim_manager.set_engine(self.engine)
        self.kelly = KellySizer(self.config, self.fee_calc)

        self.reward_optimizer = RewardOptimizer(self.config, self.fee_calc, self.logger)
        self.vol_tracker = VolatilityTracker(self.config, self.logger)
        self.churn_manager = OrderChurnManager(self.config, self.logger)
        self.merge_detector = MergeDetector(self.config, self.logger)
        self.auto_merger = AutoMerger(self.config, self.logger, self.engine)

        if self.config.dry_run:
            self.sim_engine = SimulatedFillEngine(
                self.config, self.fee_calc, self.logger, self.engine)
            self.engine.sim_engine = self.sim_engine
        else:
            self.sim_engine = None

        self.window_conditions = {}

        self.mm_strategy = MarketMakingStrategy(
            self.config, self.engine, self.book_reader,
            self.price_feed, self.kelly, self.fee_calc, self.logger,
            self.reward_optimizer, self.vol_tracker, self.churn_manager)
        self.sniper = LateSniper(
            self.config, self.engine, self.book_reader,
            self.price_feed, self.kelly, self.fee_calc, self.logger)
        self.arb = CombinedProbArb(
            self.config, self.engine, self.book_reader, self.fee_calc, self.logger)
        self.contrarian = ContrarianFade(
            self.config, self.engine, self.book_reader,
            self.price_feed, self.fee_calc, self.logger)
        self._loss_stop_until = 0.0
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, signum, frame):
        if not self.running:
            # Second Ctrl+C: force exit immediately
            self.logger.info("\nForce exit (second signal). Goodbye.")
            os._exit(1)
        self.logger.info("\nShutdown signal received. Cancelling all orders...")
        self.logger.info("  (Press Ctrl+C again to force-quit immediately)")
        self.running = False
        try:
            self.engine.cancel_all()
        except Exception:
            pass
        self._print_summary("FINAL")
        self._print_claim_summary()
        self._print_v15_1_summary()
        self.logger.info("All orders cancelled. Exiting.")
        os._exit(0)

    def _process_immediate_pair_completions(self):
        if not self.config.immediate_pair_completion:
            return 0
        completed = 0
        for hedge in list(self.engine._pending_hedges):
            wid = hedge["window_id"]
            filled_side = hedge["filled_side"]
            filled_price = hedge["filled_price"]
            filled_size = hedge["filled_size"]
            sides = self.engine.window_fill_sides.get(wid, {})
            other_side = "DOWN" if filled_side == "UP" else "UP"
            if other_side in sides and len(sides[other_side]) > 0:
                self.engine._pending_hedges.remove(hedge)
                continue
            market = self.engine._market_cache.get(wid)
            if not market:
                continue
            other_token = market["token_up"] if other_side == "UP" else market["token_down"]
            spread = self.book_reader.get_spread(other_token)
            if not spread:
                continue
            other_ask = spread["ask"]
            fee_filled = self.fee_calc._interp_fee_per_share(filled_price)
            fee_other = self.fee_calc._interp_fee_per_share(other_ask)
            total_pair_cost = filled_price + other_ask + fee_filled + fee_other
            loss_per_share = total_pair_cost - 1.0
            if loss_per_share > self.config.pair_completion_max_loss:
                self.logger.info(
                    "  PAIR-IMM SKIP | {} | {} @ ${:.2f} | {} ask ${:.2f} | "
                    "Loss ${:.3f}/sh > max ${:.3f} | Defer to hedge".format(
                        wid, filled_side, filled_price, other_side, other_ask,
                        loss_per_share, self.config.pair_completion_max_loss))
                continue
            profit_str = "${:+.3f}/sh".format(-loss_per_share)
            self.logger.info(
                "\n  PAIR-IMM | {} | {} @ ${:.2f} -> Buy {} @ ${:.2f} | "
                "{} | {:.0f} shares".format(
                    wid, filled_side, filled_price, other_side, other_ask,
                    profit_str, filled_size))
            result = self.engine.place_order(
                other_token, "BUY", other_ask, filled_size,
                wid, "PAIR-{}-IMM".format(other_side), "mm", is_taker=True)
            if result:
                self.engine._pending_hedges.remove(hedge)
                self.engine.hedges_completed += 1
                completed += 1
        return completed

    # V15-1: Compute both taker edge and maker edge for each market
    def _compute_market_edges(self, markets):
        for market in markets:
            spread_up = self.book_reader.get_spread(market["token_up"])
            spread_down = self.book_reader.get_spread(market["token_down"])
            if spread_up and spread_down:
                ask_up = spread_up["ask"]
                ask_down = spread_down["ask"]
                fee_up = self.fee_calc._interp_fee_per_share(ask_up)
                fee_down = self.fee_calc._interp_fee_per_share(ask_down)
                edge = 1.0 - (ask_up + ask_down + fee_up + fee_down)
                market["edge"] = edge
                market["ask_sum"] = ask_up + ask_down
                market["spread_data_up"] = spread_up
                market["spread_data_down"] = spread_down

                # Maker edge: estimate using midpoint - base_spread
                mid_up = spread_up["midpoint"]
                d = self.config.mm_base_spread
                buy_up_est = mid_up - d
                buy_down_est = (1.0 - mid_up) - d
                if buy_up_est > 0.02 and buy_down_est > 0.02:
                    mfee_up = self.fee_calc._interp_fee_per_share(buy_up_est)
                    mfee_down = self.fee_calc._interp_fee_per_share(buy_down_est)
                    maker_edge = 1.0 - (buy_up_est + buy_down_est + mfee_up + mfee_down)
                else:
                    maker_edge = edge
                market["maker_edge"] = maker_edge
            else:
                gamma_sum = market.get("gamma_sum", 1.0)
                market["edge"] = max(0, 1.0 - gamma_sum - 0.015)
                market["ask_sum"] = gamma_sum
                market["maker_edge"] = market["edge"]

    # V15-4: Edge map shows both taker and maker edge
    def _print_edge_map(self, all_markets, tradeable_markets):
        if not all_markets:
            return
        self.logger.info("\n  EDGE MAP ({} discovered, {} tradeable):".format(
            len(all_markets), len(tradeable_markets)))
        sorted_m = sorted(all_markets, key=lambda m: m.get("maker_edge", 0), reverse=True)
        for m in sorted_m[:12]:
            edge = m.get("edge", 0)
            maker_edge = m.get("maker_edge", edge)
            ask_sum = m.get("ask_sum", m.get("gamma_sum", 1.0))
            time_left = m.get("time_left", 0)
            wid = m["window_id"]
            is_active = wid in self.engine.known_windows
            if maker_edge >= 0.05:
                tier = "***"
            elif maker_edge >= 0.03:
                tier = "** "
            elif maker_edge >= self.config.pair_min_profit:
                tier = "*  "
            else:
                tier = "   "
            active_tag = " [ACTIVE]" if is_active else ""
            self.logger.info(
                "    {} {:30s} | {:>5.0f}s | taker: {:>5.1%} | maker: {:>5.1%} | sum: {:.3f}{}".format(
                    tier, wid[:30], time_left, edge, maker_edge, ask_sum, active_tag))
        premium = sum(1 for m in all_markets if m.get("maker_edge", 0) >= 0.05)
        good = sum(1 for m in all_markets if 0.03 <= m.get("maker_edge", 0) < 0.05)
        marginal = sum(1 for m in all_markets
                       if self.config.pair_min_profit <= m.get("maker_edge", 0) < 0.03)
        below = sum(1 for m in all_markets if m.get("maker_edge", 0) < self.config.pair_min_profit)
        self.logger.info("    Tiers: ***={} premium  **={} good  *={} marginal  {}=below min".format(
            premium, good, marginal, below))

    def _print_summary(self, label=""):
        stats = self.engine.get_stats()
        sep = "=" * 70
        self.logger.info("\n" + sep)
        self.logger.info("  SUMMARY " + label)
        self.logger.info(sep)
        self.logger.info("  Active Orders:    {}".format(stats["active_orders"]))
        self.logger.info("  Total Placed:     {}".format(stats["total_placed"]))
        self.logger.info("  Active Windows:   {}".format(stats["windows_active"]))
        self.logger.info("  Exposure:         ${:.2f}".format(stats["total_exposure"]))
        self.logger.info("  Capital in Pos:   ${:.2f}".format(stats["capital_in_positions"]))
        self.logger.info("  Available Cap:    ${:.2f}".format(stats["available_capital"]))
        self.logger.info("  Session Spent:    ${:.2f}".format(stats["session_spent"]))
        self.logger.info("  Hedges:           {} completed / {} skipped".format(
            stats["hedges_completed"], stats["hedges_skipped"]))
        self.logger.info("  Paired Windows:   {}".format(stats["paired_windows"]))
        merge_stats = self.auto_merger.get_stats()
        self.logger.info("  Merges:           {} ok / {} fail | ${:.2f} returned".format(
            merge_stats["merges_completed"], merge_stats["merges_failed"],
            merge_stats["total_merged_usd"]))
        wallet_delta = stats.get("wallet_delta")
        if wallet_delta is not None:
            tag = "PROFIT" if wallet_delta >= 0 else "LOSS"
            self.logger.info("  [{}] Wallet \u0394:    ${:+,.2f}".format(tag, wallet_delta))
            held_val = stats.get("held_value", 0)
            total_est = stats.get("total_pnl_est")
            if total_est is not None:
                self.logger.info("  Est Total P&L: ${:+,.2f} (W\u0394 + ${:.0f} deployed + ${:.0f} held)".format(
                    total_est, stats.get("capital_deployed", 0), held_val))
            self.logger.info("  Held Windows:  {}".format(stats.get("held_windows", 0)))
        self.logger.info("  -- Strategies --")
        self.logger.info("  MM: {}  Sniper: {}  Arb: {}  Contrarian: {}".format(
            stats["mm_trades"], stats["sniper_trades"],
            stats["arb_trades"], stats["contrarian_trades"]))
        cs = self.claim_manager.get_claim_stats()
        self.logger.info("  -- Claims --")
        self.logger.info("  Claimed: {} (${:.2f}) | Pending: {} | Blind: {}/{}".format(
            cs["claimed_total"], cs["total_claimed_usd"], cs["pending_claims"],
            cs.get("blind_successes", 0), cs.get("blind_attempts", 0)))
        if self.sim_engine:
            s = self.sim_engine.get_summary()
            tag = "PROFIT" if s["realized_pnl"] >= 0 else "LOSS"
            self.logger.info("  -- Sim P&L --")
            self.logger.info("  [{}] ${:+,.2f} ({:+.2f}%) | WR: {:.0%} ({}W/{}L)".format(
                tag, s["realized_pnl"], s["pnl_pct"],
                s["win_rate"], s["total_wins"], s["total_losses"]))
        self.logger.info(sep)

    def _print_v15_1_summary(self):
        sep = "-" * 50
        self.logger.info("\n" + sep)
        self.logger.info("  V15.1 COMPONENT STATS")
        self.logger.info(sep)
        ro_stats = self.reward_optimizer.get_stats()
        self.logger.info("  Reward Optimizer: {} calcs, {} fallbacks".format(
            ro_stats["calculations"], ro_stats["fallbacks"]))
        vol_stats = self.vol_tracker.get_all_stats()
        if vol_stats:
            for key, info in list(vol_stats.items())[:8]:
                asset_tag = " [{}]".format(info.get("asset", "?"))
                vol_val = info.get("vol_sum")
                vol_str = "{:.4f}".format(vol_val) if vol_val is not None else "N/A"
                self.logger.info("  Vol {}{}: {} ({})".format(
                    key, asset_tag, info["level"], vol_str))
        churn_stats = self.churn_manager.get_stats()
        self.logger.info("  Churn: {} suppressed, {} allowed ({:.0f}% reduction)".format(
            churn_stats["suppressed"], churn_stats["allowed"], churn_stats["reduction_pct"]))
        mm_stats = self.mm_strategy.get_reward_stats()
        self.logger.info("  Est. Maker Rewards: ${:.4f}".format(
            mm_stats["total_estimated_reward"]))
        disc_stats = self.market_discovery.get_cache_stats()
        self.logger.info("  Discovery: {} scans | Cache: +{} -{} entries".format(
            disc_stats["discoveries"], disc_stats["pos_cache"], disc_stats["neg_cache"]))
        merge_stats = self.auto_merger.get_stats()
        self.logger.info("  Merger: {} completed, {} failed, ${:.2f} returned".format(
            merge_stats["merges_completed"], merge_stats["merges_failed"],
            merge_stats["total_merged_usd"]))
        self.logger.info("  Pair Sizing: {} | Min Taker Edge: {:.1%} | MM Min Profit: {:.1%} | Hedge: ${:.3f}".format(
            "EQUAL_SHARES" if self.config.pair_sizing_equal_shares else "EQUAL_DOLLARS",
            self.config.min_pair_edge, self.config.pair_min_profit,
            self.config.hedge_max_loss_per_share))
        self.logger.info("  Circuit Breaker: {:.1%} | Claim Timeout: {:.0f}s | Blind Redeem: {}".format(
            self.config.vol_circuit_breaker, self.config.claim_timeout_seconds,
            "ON" if self.config.blind_redeem_enabled else "OFF"))
        self.logger.info("  Max Exposure: ${:.0f} | Max/Market: ${:.0f} | Horizon: {:.0f}s".format(
            self.config.max_total_exposure, self.config.max_position_per_market,
            self.config.max_order_horizon))
        self.logger.info(sep)

    def _print_claim_summary(self):
        cs = self.claim_manager.get_claim_stats()
        pending = self.engine.expired_windows_pending_claim
        if not cs["claim_results"] and not pending:
            return
        self.logger.info("\n  -- CLAIM DETAILS --")
        for r in cs["claim_results"]:
            self.logger.info("  OK  | {} | {} | ${:.2f}".format(
                r["window_id"], r["method"], r["est_amount"]))
        for wid, info in pending.items():
            self.logger.info("  PENDING | {} | tokens: {} | cost: ${:.2f}".format(
                wid, len(info.get("tokens", [])), info.get("fill_cost", 0)))

    def _resolve_expired_windows(self, active_markets):
        if not self.sim_engine:
            return
        active_ids = {m["window_id"] for m in active_markets}
        for wid in list(self.engine.known_windows):
            if wid in active_ids or wid in self.sim_engine.resolved_windows:
                continue
            parts = wid.split("-")
            if len(parts) < 3:
                continue
            asset = parts[0]
            end_price = None
            if self.price_feed.use_chainlink:
                end_price = self.price_feed.chainlink.get_price(asset)
            if not end_price:
                end_price = self.price_feed.get_current_price(asset)
            start_price = self.sim_engine.window_start_prices.get(wid)
            if end_price and start_price:
                self.sim_engine.resolve_window(wid, asset, end_price, start_price)
                if self.kelly.enabled:
                    self.kelly.update_bankroll(self.sim_engine.current_bankroll)

    def _schedule_live_claims(self):
        for wid in list(self.engine.expired_windows_pending_claim.keys()):
            info = self.engine.expired_windows_pending_claim[wid]
            condition_id = info.get("condition_id", "") or self.window_conditions.get(wid, "")
            if not condition_id:
                if time.time() - info.get("expired_at", 0) > 120:
                    del self.engine.expired_windows_pending_claim[wid]
                continue
            end_ts = info.get("end_time", 0)
            if isinstance(end_ts, float):
                end_ts = int(end_ts)
            self.claim_manager.schedule_claim(
                condition_id=condition_id, window_id=wid, end_time=end_ts,
                slug=info.get("slug", ""), tokens=info.get("tokens", []),
                token_up=info.get("token_up", ""), token_down=info.get("token_down", ""))
            del self.engine.expired_windows_pending_claim[wid]

    # V15-6 + V15.1-6: Score/sort uses maker_edge for priority
    def _score_and_sort_markets(self, markets):
        """V15.1-19: Enhanced scoring with fill-probability factors.

        Score = edge * vol_penalty * fill_prob_bonus
        fill_prob_bonus considers:
        - Spread symmetry (UP vs DN spread similarity)
        - Momentum (lower = better for pair fill)
        - Time remaining (more time = more chance to fill both sides)
        """
        if not markets:
            return markets
        for market in markets:
            token_up = market["token_up"]
            token_dn = market.get("token_down", "")
            vol_level, vol_sum = self.vol_tracker.get_volatility_level(token_up)
            edge = market.get("maker_edge", market.get("edge", 0))
            vol_penalty = 1.0
            if vol_level == "EXTREME":
                vol_penalty = 0.1
            elif vol_level == "HIGH":
                vol_penalty = 0.5
            elif vol_level == "MEDIUM":
                vol_penalty = 0.8

            # Fill probability bonus (1.0 = neutral, >1 = better fill chance)
            fill_prob = 1.0

            # Factor 1: Spread symmetry — similar spreads = balanced book
            spread_up = self.book_reader.get_spread(token_up)
            spread_dn = self.book_reader.get_spread(token_dn) if token_dn else None
            if spread_up and spread_dn:
                sp_delta = abs(spread_up["spread"] - spread_dn["spread"])
                if sp_delta < 0.005:
                    fill_prob *= 1.15  # Very symmetric — bonus
                elif sp_delta < 0.01:
                    fill_prob *= 1.05
                elif sp_delta > 0.03:
                    fill_prob *= 0.85  # Asymmetric — penalty

            # Factor 2: Momentum — low momentum = balanced fills
            asset = market.get("asset", "")
            mom = self.price_feed.get_momentum(asset, self.config.tf_lookback_minutes)
            if mom is not None:
                abs_mom = abs(mom)
                if abs_mom < 0.001:
                    fill_prob *= 1.10  # Very calm — bonus
                elif abs_mom > 0.005:
                    fill_prob *= 0.90  # Trending — penalty

            # Factor 3: Time remaining — more time = more fill opportunity
            time_left = market.get("time_left", 0)
            if time_left > 600:
                fill_prob *= 1.05  # >10 min left
            elif time_left < 120:
                fill_prob *= 0.90  # <2 min left — risky

            market["_sort_score"] = edge * vol_penalty * fill_prob
        markets.sort(key=lambda m: m.get("_sort_score", 0), reverse=True)
        return markets

    def run(self):
        mode = "DRY RUN" if self.config.dry_run else "LIVE TRADING"
        self.logger.info("=" * 70)
        self.logger.info("  POLYMARKET CRYPTO TRADING BOT v15.1")
        self.logger.info("  Pair Integrity Edition")
        self.logger.info("  Mode: {} | Price: {} | Assets: BTC, ETH, SOL, XRP".format(
            mode, self.price_feed.get_price_source()))
        pk = self.config.private_key
        pw = self.config.proxy_wallet
        self.logger.info("  Key: {} | Proxy: {}".format(
            "{}...{} ({}ch)".format(pk[:6], pk[-4:], len(pk)) if len(pk) > 10 else "MISSING",
            "{}...{}".format(pw[:8], pw[-4:]) if len(pw) > 12 else "MISSING"))
        self.logger.info("  V15.1: Pair integrity | MM min: {:.1%} | Arb min: {:.1%}".format(
            self.config.pair_min_profit, self.config.min_pair_edge))
        self.logger.info("  Merge: {} | Pair-IMM: {} | Blind Redeem: {} | "
                         "Hedge: ${:.3f} | CB: {:.1%}".format(
            "ON" if self.config.auto_merge_enabled else "OFF",
            "ON" if self.config.immediate_pair_completion else "OFF",
            "ON" if self.config.blind_redeem_enabled else "OFF",
            self.config.hedge_max_loss_per_share,
            self.config.vol_circuit_breaker))
        self.logger.info("  Equal shares: {} | Scan ahead: {} windows | Horizon: {:.0f}s".format(
            "ON" if self.config.pair_sizing_equal_shares else "OFF",
            self.config.scan_windows_ahead,
            self.config.max_order_horizon))
        self.logger.info("  MM: reward-optimized | Pair validation: {} | Churn reduction: {}".format(
            "ON" if self.config.pair_validation_enabled else "OFF",
            "ON" if self.config.churn_enabled else "OFF"))
        rpc_name = ("polygon-rpc.com (WARNING: rate limited)"
                     if "polygon-rpc.com" in self.config.polygon_rpc
                     else self.config.polygon_rpc[:40])
        self.logger.info("  RPC: {}".format(rpc_name))
        if "polygon-rpc.com" in self.config.polygon_rpc:
            self.logger.warning("  !! Free RPC will rate-limit. Set POLYGON_RPC_URL to Alchemy/Infura.")
        self.logger.info("=" * 70)

        # V15.1-5: Bankroll auto-detect with more retries
        if self.config.auto_detect_bankroll and self.balance_checker and not self.config.dry_run:
            wallet_bal = None
            for _attempt in range(5):
                self.balance_checker._cache_time = 0  # force fresh read
                wallet_bal = self.balance_checker.get_balance()
                if wallet_bal is not None:
                    break
                self.logger.info("  Bankroll detect attempt {} failed, retrying...".format(_attempt + 1))
                time.sleep(3)
            if wallet_bal is not None and wallet_bal > 0:
                old_bankroll = self.config.kelly_bankroll
                self.config.kelly_bankroll = wallet_bal
                self.engine.starting_wallet_balance = wallet_bal
                self.logger.info("  Bankroll: ${:.2f} (was ${:.2f})".format(
                    wallet_bal, old_bankroll))
            else:
                self.logger.warning("  Bankroll auto-detect FAILED. Using KELLY_BANKROLL=${:.0f}".format(
                    self.config.kelly_bankroll))

        # V15.1-1: ALWAYS scale exposure with bankroll (whether auto-detect succeeded or not)
        self.config.max_total_exposure = self.config.kelly_bankroll * 0.80
        self.config.max_position_per_market = min(
            self.config.max_position_per_market,
            self.config.max_total_exposure * 0.45)
        self.logger.info("  Exposure limits: max_total=${:.0f} | max_per_market=${:.0f} | bankroll=${:.0f}".format(
            self.config.max_total_exposure, self.config.max_position_per_market,
            self.config.kelly_bankroll))

        self.running = True
        cycle = 0

        while self.running:
            cycle += 1
            try:
                self.engine.check_daily_reset()
                self.engine.sync_exchange_balance()
                self.engine.reset_cycle_counters()
                stats = self.engine.get_stats()

                wallet_str = ""
                pnl_str = ""
                if self.balance_checker and not self.config.dry_run:
                    bal = self.balance_checker.get_balance()
                    if bal is not None:
                        wallet_str = " | W:${:.0f}".format(bal)
                    # V15.1-20: Show wallet delta as primary P&L (hard fact)
                    wallet_delta = stats.get("wallet_delta")
                    if wallet_delta is not None:
                        pnl_str = " | W\u0394:${:+.2f}".format(wallet_delta)
                        held_val = stats.get("held_value", 0)
                        if held_val > 0:
                            pnl_str += " +${:.0f}held".format(held_val)

                cs = self.claim_manager.get_claim_stats()
                claim_str = ""
                if cs["pending_claims"] > 0 or cs["claimed_total"] > 0:
                    claim_str = " | Cl:{}ok/{}p".format(cs["claimed_total"], cs["pending_claims"])

                hedge_str = ""
                if stats["hedges_completed"] > 0 or stats["hedges_skipped"] > 0:
                    hedge_str = " | H:{}/{}".format(
                        stats["hedges_completed"], stats["hedges_skipped"])

                merge_stats = self.auto_merger.get_stats()
                merge_str = ""
                if merge_stats["merges_completed"] > 0:
                    merge_str = " | M:{}/${:.0f}".format(
                        merge_stats["merges_completed"], merge_stats["total_merged_usd"])

                churn_str = ""
                if cycle % 10 == 0:
                    cs2 = self.churn_manager.get_stats()
                    if cs2["suppressed"] > 0:
                        churn_str = " | Churn:-{:.0f}%".format(cs2["reduction_pct"])

                held_str = ""
                held_count = stats.get("held_windows", 0)
                if held_count > 0:
                    held_str = " | Held:{}".format(held_count)

                self.logger.info(
                    "\n{}\n  C{} | {} | Ord:{} | Exp:${:.0f} | Avail:${:.0f} | MaxExp:${:.0f}"
                    "{}{}{}{}{}{}{}".format(
                        "_" * 60, cycle,
                        datetime.now(timezone.utc).strftime("%H:%M:%S"),
                        stats["active_orders"], stats["total_exposure"],
                        stats["available_capital"],
                        self.config.max_total_exposure,
                        wallet_str, pnl_str, claim_str, hedge_str,
                        merge_str, churn_str, held_str,
                        "_" * 60))

                self.price_feed.update()
                all_assets = list(set(self.config.assets_15m + self.config.assets_5m))
                for asset in all_assets:
                    price = self.price_feed.get_current_price(asset)
                    if price:
                        self.vol_tracker.update_price(asset, price)

                self.book_reader.invalidate_cache()
                markets = self.market_discovery.discover()

                for market in markets:
                    cid = market.get("condition_id", "")
                    if cid:
                        self.window_conditions[market["window_id"]] = cid
                        self.vol_tracker.register_condition(cid, market["asset"])
                    self.vol_tracker.register_token(market["token_up"], market["asset"])
                    self.vol_tracker.register_token(market["token_down"], market["asset"])
                    self.engine.register_window_metadata(market)

                self._compute_market_edges(markets)
                self._resolve_expired_windows(markets)
                self.engine.cleanup_expired_windows(markets, self.churn_manager)
                self.engine.prune_stale_orders()
                self.engine.purge_recently_cancelled()

                if not self.config.dry_run:
                    self.engine.reconcile_capital_from_wallet()
                    self._schedule_live_claims()
                    claimed = self.claim_manager.process_claims()
                    if claimed > 0:
                        self.logger.info("  Auto-claimed {} positions".format(claimed))
                        if self.balance_checker:
                            self.balance_checker._cache_time = 0
                    exits = self.claim_manager.execute_pre_exits(
                        markets, self.price_feed, self.book_reader)
                    if exits > 0:
                        self.logger.info("  Pre-exit: {} sells placed".format(exits))
                    live_fills = self.engine.check_fills()
                    if live_fills:
                        self.logger.info("  {} orders filled".format(live_fills))
                        for wid in self.engine.window_fill_sides:
                            self.churn_manager.force_allow(wid)
                        imm_completed = self._process_immediate_pair_completions()
                        if imm_completed:
                            self.logger.info("  {} immediate pair completions".format(imm_completed))
                    hedges = self.engine.process_hedge_completions(
                        self.book_reader, self.vol_tracker)
                    if hedges:
                        self.logger.info("  {} hedges completed".format(hedges))
                    merged = self.auto_merger.check_and_merge_all(
                        self.engine._market_cache, self.engine.token_holdings)
                    if merged:
                        self.logger.info("  Auto-merged {} positions | ${:.2f} returned".format(
                            merged, self.auto_merger.total_merged_usd))
                        if self.balance_checker:
                            self.balance_checker._cache_time = 0

                for market in markets:
                    self.engine._is_up_token_cache[market["token_up"]] = True
                    self.engine._is_up_token_cache[market["token_down"]] = False
                    if self.sim_engine:
                        price = self.price_feed.get_current_price(market["asset"])
                        if price:
                            self.sim_engine.record_window_start_price(
                                market["window_id"], market["asset"], price)

                if self.sim_engine:
                    fills = self.sim_engine.simulate_fills(self.book_reader, markets)
                    if fills > 0:
                        self.logger.info("  Simulated {} fills".format(fills))
                    merged = self.auto_merger.check_and_merge_all(
                        self.engine._market_cache, self.engine.token_holdings)
                    if merged:
                        self.logger.info("  Sim-merged {} positions".format(merged))

                if cycle % 5 == 0:
                    self.merge_detector.check_merges(
                        self.engine.token_holdings, self.engine._market_cache)

                trading_halted = False
                now = time.time()
                if now < self._loss_stop_until:
                    if cycle % 10 == 1:
                        self.logger.info("  LOSS COOLOFF -- {}s remaining".format(
                            int(self._loss_stop_until - now)))
                    trading_halted = True
                elif self.sim_engine:
                    s = self.sim_engine.get_summary()
                    loss_limit = -self.config.hard_loss_stop_pct * self.config.kelly_bankroll
                    if s["realized_pnl"] < loss_limit:
                        self._loss_stop_until = now + self.config.hard_loss_cooloff
                        trading_halted = True
                elif not self.config.dry_run and self.engine.starting_wallet_balance:
                    # V15.1-20: LIVE-MODE LOSS STOP using wallet delta
                    # The sim_engine check above only works in dry run.
                    # In live mode, use actual wallet balance change as the loss metric.
                    wallet_now = None
                    if self.balance_checker:
                        wallet_now = self.balance_checker.get_balance()
                    if wallet_now is not None:
                        wallet_delta = wallet_now - self.engine.starting_wallet_balance
                        loss_limit = -self.config.hard_loss_stop_pct * self.config.kelly_bankroll
                        if wallet_delta < loss_limit:
                            self._loss_stop_until = now + self.config.hard_loss_cooloff
                            self.logger.warning(
                                "  LIVE LOSS STOP | W\u0394: ${:.2f} < limit ${:.2f} "
                                "({}% of ${:.0f} bankroll) | Halting for {}s".format(
                                    wallet_delta, loss_limit,
                                    self.config.hard_loss_stop_pct * 100,
                                    self.config.kelly_bankroll,
                                    self.config.hard_loss_cooloff))
                            trading_halted = True
                        elif cycle % 10 == 1:
                            # Periodic wallet delta status (every 10 cycles)
                            self.logger.info(
                                "  LOSS MONITOR | W\u0394: ${:+.2f} / limit ${:.2f}".format(
                                    wallet_delta, loss_limit))

                # V15.1-19: Session blackout windows
                if self.config.trading_blackout_windows and not trading_halted:
                    import datetime
                    utc_now = datetime.datetime.utcnow()
                    current_hour_min = utc_now.hour + utc_now.minute / 60.0
                    for bw in self.config.trading_blackout_windows:
                        if len(bw) == 2:
                            start_h, end_h = float(bw[0]), float(bw[1])
                            if start_h <= current_hour_min < end_h:
                                if cycle % 10 == 1:
                                    self.logger.info(
                                        "  SESSION BLACKOUT | {:.2f}-{:.2f} UTC | "
                                        "Current: {:.2f} | MM paused".format(
                                            start_h, end_h, current_hour_min))
                                trading_halted = True
                                break

                # V15-2 + V15.1-6 + V15.1-10: Dual-path tradeable filter
                # with condition_id deduplication
                tradeable_markets = []
                # V15.1-20: Include held_windows in active count.
                # held_windows persists even after cleanup_expired_windows (unlike filled_windows).
                # This prevents unlimited window accumulation when merge/claim is broken.
                active_window_ids = (set(self.engine.window_exposure.keys())
                                     | self.engine.filled_windows
                                     | self.engine.held_windows)
                # V15.1-10: Track condition_ids with active exposure
                active_condition_ids = set()
                for awid in active_window_ids:
                    meta = self.engine._window_metadata.get(awid, {})
                    cid = meta.get("condition_id", "")
                    if cid:
                        active_condition_ids.add(cid)
                for market in markets:
                    edge = market.get("edge", 0)
                    maker_edge = market.get("maker_edge", edge)

                    # Market passes if EITHER taker edge >= min_pair_edge (arb viable)
                    # OR maker_edge >= pair_min_profit (MM viable).
                    if (edge < self.config.min_pair_edge
                            and maker_edge < self.config.pair_min_profit):
                        if market["window_id"] not in active_window_ids:
                            continue

                    if market.get("is_advance", False) and not self.config.trade_advance_windows:
                        continue
                    # V15.1-6: Skip windows too far out unless already active
                    if market.get("time_left", 0) > self.config.max_order_horizon:
                        if market["window_id"] not in active_window_ids:
                            continue
                    if len(active_window_ids) >= self.config.max_concurrent_windows:
                        if market["window_id"] not in active_window_ids:
                            continue
                    # V15.1-10: Condition_id dedup
                    mkt_cid = market.get("condition_id", "")
                    if mkt_cid and mkt_cid in active_condition_ids:
                        if market["window_id"] not in active_window_ids:
                            continue
                    tradeable_markets.append(market)

                tradeable_markets = self._score_and_sort_markets(tradeable_markets)

                if cycle % self.config.edge_map_interval == 1:
                    self._print_edge_map(markets, tradeable_markets)

                for market in tradeable_markets:
                    try:
                        if trading_halted:
                            continue
                        # V15.1-7/20: Dynamic max_concurrent_windows enforcement
                        # Include held_windows so on-chain positions count toward the limit
                        current_active = (set(self.engine.window_exposure.keys())
                                          | self.engine.filled_windows
                                          | self.engine.held_windows)
                        wid = market["window_id"]
                        if (len(current_active) >= self.config.max_concurrent_windows
                                and wid not in current_active):
                            continue
                        # V15.1-10: Dynamic condition_id dedup
                        cur_cids = set()
                        for awid in current_active:
                            meta = self.engine._window_metadata.get(awid, {})
                            c = meta.get("condition_id", "")
                            if c:
                                cur_cids.add(c)
                        mkt_cid = market.get("condition_id", "")
                        if mkt_cid and mkt_cid in cur_cids and wid not in current_active:
                            continue
                        self.mm_strategy.execute(market)
                        self.sniper.execute(market)
                        self.arb.execute(market)
                        self.contrarian.execute(market)
                    except Exception as e:
                        self.logger.error("  Strategy error on {}: {}".format(
                            market["slug"], e))

                if self.sim_engine:
                    s = self.sim_engine.get_summary()
                    self.logger.info(
                        "  Sim: ${:,.2f} | P&L: ${:+,.2f} | Fills: {} | Open: {}".format(
                            s["current_bankroll"], s["realized_pnl"],
                            s["total_fills"], s["open_positions"]))

                if cycle % self.config.summary_interval == 0:
                    self._print_summary("(Cycle {})".format(cycle))
                    self._print_v15_1_summary()

            except Exception as e:
                self.logger.error("  Cycle error: {}".format(e))

            time.sleep(self.config.cycle_interval)


# -----------------------------------------------------------------
# Entry Point
# -----------------------------------------------------------------

if __name__ == "__main__":
    bot = PolymarketBot()
    bot.run()