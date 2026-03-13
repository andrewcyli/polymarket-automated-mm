"""Bot configuration dataclass with all trading parameters."""

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


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

    # V15.7: Portfolio-based P&L and bankroll tracking
    # portfolio_pnl = wallet_delta + held_position_value
    # Use portfolio P&L (not just realized) for loss stop decisions
    portfolio_loss_stop_enabled: bool = True
    portfolio_loss_stop_pct: float = 0.15     # Stop when portfolio P&L < -15% of bankroll
    portfolio_loss_cooloff: int = 900         # 15 min cooloff after portfolio loss stop

    stale_order_max_age: float = 120.0

    # V15.1-6: Don't place orders on windows too far out
    max_order_horizon: float = 1200.0  # 20 minutes (V15.2: reduced from 45min to enter ~5min before open for 15m)

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
    hedge_max_combined_cost: float = 0.98     # LEGACY (V15.1-13): kept for backward compat
    hedge_min_profit_per_share: float = -1.0   # V15.8: DISABLED — max_ask controls profitability

    # V15.8: hedge_tiers now use (pct, max_ask) format instead of (pct, max_combined_cost)
    # max_ask = the maximum opposing ask price the bot will pay to complete the hedge.
    # T4 uses min_bid = the minimum sell bid price to accept a last-resort sell.
    hedge_t4_sell_pct: float = 33.0      # Trigger when <33% remaining
    hedge_t4_min_bid: float = 0.15       # V15.8: Min bid to accept T4 sell ($0.15)
                                          # Replaces hedge_t4_max_loss — easier to reason about
                                          # "Sell if bid >= $0.15" vs "sell if loss <= $0.30"
    hedge_t4_max_loss: float = 0.30      # LEGACY: kept for backward compat, not used if min_bid set
    hedge_t4_enabled: bool = True        # Enable/disable T4 sell tier

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

    # V15.8: Post-WS-RECOVERED immediate assessment
    # When a cancelled order fills anyway (WS-RECOVERED), check the opposing
    # side's ask price. If ask > this threshold, skip hedge tiers entirely
    # and fast-track to momentum exit (backdate fill time so exit checker
    # evaluates immediately). Set to 0 to disable.
    ws_recovered_max_opposing_ask: float = 0.80

    # V15.6: VPIN toxicity filter — pause when order flow is one-sided
    vpin_enabled: bool = True
    vpin_lookback_secs: float = 60.0       # Window to measure buy/sell volume
    vpin_threshold: float = 0.70           # |buy_vol - sell_vol| / total_vol > this = toxic
    vpin_min_trades: int = 5               # Need at least N trades to compute VPIN
    vpin_spread_multiplier: float = 1.5    # Widen spread by this when VPIN is elevated (0.4-0.7)
    vpin_block_multiplier: float = 2.0     # Widen spread by this when VPIN > threshold (>0.7)

    # V15.8: VPIN volatility fallback — when trade data is insufficient for VPIN,
    # use price volatility as a proxy for informed trading activity
    vpin_vol_fallback_enabled: bool = True

    # V15.6: Dynamic spread — scale mm_base_spread with realized volatility
    dynamic_spread_enabled: bool = True
    dynamic_spread_vol_floor: float = 1.0   # Multiplier at LOW vol (no change)
    dynamic_spread_vol_medium: float = 1.3  # Multiplier at MEDIUM vol
    dynamic_spread_vol_high: float = 1.8    # Multiplier at HIGH vol
    dynamic_spread_vol_extreme: float = 2.5 # Multiplier at EXTREME vol (belt-and-suspenders with circuit breaker)

    # V15.6: Extended pre-exit — timeframe-aware exit timing
    pre_exit_time_5m: float = 60.0         # 60s before end for 5-min windows
    pre_exit_time_15m: float = 120.0       # 120s before end for 15-min windows

    # V15.7: Combined spread multiplier cap (VPIN × DynSpread)
    # Prevents over-widening when both VPIN and volatility are elevated
    spread_multiplier_cap: float = 3.0     # Max combined multiplier (vol × vpin)

    # V15.7: Graduated spread near window close (time-decay widening)
    # Instead of binary stop at min_time_remaining, gradually widen spread
    # as window approaches close to reduce late orphans.
    graduated_spread_enabled: bool = True
    graduated_spread_start_secs_5m: float = 120.0   # Start widening 120s before close (5m windows)
    graduated_spread_start_secs_15m: float = 240.0  # Start widening 240s before close (15m windows)
    graduated_spread_stop_secs: float = 30.0         # Stop quoting entirely at 30s
    graduated_spread_max_multiplier: float = 3.0     # Max spread multiplier at stop_secs boundary

    # V15.1-19: Pre-entry filters for orphan reduction
    momentum_gate_threshold: float = 0.010   # V15.5: Raised from 0.2% to 1.0% — tighter gate rejects
                                               # volatile windows. Data: at 0.5% gate, Kelly=+36%.
                                               # At 1.0%, pair rate ~93% (from 89.9%), fewer windows
                                               # but much higher quality. Reduces volume ~17%.
    momentum_gate_lookback: int = 5          # Lookback minutes for momentum gate
    momentum_gate_max_consec: int = 3         # After N consecutive blocks, relax threshold
    # V15.1-29 Strategy 4: Asset-specific momentum scaling factors
    # More volatile assets (SOL, XRP) get a higher multiplier so the effective
    # threshold is proportional to their typical volatility.
    momentum_gate_asset_scale: dict = field(default_factory=lambda: {
        "btc": 1.0,   # BTC is the baseline
        "eth": 1.2,   # ETH slightly more volatile
        "sol": 1.8,   # SOL significantly more volatile
        "xrp": 1.8,   # XRP significantly more volatile
    })
    min_book_depth: float = 5.0              # Min $ depth within 2c of target price
    max_spread_asymmetry: float = 0.02       # Max spread difference between UP/DN
    # V15.1-29: Midpoint directional filter — skip if UP midpoint deviates from 0.50
    midpoint_skew_limit: float = 0.03         # Skip if |midpoint - 0.50| > this (0.03 = skip at 0.47/0.53)
    # V15.1-19: Session blackout windows (list of [start_hour_utc, end_hour_utc] pairs)
    trading_blackout_windows: list = field(default_factory=list)

    # V15.9: Auto-pause on consecutive momentum gate blocks
    # When enabled, halts NEW window creation (but continues managing existing
    # windows: merges, hedges, claims, exits) after N consecutive gate blocks
    # across ALL assets. Resumes when M consecutive passes occur.
    auto_pause_enabled: bool = False
    auto_pause_gate_threshold: int = 10   # Consecutive blocks to trigger pause
    auto_pause_resume_threshold: int = 3  # Consecutive passes to resume

    # V16: Smart Exit Engine
    # Replaces T1/T2/T3/T4 tier waterfall with a continuous scoring model
    # that combines order book signals, CEX momentum, and time pressure.
    # When exit_mode="tiers", the existing tier logic runs unchanged.
    # When exit_mode="smart", the SmartExitEngine makes hedge/sell decisions.
    exit_mode: str = "tiers"                    # "tiers" or "smart"
    smart_exit_immediate_sell_ask: float = 0.85  # Ask above this → sell immediately
    smart_exit_sell_threshold: float = 0.55      # Score above this → sell
    smart_exit_hedge_threshold: float = 0.40     # Score below this → hedge
    smart_exit_velocity_window: float = 15.0     # Seconds for velocity computation
    smart_exit_ask_weight: float = 0.40          # Weight: opposing ask level
    smart_exit_velocity_weight: float = 0.20     # Weight: ask velocity
    smart_exit_time_weight: float = 0.20         # Weight: time pressure
    smart_exit_cex_weight: float = 0.20          # Weight: CEX momentum
    smart_exit_binance_enabled: bool = True       # Enable Binance WebSocket feed
    smart_exit_binance_history: int = 300         # Seconds of price history to keep

    rpc_gas_cache_ttl: float = 30.0
    rpc_min_call_interval: float = 0.5  # V15.1-27: Reduced from 1.5s (smart token selection means fewer calls)

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

