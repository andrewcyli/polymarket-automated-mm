"""
V16: Smart Exit Engine — Replaces tier-based hedge/sell decisions with a
continuous scoring model that combines order book signals, CEX momentum,
and time pressure to make optimal exit decisions.

Architecture:
    AskVelocityTracker  → tracks opposing ask price over time per window
    ExitScorer          → combines 4 signals into a single exit score [0, 1]
    SmartExitEngine     → orchestrates tracking + scoring + decision-making

The engine plugs into AutoMerger._check_hedge_completion() as an alternative
to the T1/T2/T3/T4 tier waterfall. When exitMode="smart", the engine is
called instead of the tier logic. When exitMode="tiers", the existing
tier logic runs unchanged.

Signals:
    1. Ask Level (40%)   — Higher opposing ask → higher exit urgency
    2. Ask Velocity (20%) — Rising ask → sell now; falling → wait
    3. Time Pressure (20%) — Less time remaining → higher urgency
    4. CEX Momentum (20%) — Binance price moving against held position → sell

Decision:
    score >= sell_threshold  → SELL held token immediately (SMART-SELL)
    score <  sell_threshold  → HEDGE (buy opposing side, like T1/T2)
    ask > immediate_sell_threshold → SELL immediately (bypass scoring)
"""

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple, Any

logger = logging.getLogger("smart_exit")


@dataclass
class AskSnapshot:
    """A single opposing ask observation."""
    timestamp: float
    ask_price: float
    bid_price: float = 0.0  # Held token's bid (for sell value estimation)


class AskVelocityTracker:
    """
    Tracks opposing ask price over time for each pending hedge window.
    Computes velocity (rate of change) and acceleration.
    
    Thread-safe: each window has its own deque, accessed only from the
    main bot thread.
    """
    
    def __init__(self, max_history: int = 120, velocity_window: float = 15.0):
        """
        Args:
            max_history: Max observations to keep per window
            velocity_window: Seconds over which to compute velocity
        """
        self.max_history = max_history
        self.velocity_window = velocity_window
        self._history: Dict[str, deque] = {}  # window_id -> deque of AskSnapshot
    
    def record(self, window_id: str, ask_price: float, bid_price: float = 0.0):
        """Record an opposing ask observation for a window."""
        if window_id not in self._history:
            self._history[window_id] = deque(maxlen=self.max_history)
        
        self._history[window_id].append(AskSnapshot(
            timestamp=time.time(),
            ask_price=ask_price,
            bid_price=bid_price,
        ))
    
    def get_velocity(self, window_id: str) -> float:
        """
        Get the ask velocity (price change per second) for a window.
        
        Positive = ask is rising (bad — opposing side getting more expensive)
        Negative = ask is falling (good — opposing side getting cheaper)
        Zero = no change or insufficient data
        """
        history = self._history.get(window_id)
        if not history or len(history) < 2:
            return 0.0
        
        now = time.time()
        cutoff = now - self.velocity_window
        
        # Find oldest observation within velocity window
        old_snap = None
        for snap in history:
            if snap.timestamp >= cutoff:
                old_snap = snap
                break
        
        if old_snap is None:
            # All observations are older than velocity window — use most recent two
            old_snap = history[-2]
        
        latest = history[-1]
        dt = latest.timestamp - old_snap.timestamp
        if dt < 0.5:  # Need at least 0.5s between observations
            return 0.0
        
        return (latest.ask_price - old_snap.ask_price) / dt
    
    def get_ask_trend(self, window_id: str) -> Dict:
        """
        Get comprehensive ask trend data for a window.
        
        Returns:
            dict with velocity, acceleration, min/max/avg ask, observation count
        """
        history = self._history.get(window_id)
        if not history:
            return {
                "velocity": 0.0,
                "acceleration": 0.0,
                "current_ask": 0.0,
                "min_ask": 0.0,
                "max_ask": 0.0,
                "avg_ask": 0.0,
                "observations": 0,
                "trend": "unknown",
            }
        
        now = time.time()
        cutoff = now - self.velocity_window
        recent = [s for s in history if s.timestamp >= cutoff]
        if not recent:
            recent = list(history)[-5:]  # Use last 5 if all old
        
        asks = [s.ask_price for s in recent]
        current_ask = history[-1].ask_price
        velocity = self.get_velocity(window_id)
        
        # Compute acceleration (change in velocity)
        acceleration = 0.0
        if len(recent) >= 3:
            mid_idx = len(recent) // 2
            first_half = recent[:mid_idx]
            second_half = recent[mid_idx:]
            if first_half and second_half:
                v1_dt = first_half[-1].timestamp - first_half[0].timestamp
                v2_dt = second_half[-1].timestamp - second_half[0].timestamp
                if v1_dt > 0.5 and v2_dt > 0.5:
                    v1 = (first_half[-1].ask_price - first_half[0].ask_price) / v1_dt
                    v2 = (second_half[-1].ask_price - second_half[0].ask_price) / v2_dt
                    total_dt = (second_half[-1].timestamp - first_half[0].timestamp)
                    if total_dt > 0:
                        acceleration = (v2 - v1) / total_dt
        
        # Classify trend
        if velocity > 0.001:
            trend = "rising_fast"
        elif velocity > 0.0002:
            trend = "rising"
        elif velocity < -0.001:
            trend = "falling_fast"
        elif velocity < -0.0002:
            trend = "falling"
        else:
            trend = "stable"
        
        return {
            "velocity": velocity,
            "acceleration": acceleration,
            "current_ask": current_ask,
            "min_ask": min(asks),
            "max_ask": max(asks),
            "avg_ask": sum(asks) / len(asks),
            "observations": len(recent),
            "trend": trend,
        }
    
    def remove_window(self, window_id: str):
        """Clean up tracking data for a resolved window."""
        self._history.pop(window_id, None)
    
    def get_tracked_windows(self) -> List[str]:
        """Get list of currently tracked window IDs."""
        return list(self._history.keys())
    
    def cleanup_stale(self, max_age: float = 600.0):
        """Remove windows with no recent observations."""
        now = time.time()
        stale = []
        for wid, history in self._history.items():
            if not history or (now - history[-1].timestamp) > max_age:
                stale.append(wid)
        for wid in stale:
            del self._history[wid]


class ExitScorer:
    """
    Computes a continuous exit score [0, 1] from four signals.
    
    Higher score = more urgency to exit (sell held token).
    Lower score = safe to hedge (buy opposing side).
    
    Weights are configurable via BotConfig / CC settings.
    """
    
    def __init__(
        self,
        ask_weight: float = 0.40,
        velocity_weight: float = 0.20,
        time_weight: float = 0.20,
        cex_weight: float = 0.20,
    ):
        self.ask_weight = ask_weight
        self.velocity_weight = velocity_weight
        self.time_weight = time_weight
        self.cex_weight = cex_weight
        
        # Normalize weights
        total = self.ask_weight + self.velocity_weight + self.time_weight + self.cex_weight
        if total > 0 and abs(total - 1.0) > 0.01:
            self.ask_weight /= total
            self.velocity_weight /= total
            self.time_weight /= total
            self.cex_weight /= total
    
    def compute_score(
        self,
        opposing_ask: float,
        ask_velocity: float,
        pct_remaining: float,
        cex_momentum: float,
        filled_side: str,
        window_duration: float = 300.0,
    ) -> Dict:
        """
        Compute exit score from the four signals.
        
        Args:
            opposing_ask: Current ask price of the opposing side (0-1)
            ask_velocity: Rate of change of opposing ask ($/sec)
            pct_remaining: % of window time remaining (0-100)
            cex_momentum: CEX price momentum (% change, signed)
            filled_side: "UP" or "DOWN" — which side the bot holds
            window_duration: Total window duration in seconds
            
        Returns:
            dict with score, component scores, and decision recommendation
        """
        # ── Signal 1: Ask Level Score (0-1) ──
        # Maps opposing ask from [0.45, 0.95] to [0, 1]
        # At ask=0.45, score=0 (cheap to hedge)
        # At ask=0.95, score=1 (too expensive, sell instead)
        ask_score = max(0.0, min(1.0, (opposing_ask - 0.45) / 0.50))
        
        # ── Signal 2: Ask Velocity Score (0-1) ──
        # Positive velocity (rising ask) → high score (sell urgency)
        # Negative velocity (falling ask) → low score (wait/hedge)
        # Velocity is in $/sec; typical range is [-0.005, +0.005]
        if ask_velocity > 0:
            velocity_score = min(1.0, ask_velocity / 0.003)  # Saturates at +0.3c/sec
        else:
            velocity_score = max(0.0, 0.3 + ask_velocity / 0.003)  # Falling ask → low score
        
        # ── Signal 3: Time Pressure Score (0-1) ──
        # More time remaining → low score (can wait)
        # Less time remaining → high score (must decide now)
        # Non-linear: urgency accelerates in the last 30%
        time_frac = 1.0 - (pct_remaining / 100.0)  # 0=start, 1=end
        if time_frac < 0.5:
            time_score = time_frac * 0.4  # Gentle ramp first half
        elif time_frac < 0.7:
            time_score = 0.2 + (time_frac - 0.5) * 1.5  # Steeper ramp
        else:
            time_score = 0.5 + (time_frac - 0.7) * 1.67  # Urgent ramp
        time_score = max(0.0, min(1.0, time_score))
        
        # ── Signal 4: CEX Momentum Score (0-1) ──
        # If holding UP and CEX price is falling → high score (sell UP)
        # If holding UP and CEX price is rising → low score (UP will win)
        # If holding DOWN and CEX price is rising → high score (sell DOWN)
        # If holding DOWN and CEX price is falling → low score (DOWN will win)
        if filled_side == "UP":
            # Holding UP tokens: CEX falling is bad for us
            directional_momentum = -cex_momentum  # Invert: falling CEX → positive score
        else:
            # Holding DOWN tokens: CEX rising is bad for us
            directional_momentum = cex_momentum  # Rising CEX → positive score
        
        # Map momentum from [-0.005, +0.005] to [0, 1]
        # Neutral at 0 → score 0.5
        cex_score = max(0.0, min(1.0, 0.5 + directional_momentum / 0.010))
        
        # ── Combined Score ──
        score = (
            self.ask_weight * ask_score +
            self.velocity_weight * velocity_score +
            self.time_weight * time_score +
            self.cex_weight * cex_score
        )
        score = max(0.0, min(1.0, score))
        
        return {
            "score": score,
            "components": {
                "ask_level": {"value": opposing_ask, "score": ask_score, "weight": self.ask_weight},
                "ask_velocity": {"value": ask_velocity, "score": velocity_score, "weight": self.velocity_weight},
                "time_pressure": {"value": pct_remaining, "score": time_score, "weight": self.time_weight},
                "cex_momentum": {"value": cex_momentum, "score": cex_score, "weight": self.cex_weight},
            },
        }


class SmartExitEngine:
    """
    Orchestrates the smart exit decision for pending hedge windows.
    
    Replaces the T1/T2/T3/T4 tier waterfall when exitMode="smart".
    
    Decision flow:
        1. Check immediate sell threshold (ask > 0.85 → sell now)
        2. Record ask observation for velocity tracking
        3. Compute exit score from 4 signals
        4. If score >= sell_threshold → SELL held token
        5. If score < sell_threshold → HEDGE (buy opposing side)
        6. If score is borderline and velocity is rising → SELL
        7. If no CEX data → use 3-signal model (weights redistributed)
    """
    
    def __init__(
        self,
        # Thresholds
        immediate_sell_ask: float = 0.85,    # Ask above this → sell immediately
        sell_score_threshold: float = 0.55,   # Score above this → sell
        hedge_score_threshold: float = 0.40,  # Score below this → hedge
        # Velocity tracker config
        velocity_window: float = 15.0,
        # Scorer weights
        ask_weight: float = 0.40,
        velocity_weight: float = 0.20,
        time_weight: float = 0.20,
        cex_weight: float = 0.20,
        # Binance feed reference (optional)
        binance_feed: Any = None,
        # Logging
        log_scores: bool = True,
    ):
        self.immediate_sell_ask = immediate_sell_ask
        self.sell_score_threshold = sell_score_threshold
        self.hedge_score_threshold = hedge_score_threshold
        self.binance_feed = binance_feed
        self.log_scores = log_scores
        
        self.velocity_tracker = AskVelocityTracker(
            velocity_window=velocity_window,
        )
        self.scorer = ExitScorer(
            ask_weight=ask_weight,
            velocity_weight=velocity_weight,
            time_weight=time_weight,
            cex_weight=cex_weight,
        )
        
        # Analytics
        self._decisions: Dict[str, List[Dict]] = {}  # window_id -> list of decisions
        self._stats = {
            "total_evaluations": 0,
            "immediate_sells": 0,
            "score_sells": 0,
            "score_hedges": 0,
            "score_waits": 0,
        }
    
    def evaluate(
        self,
        window_id: str,
        filled_side: str,
        filled_price: float,
        opposing_ask: float,
        opposing_bid: float,
        pct_remaining: float,
        window_duration: float = 300.0,
        asset: str = "",
    ) -> Dict:
        """
        Evaluate a pending hedge window and return a decision.
        
        Args:
            window_id: The window identifier
            filled_side: "UP" or "DOWN"
            filled_price: Price at which the held side was filled
            opposing_ask: Current ask price of the opposing side
            opposing_bid: Current bid of the held side (for sell estimation)
            pct_remaining: % of window time remaining
            window_duration: Total window duration in seconds
            asset: Asset name (e.g., "btc") for CEX lookup
            
        Returns:
            dict with:
                action: "SELL" | "HEDGE" | "WAIT"
                reason: Human-readable reason
                score: Exit score (0-1)
                components: Score breakdown
                sell_value: Estimated sell proceeds if selling
                hedge_cost: Estimated hedge cost if hedging
        """
        self._stats["total_evaluations"] += 1
        
        # Record observation for velocity tracking
        self.velocity_tracker.record(window_id, opposing_ask, opposing_bid)
        
        # ── Check 1: Immediate sell threshold ──
        if opposing_ask >= self.immediate_sell_ask:
            self._stats["immediate_sells"] += 1
            decision = {
                "action": "SELL",
                "reason": f"IMMEDIATE: ask ${opposing_ask:.3f} >= threshold ${self.immediate_sell_ask:.3f}",
                "score": 1.0,
                "components": {},
                "trigger": "immediate_threshold",
            }
            self._log_decision(window_id, decision)
            return decision
        
        # ── Get signals ──
        ask_velocity = self.velocity_tracker.get_velocity(window_id)
        ask_trend = self.velocity_tracker.get_ask_trend(window_id)
        
        # CEX momentum (0 if feed not available)
        cex_momentum = 0.0
        cex_available = False
        if self.binance_feed and asset:
            signal = self.binance_feed.get_directional_signal(asset, lookback=30)
            if signal.get("available", False):
                cex_momentum = signal["momentum"]
                cex_available = True
        
        # ── Compute score ──
        result = self.scorer.compute_score(
            opposing_ask=opposing_ask,
            ask_velocity=ask_velocity,
            pct_remaining=pct_remaining,
            cex_momentum=cex_momentum,
            filled_side=filled_side,
            window_duration=window_duration,
        )
        
        score = result["score"]
        
        # ── Make decision ──
        # Estimate economics
        fee_rate = 0.02  # Approximate fee
        sell_value = opposing_bid * (1 - fee_rate) if opposing_bid > 0 else 0
        hedge_cost = opposing_ask * (1 + fee_rate)
        pair_cost = filled_price + hedge_cost
        pair_profit = 1.0 - pair_cost
        
        if score >= self.sell_score_threshold:
            # High score → SELL
            self._stats["score_sells"] += 1
            action = "SELL"
            reason = (
                f"SCORE SELL: {score:.3f} >= {self.sell_score_threshold:.3f} | "
                f"ask=${opposing_ask:.3f} vel={ask_velocity:+.4f}/s "
                f"trend={ask_trend['trend']} "
                f"cex={'%.4f' % cex_momentum if cex_available else 'N/A'} "
                f"{pct_remaining:.0f}%rem"
            )
            trigger = "score_sell"
            
        elif score <= self.hedge_score_threshold:
            # Low score → HEDGE
            self._stats["score_hedges"] += 1
            action = "HEDGE"
            reason = (
                f"SCORE HEDGE: {score:.3f} <= {self.hedge_score_threshold:.3f} | "
                f"ask=${opposing_ask:.3f} vel={ask_velocity:+.4f}/s "
                f"pair=${pair_cost:.3f} profit=${pair_profit:+.3f}/sh"
            )
            trigger = "score_hedge"
            
        else:
            # Borderline → WAIT (check velocity tiebreaker)
            if ask_velocity > 0.0005 and opposing_ask > 0.65:
                # Ask rising and already elevated → lean SELL
                self._stats["score_sells"] += 1
                action = "SELL"
                reason = (
                    f"VELOCITY TIEBREAK SELL: score={score:.3f} borderline | "
                    f"ask=${opposing_ask:.3f} rising ({ask_velocity:+.4f}/s) "
                    f"trend={ask_trend['trend']}"
                )
                trigger = "velocity_tiebreak"
            else:
                # Wait for more data
                self._stats["score_waits"] += 1
                action = "WAIT"
                reason = (
                    f"WAIT: score={score:.3f} in dead zone "
                    f"[{self.hedge_score_threshold:.2f}, {self.sell_score_threshold:.2f}] | "
                    f"ask=${opposing_ask:.3f} vel={ask_velocity:+.4f}/s"
                )
                trigger = "wait"
        
        decision = {
            "action": action,
            "reason": reason,
            "score": score,
            "components": result["components"],
            "trigger": trigger,
            "sell_value": sell_value,
            "hedge_cost": hedge_cost,
            "pair_profit": pair_profit,
            "cex_available": cex_available,
            "ask_trend": ask_trend["trend"],
        }
        
        self._log_decision(window_id, decision)
        return decision
    
    def on_window_resolved(self, window_id: str):
        """Clean up tracking data when a window is resolved."""
        self.velocity_tracker.remove_window(window_id)
        # Keep decision history for analytics (pruned periodically)
    
    def get_stats(self) -> Dict:
        """Get engine statistics for monitoring."""
        return {
            **self._stats,
            "tracked_windows": len(self.velocity_tracker.get_tracked_windows()),
            "decision_history_size": sum(len(v) for v in self._decisions.values()),
        }
    
    def get_window_decisions(self, window_id: str) -> List[Dict]:
        """Get decision history for a specific window."""
        return self._decisions.get(window_id, [])
    
    def update_config(
        self,
        immediate_sell_ask: float = None,
        sell_score_threshold: float = None,
        hedge_score_threshold: float = None,
        ask_weight: float = None,
        velocity_weight: float = None,
        time_weight: float = None,
        cex_weight: float = None,
        velocity_window: float = None,
    ):
        """Hot-update configuration from CC config reload."""
        if immediate_sell_ask is not None:
            self.immediate_sell_ask = immediate_sell_ask
        if sell_score_threshold is not None:
            self.sell_score_threshold = sell_score_threshold
        if hedge_score_threshold is not None:
            self.hedge_score_threshold = hedge_score_threshold
        if velocity_window is not None:
            self.velocity_tracker.velocity_window = velocity_window
        
        # Update scorer weights
        weights_changed = False
        if ask_weight is not None:
            self.scorer.ask_weight = ask_weight
            weights_changed = True
        if velocity_weight is not None:
            self.scorer.velocity_weight = velocity_weight
            weights_changed = True
        if time_weight is not None:
            self.scorer.time_weight = time_weight
            weights_changed = True
        if cex_weight is not None:
            self.scorer.cex_weight = cex_weight
            weights_changed = True
        
        if weights_changed:
            # Re-normalize
            total = (self.scorer.ask_weight + self.scorer.velocity_weight +
                     self.scorer.time_weight + self.scorer.cex_weight)
            if total > 0:
                self.scorer.ask_weight /= total
                self.scorer.velocity_weight /= total
                self.scorer.time_weight /= total
                self.scorer.cex_weight /= total
    
    def _log_decision(self, window_id: str, decision: Dict):
        """Record decision for analytics."""
        if window_id not in self._decisions:
            self._decisions[window_id] = []
        self._decisions[window_id].append({
            "timestamp": time.time(),
            **decision,
        })
        
        # Prune old decisions (keep last 500 windows)
        if len(self._decisions) > 500:
            oldest_keys = sorted(self._decisions.keys())[:100]
            for k in oldest_keys:
                del self._decisions[k]
    
    def cleanup_stale(self, max_age: float = 600.0):
        """Periodic cleanup of stale tracking data."""
        self.velocity_tracker.cleanup_stale(max_age)
