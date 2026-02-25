"""
PolyMaker Command Center Client
================================
Connects the trading bot to the PolyMaker Command Center for:
- Real-time trade logging and monitoring
- Run lifecycle management (start/stop/update)
- Config fetching from the Command Center
- Market reporting for the Markets page watchlist

This module is designed to be non-blocking and fault-tolerant:
- All API calls are wrapped in try/except so they never crash the bot
- Failed calls are logged but silently ignored
- The bot continues trading even if the Command Center is unreachable

Usage:
    from polymaker_client import cc  # cc = Command Center client (singleton)
    
    # Start a run when bot starts
    run_id = cc.start_run(config_id=1, mode="live", bankroll=500.0)
    
    # Log trades as they happen
    trade_id = cc.log_trade(run_id=run_id, cycle=1, asset="BTC", ...)
    
    # Update metrics periodically
    cc.update_run(run_id=run_id, total_pnl=12.5, total_cycles=100, ...)
    
    # Stop run when bot shuts down
    cc.stop_run(run_id=run_id, status="completed", total_pnl=12.5, ...)
"""

import os
import time
import logging
import requests
from typing import Optional, Dict, Any, List
from datetime import datetime

logger = logging.getLogger(__name__)


class PolyMakerClient:
    """Client for the PolyMaker Command Center REST API."""

    def __init__(self):
        """
        Initialize the client from environment variables.
        
        Required env vars:
            POLYMAKER_URL: Base URL of the Command Center (e.g., https://your-app.manus.space)
            POLYMAKER_API_KEY: API key for authentication (set in Command Center Settings > Secrets)
        
        Optional env vars:
            POLYMAKER_ENABLED: Set to 'false' to disable all API calls (default: 'true')
            POLYMAKER_TIMEOUT: Request timeout in seconds (default: 10)
        """
        self.base_url = os.getenv("POLYMAKER_URL", "").rstrip("/")
        self.api_key = os.getenv("POLYMAKER_API_KEY", "")
        self.enabled = os.getenv("POLYMAKER_ENABLED", "true").lower() == "true"
        self.timeout = int(os.getenv("POLYMAKER_TIMEOUT", "10"))
        
        # Current run tracking
        self.current_run_id: Optional[int] = None
        self.run_start_time: Optional[float] = None
        self.cycle_count: int = 0
        self.total_orders: int = 0
        self.total_fills: int = 0
        self.win_count: int = 0
        self.loss_count: int = 0
        
        # Metrics accumulator (updated periodically)
        self._pending_metrics: Dict[str, Any] = {}
        self._last_metrics_push: float = 0
        self._metrics_push_interval: float = 30.0  # Push metrics every 30 seconds
        
        if self.enabled and self.base_url and self.api_key:
            logger.info(f"✓ PolyMaker Command Center client initialized: {self.base_url}")
        elif self.enabled and (not self.base_url or not self.api_key):
            logger.warning(
                "⚠️  PolyMaker Command Center: POLYMAKER_URL or POLYMAKER_API_KEY not set. "
                "Trade logging to Command Center is disabled. "
                "Set these in your .env file to enable."
            )
            self.enabled = False
        else:
            logger.info("PolyMaker Command Center client disabled (POLYMAKER_ENABLED=false)")

    def _is_ready(self) -> bool:
        """Check if the client is configured and enabled."""
        return self.enabled and bool(self.base_url) and bool(self.api_key)

    def _headers(self) -> Dict[str, str]:
        """Build request headers with API key authentication."""
        return {
            "Content-Type": "application/json",
            "X-Api-Key": self.api_key,
        }

    def _get(self, path: str) -> Optional[Dict[str, Any]]:
        """Make a GET request to the Command Center. Returns None on failure."""
        if not self._is_ready():
            return None
        try:
            url = f"{self.base_url}/api/bot{path}"
            resp = requests.get(url, headers=self._headers(), timeout=self.timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            logger.warning(f"⚠️  Command Center timeout: GET {path}")
            return None
        except requests.exceptions.ConnectionError:
            logger.warning(f"⚠️  Command Center unreachable: GET {path}")
            return None
        except Exception as e:
            logger.warning(f"⚠️  Command Center error: GET {path} - {e}")
            return None

    def _post(self, path: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Make a POST request to the Command Center. Returns None on failure."""
        if not self._is_ready():
            return None
        try:
            url = f"{self.base_url}/api/bot{path}"
            resp = requests.post(url, json=data, headers=self._headers(), timeout=self.timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            logger.warning(f"⚠️  Command Center timeout: POST {path}")
            return None
        except requests.exceptions.ConnectionError:
            logger.warning(f"⚠️  Command Center unreachable: POST {path}")
            return None
        except Exception as e:
            logger.warning(f"⚠️  Command Center error: POST {path} - {e}")
            return None

    # ========== Status & Config ==========

    def check_status(self) -> Optional[Dict[str, Any]]:
        """Check Command Center health and get active config/run info."""
        return self._get("/status")

    def get_config(self) -> Optional[Dict[str, Any]]:
        """
        Fetch the active bot configuration from the Command Center.
        
        Returns the config dict with fields like:
            kellyBankroll, mmOrderSize, mmSpread, targetAssets, etc.
        
        Returns None if no active config or Command Center is unreachable.
        """
        return self._get("/config")

    # ========== Run Lifecycle ==========

    def start_run(
        self,
        config_id: int,
        mode: str = "dry_run",
        bankroll: float = 0,
    ) -> Optional[int]:
        """
        Start a new bot run session in the Command Center.
        
        Args:
            config_id: ID of the config being used (from get_config())
            mode: "dry_run" or "live"
            bankroll: Starting bankroll in USDC
            
        Returns:
            The run ID (int) on success, None on failure.
        """
        result = self._post("/run/start", {
            "configId": config_id,
            "mode": mode,
            "startingBankroll": bankroll,
        })
        if result and "runId" in result:
            self.current_run_id = result["runId"]
            self.run_start_time = time.time()
            self.cycle_count = 0
            self.total_orders = 0
            self.total_fills = 0
            self.win_count = 0
            self.loss_count = 0
            self._pending_metrics = {}
            logger.info(f"✓ Command Center run started: #{self.current_run_id} ({mode})")
            return self.current_run_id
        return None

    def update_run(self, run_id: Optional[int] = None, **metrics) -> bool:
        """
        Update run metrics in the Command Center.
        
        Call this periodically (e.g., every 30-60 seconds) with accumulated metrics.
        
        Supported kwargs (use snake_case):
            total_cycles, total_orders, total_fills, total_pnl,
            peak_pnl, max_drawdown, win_count, loss_count,
            avg_spread, avg_fill_rate, ending_bankroll, max_capital
        
        Returns True on success, False on failure.
        """
        rid = run_id or self.current_run_id
        if not rid:
            return False

        # Convert snake_case to camelCase for the API
        key_map = {
            "total_cycles": "totalCycles",
            "total_orders": "totalOrders",
            "total_fills": "totalFills",
            "total_pnl": "totalPnl",
            "peak_pnl": "peakPnl",
            "max_drawdown": "maxDrawdown",
            "win_count": "winCount",
            "loss_count": "lossCount",
            "avg_spread": "avgSpreadCaptured",
            "avg_fill_rate": "avgFillRate",
            "ending_bankroll": "endingBankroll",
            "max_capital": "maxCapitalDeployed",
             "wallet_balance": "walletBalance",
            "starting_wallet": "startingWallet",
            "merges_completed": "mergesCompleted",
            "total_merged_usd": "totalMergedUsd",
            "claims_completed": "claimsCompleted",
            "total_claimed_usd": "totalClaimedUsd",
            "claims_pending": "claimsPending",
            "position_value": "positionValue",
            "hedge_analytics": "hedgeAnalytics",
        }
        data: Dict[str, Any] = {"runId": rid}
        for k, v in metrics.items():
            api_key = key_map.get(k, k)
            data[api_key] = v
        result = self._post("/run/update", data)
        return result is not None

    def stop_run(
        self,
        run_id: Optional[int] = None,
        status: str = "completed",
        error_message: Optional[str] = None,
        **final_metrics,
    ) -> Optional[Dict[str, Any]]:
        """
        Stop a run. This auto-triggers analysis and owner notification in the Command Center.
        
        Args:
            run_id: Run ID to stop (defaults to current_run_id)
            status: "completed", "stopped", or "error"
            error_message: Error message if status is "error"
            **final_metrics: Final metric values (same kwargs as update_run)
            
        Returns:
            Analysis results dict on success, None on failure.
        """
        rid = run_id or self.current_run_id
        if not rid:
            return None

        key_map = {
            "total_cycles": "totalCycles",
            "total_orders": "totalOrders",
            "total_fills": "totalFills",
            "total_pnl": "totalPnl",
            "peak_pnl": "peakPnl",
            "max_drawdown": "maxDrawdown",
            "win_count": "winCount",
            "loss_count": "lossCount",
            "avg_spread": "avgSpreadCaptured",
            "avg_fill_rate": "avgFillRate",
            "ending_bankroll": "endingBankroll",
            "max_capital": "maxCapitalDeployed",
            "wallet_balance": "walletBalance",
            "starting_wallet": "startingWallet",
            "merges_completed": "mergesCompleted",
            "total_merged_usd": "totalMergedUsd",
            "claims_completed": "claimsCompleted",
            "total_claimed_usd": "totalClaimedUsd",
            "claims_pending": "claimsPending",
            "hedge_analytics": "hedgeAnalytics",
        }
        data: Dict[str, Any] = {"runId": rid, "status": status}
        if error_message:
            data["errorMessage"] = error_message
        for k, v in final_metrics.items():
            data[key_map.get(k, k)] = v

        result = self._post("/run/stop", data)
        if result:
            logger.info(f"✓ Command Center run #{rid} stopped ({status})")
            if result.get("analysis"):
                score = result["analysis"].get("overallScore", "N/A")
                logger.info(f"  Analysis score: {score}/100")
        
        self.current_run_id = None
        self.run_start_time = None
        return result

    # ========== Trade Logging ==========

    def log_trade(
        self,
        run_id: Optional[int] = None,
        cycle: int = 0,
        asset: str = "",
        window: str = "15m",
        side: str = "buy",
        outcome: str = "",
        price: float = 0,
        size: float = 0,
        condition_id: Optional[str] = None,
        token_id: Optional[str] = None,
        strategy: str = "MM",
    ) -> Optional[int]:
        """
        Log a new trade order to the Command Center.
        
        Args:
            run_id: Run ID (defaults to current_run_id)
            cycle: Current cycle number
            asset: Asset name (e.g., "BTC", "ETH")
            window: Window duration (e.g., "5m", "15m")
            side: "buy" or "sell"
            outcome: Market outcome (e.g., "Up", "Down")
            price: Order price
            size: Order size in USDC
            condition_id: Polymarket condition ID
            token_id: Polymarket token ID
            strategy: Trading strategy name
            
        Returns:
            Trade ID on success, None on failure.
        """
        rid = run_id or self.current_run_id
        if not rid:
            return None

        data = {
            "runId": rid,
            "cycle": cycle,
            "asset": asset,
            "windowDuration": window,
            "side": side,
            "outcome": outcome,
            "price": price,
            "size": size,
            "strategy": strategy,
        }
        if condition_id:
            data["conditionId"] = condition_id
        if token_id:
            data["tokenId"] = str(token_id)

        result = self._post("/trade", data)
        if result and "tradeId" in result:
            self.total_orders += 1
            return result["tradeId"]
        return None

    def update_trade(self, trade_id: int, **updates) -> bool:
        """
        Update a trade's status or fill information.
        
        Supported kwargs:
            status: "placed", "filled", "cancelled", "failed"
            fill_price: Actual fill price
            fill_size: Actual fill size
            fill_timestamp: Fill timestamp (ISO string or datetime)
            resolved_outcome: Final outcome after market resolution
            pnl: Profit/loss for this trade
            
        Returns True on success, False on failure.
        """
        key_map = {
            "fill_price": "fillPrice",
            "fill_size": "fillSize",
            "fill_timestamp": "fillTimestamp",
            "resolved_outcome": "resolvedOutcome",
        }

        data: Dict[str, Any] = {"tradeId": trade_id}
        for k, v in updates.items():
            api_key = key_map.get(k, k)
            if isinstance(v, datetime):
                v = v.isoformat()
            data[api_key] = v

        result = self._post("/trade/update", data)
        if result and updates.get("status") == "filled":
            self.total_fills += 1
        return result is not None

    def log_trades_batch(self, trades: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Log multiple trades at once.
        
        Args:
            trades: List of trade dicts with the same fields as log_trade()
            
        Returns:
            Response dict with tradeIds on success, None on failure.
        """
        return self._post("/trade/batch", {"trades": trades})

    # ========== Market Reporting ==========

    def report_markets(self, markets: List[Dict[str, str]]) -> bool:
        """
        Report active markets to the Command Center for the Markets page watchlist.
        
        Args:
            markets: List of dicts with keys: tokenId, asset, window
            
        Returns True on success, False on failure.
        """
        result = self._post("/markets/report", {"markets": markets})
        return result is not None

    # ========== Convenience Methods ==========

    def increment_cycle(self):
        """Increment the cycle counter. Call this at the start of each trading cycle."""
        self.cycle_count += 1

    def record_win(self):
        """Record a winning trade."""
        self.win_count += 1

    def record_loss(self):
        """Record a losing trade."""
        self.loss_count += 1

    def push_metrics_if_due(
        self,
        total_pnl: float = 0,
        ending_bankroll: float = 0,
        max_capital: float = 0,
        avg_spread: float = 0,
        avg_fill_rate: float = 0,
    ):
        """
        Push accumulated metrics to the Command Center if enough time has elapsed.
        
        Call this in the periodic update loop (e.g., every 10 seconds).
        Metrics are only actually sent every 30 seconds to avoid API spam.
        """
        now = time.time()
        if now - self._last_metrics_push < self._metrics_push_interval:
            return
        
        if not self.current_run_id:
            return

        self._last_metrics_push = now
        self.update_run(
            total_cycles=self.cycle_count,
            total_orders=self.total_orders,
            total_fills=self.total_fills,
            total_pnl=total_pnl,
            win_count=self.win_count,
            loss_count=self.loss_count,
            ending_bankroll=ending_bankroll,
            max_capital=max_capital,
            avg_spread=avg_spread,
            avg_fill_rate=avg_fill_rate,
        )

    def get_run_duration_str(self) -> str:
        """Get a human-readable string of how long the current run has been active."""
        if not self.run_start_time:
            return "0s"
        elapsed = time.time() - self.run_start_time
        hours = int(elapsed // 3600)
        minutes = int((elapsed % 3600) // 60)
        seconds = int(elapsed % 60)
        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        return f"{seconds}s"


# ========== Singleton Instance ==========
# Import this in other modules: `from polymaker_client import cc`
cc = PolyMakerClient()
