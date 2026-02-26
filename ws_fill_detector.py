"""
WebSocket-based fill detector for Polymarket automated market maker.

Bridges real-time fill events from the WebSocket user channel to the
TradingEngine's existing fill processing logic, replacing the REST-based
polling in check_fills().

Architecture:
    WS User Channel → EventBus (ORDER_FILL / USER_TRADE) → WSFillDetector → TradingEngine

The WSFillDetector subscribes to ORDER_FILL events from the EventBus and
queues them for the TradingEngine to process during its normal cycle.
This provides:
    - Sub-second fill detection (vs 10s+ REST polling)
    - Lower API usage (no more get_orders() every cycle)
    - Graceful fallback to REST when WS is unavailable

Usage:
    from ws_fill_detector import WSFillDetector

    detector = WSFillDetector(ws_manager, engine, logger)
    detector.start()

    # In the main loop:
    ws_fills = detector.check_fills_ws()  # Process WS-detected fills
    if ws_fills == 0 and not detector.is_healthy():
        # Fallback to REST polling
        rest_fills = engine.check_fills()
"""

import time
import threading
import logging
from collections import deque
from typing import Optional, Dict, Any, List

try:
    from ws_manager import WebSocketManager, EventType, Channel
    HAS_WS = True
except ImportError:
    HAS_WS = False


class WSFillDetector:
    """
    Bridges WebSocket fill events to the TradingEngine.

    Subscribes to ORDER_FILL and USER_TRADE events from the WebSocket
    EventBus and translates them into the same data format that
    TradingEngine.check_fills() expects.

    Thread-safe: WS events arrive on the background WS thread,
    while check_fills_ws() is called from the main bot thread.
    """

    def __init__(self, ws_manager, engine, logger: Optional[logging.Logger] = None):
        """
        Args:
            ws_manager: WebSocketManager instance (from ws_manager.py)
            engine: TradingEngine instance (from trading_bot_v15.py)
            logger: Logger instance (uses module logger if not provided)
        """
        self.ws_manager = ws_manager
        self.engine = engine
        self.logger = logger or logging.getLogger(__name__)

        # Thread-safe queue for incoming fill events from WS
        self._fill_queue: deque = deque()
        self._lock = threading.Lock()

        # Track processed fills to avoid duplicates
        self._processed_order_ids: set = set()
        # Limit the set size to prevent unbounded growth
        self._max_processed_ids = 10000

        # Phase 4: WS order lifecycle tracking
        # Track orders confirmed by exchange via PLACEMENT events
        self._ws_confirmed_orders: set = set()
        # Track orders cancelled by exchange via CANCELLATION events
        self._ws_cancelled_orders: Dict[str, dict] = {}  # order_id -> cancel data
        # Periodic REST reconciliation counter
        self._cycle_count = 0
        self._rest_reconcile_interval = 10  # Run REST reconciliation every N cycles
        self._last_rest_reconcile: float = 0
        # Track partial fills from cancellation events
        self._partial_fill_queue: deque = deque()

        # Stats
        self._ws_fills_total = 0
        self._ws_fills_session = 0
        self._rest_fallback_count = 0
        self._last_ws_fill_time: float = 0
        self._ws_cancellations_detected = 0
        self._ws_placements_confirmed = 0
        self._started = False

        # Health tracking
        self._last_health_check: float = 0
        self._health_check_interval = 30.0  # seconds

    def start(self):
        """
        Subscribe to fill events from the WebSocket EventBus.
        Must be called after ws_manager.start().
        """
        if not HAS_WS or not self.ws_manager:
            self.logger.warning("WSFillDetector: WebSocket not available — running in REST-only mode")
            return

        # Subscribe to ORDER_FILL events (emitted for each maker order in a trade)
        self.ws_manager.event_bus.subscribe(EventType.ORDER_FILL, self._on_order_fill)

        # Subscribe to USER_TRADE events (emitted for each trade match)
        self.ws_manager.event_bus.subscribe(EventType.USER_TRADE, self._on_user_trade)

        # Subscribe to ORDER_UPDATE events (for cancellation/placement tracking)
        self.ws_manager.event_bus.subscribe(EventType.ORDER_UPDATE, self._on_order_update)

        self._started = True
        self.logger.info("WSFillDetector: started — listening for real-time fills")

    def stop(self):
        """Unsubscribe from events."""
        if not HAS_WS or not self.ws_manager:
            return

        self.ws_manager.event_bus.unsubscribe(EventType.ORDER_FILL, self._on_order_fill)
        self.ws_manager.event_bus.unsubscribe(EventType.USER_TRADE, self._on_user_trade)
        self.ws_manager.event_bus.unsubscribe(EventType.ORDER_UPDATE, self._on_order_update)
        self._started = False
        self.logger.info("WSFillDetector: stopped")

    # ── Event Handlers (called from WS background thread) ──

    def _on_order_fill(self, event_type, fill_data: dict):
        """
        Handle ORDER_FILL event from the EventBus.

        ORDER_FILL is emitted for each maker order that was matched in a trade.
        This is the primary fill detection mechanism — it tells us which specific
        order was filled, at what price, and for how many shares.

        Expected fill_data format (from ws_manager._handle_user_message):
        {
            "order_id": "abc123",
            "price": 0.47,
            "size": 31.2,
            "asset_id": "token_id_hex",
            "outcome": "Yes" or "No",
            "status": "MATCHED"
        }
        """
        order_id = fill_data.get("order_id", "")
        if not order_id:
            return

        # Deduplicate: skip if we've already processed this order fill
        with self._lock:
            if order_id in self._processed_order_ids:
                return

            # Check if this order is one of ours (in active_orders or recently cancelled)
            is_active = order_id in self.engine.active_orders
            is_recently_cancelled = (
                not is_active and order_id in self.engine._recently_cancelled
            )
            if not is_active and not is_recently_cancelled:
                # Not our order — could be a taker order or already processed
                self.logger.debug(
                    f"WSFillDetector: ORDER_FILL for unknown order {order_id[:12]}... — skipping"
                )
                return

            if is_recently_cancelled:
                self.logger.info(
                    f"WSFillDetector: ORDER_FILL for recently-cancelled order "
                    f"{order_id[:12]}... — recovering fill"
                )

            # Queue the fill for processing
            self._fill_queue.append({
                "type": "order_fill",
                "order_id": order_id,
                "price": fill_data.get("price", 0),
                "size": fill_data.get("size", 0),
                "asset_id": fill_data.get("asset_id", ""),
                "status": fill_data.get("status", ""),
                "ts": time.time(),
                "recovered": is_recently_cancelled,
            })

    def _on_user_trade(self, event_type, trade_data: dict):
        """
        Handle USER_TRADE event from the EventBus.

        USER_TRADE is emitted for each trade match (taker perspective).
        We use this as a secondary signal — if we're the taker (e.g., hedge orders),
        this tells us the trade was matched.

        Expected trade_data format:
        {
            "taker_order_id": "abc123",
            "price": 0.47,
            "size": 31.2,
            "side": "BUY",
            "asset_id": "token_id_hex",
            "market": "condition_id_hex",
            "status": "MATCHED",
            "maker_orders": [...]
        }
        """
        taker_order_id = trade_data.get("taker_order_id", "")
        if not taker_order_id:
            return

        with self._lock:
            if taker_order_id in self._processed_order_ids:
                return

            # Check if the taker order is one of ours (active or recently cancelled)
            is_active = taker_order_id in self.engine.active_orders
            is_recently_cancelled = (
                not is_active and taker_order_id in self.engine._recently_cancelled
            )
            if not is_active and not is_recently_cancelled:
                return

            if is_recently_cancelled:
                self.logger.info(
                    f"WSFillDetector: USER_TRADE for recently-cancelled order "
                    f"{taker_order_id[:12]}... — recovering fill"
                )

            # Queue the fill
            self._fill_queue.append({
                "type": "user_trade",
                "order_id": taker_order_id,
                "price": float(trade_data.get("price", 0)),
                "size": float(trade_data.get("size", 0)),
                "side": trade_data.get("side", ""),
                "asset_id": trade_data.get("asset_id", ""),
                "market": trade_data.get("market", ""),
                "status": trade_data.get("status", ""),
                "ts": time.time(),
                "recovered": is_recently_cancelled,
            })

    def _on_order_update(self, event_type, update_data: dict):
        """
        Handle ORDER_UPDATE event from the EventBus.

        Phase 4: Full order lifecycle tracking.
        - PLACEMENT: Exchange acknowledged our order — track confirmation
        - CANCELLATION: Exchange confirmed cancel — check for partial fills
          and remove from active_orders
        - Trade status updates (MATCHED, MINED, CONFIRMED, FAILED): logged
        """
        evt = update_data.get("event_type", "")

        if evt == "order":
            order_type = update_data.get("type", "")
            order_id = update_data.get("order_id", "")
            if not order_id:
                return

            if order_type == "PLACEMENT":
                # Exchange confirmed our order placement
                with self._lock:
                    self._ws_confirmed_orders.add(order_id)
                    self._ws_placements_confirmed += 1
                self.logger.debug(
                    f"WSFillDetector: Order {order_id[:12]}... confirmed on exchange"
                )

            elif order_type == "CANCELLATION":
                size_matched = float(update_data.get("size_matched", "0") or "0")
                self._ws_cancellations_detected += 1

                with self._lock:
                    # Remove from confirmed set
                    self._ws_confirmed_orders.discard(order_id)

                    # Store cancellation data
                    self._ws_cancelled_orders[order_id] = {
                        "size_matched": size_matched,
                        "ts": time.time(),
                    }

                # If there was a partial fill (size_matched > 0), queue it
                if size_matched > 0:
                    info = self.engine.active_orders.get(order_id)
                    if not info:
                        info = self.engine._recently_cancelled.get(order_id)
                    if info:
                        original_size = info.get("size", 0)
                        if size_matched < original_size:
                            # Partial fill — queue the filled portion
                            with self._lock:
                                self._partial_fill_queue.append({
                                    "type": "partial_cancel",
                                    "order_id": order_id,
                                    "price": info.get("price", 0),
                                    "size": size_matched,
                                    "asset_id": info.get("token_id", ""),
                                    "side": info.get("side", "BUY"),
                                    "window_id": info.get("window_id", ""),
                                    "ts": time.time(),
                                })
                            self.logger.info(
                                f"  PARTIAL FILL via CANCEL | Order {order_id[:12]}... "
                                f"| {size_matched:.1f}/{original_size:.1f} matched"
                            )

                # Move from active_orders to _recently_cancelled so that
                # a late ORDER_FILL event can still recover the fill.
                # (V15.1-21: Fix race condition where CANCELLATION event
                #  arrived before ORDER_FILL, causing lost fills)
                cancelled_info = self.engine.active_orders.pop(order_id, None)
                if cancelled_info:
                    wid = cancelled_info.get("window_id", "")
                    # Preserve in _recently_cancelled for fill recovery
                    self.engine._recently_cancelled[order_id] = {
                        **cancelled_info, "cancelled_at": time.time()}
                    if wid in self.engine.orders_by_window:
                        self.engine.orders_by_window[wid] = [
                            o for o in self.engine.orders_by_window[wid] if o != order_id
                        ]
                    self.logger.debug(
                        f"WSFillDetector: Order {order_id[:12]}... cancelled — "
                        f"moved to _recently_cancelled | {wid}"
                    )

        elif evt == "trade":
            # Trade lifecycle update (MATCHED already handled by ORDER_FILL)
            status = update_data.get("status", "")
            if status in ("FAILED", "RETRYING"):
                taker_id = update_data.get("taker_order_id", "")
                self.logger.warning(
                    f"WSFillDetector: Trade {status} for order {taker_id[:12]}..."
                )

    # ── Main Thread Fill Processing ──

    def check_fills_ws(self) -> int:
        """
        Process queued WS fill events and apply them to the TradingEngine.

        This method is called from the main bot thread during each cycle,
        replacing the REST-based check_fills() when WS is healthy.

        Returns:
            Number of fills processed.
        """
        if not self._started:
            return 0

        filled = 0

        with self._lock:
            # Drain the queue
            pending = list(self._fill_queue)
            self._fill_queue.clear()

        for fill in pending:
            order_id = fill.get("order_id", "")
            if not order_id:
                continue

            # Skip if already processed (double-check after draining)
            if order_id in self._processed_order_ids:
                continue

            # Look up the order in the engine's active_orders
            info = self.engine.active_orders.get(order_id)
            recovered = False
            if not info:
                # Check the recently-cancelled buffer — the order may have
                # filled on Polymarket before our cancel took effect.
                info = self.engine._recently_cancelled.pop(order_id, None)
                if info:
                    recovered = True
                    # V15.4-FIX: Remove from _bot_cancelled_orders since WS confirmed fill
                    if hasattr(self.engine, '_bot_cancelled_orders'):
                        self.engine._bot_cancelled_orders.discard(order_id)
                    self.logger.info(
                        f"  FILL [WS-RECOVERED] | Order {order_id[:12]}... "
                        f"filled after cancel — recovering from buffer"
                    )
                else:
                    # Truly unknown — already processed by REST or not ours
                    self.logger.debug(
                        f"WSFillDetector: Order {order_id[:12]}... not in active_orders — "
                        f"may have been processed by REST fallback"
                    )
                    continue

            # Process the fill using the same logic as check_fills()
            wid = info.get("window_id", "")

            # Remove from active_orders (no-op for recovered fills)
            self.engine.active_orders.pop(order_id, None)

            # Remove from orders_by_window
            if wid in self.engine.orders_by_window:
                self.engine.orders_by_window[wid] = [
                    o for o in self.engine.orders_by_window[wid] if o != order_id
                ]

            # Use fill data from WS if available, otherwise use order info
            fill_price = fill.get("price") or info.get("price", 0)
            fill_size = fill.get("size") or info.get("size", 0)
            fill_side = fill.get("side") or info.get("side", "BUY")
            fill_token = fill.get("asset_id") or info.get("token_id", "")

            # Record the fill in the engine
            self.engine.record_fill(fill_token, fill_side, fill_price, fill_size)

            if fill_side == "BUY":
                cost = fill_price * fill_size
                self.engine.window_fill_cost[wid] = (
                    self.engine.window_fill_cost.get(wid, 0) + cost
                )
                # Mark window as filled — prevents re-entry
                self.engine.filled_windows.add(wid)
                self.engine.window_entry_count[wid] = (
                    self.engine.window_entry_count.get(wid, 0) + 1
                )

                if wid not in self.engine.window_fill_tokens:
                    self.engine.window_fill_tokens[wid] = []
                self.engine.window_fill_tokens[wid].append({
                    "token_id": fill_token,
                    "size": fill_size,
                    "price": fill_price,
                    "is_up": self.engine._is_up_token_cache.get(fill_token),
                    "time": time.time(),
                })

                is_up = self.engine._is_up_token_cache.get(fill_token)
                side_label = "UP" if is_up else "DOWN"
                if wid not in self.engine.window_fill_sides:
                    self.engine.window_fill_sides[wid] = {}
                if side_label not in self.engine.window_fill_sides[wid]:
                    self.engine.window_fill_sides[wid][side_label] = []
                self.engine.window_fill_sides[wid][side_label].append({
                    "token_id": fill_token,
                    "price": fill_price,
                    "size": fill_size,
                    "time": time.time(),
                })

                sides = self.engine.window_fill_sides.get(wid, {})
                if "UP" in sides and "DOWN" in sides:
                    self.engine.paired_windows.add(wid)
                elif self.engine.config.hedge_completion_enabled:
                    # V15.4-FIX: Dedup — only add hedge if window not already pending
                    already_pending = any(
                        h["window_id"] == wid and h["filled_side"] == side_label
                        for h in self.engine._pending_hedges
                    )
                    if not already_pending:
                        self.engine._pending_hedges.append({
                            "window_id": wid,
                            "filled_side": side_label,
                            "filled_price": fill_price,
                            "filled_size": fill_size,
                            "filled_token": fill_token,
                            "time": time.time(),
                        })

            # Mark as processed
            self._processed_order_ids.add(order_id)
            self._trim_processed_ids()

            filled += 1
            self._ws_fills_total += 1
            self._ws_fills_session += 1
            self._last_ws_fill_time = time.time()

            latency_ms = (time.time() - fill.get("ts", time.time())) * 1000
            tag = "WS-RECOVERED" if recovered else "WS"
            self.logger.info(
                "  FILL [{}] | {} {} {:.1f} @ ${:.2f} | {} | {:.0f}ms".format(
                    tag, fill_side, fill_token[:12] + "...",
                    fill_size, fill_price, wid, latency_ms
                )
            )

        # Phase 4: Also process partial fills from cancellation events
        with self._lock:
            partial_pending = list(self._partial_fill_queue)
            self._partial_fill_queue.clear()

        for pfill in partial_pending:
            order_id = pfill.get("order_id", "")
            if not order_id or order_id in self._processed_order_ids:
                continue

            # For partial fills, use the data we captured from the cancel event
            fill_price = pfill.get("price", 0)
            fill_size = pfill.get("size", 0)
            fill_side = pfill.get("side", "BUY")
            fill_token = pfill.get("asset_id", "")
            wid = pfill.get("window_id", "")

            if fill_size <= 0:
                continue

            # Record the partial fill
            self.engine.record_fill(fill_token, fill_side, fill_price, fill_size)

            if fill_side == "BUY":
                cost = fill_price * fill_size
                self.engine.window_fill_cost[wid] = (
                    self.engine.window_fill_cost.get(wid, 0) + cost
                )
                self.engine.filled_windows.add(wid)
                self.engine.window_entry_count[wid] = (
                    self.engine.window_entry_count.get(wid, 0) + 1
                )

                if wid not in self.engine.window_fill_tokens:
                    self.engine.window_fill_tokens[wid] = []
                self.engine.window_fill_tokens[wid].append({
                    "token_id": fill_token,
                    "size": fill_size,
                    "price": fill_price,
                    "is_up": self.engine._is_up_token_cache.get(fill_token),
                    "time": time.time(),
                })

                is_up = self.engine._is_up_token_cache.get(fill_token)
                side_label = "UP" if is_up else "DOWN"
                if wid not in self.engine.window_fill_sides:
                    self.engine.window_fill_sides[wid] = {}
                if side_label not in self.engine.window_fill_sides[wid]:
                    self.engine.window_fill_sides[wid][side_label] = []
                self.engine.window_fill_sides[wid][side_label].append({
                    "token_id": fill_token,
                    "price": fill_price,
                    "size": fill_size,
                    "time": time.time(),
                })

                sides = self.engine.window_fill_sides.get(wid, {})
                if "UP" in sides and "DOWN" in sides:
                    self.engine.paired_windows.add(wid)

            self._processed_order_ids.add(order_id)
            self._trim_processed_ids()

            filled += 1
            self._ws_fills_total += 1
            self._ws_fills_session += 1
            self._last_ws_fill_time = time.time()

            self.logger.info(
                "  FILL [WS-PARTIAL] | {} {} {:.1f} @ ${:.2f} | {}".format(
                    fill_side, fill_token[:12] + "...",
                    fill_size, fill_price, wid
                )
            )

        if filled:
            self.engine._recalc_exposure()

        return filled

    def rest_reconcile(self) -> int:
        """
        Phase 4: Periodic REST reconciliation.

        Called every N cycles (configured by _rest_reconcile_interval) as a
        safety net to catch any fills that WS might have missed.

        This replaces the per-cycle REST check_fills() with a much less
        frequent reconciliation pass.

        Returns:
            Number of fills detected by REST reconciliation.
        """
        self._cycle_count += 1

        if self._cycle_count % self._rest_reconcile_interval != 0:
            return 0

        if self.engine.config.dry_run or not self.engine.client:
            return 0

        self._last_rest_reconcile = time.time()

        try:
            open_orders = self.engine.client.get_orders()
            open_ids = set()
            if isinstance(open_orders, list):
                open_ids = {o.get("id", o.get("orderID", "")) for o in open_orders}

            filled = 0
            # Check active orders that are NOT in the exchange's open list
            # AND were NOT cancelled via WS (those are already handled)
            for oid in list(self.engine.active_orders.keys()):
                if oid.startswith("DRY-"):
                    continue
                if oid in open_ids:
                    continue
                if oid in self._processed_order_ids:
                    continue

                # Check if WS already told us this was cancelled
                with self._lock:
                    if oid in self._ws_cancelled_orders:
                        # WS already handled this cancellation
                        continue

                # Order not on exchange and not cancelled via WS = filled
                info = self.engine.active_orders.pop(oid, None)
                if not info:
                    continue

                wid = info.get("window_id", "")
                if wid in self.engine.orders_by_window:
                    self.engine.orders_by_window[wid] = [
                        o for o in self.engine.orders_by_window[wid] if o != oid
                    ]

                fill_price = info.get("price", 0)
                fill_size = info.get("size", 0)
                fill_side = info.get("side", "BUY")
                fill_token = info.get("token_id", "")

                self.engine.record_fill(fill_token, fill_side, fill_price, fill_size)

                if fill_side == "BUY":
                    cost = fill_price * fill_size
                    self.engine.window_fill_cost[wid] = (
                        self.engine.window_fill_cost.get(wid, 0) + cost
                    )
                    self.engine.filled_windows.add(wid)
                    self.engine.window_entry_count[wid] = (
                        self.engine.window_entry_count.get(wid, 0) + 1
                    )
                    if wid not in self.engine.window_fill_tokens:
                        self.engine.window_fill_tokens[wid] = []
                    self.engine.window_fill_tokens[wid].append({
                        "token_id": fill_token, "size": fill_size,
                        "price": fill_price,
                        "is_up": self.engine._is_up_token_cache.get(fill_token),
                        "time": time.time(),
                    })
                    is_up = self.engine._is_up_token_cache.get(fill_token)
                    side_label = "UP" if is_up else "DOWN"
                    if wid not in self.engine.window_fill_sides:
                        self.engine.window_fill_sides[wid] = {}
                    if side_label not in self.engine.window_fill_sides[wid]:
                        self.engine.window_fill_sides[wid][side_label] = []
                    self.engine.window_fill_sides[wid][side_label].append({
                        "token_id": fill_token, "price": fill_price,
                        "size": fill_size, "time": time.time(),
                    })
                    sides = self.engine.window_fill_sides.get(wid, {})
                    if "UP" in sides and "DOWN" in sides:
                        self.engine.paired_windows.add(wid)
                    elif self.engine.config.hedge_completion_enabled:
                        # V15.4-FIX: Dedup — only add hedge if window not already pending
                        already_pending = any(
                            h["window_id"] == wid and h["filled_side"] == side_label
                            for h in self.engine._pending_hedges
                        )
                        if not already_pending:
                            self.engine._pending_hedges.append({
                                "window_id": wid, "filled_side": side_label,
                                "filled_price": fill_price,
                                "filled_size": fill_size,
                                "filled_token": fill_token,
                                "time": time.time(),
                            })

                self._processed_order_ids.add(oid)
                self._trim_processed_ids()
                filled += 1
                self._ws_fills_total += 1
                self._last_ws_fill_time = time.time()

                self.logger.info(
                    "  FILL [REST-RECONCILE] | {} {} {:.1f} @ ${:.2f} | {}".format(
                        fill_side, fill_token[:12] + "...",
                        fill_size, fill_price, wid
                    )
                )

            # Also check recently cancelled orders
            for oid in list(self.engine._recently_cancelled.keys()):
                if oid.startswith("DRY-"):
                    continue
                if oid in open_ids:
                    continue
                if oid in self._processed_order_ids:
                    continue
                with self._lock:
                    if oid in self._ws_cancelled_orders:
                        continue
                # V15.4-FIX: Skip orders that were explicitly cancelled by the bot.
                # These are NOT fills — they were cancelled via cancel_window_orders(),
                # prune_stale_orders(), or cleanup_expired_windows().
                # Without this check, REST reconcile treats them as fills because
                # they're not in open_ids (cancelled) and not in _ws_cancelled_orders
                # (WS event may be delayed or lost).
                if hasattr(self.engine, '_bot_cancelled_orders') and oid in self.engine._bot_cancelled_orders:
                    self.logger.debug(
                        f"  REST-RECONCILE SKIP (bot-cancelled) | {oid[:12]}...")
                    continue

                info = self.engine._recently_cancelled.pop(oid, None)
                if not info:
                    continue

                wid = info.get("window_id", "")
                fill_price = info.get("price", 0)
                fill_size = info.get("size", 0)
                fill_side = info.get("side", "BUY")
                fill_token = info.get("token_id", "")

                self.engine.record_fill(fill_token, fill_side, fill_price, fill_size)

                if fill_side == "BUY":
                    cost = fill_price * fill_size
                    self.engine.window_fill_cost[wid] = (
                        self.engine.window_fill_cost.get(wid, 0) + cost
                    )
                    self.engine.filled_windows.add(wid)
                    self.engine.window_entry_count[wid] = (
                        self.engine.window_entry_count.get(wid, 0) + 1
                    )
                    if wid not in self.engine.window_fill_tokens:
                        self.engine.window_fill_tokens[wid] = []
                    self.engine.window_fill_tokens[wid].append({
                        "token_id": fill_token, "size": fill_size,
                        "price": fill_price,
                        "is_up": self.engine._is_up_token_cache.get(fill_token),
                        "time": time.time(),
                    })
                    is_up = self.engine._is_up_token_cache.get(fill_token)
                    side_label = "UP" if is_up else "DOWN"
                    if wid not in self.engine.window_fill_sides:
                        self.engine.window_fill_sides[wid] = {}
                    if side_label not in self.engine.window_fill_sides[wid]:
                        self.engine.window_fill_sides[wid][side_label] = []
                    self.engine.window_fill_sides[wid][side_label].append({
                        "token_id": fill_token, "price": fill_price,
                        "size": fill_size, "time": time.time(),
                    })
                    sides = self.engine.window_fill_sides.get(wid, {})
                    if "UP" in sides and "DOWN" in sides:
                        self.engine.paired_windows.add(wid)
                    elif self.engine.config.hedge_completion_enabled:
                        # V15.4-FIX: Dedup — only add hedge if window not already pending
                        already_pending = any(
                            h["window_id"] == wid and h["filled_side"] == side_label
                            for h in self.engine._pending_hedges
                        )
                        if not already_pending:
                            self.engine._pending_hedges.append({
                                "window_id": wid, "filled_side": side_label,
                                "filled_price": fill_price,
                                "filled_size": fill_size,
                                "filled_token": fill_token,
                                "time": time.time(),
                            })

                self._processed_order_ids.add(oid)
                self._trim_processed_ids()
                filled += 1
                self._ws_fills_total += 1

                self.logger.info(
                    "  FILL [REST-RECONCILE-RECOVERED] | {} {} {:.1f} @ ${:.2f} | {}".format(
                        fill_side, fill_token[:12] + "...",
                        fill_size, fill_price, wid
                    )
                )

            if filled:
                self.engine._recalc_exposure()
                self.logger.info(f"  REST reconciliation found {filled} missed fills")

            return filled

        except Exception as e:
            self.logger.debug(f"  REST reconciliation error: {e}")
            return 0

    def purge_ws_cancelled_orders(self, max_age: float = 300.0):
        """
        Purge stale entries from _ws_cancelled_orders and _ws_confirmed_orders.
        Called periodically from the main cycle.
        """
        now = time.time()
        with self._lock:
            stale = [oid for oid, data in self._ws_cancelled_orders.items()
                     if now - data.get("ts", 0) > max_age]
            for oid in stale:
                del self._ws_cancelled_orders[oid]

            # Trim confirmed orders set
            if len(self._ws_confirmed_orders) > 10000:
                # Just clear it — it's informational only
                self._ws_confirmed_orders.clear()

    # ── Health & Fallback ──

    def is_healthy(self) -> bool:
        """
        Check if the WS fill detector is receiving data.

        Returns True if:
        - WS manager is running and user channel is connected
        - OR we've received a fill via WS in the last 5 minutes
          (no fills doesn't mean unhealthy — could just be no activity)
        """
        if not self._started or not self.ws_manager:
            return False

        # Check user channel connection
        user_conn = self.ws_manager._connections.get(Channel.USER)
        if user_conn and user_conn.connected:
            return True

        return False

    def should_fallback_to_rest(self) -> bool:
        """
        Determine if we should fall back to REST polling.

        Returns True if:
        - WS fill detector is not started
        - User channel is not connected
        - User channel has been disconnected for more than 30 seconds
        """
        if not self._started:
            return True

        if not self.ws_manager:
            return True

        user_conn = self.ws_manager._connections.get(Channel.USER)
        if not user_conn:
            return True

        if not user_conn.connected:
            return True

        return False

    def get_stats(self) -> dict:
        """Get fill detector statistics."""
        return {
            "ws_fills_total": self._ws_fills_total,
            "ws_fills_session": self._ws_fills_session,
            "rest_fallback_count": self._rest_fallback_count,
            "last_ws_fill_time": self._last_ws_fill_time,
            "queue_size": len(self._fill_queue),
            "processed_ids_count": len(self._processed_order_ids),
            "healthy": self.is_healthy(),
            "fallback_mode": self.should_fallback_to_rest(),
            # Phase 4 stats
            "ws_placements_confirmed": self._ws_placements_confirmed,
            "ws_cancellations_detected": self._ws_cancellations_detected,
            "ws_confirmed_orders": len(self._ws_confirmed_orders),
            "ws_cancelled_orders_tracked": len(self._ws_cancelled_orders),
            "cycle_count": self._cycle_count,
            "last_rest_reconcile": self._last_rest_reconcile,
        }

    def record_rest_fallback(self):
        """Record that REST fallback was used this cycle."""
        self._rest_fallback_count += 1

    def _trim_processed_ids(self):
        """Trim processed order IDs set to prevent unbounded growth."""
        if len(self._processed_order_ids) > self._max_processed_ids:
            # Remove oldest entries (convert to list, slice, convert back)
            # Since sets are unordered, we just remove a batch
            excess = len(self._processed_order_ids) - (self._max_processed_ids // 2)
            for _ in range(excess):
                self._processed_order_ids.pop()
