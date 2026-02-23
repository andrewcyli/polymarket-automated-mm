"""
WebSocket Manager for Polymarket Trading Bot
=============================================
Phase 1 of the async/WebSocket migration.

Manages persistent WebSocket connections to Polymarket's three channels:
1. Market: wss://ws-subscriptions-clob.polymarket.com/ws/market
   - Orderbook updates, price changes, trades/fills, new markets, resolutions
2. User: wss://ws-subscriptions-clob.polymarket.com/ws/user
   - Real-time order status updates (requires L2 derived API key auth)
3. RTDS: wss://ws-live-data.polymarket.com
   - Real-time Binance + Chainlink prices for BTC/ETH/SOL/XRP

Features:
- Automatic reconnection with exponential backoff
- Text-based PING heartbeat (per Polymarket protocol)
- Dynamic subscribe/unsubscribe per channel
- Thread-safe state store for sharing data with synchronous bot
- Graceful degradation: bot falls back to REST polling if WS disconnects

Architecture:
- WebSocket connections run in a background asyncio event loop (separate thread)
- The synchronous bot reads from the shared StateStore (thread-safe)
- No changes to the existing bot's synchronous execution model in Phase 1

Protocol Reference (Feb 2026):
- Market channel: https://docs.polymarket.com/market-data/websocket/market-channel
- User channel: https://docs.polymarket.com/market-data/websocket/user-channel
- RTDS: https://docs.polymarket.com/market-data/websocket/rtds
"""

import asyncio
import json
import logging
import threading
import time
import traceback
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import warnings
try:
    import websockets
    from websockets.exceptions import ConnectionClosed
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        try:
            from websockets.exceptions import InvalidStatusCode
        except ImportError:
            InvalidStatusCode = ConnectionClosed
    HAS_WEBSOCKETS = True
except ImportError:
    HAS_WEBSOCKETS = False

logger = logging.getLogger("polybot_v15_1.ws")


# ─────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────

class Channel(Enum):
    MARKET = "market"
    USER = "user"
    RTDS = "rtds"


WS_URLS = {
    Channel.MARKET: "wss://ws-subscriptions-clob.polymarket.com/ws/market",
    Channel.USER: "wss://ws-subscriptions-clob.polymarket.com/ws/user",
    Channel.RTDS: "wss://ws-live-data.polymarket.com",
}

# Reconnection parameters
INITIAL_RECONNECT_DELAY = 1.0      # seconds
MAX_RECONNECT_DELAY = 60.0         # seconds
RECONNECT_BACKOFF_FACTOR = 2.0
CONNECTION_TIMEOUT = 15.0          # seconds to establish connection

# Heartbeat intervals (text PING per Polymarket protocol)
MARKET_USER_PING_INTERVAL = 10.0   # seconds for market/user channels
RTDS_PING_INTERVAL = 5.0           # seconds for RTDS channel

# Minimum session duration before resetting backoff (prevents rapid reconnect loops)
MIN_SESSION_DURATION = 5.0         # seconds — if session < this, backoff continues


# ─────────────────────────────────────────────────────────────────
# Event Types (for the event bus)
# ─────────────────────────────────────────────────────────────────

class EventType(Enum):
    # Market channel events
    BOOK_UPDATE = "book_update"          # Orderbook snapshot/delta
    PRICE_CHANGE = "price_change"        # Market price change
    TRADE = "trade"                      # Trade/fill on the market
    LAST_TRADE_PRICE = "last_trade_price"  # Last trade price event
    BEST_BID_ASK = "best_bid_ask"        # Best bid/ask update
    NEW_MARKET = "new_market"            # New market created
    MARKET_RESOLVED = "market_resolved"  # Market resolved (UP/DOWN)
    TICK_SIZE_CHANGE = "tick_size_change"  # Tick size changed

    # User channel events
    ORDER_UPDATE = "order_update"        # Order status change (placed, filled, cancelled)
    ORDER_FILL = "order_fill"           # Order filled (partial or full)
    USER_TRADE = "user_trade"           # Trade matched for user

    # RTDS channel events
    CRYPTO_PRICE = "crypto_price"        # Real-time crypto price (Binance/Chainlink)

    # Connection events
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    RECONNECTING = "reconnecting"
    ERROR = "error"


# ─────────────────────────────────────────────────────────────────
# Shared State Store (Thread-Safe)
# ─────────────────────────────────────────────────────────────────

class StateStore:
    """
    Thread-safe state store for WebSocket data.
    
    The synchronous bot reads from this store instead of making REST calls.
    WebSocket handlers write to this store from the async event loop.
    All reads/writes are protected by threading.Lock.
    """

    def __init__(self):
        self._lock = threading.Lock()

        # ── Market Channel State ──
        # Orderbooks: {token_id: {"bids": [...], "asks": [...], "timestamp": float}}
        self._orderbooks: Dict[str, dict] = {}
        # Market prices: {condition_id: {"price_up": float, "price_down": float, "ts": float}}
        self._market_prices: Dict[str, dict] = {}
        # Best bid/ask: {asset_id: {"best_bid": str, "best_ask": str, "spread": str, "ts": float}}
        self._best_bid_ask: Dict[str, dict] = {}
        # Recent trades: {token_id: [{"price": float, "size": float, "side": str, "ts": float}]}
        self._recent_trades: Dict[str, list] = defaultdict(list)
        # Resolved markets: {condition_id: {"outcome": str, "ts": float}}
        self._resolved_markets: Dict[str, dict] = {}

        # ── User Channel State ──
        # Order statuses: {order_id: {"status": str, "filled_size": float, "ts": float}}
        self._order_statuses: Dict[str, dict] = {}
        # Recent fills: [{"order_id": str, "price": float, "size": float, "ts": float, "fill_id": int}]
        self._recent_fills: list = []
        self._fill_counter: int = 0

        # ── RTDS Channel State ──
        # Crypto prices: {asset: {"price": float, "source": str, "ts": float}}
        self._crypto_prices: Dict[str, dict] = {}

        # ── Connection State ──
        self._connection_status: Dict[str, dict] = {
            Channel.MARKET.value: {"connected": False, "last_message": 0, "reconnects": 0},
            Channel.USER.value: {"connected": False, "last_message": 0, "reconnects": 0},
            Channel.RTDS.value: {"connected": False, "last_message": 0, "reconnects": 0},
        }

        # ── Metrics ──
        self._messages_received: Dict[str, int] = defaultdict(int)
        self._errors: Dict[str, int] = defaultdict(int)
        self._start_time = time.time()

    # ── Market Channel Accessors ──

    def update_orderbook(self, token_id: str, book_data: dict):
        with self._lock:
            self._orderbooks[token_id] = {
                **book_data,
                "timestamp": time.time(),
            }

    def get_orderbook(self, token_id: str) -> Optional[dict]:
        with self._lock:
            entry = self._orderbooks.get(token_id)
            if entry and time.time() - entry.get("timestamp", 0) < 30:
                return entry
            return None

    def update_market_price(self, condition_id: str, price_up: float, price_down: float):
        with self._lock:
            self._market_prices[condition_id] = {
                "price_up": price_up,
                "price_down": price_down,
                "ts": time.time(),
            }

    def get_market_price(self, condition_id: str) -> Optional[dict]:
        with self._lock:
            entry = self._market_prices.get(condition_id)
            if entry and time.time() - entry.get("ts", 0) < 30:
                return entry
            return None

    def update_best_bid_ask(self, asset_id: str, best_bid: str, best_ask: str, spread: str):
        with self._lock:
            self._best_bid_ask[asset_id] = {
                "best_bid": best_bid,
                "best_ask": best_ask,
                "spread": spread,
                "ts": time.time(),
            }

    def get_best_bid_ask(self, asset_id: str) -> Optional[dict]:
        with self._lock:
            entry = self._best_bid_ask.get(asset_id)
            if entry and time.time() - entry.get("ts", 0) < 30:
                return entry
            return None

    def record_trade(self, token_id: str, trade: dict):
        with self._lock:
            self._recent_trades[token_id].append({
                **trade,
                "ts": time.time(),
            })
            # Keep only last 50 trades per token
            if len(self._recent_trades[token_id]) > 50:
                self._recent_trades[token_id] = self._recent_trades[token_id][-50:]

    def get_recent_trades(self, token_id: str, max_age: float = 60.0) -> list:
        with self._lock:
            cutoff = time.time() - max_age
            return [t for t in self._recent_trades.get(token_id, []) if t.get("ts", 0) > cutoff]

    def mark_resolved(self, condition_id: str, outcome: str):
        with self._lock:
            self._resolved_markets[condition_id] = {
                "outcome": outcome,
                "ts": time.time(),
            }

    def is_resolved(self, condition_id: str) -> Optional[str]:
        with self._lock:
            entry = self._resolved_markets.get(condition_id)
            return entry.get("outcome") if entry else None

    # ── User Channel Accessors ──

    def update_order_status(self, order_id: str, status: str, filled_size: float = 0):
        with self._lock:
            self._order_statuses[order_id] = {
                "status": status,
                "filled_size": filled_size,
                "ts": time.time(),
            }

    def get_order_status(self, order_id: str) -> Optional[dict]:
        with self._lock:
            return self._order_statuses.get(order_id)

    def record_fill(self, fill_data: dict):
        with self._lock:
            self._fill_counter += 1
            self._recent_fills.append({
                **fill_data,
                "ts": time.time(),
                "fill_id": self._fill_counter,
            })
            # Keep only last 200 fills
            if len(self._recent_fills) > 200:
                self._recent_fills = self._recent_fills[-200:]

    def get_recent_fills(self, since: float = 0) -> list:
        with self._lock:
            return [f for f in self._recent_fills if f.get("ts", 0) > since]

    def get_pending_fills(self) -> list:
        """Get fills that haven't been processed by the bot yet."""
        with self._lock:
            return [f for f in self._recent_fills if not f.get("processed")]

    def mark_fill_processed(self, fill_ts: float = None, fill_id: int = None):
        """Mark a fill as processed by fill_id (preferred) or timestamp."""
        with self._lock:
            for f in self._recent_fills:
                if fill_id is not None and f.get("fill_id") == fill_id:
                    f["processed"] = True
                    return
                elif fill_ts is not None and f.get("ts") == fill_ts and not f.get("processed"):
                    f["processed"] = True
                    return

    # ── RTDS Channel Accessors ──

    def update_crypto_price(self, asset: str, price: float, source: str = "binance"):
        with self._lock:
            self._crypto_prices[asset] = {
                "price": price,
                "source": source,
                "ts": time.time(),
            }

    def get_crypto_price(self, asset: str, max_age: float = 10.0) -> Optional[float]:
        """Get real-time crypto price. Returns None if stale (>max_age seconds)."""
        with self._lock:
            entry = self._crypto_prices.get(asset)
            if entry and time.time() - entry.get("ts", 0) < max_age:
                return entry["price"]
            return None

    def get_all_crypto_prices(self) -> Dict[str, dict]:
        with self._lock:
            return dict(self._crypto_prices)

    # ── Connection State ──

    def set_connected(self, channel: str, connected: bool):
        with self._lock:
            self._connection_status[channel]["connected"] = connected
            if connected:
                self._connection_status[channel]["last_message"] = time.time()

    def record_reconnect(self, channel: str):
        with self._lock:
            self._connection_status[channel]["reconnects"] += 1

    def is_connected(self, channel: str) -> bool:
        with self._lock:
            return self._connection_status[channel]["connected"]

    def record_message(self, channel: str):
        with self._lock:
            self._connection_status[channel]["last_message"] = time.time()
            self._messages_received[channel] += 1

    def record_error(self, channel: str):
        with self._lock:
            self._errors[channel] += 1

    # ── Diagnostics ──

    def get_status(self) -> dict:
        with self._lock:
            uptime = time.time() - self._start_time
            return {
                "uptime_seconds": uptime,
                "connections": dict(self._connection_status),
                "messages_received": dict(self._messages_received),
                "errors": dict(self._errors),
                "orderbooks_cached": len(self._orderbooks),
                "crypto_prices_cached": len(self._crypto_prices),
                "order_statuses_tracked": len(self._order_statuses),
                "recent_fills": len(self._recent_fills),
            }


# ─────────────────────────────────────────────────────────────────
# Event Bus (Thread-Safe)
# ─────────────────────────────────────────────────────────────────

class EventBus:
    """
    Simple pub/sub event bus for WebSocket events.
    
    Handlers can be registered from the synchronous bot thread.
    Events are emitted from the async WebSocket thread.
    Thread-safe via Lock.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._handlers: Dict[EventType, List[Callable]] = defaultdict(list)
        self._event_log: list = []
        self._max_log_size = 500

    def subscribe(self, event_type: EventType, handler: Callable):
        """Register a handler for an event type. Thread-safe."""
        with self._lock:
            self._handlers[event_type].append(handler)

    def unsubscribe(self, event_type: EventType, handler: Callable):
        """Remove a handler. Thread-safe."""
        with self._lock:
            if handler in self._handlers[event_type]:
                self._handlers[event_type].remove(handler)

    def emit(self, event_type: EventType, data: dict):
        """Emit an event to all registered handlers. Thread-safe."""
        with self._lock:
            handlers = list(self._handlers.get(event_type, []))
            self._event_log.append({
                "type": event_type.value,
                "ts": time.time(),
                "data_keys": list(data.keys()) if isinstance(data, dict) else [],
            })
            if len(self._event_log) > self._max_log_size:
                self._event_log = self._event_log[-self._max_log_size:]

        # Call handlers outside the lock to prevent deadlocks
        for handler in handlers:
            try:
                handler(event_type, data)
            except Exception as e:
                logger.error(f"Event handler error for {event_type.value}: {e}")

    def get_event_log(self, last_n: int = 50) -> list:
        with self._lock:
            return self._event_log[-last_n:]


# ─────────────────────────────────────────────────────────────────
# WebSocket Connection (Single Channel)
# ─────────────────────────────────────────────────────────────────

class WSConnection:
    """
    Manages a single WebSocket connection with:
    - Automatic reconnection with exponential backoff
    - Text-based PING heartbeat (Polymarket protocol)
    - Dynamic subscribe/unsubscribe
    - Message routing to handlers
    """

    def __init__(
        self,
        channel: Channel,
        state_store: StateStore,
        event_bus: EventBus,
        auth_creds: Optional[dict] = None,
    ):
        self.channel = channel
        self.url = WS_URLS[channel]
        self.state_store = state_store
        self.event_bus = event_bus
        # Auth creds for user channel: {"apiKey": ..., "secret": ..., "passphrase": ...}
        self.auth_creds = auth_creds

        self._ws = None
        self._running = False
        self._reconnect_delay = INITIAL_RECONNECT_DELAY
        self._subscriptions: Set[str] = set()
        self._pending_subscribes: list = []
        self._pending_unsubscribes: list = []
        self._message_count = 0
        self._session_start = 0.0
        self._ping_task = None

    async def connect(self):
        """Main connection loop with auto-reconnection."""
        self._running = True
        while self._running:
            try:
                logger.info(f"WS [{self.channel.value}] Connecting to {self.url}...")

                # Disable websockets library's built-in ping — we send text PING ourselves
                connect_kwargs = {
                    "ping_interval": None,
                    "ping_timeout": None,
                    "close_timeout": 5,
                    "open_timeout": CONNECTION_TIMEOUT,
                    "max_size": 2**20,  # 1MB max message size
                }

                async with websockets.connect(
                    self.url,
                    **connect_kwargs,
                ) as ws:
                    self._ws = ws
                    self._session_start = time.time()

                    self.state_store.set_connected(self.channel.value, True)
                    self.event_bus.emit(EventType.CONNECTED, {
                        "channel": self.channel.value,
                    })
                    logger.info(f"WS [{self.channel.value}] Connected!")

                    # Start text-based PING heartbeat
                    self._ping_task = asyncio.ensure_future(self._heartbeat_loop())

                    # Re-subscribe to any active subscriptions after reconnect
                    await self._resubscribe_all()

                    # Process pending subscribes
                    await self._process_pending()

                    # Message receive loop
                    async for message in ws:
                        if not self._running:
                            break
                        try:
                            await self._handle_message(message)
                        except Exception as e:
                            logger.error(
                                f"WS [{self.channel.value}] Message handler error: {e}\n"
                                f"{traceback.format_exc()}"
                            )
                            self.state_store.record_error(self.channel.value)

            except ConnectionClosed as e:
                logger.warning(
                    f"WS [{self.channel.value}] Connection closed: code={e.code} reason={e.reason}"
                )
            except InvalidStatusCode as e:
                logger.error(
                    f"WS [{self.channel.value}] Invalid status code: {getattr(e, 'status_code', e)}"
                )
            except asyncio.CancelledError:
                logger.info(f"WS [{self.channel.value}] Connection cancelled")
                break
            except Exception as e:
                logger.error(
                    f"WS [{self.channel.value}] Connection error: {e}\n"
                    f"{traceback.format_exc()}"
                )

            # Cancel ping task
            if self._ping_task and not self._ping_task.done():
                self._ping_task.cancel()
                try:
                    await self._ping_task
                except asyncio.CancelledError:
                    pass
            self._ping_task = None

            # Mark disconnected
            session_duration = time.time() - self._session_start if self._session_start else 0
            self._ws = None
            self.state_store.set_connected(self.channel.value, False)
            self.state_store.record_reconnect(self.channel.value)
            self.event_bus.emit(EventType.DISCONNECTED, {
                "channel": self.channel.value,
            })

            if not self._running:
                break

            # Only reset backoff if session lasted long enough (prevents rapid reconnect loops)
            if session_duration >= MIN_SESSION_DURATION:
                self._reconnect_delay = INITIAL_RECONNECT_DELAY
            else:
                # Session was too short — increase backoff
                self._reconnect_delay = min(
                    self._reconnect_delay * RECONNECT_BACKOFF_FACTOR,
                    MAX_RECONNECT_DELAY,
                )

            logger.info(
                f"WS [{self.channel.value}] Reconnecting in {self._reconnect_delay:.1f}s "
                f"(session lasted {session_duration:.1f}s)..."
            )
            self.event_bus.emit(EventType.RECONNECTING, {
                "channel": self.channel.value,
                "delay": self._reconnect_delay,
            })
            await asyncio.sleep(self._reconnect_delay)

    async def _heartbeat_loop(self):
        """Send text PING messages at the interval required by Polymarket."""
        interval = RTDS_PING_INTERVAL if self.channel == Channel.RTDS else MARKET_USER_PING_INTERVAL
        try:
            while self._running and self._ws:
                await asyncio.sleep(interval)
                if self._ws and self._running:
                    try:
                        await self._ws.send("PING")
                        logger.debug(f"WS [{self.channel.value}] Sent PING")
                    except Exception as e:
                        logger.debug(f"WS [{self.channel.value}] PING failed: {e}")
                        break
        except asyncio.CancelledError:
            pass

    async def disconnect(self):
        """Gracefully disconnect."""
        self._running = False
        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        self._ws = None
        self.state_store.set_connected(self.channel.value, False)

    async def subscribe(self, subscription_msg: dict):
        """Subscribe to a topic on this channel."""
        if self._ws:
            try:
                await self._ws.send(json.dumps(subscription_msg))
                sub_key = json.dumps(subscription_msg, sort_keys=True)
                self._subscriptions.add(sub_key)
                logger.debug(f"WS [{self.channel.value}] Subscribed: {subscription_msg}")
            except Exception as e:
                logger.error(f"WS [{self.channel.value}] Subscribe error: {e}")
                self._pending_subscribes.append(subscription_msg)
        else:
            self._pending_subscribes.append(subscription_msg)

    async def unsubscribe(self, unsubscription_msg: dict):
        """Unsubscribe from a topic on this channel."""
        if self._ws:
            try:
                await self._ws.send(json.dumps(unsubscription_msg))
                sub_key = json.dumps(unsubscription_msg, sort_keys=True)
                self._subscriptions.discard(sub_key)
                logger.debug(f"WS [{self.channel.value}] Unsubscribed: {unsubscription_msg}")
            except Exception as e:
                logger.error(f"WS [{self.channel.value}] Unsubscribe error: {e}")

    async def _resubscribe_all(self):
        """Re-subscribe to all active subscriptions after reconnect."""
        for sub_key in list(self._subscriptions):
            try:
                msg = json.loads(sub_key)
                await self._ws.send(json.dumps(msg))
                logger.debug(f"WS [{self.channel.value}] Re-subscribed: {msg}")
            except Exception as e:
                logger.error(f"WS [{self.channel.value}] Re-subscribe error: {e}")

    async def _process_pending(self):
        """Process any pending subscribe/unsubscribe requests."""
        while self._pending_subscribes:
            msg = self._pending_subscribes.pop(0)
            await self.subscribe(msg)
        while self._pending_unsubscribes:
            msg = self._pending_unsubscribes.pop(0)
            await self.unsubscribe(msg)

    async def _handle_message(self, raw_message: str):
        """Route incoming message to the appropriate handler."""
        # Handle PONG responses
        if raw_message == "PONG":
            logger.debug(f"WS [{self.channel.value}] Received PONG")
            return

        self._message_count += 1
        self.state_store.record_message(self.channel.value)

        try:
            data = json.loads(raw_message)
        except json.JSONDecodeError:
            logger.debug(f"WS [{self.channel.value}] Non-JSON message: {raw_message[:200]}")
            return

        if self.channel == Channel.MARKET:
            await self._handle_market_message(data)
        elif self.channel == Channel.USER:
            await self._handle_user_message(data)
        elif self.channel == Channel.RTDS:
            await self._handle_rtds_message(data)

    async def _handle_market_message(self, data: dict):
        """
        Handle Market channel messages.
        
        Per docs, market channel uses `event_type` field:
        - book: Orderbook snapshot (on subscribe + on trade)
        - price_change: New order placed or cancelled
        - last_trade_price: Trade matched
        - best_bid_ask: Best bid/ask changed (requires custom_feature_enabled)
        - tick_size_change: Tick size changed
        - new_market: New market created (requires custom_feature_enabled)
        - market_resolved: Market resolved (requires custom_feature_enabled)
        """
        event_type = data.get("event_type", "")

        if event_type == "book":
            # Full orderbook snapshot
            token_id = data.get("asset_id", "")
            if token_id:
                book = {
                    "bids": data.get("bids", []),
                    "asks": data.get("asks", []),
                    "hash": data.get("hash", ""),
                    "market": data.get("market", ""),
                }
                self.state_store.update_orderbook(token_id, book)
                self.event_bus.emit(EventType.BOOK_UPDATE, {
                    "token_id": token_id,
                    "book": book,
                })

        elif event_type == "price_change":
            # Order placed or cancelled — contains price_changes array
            market_id = data.get("market", "")
            price_changes = data.get("price_changes", [])
            for change in price_changes:
                asset_id = change.get("asset_id", "")
                if asset_id:
                    self.event_bus.emit(EventType.PRICE_CHANGE, {
                        "market_id": market_id,
                        "asset_id": asset_id,
                        "price": change.get("price", ""),
                        "size": change.get("size", ""),
                        "side": change.get("side", ""),
                        "best_bid": change.get("best_bid", ""),
                        "best_ask": change.get("best_ask", ""),
                    })

        elif event_type == "last_trade_price":
            # Trade matched — maker/taker matched
            token_id = data.get("asset_id", "")
            if token_id:
                trade = {
                    "price": float(data.get("price", 0)),
                    "size": float(data.get("size", 0)),
                    "side": data.get("side", ""),
                    "fee_rate_bps": data.get("fee_rate_bps", "0"),
                }
                self.state_store.record_trade(token_id, trade)
                self.event_bus.emit(EventType.LAST_TRADE_PRICE, {
                    "token_id": token_id,
                    "market": data.get("market", ""),
                    **trade,
                })

        elif event_type == "best_bid_ask":
            # Best bid/ask changed (requires custom_feature_enabled)
            asset_id = data.get("asset_id", "")
            if asset_id:
                self.state_store.update_best_bid_ask(
                    asset_id,
                    data.get("best_bid", ""),
                    data.get("best_ask", ""),
                    data.get("spread", ""),
                )
                self.event_bus.emit(EventType.BEST_BID_ASK, {
                    "asset_id": asset_id,
                    "market": data.get("market", ""),
                    "best_bid": data.get("best_bid", ""),
                    "best_ask": data.get("best_ask", ""),
                    "spread": data.get("spread", ""),
                })

        elif event_type == "tick_size_change":
            self.event_bus.emit(EventType.TICK_SIZE_CHANGE, {
                "asset_id": data.get("asset_id", ""),
                "market": data.get("market", ""),
                "old_tick_size": data.get("old_tick_size", ""),
                "new_tick_size": data.get("new_tick_size", ""),
            })

        elif event_type == "market_resolved":
            # Market resolved — extract winning info
            market_id = data.get("market", "")
            winning_outcome = data.get("winning_outcome", "")
            if market_id:
                self.state_store.mark_resolved(market_id, winning_outcome)
                self.event_bus.emit(EventType.MARKET_RESOLVED, {
                    "market": market_id,
                    "question": data.get("question", ""),
                    "winning_asset_id": data.get("winning_asset_id", ""),
                    "winning_outcome": winning_outcome,
                    "assets_ids": data.get("assets_ids", []),
                    "outcomes": data.get("outcomes", []),
                })

        elif event_type == "new_market":
            self.event_bus.emit(EventType.NEW_MARKET, {
                "id": data.get("id", ""),
                "question": data.get("question", ""),
                "market": data.get("market", ""),
                "slug": data.get("slug", ""),
                "assets_ids": data.get("assets_ids", []),
                "outcomes": data.get("outcomes", []),
            })

    async def _handle_user_message(self, data: dict):
        """
        Handle User channel messages.
        
        Per docs, user channel has two event_types:
        - trade: Order matched (MATCHED → MINED → CONFIRMED, or RETRYING → FAILED)
        - order: Order placed (PLACEMENT), updated (UPDATE), or cancelled (CANCELLATION)
        """
        event_type = data.get("event_type", "")

        if event_type == "trade":
            # Trade matched for user
            trade_type = data.get("type", "")  # TRADE
            status = data.get("status", "")    # MATCHED, MINED, CONFIRMED, RETRYING, FAILED
            taker_order_id = data.get("taker_order_id", "")
            asset_id = data.get("asset_id", "")
            price = data.get("price", "0")
            size = data.get("size", "0")
            side = data.get("side", "")

            # Update order status for taker
            if taker_order_id:
                self.state_store.update_order_status(
                    taker_order_id, status, float(size)
                )

            # Update maker orders
            for maker in data.get("maker_orders", []):
                maker_order_id = maker.get("order_id", "")
                matched_amount = maker.get("matched_amount", "0")
                if maker_order_id:
                    self.state_store.update_order_status(
                        maker_order_id, status, float(matched_amount)
                    )

            # Record fill on MATCHED status
            if status == "MATCHED":
                fill_data = {
                    "taker_order_id": taker_order_id,
                    "price": float(price),
                    "size": float(size),
                    "side": side,
                    "asset_id": asset_id,
                    "market": data.get("market", ""),
                    "status": status,
                    "maker_orders": data.get("maker_orders", []),
                }
                self.state_store.record_fill(fill_data)
                self.event_bus.emit(EventType.USER_TRADE, fill_data)

                # Also emit ORDER_FILL for each maker order
                for maker in data.get("maker_orders", []):
                    self.event_bus.emit(EventType.ORDER_FILL, {
                        "order_id": maker.get("order_id", ""),
                        "price": float(maker.get("price", 0)),
                        "size": float(maker.get("matched_amount", 0)),
                        "asset_id": maker.get("asset_id", asset_id),
                        "outcome": maker.get("outcome", ""),
                        "status": status,
                    })

            # Emit general order update
            self.event_bus.emit(EventType.ORDER_UPDATE, {
                "event_type": "trade",
                "status": status,
                "taker_order_id": taker_order_id,
                "asset_id": asset_id,
                "price": price,
                "size": size,
                "side": side,
                "raw": data,
            })

        elif event_type == "order":
            # Order lifecycle event
            order_type = data.get("type", "")  # PLACEMENT, UPDATE, CANCELLATION
            order_id = data.get("id", "")
            asset_id = data.get("asset_id", "")
            size_matched = data.get("size_matched", "0")

            if order_id:
                self.state_store.update_order_status(
                    order_id, order_type, float(size_matched)
                )
                self.event_bus.emit(EventType.ORDER_UPDATE, {
                    "event_type": "order",
                    "order_id": order_id,
                    "type": order_type,
                    "asset_id": asset_id,
                    "market": data.get("market", ""),
                    "price": data.get("price", ""),
                    "side": data.get("side", ""),
                    "original_size": data.get("original_size", ""),
                    "size_matched": size_matched,
                    "outcome": data.get("outcome", ""),
                    "raw": data,
                })

    async def _handle_rtds_message(self, data: dict):
        """
        Handle RTDS channel messages.
        
        Per docs, RTDS messages have structure:
        {
            "topic": "crypto_prices" | "crypto_prices_chainlink",
            "type": "update",
            "timestamp": 1753314064237,
            "payload": {
                "symbol": "btcusdt" | "btc/usd",
                "timestamp": 1753314088395,
                "value": 67234.50
            }
        }
        """
        topic = data.get("topic", "")
        payload = data.get("payload", {})

        if topic in ("crypto_prices", "crypto_prices_chainlink"):
            symbol = payload.get("symbol", "")
            value = payload.get("value", 0)
            source = "chainlink" if "chainlink" in topic else "binance"

            # Normalize symbol to short form
            asset = self._normalize_asset(symbol)

            if asset and value:
                try:
                    price = float(value)
                    self.state_store.update_crypto_price(asset, price, source)
                    self.event_bus.emit(EventType.CRYPTO_PRICE, {
                        "asset": asset,
                        "price": price,
                        "source": source,
                        "symbol": symbol,
                    })
                except (ValueError, TypeError):
                    pass

    @staticmethod
    def _normalize_asset(raw: str) -> str:
        """Normalize asset names from various formats to lowercase short form."""
        raw = raw.lower().strip()
        # Handle Binance-style pairs (btcusdt → btc)
        for suffix in ("usdt", "usd", "usdc"):
            if raw.endswith(suffix):
                raw = raw[:-len(suffix)]
                break
        # Handle Chainlink-style pairs (btc/usd → btc)
        if "/" in raw:
            raw = raw.split("/")[0]
        # Map common names
        name_map = {
            "bitcoin": "btc", "ethereum": "eth", "solana": "sol", "ripple": "xrp",
        }
        return name_map.get(raw, raw)

    @property
    def connected(self) -> bool:
        return self._ws is not None and self._running

    @property
    def message_count(self) -> int:
        return self._message_count


# ─────────────────────────────────────────────────────────────────
# WebSocket Manager (Orchestrator)
# ─────────────────────────────────────────────────────────────────

class WebSocketManager:
    """
    Orchestrates all WebSocket connections.
    
    Runs in a background thread with its own asyncio event loop.
    The synchronous bot interacts via:
    - state_store: Read latest data (thread-safe)
    - event_bus: Subscribe to events (thread-safe)
    - subscribe/unsubscribe: Manage channel subscriptions (thread-safe)
    
    Usage:
        manager = WebSocketManager(api_key="...", api_secret="...", api_passphrase="...")
        manager.start()
        
        # Read data from the state store
        price = manager.state_store.get_crypto_price("btc")
        book = manager.state_store.get_orderbook(token_id)
        
        # Subscribe to events
        manager.event_bus.subscribe(EventType.ORDER_FILL, my_handler)
        
        # Subscribe to market data
        manager.subscribe_market(["token_id_1", "token_id_2"])
        
        # Graceful shutdown
        manager.stop()
    """

    def __init__(
        self,
        api_key: str = "",
        api_secret: str = "",
        api_passphrase: str = "",
        enable_market: bool = True,
        enable_user: bool = True,
        enable_rtds: bool = True,
        logger_instance=None,
    ):
        global logger
        if logger_instance:
            logger = logger_instance

        if not HAS_WEBSOCKETS:
            logger.error("websockets library not installed. WebSocket features disabled.")

        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase

        self.enable_market = enable_market
        self.enable_user = enable_user and bool(api_key)
        self.enable_rtds = enable_rtds

        # Shared state and event bus
        self.state_store = StateStore()
        self.event_bus = EventBus()

        # Connections
        self._connections: Dict[Channel, WSConnection] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False

        # Subscription tracking for thread-safe queue
        self._sub_queue: list = []
        self._unsub_queue: list = []
        self._queue_lock = threading.Lock()

    def set_derived_creds(self, api_key: str, api_secret: str, api_passphrase: str):
        """
        Update with L2 derived API credentials (from ClobClient.create_or_derive_api_creds()).
        Must be called before start() if using the user channel.
        The user channel requires L2 (derived) keys, not the L1 keys from config.
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        logger.info(f"WS manager: updated with derived L2 creds (key: {api_key[:8]}...{api_key[-4:]})")

    def start(self):
        """Start all WebSocket connections in a background thread."""
        if not HAS_WEBSOCKETS:
            logger.warning("WebSocket manager: websockets library not available. Skipping.")
            return False

        if self._running:
            logger.warning("WebSocket manager already running.")
            return True

        self._running = True
        self._thread = threading.Thread(
            target=self._run_event_loop,
            name="ws-manager",
            daemon=True,
        )
        self._thread.start()
        logger.info("WebSocket manager started (background thread)")
        return True

    def stop(self):
        """Stop all WebSocket connections and the background thread."""
        logger.info("WebSocket manager stopping...")
        self._running = False

        # Signal all connections to stop
        for conn in self._connections.values():
            conn._running = False

        if self._loop and not self._loop.is_closed():
            # Schedule disconnect tasks
            for conn in self._connections.values():
                try:
                    asyncio.run_coroutine_threadsafe(conn.disconnect(), self._loop)
                except Exception:
                    pass

            # Give connections a moment to close gracefully
            time.sleep(0.5)

            # Stop the event loop
            try:
                self._loop.call_soon_threadsafe(self._loop.stop)
            except Exception:
                pass

        if self._thread:
            self._thread.join(timeout=5)
            if self._thread.is_alive():
                logger.warning("WebSocket manager thread did not stop cleanly")

        self._connections.clear()
        logger.info("WebSocket manager stopped")

    def _run_event_loop(self):
        """Background thread: create and run the asyncio event loop."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        try:
            self._loop.run_until_complete(self._run_connections())
        except Exception as e:
            if self._running:  # Only log if not intentionally stopped
                logger.error(f"WebSocket event loop error: {e}\n{traceback.format_exc()}")
        finally:
            # Clean up pending tasks
            try:
                pending = asyncio.all_tasks(self._loop)
                for task in pending:
                    task.cancel()
                if pending:
                    self._loop.run_until_complete(
                        asyncio.gather(*pending, return_exceptions=True)
                    )
            except Exception:
                pass
            try:
                self._loop.close()
            except Exception:
                pass
            self._loop = None

    async def _run_connections(self):
        """Start all enabled WebSocket connections as concurrent tasks."""
        tasks = []

        if self.enable_market:
            conn = WSConnection(
                Channel.MARKET, self.state_store, self.event_bus
            )
            self._connections[Channel.MARKET] = conn
            tasks.append(asyncio.create_task(conn.connect()))

        if self.enable_user:
            # User channel auth goes in the subscription message, not HTTP headers
            auth_creds = None
            if self.api_key:
                auth_creds = {
                    "apiKey": self.api_key,
                    "secret": self.api_secret,
                    "passphrase": self.api_passphrase,
                }
            conn = WSConnection(
                Channel.USER, self.state_store, self.event_bus,
                auth_creds=auth_creds,
            )
            self._connections[Channel.USER] = conn
            tasks.append(asyncio.create_task(conn.connect()))

        if self.enable_rtds:
            conn = WSConnection(
                Channel.RTDS, self.state_store, self.event_bus
            )
            self._connections[Channel.RTDS] = conn
            tasks.append(asyncio.create_task(conn.connect()))

        # Also run the subscription processor
        tasks.append(asyncio.create_task(self._process_subscription_queue()))

        if tasks:
            logger.info(
                f"WebSocket manager: starting {len(tasks) - 1} connections "
                f"(market={self.enable_market}, user={self.enable_user}, rtds={self.enable_rtds})"
            )
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _process_subscription_queue(self):
        """Process subscription requests from the synchronous thread."""
        while self._running:
            with self._queue_lock:
                subs = list(self._sub_queue)
                unsubs = list(self._unsub_queue)
                self._sub_queue.clear()
                self._unsub_queue.clear()

            for channel, msg in subs:
                conn = self._connections.get(channel)
                if conn:
                    await conn.subscribe(msg)

            for channel, msg in unsubs:
                conn = self._connections.get(channel)
                if conn:
                    await conn.unsubscribe(msg)

            await asyncio.sleep(0.5)

    # ── Thread-Safe Subscription Methods ──
    # These match the EXACT Polymarket WebSocket protocol formats from the official docs.

    def subscribe_market(self, token_ids: list):
        """
        Subscribe to market data for given token IDs. Thread-safe.
        
        Per docs: {"assets_ids": ["<token_id>"], "type": "market", "custom_feature_enabled": true}
        """
        if not token_ids:
            return
        msg = {
            "assets_ids": token_ids if isinstance(token_ids, list) else [token_ids],
            "type": "market",
            "custom_feature_enabled": True,
        }
        with self._queue_lock:
            self._sub_queue.append((Channel.MARKET, msg))

    def subscribe_market_additional(self, token_ids: list):
        """
        Subscribe to additional market tokens (add to existing subscription). Thread-safe.
        
        Per docs: {"assets_ids": ["<new_id>"], "operation": "subscribe", "custom_feature_enabled": true}
        """
        if not token_ids:
            return
        msg = {
            "assets_ids": token_ids if isinstance(token_ids, list) else [token_ids],
            "operation": "subscribe",
            "custom_feature_enabled": True,
        }
        with self._queue_lock:
            self._sub_queue.append((Channel.MARKET, msg))

    def unsubscribe_market(self, token_ids: list):
        """
        Unsubscribe from market data for given token IDs. Thread-safe.
        
        Per docs: {"assets_ids": ["<id>"], "operation": "unsubscribe"}
        """
        if not token_ids:
            return
        msg = {
            "assets_ids": token_ids if isinstance(token_ids, list) else [token_ids],
            "operation": "unsubscribe",
        }
        with self._queue_lock:
            self._unsub_queue.append((Channel.MARKET, msg))

    def subscribe_user_orders(self, condition_ids: list = None):
        """
        Subscribe to user order updates. Thread-safe.
        
        Per docs: {
            "auth": {"apiKey": "...", "secret": "...", "passphrase": "..."},
            "markets": ["condition_id"],
            "type": "user"
        }
        
        If condition_ids is empty, subscribes with empty markets list.
        """
        conn = self._connections.get(Channel.USER)
        if not conn or not conn.auth_creds:
            logger.warning("WS: Cannot subscribe to user channel — no auth credentials")
            return

        msg = {
            "auth": conn.auth_creds,
            "markets": condition_ids or [],
            "type": "user",
        }
        with self._queue_lock:
            self._sub_queue.append((Channel.USER, msg))

    def subscribe_user_additional(self, condition_ids: list):
        """
        Add more markets to user subscription. Thread-safe.
        
        Per docs: {"markets": ["condition_id"], "operation": "subscribe"}
        """
        if not condition_ids:
            return
        msg = {
            "markets": condition_ids,
            "operation": "subscribe",
        }
        with self._queue_lock:
            self._sub_queue.append((Channel.USER, msg))

    def subscribe_rtds_binance(self, symbols: list = None):
        """
        Subscribe to Binance crypto prices via RTDS. Thread-safe.
        
        Per docs: {
            "action": "subscribe",
            "subscriptions": [{"topic": "crypto_prices", "type": "update", "filters": "btcusdt,ethusdt"}]
        }
        """
        sub = {
            "topic": "crypto_prices",
            "type": "update",
        }
        if symbols:
            # Normalize to lowercase concatenated format (e.g., btcusdt)
            normalized = []
            for s in symbols:
                s = s.lower().strip()
                if not s.endswith("usdt"):
                    s = s + "usdt"
                normalized.append(s)
            sub["filters"] = ",".join(normalized)

        msg = {
            "action": "subscribe",
            "subscriptions": [sub],
        }
        with self._queue_lock:
            self._sub_queue.append((Channel.RTDS, msg))

    def subscribe_rtds_chainlink(self, symbols: list = None):
        """
        Subscribe to Chainlink crypto prices via RTDS. Thread-safe.
        
        Per docs: {
            "action": "subscribe",
            "subscriptions": [{"topic": "crypto_prices_chainlink", "type": "*", "filters": ""}]
        }
        """
        sub = {
            "topic": "crypto_prices_chainlink",
            "type": "*",
            "filters": "",
        }
        if symbols:
            # Chainlink uses slash format: btc/usd
            normalized = []
            for s in symbols:
                s = s.lower().strip()
                if "/" not in s:
                    s = s + "/usd"
                normalized.append(s)
            sub["filters"] = json.dumps({"symbol": normalized[0]}) if len(normalized) == 1 else ""

        msg = {
            "action": "subscribe",
            "subscriptions": [sub],
        }
        with self._queue_lock:
            self._sub_queue.append((Channel.RTDS, msg))

    def subscribe_rtds_all(self, assets: list = None):
        """Subscribe to both Binance and Chainlink RTDS feeds. Thread-safe."""
        if assets is None:
            assets = ["btc", "eth", "sol", "xrp"]
        self.subscribe_rtds_binance(assets)
        self.subscribe_rtds_chainlink(assets)

    # ── Status & Diagnostics ──

    def is_healthy(self) -> bool:
        """Check if all enabled connections are healthy."""
        if not self._running:
            return False
        for channel, conn in self._connections.items():
            if not conn.connected:
                return False
        return True

    def get_status(self) -> dict:
        """Get comprehensive status of all connections."""
        return {
            "running": self._running,
            "healthy": self.is_healthy(),
            "state_store": self.state_store.get_status(),
            "connections": {
                ch.value: {
                    "enabled": True,
                    "connected": conn.connected,
                    "messages": conn.message_count,
                }
                for ch, conn in self._connections.items()
            },
        }

    def get_connection_summary(self) -> str:
        """Human-readable connection summary for logging."""
        parts = []
        for ch, conn in self._connections.items():
            status = "OK" if conn.connected else "DOWN"
            parts.append(f"{ch.value}:{status}({conn.message_count})")
        return " | ".join(parts) if parts else "No connections"
