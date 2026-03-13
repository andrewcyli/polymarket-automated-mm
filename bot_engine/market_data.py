"""Market data tracking: volatility, VPIN, order books, price feeds, discovery."""

import time
import math
import logging
import requests
import numpy as np
from datetime import datetime, timezone


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
# V15.6: VPIN Toxicity Tracker
# -----------------------------------------------------------------

class VPINTracker:
    """
    Volume-Synchronized Probability of Informed Trading (VPIN).
    
    Measures the imbalance between buy-initiated and sell-initiated volume
    from the WebSocket trade stream. High VPIN indicates informed traders
    are active (adverse selection risk), signaling the bot to widen spreads
    or pause quoting.
    
    Uses the Lee-Ready tick rule: trades above midpoint are buy-initiated,
    trades below are sell-initiated. Trades at midpoint use the "side" field
    from the WebSocket event.
    
    V15.8: When trade data is insufficient (NO_DATA), falls back to a
    price-volatility proxy using the VolatilityTracker. If short-term
    price volatility is HIGH/EXTREME, applies a spread multiplier as if
    VPIN were elevated, since rapid price moves correlate with informed
    trading even when trade-level data is sparse.
    """

    def __init__(self, config, state_store=None, logger=None, vol_tracker=None):
        self.config = config
        self.state_store = state_store
        self.logger = logger
        self.vol_tracker = vol_tracker  # V15.8: For fallback when trade data insufficient
        # Per-token VPIN cache: {token_id: (vpin_value, timestamp)}
        self._cache = {}
        self._cache_ttl = 5.0  # Recompute every 5s max
        # Analytics
        self._total_checks = 0
        self._blocks = 0
        self._widens = 0
        self._no_data_count = 0        # V15.8: Track NO_DATA occurrences
        self._fallback_activations = 0  # V15.8: Track vol-fallback activations
        self._fallback_widens = 0       # V15.8: Fallback-triggered widens

    def compute_vpin(self, token_id):
        """
        Compute VPIN for a token using recent trades from WebSocket.
        Returns (vpin_value, num_trades) where vpin_value is in [0, 1].
        0 = perfectly balanced, 1 = completely one-sided.
        Returns (None, 0) if insufficient data.
        """
        now = time.time()
        # Check cache
        if token_id in self._cache:
            cached_vpin, cached_trades, cached_ts = self._cache[token_id]
            if now - cached_ts < self._cache_ttl:
                return cached_vpin, cached_trades

        if not self.state_store:
            return None, 0

        trades = self.state_store.get_recent_trades(
            token_id, max_age=self.config.vpin_lookback_secs)

        if len(trades) < self.config.vpin_min_trades:
            return None, 0

        buy_vol = 0.0
        sell_vol = 0.0
        for t in trades:
            size = float(t.get("size", 0))
            side = t.get("side", "").upper()
            if side == "BUY":
                buy_vol += size
            elif side == "SELL":
                sell_vol += size
            else:
                # Unknown side — split evenly (conservative)
                buy_vol += size / 2
                sell_vol += size / 2

        total_vol = buy_vol + sell_vol
        if total_vol <= 0:
            return None, 0

        vpin = abs(buy_vol - sell_vol) / total_vol
        self._cache[token_id] = (vpin, len(trades), now)
        return vpin, len(trades)

    def _vol_fallback_multiplier(self, token_up, token_down):
        """
        V15.8: Volatility-based fallback when VPIN has NO_DATA.
        
        Uses the VolatilityTracker to estimate flow toxicity from price
        movements. Rapid price changes correlate with informed trading,
        so HIGH/EXTREME volatility triggers a spread widen similar to
        ELEVATED VPIN.
        
        Returns (multiplier, level_str, pseudo_vpin).
        """
        if not self.vol_tracker:
            return 1.0, "NO_DATA", 0.0

        # Get vol level for both tokens, use the worse one
        vol_up, vol_sum_up = self.vol_tracker.get_volatility_level(token_up)
        vol_dn, vol_sum_dn = self.vol_tracker.get_volatility_level(token_down)

        # Pick the more volatile side
        vol_level = vol_up
        vol_sum = vol_sum_up
        if vol_dn in ("EXTREME", "HIGH") and vol_up not in ("EXTREME", "HIGH"):
            vol_level = vol_dn
            vol_sum = vol_sum_dn
        elif vol_dn == "HIGH" and vol_up != "EXTREME":
            vol_level = vol_dn
            vol_sum = vol_sum_dn

        self._fallback_activations += 1

        # Map volatility level to a pseudo-VPIN multiplier
        # EXTREME vol -> treat like TOXIC VPIN (block multiplier)
        # HIGH vol -> treat like ELEVATED VPIN (spread multiplier)
        # MEDIUM/LOW -> no adjustment (normal)
        vpin_fallback_enabled = getattr(self.config, 'vpin_vol_fallback_enabled', True)
        if not vpin_fallback_enabled:
            return 1.0, "NO_DATA", 0.0

        if vol_level == "EXTREME":
            self._fallback_widens += 1
            # Use block multiplier for extreme vol
            pseudo_vpin = 0.85  # Synthetic value for logging
            mult = self.config.vpin_block_multiplier
            if self.logger:
                self.logger.info(
                    "  VPIN FALLBACK | vol={} ({:.4f}) | Treating as TOXIC | "
                    "mult=x{:.1f}".format(vol_level, vol_sum or 0, mult))
            return mult, "VOL-TOXIC", pseudo_vpin
        elif vol_level == "HIGH":
            self._fallback_widens += 1
            pseudo_vpin = 0.55  # Synthetic value for logging
            mult = self.config.vpin_spread_multiplier
            if self.logger:
                self.logger.info(
                    "  VPIN FALLBACK | vol={} ({:.4f}) | Treating as ELEVATED | "
                    "mult=x{:.1f}".format(vol_level, vol_sum or 0, mult))
            return mult, "VOL-ELEVATED", pseudo_vpin
        else:
            return 1.0, "NO_DATA", 0.0

    def get_spread_multiplier(self, token_up, token_down):
        """
        Get the spread multiplier based on VPIN of both tokens.
        Returns (multiplier, vpin_level_str, max_vpin_value).
        
        multiplier:
          1.0  = normal (VPIN < 0.40 or insufficient data)
          vpin_spread_multiplier = elevated (0.40 <= VPIN < threshold)
          vpin_block_multiplier  = toxic (VPIN >= threshold)
        
        V15.8: When both tokens return NO_DATA, falls back to volatility-based
        proxy to provide some protection during data-sparse periods.
        """
        if not self.config.vpin_enabled:
            return 1.0, "OFF", 0.0

        self._total_checks += 1
        vpin_up, n_up = self.compute_vpin(token_up)
        vpin_dn, n_dn = self.compute_vpin(token_down)

        # Use the higher VPIN of the two tokens
        max_vpin = 0.0
        if vpin_up is not None:
            max_vpin = max(max_vpin, vpin_up)
        if vpin_dn is not None:
            max_vpin = max(max_vpin, vpin_dn)

        if max_vpin <= 0.0 and vpin_up is None and vpin_dn is None:
            self._no_data_count += 1
            # V15.8: Fall back to volatility-based proxy
            return self._vol_fallback_multiplier(token_up, token_down)

        if max_vpin >= self.config.vpin_threshold:
            self._blocks += 1
            return self.config.vpin_block_multiplier, "TOXIC", max_vpin
        elif max_vpin >= 0.40:
            self._widens += 1
            return self.config.vpin_spread_multiplier, "ELEVATED", max_vpin
        else:
            return 1.0, "NORMAL", max_vpin

    def get_stats(self):
        return {
            "total_checks": self._total_checks,
            "blocks": self._blocks,
            "widens": self._widens,
            "block_rate": self._blocks / max(1, self._total_checks),
            "no_data_count": self._no_data_count,
            "no_data_rate": self._no_data_count / max(1, self._total_checks),
            "fallback_activations": self._fallback_activations,
            "fallback_widens": self._fallback_widens,
        }


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
