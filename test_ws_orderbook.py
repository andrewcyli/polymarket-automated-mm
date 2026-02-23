"""
Tests for Phase 3: WS-Enhanced Orderbook Reading
==================================================
Tests cover:
1. StateStore incremental book updates (apply_book_delta)
2. StateStore book freshness tracking
3. WSOrderBookReader three-tier priority (full book → bba → REST)
4. WSOrderBookReader liquidity computation
5. WSOrderBookReader spread computation
6. Integration: price_change events → book deltas → spread updates
"""

import time
import threading
import unittest
from unittest.mock import MagicMock, patch
from ws_manager import StateStore, EventBus, EventType
from ws_price_feed import WSOrderBookReader, WSPriceFeed


# ─────────────────────────────────────────────────────────────────
# StateStore: Incremental Book Updates
# ─────────────────────────────────────────────────────────────────

class TestStateStoreBookDeltas(unittest.TestCase):
    """Test apply_book_delta for incremental orderbook updates."""

    def setUp(self):
        self.store = StateStore()
        # Seed with a full snapshot
        self.store.update_orderbook("token_a", {
            "bids": [
                {"price": "0.45", "size": "100"},
                {"price": "0.44", "size": "200"},
                {"price": "0.43", "size": "50"},
            ],
            "asks": [
                {"price": "0.55", "size": "80"},
                {"price": "0.56", "size": "150"},
                {"price": "0.57", "size": "60"},
            ],
            "hash": "abc123",
            "market": "cond_1",
        })

    def test_update_existing_bid_level(self):
        """Updating size at an existing bid price level."""
        self.store.apply_book_delta("token_a", "BUY", "0.45", "250")
        book = self.store.get_orderbook("token_a")
        bids = book["bids"]
        level = next(b for b in bids if b["price"] == "0.45")
        self.assertEqual(level["size"], "250.0")
        self.assertEqual(len(bids), 3)  # Same number of levels

    def test_update_existing_ask_level(self):
        """Updating size at an existing ask price level."""
        self.store.apply_book_delta("token_a", "SELL", "0.55", "300")
        book = self.store.get_orderbook("token_a")
        asks = book["asks"]
        level = next(a for a in asks if a["price"] == "0.55")
        self.assertEqual(level["size"], "300.0")

    def test_add_new_bid_level(self):
        """Adding a new bid price level that didn't exist before."""
        self.store.apply_book_delta("token_a", "BUY", "0.46", "75")
        book = self.store.get_orderbook("token_a")
        bids = book["bids"]
        self.assertEqual(len(bids), 4)  # One new level
        level = next(b for b in bids if b["price"] == "0.46")
        self.assertEqual(level["size"], "75.0")

    def test_add_new_ask_level(self):
        """Adding a new ask price level."""
        self.store.apply_book_delta("token_a", "SELL", "0.54", "120")
        book = self.store.get_orderbook("token_a")
        asks = book["asks"]
        self.assertEqual(len(asks), 4)
        level = next(a for a in asks if a["price"] == "0.54")
        self.assertEqual(level["size"], "120.0")

    def test_remove_bid_level_zero_size(self):
        """Removing a bid level by setting size to 0."""
        self.store.apply_book_delta("token_a", "BUY", "0.44", "0")
        book = self.store.get_orderbook("token_a")
        bids = book["bids"]
        self.assertEqual(len(bids), 2)  # One level removed
        prices = [b["price"] for b in bids]
        self.assertNotIn("0.44", prices)

    def test_remove_ask_level_zero_size(self):
        """Removing an ask level by setting size to 0."""
        self.store.apply_book_delta("token_a", "SELL", "0.56", "0")
        book = self.store.get_orderbook("token_a")
        asks = book["asks"]
        self.assertEqual(len(asks), 2)

    def test_remove_nonexistent_level_noop(self):
        """Removing a level that doesn't exist is a no-op."""
        self.store.apply_book_delta("token_a", "BUY", "0.99", "0")
        book = self.store.get_orderbook("token_a")
        self.assertEqual(len(book["bids"]), 3)  # Unchanged

    def test_delta_on_unknown_token_noop(self):
        """Applying delta to a token with no snapshot is a no-op."""
        self.store.apply_book_delta("unknown_token", "BUY", "0.50", "100")
        self.assertIsNone(self.store.get_orderbook("unknown_token"))

    def test_update_count_increments(self):
        """Each delta increments the update_count."""
        book = self.store.get_orderbook("token_a")
        self.assertEqual(book["update_count"], 0)

        self.store.apply_book_delta("token_a", "BUY", "0.45", "110")
        book = self.store.get_orderbook("token_a")
        self.assertEqual(book["update_count"], 1)

        self.store.apply_book_delta("token_a", "SELL", "0.55", "90")
        book = self.store.get_orderbook("token_a")
        self.assertEqual(book["update_count"], 2)

    def test_timestamp_updates_on_delta(self):
        """Timestamp is refreshed on each delta."""
        book1 = self.store.get_orderbook("token_a")
        ts1 = book1["timestamp"]
        time.sleep(0.01)
        self.store.apply_book_delta("token_a", "BUY", "0.45", "110")
        book2 = self.store.get_orderbook("token_a")
        self.assertGreater(book2["timestamp"], ts1)

    def test_snapshot_ts_preserved_on_delta(self):
        """snapshot_ts stays the same after deltas (only resets on full snapshot)."""
        book1 = self.store.get_orderbook("token_a")
        snapshot_ts = book1["snapshot_ts"]
        self.store.apply_book_delta("token_a", "BUY", "0.45", "110")
        book2 = self.store.get_orderbook("token_a")
        self.assertEqual(book2["snapshot_ts"], snapshot_ts)

    def test_multiple_deltas_accumulate(self):
        """Multiple deltas applied sequentially produce correct final state."""
        # Add a new level
        self.store.apply_book_delta("token_a", "BUY", "0.46", "50")
        # Update it
        self.store.apply_book_delta("token_a", "BUY", "0.46", "75")
        # Remove an old level
        self.store.apply_book_delta("token_a", "BUY", "0.43", "0")

        book = self.store.get_orderbook("token_a")
        bids = book["bids"]
        self.assertEqual(len(bids), 3)  # 3 original - 1 removed + 1 added = 3
        level_46 = next(b for b in bids if b["price"] == "0.46")
        self.assertEqual(level_46["size"], "75.0")
        prices = [b["price"] for b in bids]
        self.assertNotIn("0.43", prices)

    def test_thread_safety_concurrent_deltas(self):
        """Multiple threads applying deltas concurrently don't corrupt state."""
        errors = []

        def apply_deltas(side, start_price, count):
            try:
                for i in range(count):
                    price = str(round(start_price + i * 0.01, 2))
                    self.store.apply_book_delta("token_a", side, price, "10")
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=apply_deltas, args=("BUY", 0.30, 20)),
            threading.Thread(target=apply_deltas, args=("SELL", 0.60, 20)),
            threading.Thread(target=apply_deltas, args=("BUY", 0.40, 10)),
            threading.Thread(target=apply_deltas, args=("SELL", 0.70, 10)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0)
        book = self.store.get_orderbook("token_a")
        self.assertIsNotNone(book)


class TestStateStoreBookFreshness(unittest.TestCase):
    """Test book freshness tracking."""

    def test_freshness_after_snapshot(self):
        store = StateStore()
        store.update_orderbook("t1", {"bids": [], "asks": []})
        fresh = store.get_book_freshness("t1")
        self.assertIsNotNone(fresh)
        self.assertLess(fresh["age_seconds"], 1.0)
        self.assertLess(fresh["snapshot_age"], 1.0)
        self.assertEqual(fresh["incremental_updates"], 0)

    def test_freshness_after_deltas(self):
        store = StateStore()
        store.update_orderbook("t1", {
            "bids": [{"price": "0.50", "size": "100"}],
            "asks": [{"price": "0.60", "size": "100"}],
        })
        store.apply_book_delta("t1", "BUY", "0.50", "200")
        store.apply_book_delta("t1", "SELL", "0.60", "150")
        fresh = store.get_book_freshness("t1")
        self.assertEqual(fresh["incremental_updates"], 2)

    def test_freshness_unknown_token(self):
        store = StateStore()
        self.assertIsNone(store.get_book_freshness("unknown"))

    def test_max_age_parameter(self):
        store = StateStore()
        store.update_orderbook("t1", {"bids": [], "asks": []})
        # Should be fresh with default max_age
        self.assertIsNotNone(store.get_orderbook("t1"))
        # Should be fresh with very short max_age
        self.assertIsNotNone(store.get_orderbook("t1", max_age=5.0))


# ─────────────────────────────────────────────────────────────────
# WSOrderBookReader: Three-Tier Priority
# ─────────────────────────────────────────────────────────────────

class TestWSOrderBookReaderTieredPriority(unittest.TestCase):
    """Test the three-tier priority: full book → bba → REST."""

    def setUp(self):
        self.legacy = MagicMock()
        self.store = StateStore()
        self.reader = WSOrderBookReader(self.legacy, self.store)

    def test_tier1_full_ws_book(self):
        """Tier 1: Full WS book is used when available."""
        self.store.update_orderbook("t1", {
            "bids": [{"price": "0.45", "size": "100"}],
            "asks": [{"price": "0.55", "size": "100"}],
        })
        spread = self.reader.get_spread("t1")
        self.assertIsNotNone(spread)
        self.assertAlmostEqual(spread["bid"], 0.45)
        self.assertAlmostEqual(spread["ask"], 0.55)
        self.assertAlmostEqual(spread["spread"], 0.10)
        self.assertEqual(self.reader._ws_book_hits, 1)
        self.assertEqual(self.reader._ws_bba_hits, 0)
        self.assertEqual(self.reader._fallback_count, 0)
        self.legacy.get_spread.assert_not_called()

    def test_tier2_bba_when_no_book(self):
        """Tier 2: best_bid_ask used when no full book available."""
        self.store.update_best_bid_ask("t1", "0.45", "0.55", "0.10")
        spread = self.reader.get_spread("t1")
        self.assertIsNotNone(spread)
        self.assertAlmostEqual(spread["bid"], 0.45)
        self.assertAlmostEqual(spread["ask"], 0.55)
        self.assertAlmostEqual(spread["spread"], 0.10)
        self.assertEqual(self.reader._ws_book_hits, 0)
        self.assertEqual(self.reader._ws_bba_hits, 1)
        self.assertEqual(self.reader._fallback_count, 0)

    def test_tier3_rest_when_no_ws_data(self):
        """Tier 3: REST fallback when no WS data available."""
        self.legacy.get_spread.return_value = {
            "bid": 0.45, "ask": 0.55, "spread": 0.10
        }
        spread = self.reader.get_spread("t1")
        self.assertIsNotNone(spread)
        self.assertEqual(self.reader._ws_book_hits, 0)
        self.assertEqual(self.reader._ws_bba_hits, 0)
        self.assertEqual(self.reader._fallback_count, 1)
        self.legacy.get_spread.assert_called_once_with("t1")

    def test_tier1_preferred_over_tier2(self):
        """Full book is preferred even when bba is also available."""
        self.store.update_orderbook("t1", {
            "bids": [{"price": "0.46", "size": "100"}],
            "asks": [{"price": "0.54", "size": "100"}],
        })
        self.store.update_best_bid_ask("t1", "0.45", "0.55", "0.10")
        spread = self.reader.get_spread("t1")
        # Should use the book values, not bba
        self.assertAlmostEqual(spread["bid"], 0.46)
        self.assertAlmostEqual(spread["ask"], 0.54)
        self.assertEqual(self.reader._ws_book_hits, 1)
        self.assertEqual(self.reader._ws_bba_hits, 0)

    def test_bba_invalid_values_fall_through(self):
        """Invalid bba values fall through to REST."""
        self.store.update_best_bid_ask("t1", "0", "0", "0")
        self.legacy.get_spread.return_value = {"bid": 0.45, "ask": 0.55}
        spread = self.reader.get_spread("t1")
        self.assertEqual(self.reader._fallback_count, 1)

    def test_bba_inverted_spread_fall_through(self):
        """Inverted spread (bid > ask) falls through to REST."""
        self.store.update_best_bid_ask("t1", "0.60", "0.40", "-0.20")
        self.legacy.get_spread.return_value = {"bid": 0.45, "ask": 0.55}
        spread = self.reader.get_spread("t1")
        self.assertEqual(self.reader._fallback_count, 1)


class TestWSOrderBookReaderGetBook(unittest.TestCase):
    """Test get_book method."""

    def setUp(self):
        self.legacy = MagicMock()
        self.store = StateStore()
        self.reader = WSOrderBookReader(self.legacy, self.store)

    def test_ws_book_returned(self):
        self.store.update_orderbook("t1", {
            "bids": [{"price": "0.50", "size": "100"}],
            "asks": [{"price": "0.60", "size": "100"}],
        })
        book = self.reader.get_book("t1")
        self.assertIsNotNone(book)
        self.assertEqual(len(book["bids"]), 1)
        self.assertEqual(self.reader._ws_book_hits, 1)
        self.legacy.get_book.assert_not_called()

    def test_rest_fallback(self):
        self.legacy.get_book.return_value = {"bids": [], "asks": []}
        book = self.reader.get_book("t1")
        self.assertEqual(self.reader._fallback_count, 1)
        self.legacy.get_book.assert_called_once_with("t1")

    def test_source_tracking(self):
        self.store.update_orderbook("t1", {"bids": [], "asks": []})
        self.reader.get_book("t1")
        self.assertEqual(self.reader._last_source["t1"], "ws_book")

        self.legacy.get_book.return_value = {"bids": [], "asks": []}
        self.reader.get_book("t2")
        self.assertEqual(self.reader._last_source["t2"], "rest")


# ─────────────────────────────────────────────────────────────────
# WSOrderBookReader: Liquidity Computation
# ─────────────────────────────────────────────────────────────────

class TestWSOrderBookReaderLiquidity(unittest.TestCase):
    """Test get_available_liquidity with WS book data."""

    def setUp(self):
        self.legacy = MagicMock()
        self.store = StateStore()
        self.reader = WSOrderBookReader(self.legacy, self.store)
        self.store.update_orderbook("t1", {
            "bids": [
                {"price": "0.50", "size": "100"},
                {"price": "0.49", "size": "200"},
                {"price": "0.48", "size": "300"},
            ],
            "asks": [
                {"price": "0.55", "size": "80"},
                {"price": "0.56", "size": "150"},
                {"price": "0.57", "size": "200"},
            ],
        })

    def test_buy_liquidity_sufficient(self):
        """Buy liquidity: enough asks to fill the order."""
        result = self.reader.get_available_liquidity("t1", "BUY", 0.55, 50)
        self.assertTrue(result["available"])
        self.assertEqual(result["fillable_size"], 50)
        self.assertAlmostEqual(result["avg_price"], 0.55)

    def test_buy_liquidity_crosses_levels(self):
        """Buy liquidity: needs to cross multiple ask levels."""
        result = self.reader.get_available_liquidity("t1", "BUY", 0.55, 100)
        self.assertTrue(result["available"])
        self.assertEqual(result["fillable_size"], 100)
        # 80 @ 0.55 + 20 @ 0.56 = 44 + 11.2 = 55.2 / 100 = 0.552
        expected_avg = (80 * 0.55 + 20 * 0.56) / 100
        self.assertAlmostEqual(result["avg_price"], expected_avg, places=4)

    def test_sell_liquidity_sufficient(self):
        """Sell liquidity: enough bids to fill the order."""
        result = self.reader.get_available_liquidity("t1", "SELL", 0.50, 50)
        self.assertTrue(result["available"])
        self.assertEqual(result["fillable_size"], 50)

    def test_insufficient_liquidity(self):
        """Not enough liquidity for the full order."""
        result = self.reader.get_available_liquidity("t1", "BUY", 0.55, 1000)
        self.assertFalse(result["available"])  # 430 < 1000 * 0.5
        self.assertEqual(result["fillable_size"], 430)  # 80 + 150 + 200

    def test_slippage_calculation(self):
        """Slippage is computed correctly."""
        result = self.reader.get_available_liquidity("t1", "BUY", 0.55, 200)
        self.assertGreater(result["slippage"], 0)  # Some slippage expected

    def test_rest_fallback_when_no_ws_book(self):
        """Falls back to legacy when no WS book available."""
        self.legacy.get_available_liquidity.return_value = {
            "available": True, "fillable_size": 100, "avg_price": 0.55, "slippage": 0.01
        }
        result = self.reader.get_available_liquidity("t2", "BUY", 0.55, 100)
        self.assertTrue(result["available"])
        self.legacy.get_available_liquidity.assert_called_once()


# ─────────────────────────────────────────────────────────────────
# WSOrderBookReader: Spread Computation
# ─────────────────────────────────────────────────────────────────

class TestWSOrderBookReaderSpread(unittest.TestCase):
    """Test spread computation from WS book data."""

    def test_normal_spread(self):
        legacy = MagicMock()
        store = StateStore()
        reader = WSOrderBookReader(legacy, store)
        store.update_orderbook("t1", {
            "bids": [
                {"price": "0.45", "size": "100"},
                {"price": "0.44", "size": "200"},
            ],
            "asks": [
                {"price": "0.55", "size": "80"},
                {"price": "0.56", "size": "150"},
            ],
        })
        spread = reader.get_spread("t1")
        self.assertAlmostEqual(spread["bid"], 0.45)
        self.assertAlmostEqual(spread["ask"], 0.55)
        self.assertAlmostEqual(spread["spread"], 0.10)
        self.assertAlmostEqual(spread["midpoint"], 0.50)
        self.assertEqual(spread["total_bid_size"], 300)
        self.assertEqual(spread["total_ask_size"], 230)

    def test_imbalance_calculation(self):
        """Order book imbalance is computed correctly."""
        legacy = MagicMock()
        store = StateStore()
        reader = WSOrderBookReader(legacy, store)
        store.update_orderbook("t1", {
            "bids": [{"price": "0.50", "size": "300"}],
            "asks": [{"price": "0.60", "size": "100"}],
        })
        spread = reader.get_spread("t1")
        # imbalance = (300 - 100) / (300 + 100) = 0.5
        self.assertAlmostEqual(spread["imbalance"], 0.5)

    def test_filters_extreme_prices(self):
        """Bids below 0.05 and asks above 0.95 are filtered."""
        legacy = MagicMock()
        store = StateStore()
        reader = WSOrderBookReader(legacy, store)
        store.update_orderbook("t1", {
            "bids": [
                {"price": "0.01", "size": "1000"},  # Filtered
                {"price": "0.45", "size": "100"},
            ],
            "asks": [
                {"price": "0.55", "size": "80"},
                {"price": "0.99", "size": "1000"},  # Filtered
            ],
        })
        spread = reader.get_spread("t1")
        self.assertAlmostEqual(spread["bid"], 0.45)
        self.assertAlmostEqual(spread["ask"], 0.55)

    def test_empty_book_returns_none(self):
        legacy = MagicMock()
        store = StateStore()
        reader = WSOrderBookReader(legacy, store)
        store.update_orderbook("t1", {"bids": [], "asks": []})
        # Empty book should fall through to bba or REST
        legacy.get_spread.return_value = None
        spread = reader.get_spread("t1")
        self.assertIsNone(spread)


# ─────────────────────────────────────────────────────────────────
# WSOrderBookReader: Stats & Diagnostics
# ─────────────────────────────────────────────────────────────────

class TestWSOrderBookReaderStats(unittest.TestCase):
    """Test stats and diagnostics."""

    def test_stats_initial(self):
        reader = WSOrderBookReader(MagicMock(), StateStore())
        stats = reader.get_stats()
        self.assertEqual(stats["ws_book_hits"], 0)
        self.assertEqual(stats["ws_bba_hits"], 0)
        self.assertEqual(stats["fallbacks"], 0)
        self.assertEqual(stats["ws_hits"], 0)

    def test_stats_after_mixed_usage(self):
        legacy = MagicMock()
        legacy.get_spread.return_value = {"bid": 0.45, "ask": 0.55}
        legacy.get_book.return_value = {"bids": [], "asks": []}
        store = StateStore()
        reader = WSOrderBookReader(legacy, store)

        # WS book hit
        store.update_orderbook("t1", {
            "bids": [{"price": "0.45", "size": "100"}],
            "asks": [{"price": "0.55", "size": "100"}],
        })
        reader.get_spread("t1")  # ws_book_hit

        # BBA hit
        store.update_best_bid_ask("t2", "0.40", "0.60", "0.20")
        reader.get_spread("t2")  # ws_bba_hit

        # REST fallback
        reader.get_spread("t3")  # fallback

        stats = reader.get_stats()
        self.assertEqual(stats["ws_book_hits"], 1)
        self.assertEqual(stats["ws_bba_hits"], 1)
        self.assertEqual(stats["fallbacks"], 1)
        self.assertEqual(stats["ws_hits"], 2)  # book + bba
        self.assertAlmostEqual(stats["hit_rate"], 2/3, places=2)

    def test_book_freshness(self):
        store = StateStore()
        reader = WSOrderBookReader(MagicMock(), store)
        store.update_orderbook("t1", {"bids": [], "asks": []})
        fresh = reader.get_book_freshness("t1")
        self.assertIsNotNone(fresh)
        self.assertLess(fresh["age_seconds"], 1.0)

    def test_book_freshness_unknown(self):
        reader = WSOrderBookReader(MagicMock(), StateStore())
        self.assertIsNone(reader.get_book_freshness("unknown"))

    def test_backward_compat_ws_hit_count(self):
        """_ws_hit_count property returns total of book + bba hits."""
        reader = WSOrderBookReader(MagicMock(), StateStore())
        reader._ws_book_hits = 5
        reader._ws_bba_hits = 3
        self.assertEqual(reader._ws_hit_count, 8)


# ─────────────────────────────────────────────────────────────────
# WSPriceFeed: Basic Tests
# ─────────────────────────────────────────────────────────────────

class TestWSPriceFeedBasic(unittest.TestCase):
    """Test WSPriceFeed with StateStore integration."""

    def test_ws_price_priority(self):
        legacy = MagicMock()
        store = StateStore()
        feed = WSPriceFeed(legacy, store)

        store.update_crypto_price("btc", 50000.0, "binance")
        price = feed.get_current_price("btc")
        self.assertEqual(price, 50000.0)
        self.assertEqual(feed._ws_hit_count, 1)
        legacy.get_current_price.assert_not_called()

    def test_rest_fallback(self):
        legacy = MagicMock()
        legacy.get_current_price.return_value = 49000.0
        store = StateStore()
        feed = WSPriceFeed(legacy, store)

        price = feed.get_current_price("btc")
        self.assertEqual(price, 49000.0)
        self.assertEqual(feed._fallback_count, 1)

    def test_stats(self):
        legacy = MagicMock()
        store = StateStore()
        feed = WSPriceFeed(legacy, store)
        store.update_crypto_price("btc", 50000.0, "binance")
        feed.get_current_price("btc")
        stats = feed.get_stats()
        self.assertEqual(stats["ws_hits"], 1)
        self.assertEqual(stats["fallbacks"], 0)


# ─────────────────────────────────────────────────────────────────
# Integration: price_change → book delta → spread update
# ─────────────────────────────────────────────────────────────────

class TestIntegrationPriceChangeToSpread(unittest.TestCase):
    """Test end-to-end: price_change events update book → spread changes."""

    def test_price_change_updates_spread(self):
        """Simulating a price_change event that updates the book."""
        store = StateStore()
        reader = WSOrderBookReader(MagicMock(), store)

        # Initial snapshot
        store.update_orderbook("t1", {
            "bids": [
                {"price": "0.45", "size": "100"},
                {"price": "0.44", "size": "200"},
            ],
            "asks": [
                {"price": "0.55", "size": "80"},
                {"price": "0.56", "size": "150"},
            ],
        })

        spread1 = reader.get_spread("t1")
        self.assertAlmostEqual(spread1["bid"], 0.45)
        self.assertAlmostEqual(spread1["ask"], 0.55)

        # Simulate price_change: new bid at 0.47 (tighter spread)
        store.apply_book_delta("t1", "BUY", "0.47", "50")

        spread2 = reader.get_spread("t1")
        self.assertAlmostEqual(spread2["bid"], 0.47)  # New best bid
        self.assertAlmostEqual(spread2["ask"], 0.55)
        self.assertAlmostEqual(spread2["spread"], 0.08)  # Tighter spread

    def test_ask_removal_widens_spread(self):
        """Removing the best ask widens the spread."""
        store = StateStore()
        reader = WSOrderBookReader(MagicMock(), store)

        store.update_orderbook("t1", {
            "bids": [{"price": "0.45", "size": "100"}],
            "asks": [
                {"price": "0.55", "size": "80"},
                {"price": "0.60", "size": "150"},
            ],
        })

        spread1 = reader.get_spread("t1")
        self.assertAlmostEqual(spread1["ask"], 0.55)

        # Remove best ask (order cancelled)
        store.apply_book_delta("t1", "SELL", "0.55", "0")

        spread2 = reader.get_spread("t1")
        self.assertAlmostEqual(spread2["ask"], 0.60)  # Next best ask
        self.assertAlmostEqual(spread2["spread"], 0.15)  # Wider spread

    def test_full_snapshot_resets_deltas(self):
        """A new full snapshot resets the update_count."""
        store = StateStore()
        store.update_orderbook("t1", {
            "bids": [{"price": "0.45", "size": "100"}],
            "asks": [{"price": "0.55", "size": "80"}],
        })
        store.apply_book_delta("t1", "BUY", "0.46", "50")
        store.apply_book_delta("t1", "BUY", "0.47", "30")
        fresh1 = store.get_book_freshness("t1")
        self.assertEqual(fresh1["incremental_updates"], 2)

        # New snapshot
        store.update_orderbook("t1", {
            "bids": [{"price": "0.48", "size": "200"}],
            "asks": [{"price": "0.52", "size": "100"}],
        })
        fresh2 = store.get_book_freshness("t1")
        self.assertEqual(fresh2["incremental_updates"], 0)


if __name__ == "__main__":
    unittest.main()
