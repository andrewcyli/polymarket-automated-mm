#!/usr/bin/env python3
"""
Polymarket User Channel WebSocket Diagnostic Tool v2
=====================================================

This script isolates the EXACT difference between the working diagnostic
(simple asyncio.run) and the failing ws_manager.py (background thread + 
asyncio.run_coroutine_threadsafe).

It tests 5 approaches:
1. Simple asyncio.run (known working from diagnostic v1)
2. Background thread with asyncio.new_event_loop (same as ws_manager.py)
3. Background thread + run_coroutine_threadsafe (lazy start, same as ws_manager.py user channel)
4. Background thread with max_size parameter (same as ws_manager.py)
5. Background thread with open_timeout parameter (same as ws_manager.py)

Usage:
    python ws_user_diagnostic2.py
"""

import asyncio
import json
import os
import sys
import time
import logging
import threading
import inspect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ws_diag2")

USER_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"


def mask(s: str, show: int = 8) -> str:
    if not s:
        return "<empty>"
    return s[:show] + "..." + f"({len(s)} chars)"


def load_env():
    env_file = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, value = line.partition("=")
                    os.environ.setdefault(key.strip(), value.strip())


def get_credentials():
    """Get L2 derived credentials."""
    load_env()
    private_key = os.environ.get("PK") or os.environ.get("PRIVATE_KEY")
    if not private_key:
        logger.error("No private key found. Set PK or PRIVATE_KEY in .env")
        sys.exit(1)

    from py_clob_client.client import ClobClient
    client = ClobClient("https://clob.polymarket.com", key=private_key, chain_id=137)
    creds = client.create_or_derive_api_creds()
    client.set_api_creds(creds)

    logger.info(f"L2 api_key: {mask(creds.api_key)}")
    return client, creds


def fetch_condition_id():
    """Fetch an active condition ID."""
    import requests
    resp = requests.get(
        "https://clob.polymarket.com/markets",
        params={"limit": 5, "active": "true"},
        timeout=10,
    )
    if resp.status_code == 200:
        data = resp.json()
        markets = data if isinstance(data, list) else data.get("data", [])
        for m in markets:
            cid = m.get("condition_id")
            if cid:
                return cid
    return None


# ─────────────────────────────────────────────────────────────
# Test 1: Simple asyncio.run (known working)
# ─────────────────────────────────────────────────────────────

async def test1_simple_asyncio(creds, condition_id, timeout=10):
    """Test with simple asyncio.run — known working from diagnostic v1."""
    import websockets

    auth_msg = {
        "auth": {
            "apiKey": creds.api_key,
            "secret": creds.api_secret,
            "passphrase": creds.api_passphrase,
        },
        "markets": [condition_id],
        "assets_ids": [],
        "type": "user",
        "initial_dump": True,
    }

    start = time.time()
    try:
        async with websockets.connect(
            USER_WS_URL,
            ping_interval=None,
            ping_timeout=None,
            close_timeout=5,
        ) as ws:
            logger.info(f"  [T1] Connected!")
            await ws.send(json.dumps(auth_msg))
            logger.info(f"  [T1] Auth sent")

            try:
                async with asyncio.timeout(timeout):
                    msg_count = 0
                    while True:
                        msg = await ws.recv()
                        msg_count += 1
                        elapsed = time.time() - start
                        logger.info(f"  [T1] [{elapsed:.1f}s] msg #{msg_count}: {str(msg)[:100]}")
            except asyncio.TimeoutError:
                elapsed = time.time() - start
                logger.info(f"  [T1] ✓ PASS — stayed alive {elapsed:.1f}s ({msg_count} msgs)")
                return True

    except Exception as e:
        elapsed = time.time() - start
        code = getattr(e, 'code', '?')
        logger.error(f"  [T1] ✗ FAIL — {elapsed:.1f}s — code={code} — {e}")
        return False


# ─────────────────────────────────────────────────────────────
# Test 2: Background thread with new_event_loop (like ws_manager.py)
# ─────────────────────────────────────────────────────────────

def test2_background_thread(creds, condition_id, timeout=10):
    """Test with background thread + asyncio.new_event_loop — same as ws_manager.py."""
    result = {"success": False}

    async def run_connection():
        import websockets

        auth_msg = {
            "auth": {
                "apiKey": creds.api_key,
                "secret": creds.api_secret,
                "passphrase": creds.api_passphrase,
            },
            "markets": [condition_id],
            "assets_ids": [],
            "type": "user",
            "initial_dump": True,
        }

        start = time.time()
        try:
            async with websockets.connect(
                USER_WS_URL,
                ping_interval=None,
                ping_timeout=None,
                close_timeout=5,
            ) as ws:
                logger.info(f"  [T2] Connected!")
                await ws.send(json.dumps(auth_msg))
                logger.info(f"  [T2] Auth sent")

                try:
                    async with asyncio.timeout(timeout):
                        msg_count = 0
                        while True:
                            msg = await ws.recv()
                            msg_count += 1
                            elapsed = time.time() - start
                            logger.info(f"  [T2] [{elapsed:.1f}s] msg #{msg_count}: {str(msg)[:100]}")
                except asyncio.TimeoutError:
                    elapsed = time.time() - start
                    logger.info(f"  [T2] ✓ PASS — stayed alive {elapsed:.1f}s ({msg_count} msgs)")
                    result["success"] = True

        except Exception as e:
            elapsed = time.time() - start
            code = getattr(e, 'code', '?')
            logger.error(f"  [T2] ✗ FAIL — {elapsed:.1f}s — code={code} — {e}")

    def thread_target():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_connection())
        finally:
            loop.close()

    t = threading.Thread(target=thread_target, daemon=True)
    t.start()
    t.join(timeout=timeout + 5)
    return result["success"]


# ─────────────────────────────────────────────────────────────
# Test 3: Background thread + run_coroutine_threadsafe (lazy start)
# ─────────────────────────────────────────────────────────────

def test3_lazy_start(creds, condition_id, timeout=10):
    """Test with run_coroutine_threadsafe — same as ws_manager.py user channel lazy start."""
    result = {"success": False}
    loop_holder = {"loop": None, "ready": threading.Event()}

    async def keep_loop_alive():
        """Keep the event loop running until signaled to stop."""
        loop_holder["ready"].set()
        # Just keep running — we'll stop it from the main thread
        while True:
            await asyncio.sleep(0.1)

    async def run_user_connection():
        import websockets

        auth_msg = {
            "auth": {
                "apiKey": creds.api_key,
                "secret": creds.api_secret,
                "passphrase": creds.api_passphrase,
            },
            "markets": [condition_id],
            "assets_ids": [],
            "type": "user",
            "initial_dump": True,
        }

        start = time.time()
        try:
            async with websockets.connect(
                USER_WS_URL,
                ping_interval=None,
                ping_timeout=None,
                close_timeout=5,
            ) as ws:
                logger.info(f"  [T3] Connected!")
                await ws.send(json.dumps(auth_msg))
                logger.info(f"  [T3] Auth sent")

                try:
                    async with asyncio.timeout(timeout):
                        msg_count = 0
                        while True:
                            msg = await ws.recv()
                            msg_count += 1
                            elapsed = time.time() - start
                            logger.info(f"  [T3] [{elapsed:.1f}s] msg #{msg_count}: {str(msg)[:100]}")
                except asyncio.TimeoutError:
                    elapsed = time.time() - start
                    logger.info(f"  [T3] ✓ PASS — stayed alive {elapsed:.1f}s ({msg_count} msgs)")
                    result["success"] = True

        except Exception as e:
            elapsed = time.time() - start
            code = getattr(e, 'code', '?')
            logger.error(f"  [T3] ✗ FAIL — {elapsed:.1f}s — code={code} — {e}")

    def thread_target():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop_holder["loop"] = loop
        try:
            loop.run_until_complete(keep_loop_alive())
        except asyncio.CancelledError:
            pass
        finally:
            # Clean up pending tasks
            try:
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()
                if pending:
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                pass
            loop.close()

    # Start background thread with event loop
    t = threading.Thread(target=thread_target, daemon=True)
    t.start()

    # Wait for loop to be ready
    loop_holder["ready"].wait(timeout=5)
    loop = loop_holder["loop"]

    if not loop:
        logger.error("  [T3] ✗ FAIL — event loop not created")
        return False

    # Schedule the user connection from the main thread (like ws_manager.py does)
    logger.info("  [T3] Scheduling connection via run_coroutine_threadsafe...")
    future = asyncio.run_coroutine_threadsafe(run_user_connection(), loop)

    # Wait for the connection test to complete
    try:
        future.result(timeout=timeout + 5)
    except Exception as e:
        logger.error(f"  [T3] Future error: {e}")

    # Stop the loop
    loop.call_soon_threadsafe(loop.stop)
    t.join(timeout=3)

    return result["success"]


# ─────────────────────────────────────────────────────────────
# Test 4: Same as T2 but with max_size (like ws_manager.py)
# ─────────────────────────────────────────────────────────────

def test4_with_max_size(creds, condition_id, timeout=10):
    """Test with max_size parameter — same as ws_manager.py."""
    result = {"success": False}

    async def run_connection():
        import websockets

        auth_msg = {
            "auth": {
                "apiKey": creds.api_key,
                "secret": creds.api_secret,
                "passphrase": creds.api_passphrase,
            },
            "markets": [condition_id],
            "assets_ids": [],
            "type": "user",
            "initial_dump": True,
        }

        start = time.time()
        try:
            async with websockets.connect(
                USER_WS_URL,
                ping_interval=None,
                ping_timeout=None,
                close_timeout=5,
                open_timeout=15,
                max_size=2**20,  # 1MB — same as ws_manager.py
            ) as ws:
                logger.info(f"  [T4] Connected!")
                await ws.send(json.dumps(auth_msg))
                logger.info(f"  [T4] Auth sent")

                try:
                    async with asyncio.timeout(timeout):
                        msg_count = 0
                        while True:
                            msg = await ws.recv()
                            msg_count += 1
                            elapsed = time.time() - start
                            logger.info(f"  [T4] [{elapsed:.1f}s] msg #{msg_count}: {str(msg)[:100]}")
                except asyncio.TimeoutError:
                    elapsed = time.time() - start
                    logger.info(f"  [T4] ✓ PASS — stayed alive {elapsed:.1f}s ({msg_count} msgs)")
                    result["success"] = True

        except Exception as e:
            elapsed = time.time() - start
            code = getattr(e, 'code', '?')
            logger.error(f"  [T4] ✗ FAIL — {elapsed:.1f}s — code={code} — {e}")

    def thread_target():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_connection())
        finally:
            loop.close()

    t = threading.Thread(target=thread_target, daemon=True)
    t.start()
    t.join(timeout=timeout + 5)
    return result["success"]


# ─────────────────────────────────────────────────────────────
# Test 5: Exact ws_manager.py replica with concurrent tasks
# ─────────────────────────────────────────────────────────────

def test5_concurrent_tasks(creds, condition_id, timeout=10):
    """
    Replicate the EXACT ws_manager.py pattern:
    - Background thread with new_event_loop
    - Multiple concurrent tasks (market + rtds + subscription processor)
    - User channel started via run_coroutine_threadsafe AFTER other tasks are running
    
    This tests whether the concurrent tasks interfere with the user channel.
    """
    result = {"success": False}
    loop_holder = {"loop": None, "ready": threading.Event()}

    async def fake_market_connection():
        """Simulate the market channel connection (always running)."""
        import websockets
        try:
            async with websockets.connect(
                "wss://ws-subscriptions-clob.polymarket.com/ws/market",
                ping_interval=None,
                ping_timeout=None,
                close_timeout=5,
            ) as ws:
                logger.info(f"  [T5] Market channel connected")
                # Subscribe to something
                await ws.send(json.dumps({"assets_ids": [], "type": "market"}))
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue
                    except Exception:
                        break
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.debug(f"  [T5] Market channel error: {e}")

    async def fake_subscription_processor():
        """Simulate the subscription queue processor (always running)."""
        try:
            while True:
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            pass

    async def user_connection():
        """The actual user channel connection test."""
        import websockets

        auth_msg = {
            "auth": {
                "apiKey": creds.api_key,
                "secret": creds.api_secret,
                "passphrase": creds.api_passphrase,
            },
            "markets": [condition_id],
            "assets_ids": [],
            "type": "user",
            "initial_dump": True,
        }

        start = time.time()
        try:
            async with websockets.connect(
                USER_WS_URL,
                ping_interval=None,
                ping_timeout=None,
                close_timeout=5,
                open_timeout=15,
                max_size=2**20,
            ) as ws:
                logger.info(f"  [T5] User channel connected!")
                await ws.send(json.dumps(auth_msg))
                logger.info(f"  [T5] Auth sent")

                # Also start a heartbeat task (like ws_manager.py)
                async def heartbeat():
                    try:
                        while True:
                            await asyncio.sleep(30)
                            await ws.send("PING")
                            logger.info(f"  [T5] Sent PING")
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        pass

                ping_task = asyncio.ensure_future(heartbeat())

                try:
                    async with asyncio.timeout(timeout):
                        msg_count = 0
                        while True:
                            msg = await ws.recv()
                            msg_count += 1
                            elapsed = time.time() - start
                            logger.info(f"  [T5] [{elapsed:.1f}s] msg #{msg_count}: {str(msg)[:100]}")
                except asyncio.TimeoutError:
                    elapsed = time.time() - start
                    logger.info(f"  [T5] ✓ PASS — stayed alive {elapsed:.1f}s ({msg_count} msgs)")
                    result["success"] = True
                finally:
                    ping_task.cancel()
                    try:
                        await ping_task
                    except asyncio.CancelledError:
                        pass

        except Exception as e:
            elapsed = time.time() - start
            code = getattr(e, 'code', '?')
            logger.error(f"  [T5] ✗ FAIL — {elapsed:.1f}s — code={code} — {e}")

    async def run_all():
        """Start market + sub processor, then schedule user connection."""
        loop_holder["ready"].set()

        tasks = [
            asyncio.create_task(fake_market_connection()),
            asyncio.create_task(fake_subscription_processor()),
        ]

        # Wait a moment (simulating market discovery delay)
        await asyncio.sleep(2.0)
        logger.info("  [T5] Scheduling user connection (like lazy start)...")

        # Add user connection as a new task
        user_task = asyncio.create_task(user_connection())
        tasks.append(user_task)

        # Wait for user task to complete
        await user_task

        # Cancel other tasks
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def thread_target():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop_holder["loop"] = loop
        try:
            loop.run_until_complete(run_all())
        except Exception as e:
            logger.error(f"  [T5] Loop error: {e}")
        finally:
            try:
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()
                if pending:
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                pass
            loop.close()

    t = threading.Thread(target=thread_target, daemon=True)
    t.start()
    t.join(timeout=timeout + 10)
    return result["success"]


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("WS USER CHANNEL DIAGNOSTIC v2")
    logger.info("Testing connection approaches to isolate 1006 cause")
    logger.info("=" * 60)

    # Get credentials
    logger.info("\n── Credentials ──")
    client, creds = get_credentials()
    condition_id = fetch_condition_id()
    if not condition_id:
        logger.error("Could not fetch condition ID")
        sys.exit(1)
    logger.info(f"Condition ID: {mask(condition_id, 12)}")

    results = {}

    # Test 1: Simple asyncio.run
    logger.info(f"\n{'='*60}")
    logger.info("TEST 1: Simple asyncio.run (known working)")
    logger.info(f"{'='*60}")
    results["T1_simple"] = asyncio.run(test1_simple_asyncio(creds, condition_id, timeout=10))

    time.sleep(1)  # Brief pause between tests

    # Test 2: Background thread
    logger.info(f"\n{'='*60}")
    logger.info("TEST 2: Background thread + new_event_loop")
    logger.info(f"{'='*60}")
    results["T2_bg_thread"] = test2_background_thread(creds, condition_id, timeout=10)

    time.sleep(1)

    # Test 3: Background thread + run_coroutine_threadsafe (lazy start)
    logger.info(f"\n{'='*60}")
    logger.info("TEST 3: Background thread + run_coroutine_threadsafe (lazy start)")
    logger.info(f"{'='*60}")
    results["T3_lazy_start"] = test3_lazy_start(creds, condition_id, timeout=10)

    time.sleep(1)

    # Test 4: With max_size parameter
    logger.info(f"\n{'='*60}")
    logger.info("TEST 4: Background thread + max_size=1MB + open_timeout=15s")
    logger.info(f"{'='*60}")
    results["T4_max_size"] = test4_with_max_size(creds, condition_id, timeout=10)

    time.sleep(1)

    # Test 5: Full concurrent tasks replica
    logger.info(f"\n{'='*60}")
    logger.info("TEST 5: Full ws_manager.py replica (concurrent tasks + lazy user start)")
    logger.info(f"{'='*60}")
    results["T5_concurrent"] = test5_concurrent_tasks(creds, condition_id, timeout=10)

    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("RESULTS SUMMARY")
    logger.info(f"{'='*60}")
    for name, success in results.items():
        status = "✓ PASS" if success else "✗ FAIL"
        logger.info(f"  {status} | {name}")

    logger.info(f"\n{'='*60}")
    logger.info("INTERPRETATION")
    logger.info(f"{'='*60}")

    if all(results.values()):
        logger.info("  All tests passed — the 1006 in ws_manager.py is caused by")
        logger.info("  something NOT replicated here. Check:")
        logger.info("    1. The _resubscribe_all() or _process_pending() methods")
        logger.info("    2. The heartbeat PING interfering with auth")
        logger.info("    3. Race condition between auth send and heartbeat start")
    elif results.get("T1_simple") and not results.get("T2_bg_thread"):
        logger.info("  Background thread causes 1006 — asyncio loop isolation issue")
    elif results.get("T2_bg_thread") and not results.get("T3_lazy_start"):
        logger.info("  run_coroutine_threadsafe causes 1006 — scheduling issue")
    elif results.get("T3_lazy_start") and not results.get("T5_concurrent"):
        logger.info("  Concurrent tasks cause 1006 — interference from market/rtds connections")
    else:
        failing = [k for k, v in results.items() if not v]
        logger.info(f"  Failing tests: {failing}")
        logger.info("  Compare failing vs passing to identify the root cause")

    logger.info(f"\n{'='*60}")
    logger.info("DIAGNOSTIC v2 COMPLETE")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    main()
