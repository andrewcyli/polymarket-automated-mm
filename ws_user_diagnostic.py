#!/usr/bin/env python3
"""
Polymarket User Channel WebSocket Diagnostic Tool
==================================================

Isolated script to troubleshoot the user channel 1006 disconnect.
Tests multiple auth approaches independently without touching the main bot code.

Usage:
    python ws_user_diagnostic.py

Requirements:
    pip install websockets py-clob-client

The script will:
1. Derive L2 credentials and display them (masked)
2. Test 5 different auth approaches against the user channel
3. Report which approach succeeds
"""

import asyncio
import json
import os
import sys
import time
import logging
from datetime import datetime

# ── Logging ──
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ws_diagnostic")

# ── Config ──
USER_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# Use a known active condition ID for testing
# This will be fetched dynamically from the CLOB API
TEST_CONDITION_ID = None


def mask(s: str, show: int = 8) -> str:
    """Mask a string, showing only the first `show` characters."""
    if not s:
        return "<empty>"
    return s[:show] + "..." + f"({len(s)} chars)"


def load_env():
    """Load environment variables from .env file if present."""
    env_file = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, value = line.partition("=")
                    os.environ.setdefault(key.strip(), value.strip())


def get_l1_credentials():
    """Get L1 credentials from environment."""
    load_env()
    private_key = os.environ.get("PK") or os.environ.get("PRIVATE_KEY")
    if not private_key:
        logger.error("No private key found. Set PK or PRIVATE_KEY in .env or environment.")
        sys.exit(1)
    return private_key


def derive_l2_credentials(private_key: str):
    """Derive L2 API credentials using the py-clob-client."""
    try:
        from py_clob_client.client import ClobClient
    except ImportError:
        logger.error("py-clob-client not installed. Run: pip install py-clob-client")
        sys.exit(1)

    logger.info("Creating ClobClient and deriving L2 credentials...")
    host = "https://clob.polymarket.com"
    chain_id = 137  # Polygon mainnet

    client = ClobClient(host, key=private_key, chain_id=chain_id)
    creds = client.create_or_derive_api_creds()
    client.set_api_creds(creds)

    logger.info(f"  L2 api_key:        {mask(creds.api_key)}")
    logger.info(f"  L2 api_secret:     {mask(creds.api_secret)}")
    logger.info(f"  L2 api_passphrase: {mask(creds.api_passphrase)}")
    logger.info(f"  Wallet address:    {client.get_address()}")

    return client, creds


def fetch_active_condition_id(client):
    """Fetch an active condition ID from the CLOB API."""
    try:
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
                    logger.info(f"  Using condition ID: {mask(cid, 12)}")
                    return cid
    except Exception as e:
        logger.warning(f"  Could not fetch active market: {e}")

    # Fallback: use a known BTC market condition ID format
    logger.warning("  Using fallback condition ID (may not be active)")
    return "0x0000000000000000000000000000000000000000000000000000000000000001"


async def test_connection(test_name: str, url: str, subscription_msg: dict,
                          extra_headers: dict = None, timeout: float = 10.0) -> dict:
    """
    Test a single WebSocket connection approach.
    
    Returns:
        dict with keys: success, duration, messages_received, close_code, close_reason, error
    """
    result = {
        "test": test_name,
        "success": False,
        "duration": 0,
        "messages_received": 0,
        "close_code": None,
        "close_reason": None,
        "error": None,
        "first_message": None,
    }

    try:
        import websockets
        import inspect

        # Detect websockets version for correct parameter name
        sig = inspect.signature(websockets.connect)
        if "additional_headers" in sig.parameters:
            header_kwarg = "additional_headers"
        else:
            header_kwarg = "extra_headers"

        connect_kwargs = {
            "uri": url,
            "ping_interval": None,  # We'll handle PING manually
            "ping_timeout": None,
            "close_timeout": 5,
        }
        if extra_headers:
            connect_kwargs[header_kwarg] = extra_headers

        logger.info(f"\n{'='*60}")
        logger.info(f"TEST: {test_name}")
        logger.info(f"{'='*60}")
        logger.info(f"  URL: {url}")
        if extra_headers:
            logger.info(f"  Headers: {json.dumps({k: mask(v) for k, v in extra_headers.items()})}")
        logger.info(f"  Subscription: {json.dumps(subscription_msg, indent=2)[:200]}...")

        start = time.time()

        async with websockets.connect(**connect_kwargs) as ws:
            logger.info(f"  ✓ TCP connected!")

            # Send subscription message
            await ws.send(json.dumps(subscription_msg))
            logger.info(f"  ✓ Subscription sent!")

            # Wait for messages or disconnection
            try:
                async with asyncio.timeout(timeout):
                    while True:
                        msg = await ws.recv()
                        result["messages_received"] += 1
                        elapsed = time.time() - start

                        if isinstance(msg, str):
                            if msg == "PONG":
                                logger.info(f"  [{elapsed:.1f}s] PONG received")
                            else:
                                try:
                                    data = json.loads(msg)
                                    logger.info(f"  [{elapsed:.1f}s] Message #{result['messages_received']}: {json.dumps(data)[:150]}")
                                    if result["first_message"] is None:
                                        result["first_message"] = data
                                except json.JSONDecodeError:
                                    logger.info(f"  [{elapsed:.1f}s] Non-JSON: {msg[:100]}")
                        else:
                            logger.info(f"  [{elapsed:.1f}s] Binary message ({len(msg)} bytes)")

                        # If we got at least one real message, send a PING to keep alive
                        if result["messages_received"] == 1:
                            await ws.send("PING")
                            logger.info(f"  [{elapsed:.1f}s] Sent PING")

            except asyncio.TimeoutError:
                elapsed = time.time() - start
                if result["messages_received"] > 0:
                    logger.info(f"  ✓ Connection stayed alive for {elapsed:.1f}s with {result['messages_received']} messages")
                    result["success"] = True
                else:
                    logger.info(f"  ⚠ No messages received in {elapsed:.1f}s (but connection stayed open)")
                    result["success"] = True  # Connection didn't drop, that's good

        result["duration"] = time.time() - start

    except Exception as e:
        result["duration"] = time.time() - start if 'start' in dir() else 0
        error_str = str(e)

        # Parse close code from websockets exceptions
        if hasattr(e, 'code'):
            result["close_code"] = e.code
        if hasattr(e, 'reason'):
            result["close_reason"] = e.reason
        if "1006" in error_str:
            result["close_code"] = 1006

        result["error"] = error_str
        logger.error(f"  ✗ Failed after {result['duration']:.1f}s: {error_str[:200]}")

    return result


async def run_diagnostics():
    """Run all diagnostic tests."""
    logger.info("=" * 60)
    logger.info("POLYMARKET USER CHANNEL WS DIAGNOSTIC")
    logger.info("=" * 60)
    logger.info(f"Time: {datetime.now().isoformat()}")
    logger.info(f"Python: {sys.version}")

    # ── Step 1: Derive L2 credentials ──
    logger.info("\n── Step 1: Derive L2 Credentials ──")
    private_key = get_l1_credentials()
    client, creds = derive_l2_credentials(private_key)
    address = client.get_address()

    # ── Step 2: Fetch active condition ID ──
    logger.info("\n── Step 2: Fetch Active Condition ID ──")
    condition_id = fetch_active_condition_id(client)

    # ── Step 3: Verify market channel works (baseline) ──
    logger.info("\n── Step 3: Baseline - Market Channel (no auth needed) ──")
    
    # Get a token ID for market channel test
    token_id = None
    try:
        import requests
        resp = requests.get(f"https://clob.polymarket.com/markets/{condition_id}", timeout=10)
        if resp.status_code == 200:
            market_data = resp.json()
            tokens = market_data.get("tokens", [])
            if tokens:
                token_id = tokens[0].get("token_id")
                logger.info(f"  Token ID: {mask(token_id, 12) if token_id else 'N/A'}")
    except Exception as e:
        logger.warning(f"  Could not fetch token ID: {e}")

    if token_id:
        baseline = await test_connection(
            "BASELINE: Market channel (no auth)",
            MARKET_WS_URL,
            {
                "assets_ids": [token_id],
                "type": "market",
            },
            timeout=8,
        )
    else:
        logger.warning("  Skipping baseline test (no token ID)")
        baseline = {"success": False}

    # ── Step 4: Test user channel with different auth approaches ──
    logger.info("\n── Step 4: User Channel Auth Tests ──")

    tests = []

    # Test A: Official docs format (raw L2 creds in body)
    tests.append(await test_connection(
        "A: Official docs format (L2 creds in body, with condition ID)",
        USER_WS_URL,
        {
            "auth": {
                "apiKey": creds.api_key,
                "secret": creds.api_secret,
                "passphrase": creds.api_passphrase,
            },
            "markets": [condition_id],
            "type": "user",
        },
        timeout=8,
    ))

    # Test B: With assets_ids and initial_dump (from TypeScript example)
    tests.append(await test_connection(
        "B: TypeScript example format (with assets_ids + initial_dump)",
        USER_WS_URL,
        {
            "auth": {
                "apiKey": creds.api_key,
                "secret": creds.api_secret,
                "passphrase": creds.api_passphrase,
            },
            "markets": [condition_id],
            "assets_ids": [],
            "type": "user",
            "initial_dump": True,
        },
        timeout=8,
    ))

    # Test C: Auth in HTTP headers instead of body (like REST API L2 headers)
    from py_clob_client.signing.hmac import build_hmac_signature
    timestamp = str(int(time.time()))
    hmac_sig = build_hmac_signature(
        creds.api_secret,
        timestamp,
        "GET",
        "/ws/user",
    )
    tests.append(await test_connection(
        "C: L2 HMAC auth in HTTP headers (REST-style)",
        USER_WS_URL,
        {
            "markets": [condition_id],
            "type": "user",
        },
        extra_headers={
            "POLY_ADDRESS": address,
            "POLY_SIGNATURE": hmac_sig,
            "POLY_TIMESTAMP": timestamp,
            "POLY_API_KEY": creds.api_key,
            "POLY_PASSPHRASE": creds.api_passphrase,
        },
        timeout=8,
    ))

    # Test D: Auth in both headers AND body
    timestamp2 = str(int(time.time()))
    hmac_sig2 = build_hmac_signature(
        creds.api_secret,
        timestamp2,
        "GET",
        "/ws/user",
    )
    tests.append(await test_connection(
        "D: L2 auth in BOTH headers AND body",
        USER_WS_URL,
        {
            "auth": {
                "apiKey": creds.api_key,
                "secret": creds.api_secret,
                "passphrase": creds.api_passphrase,
            },
            "markets": [condition_id],
            "type": "user",
        },
        extra_headers={
            "POLY_ADDRESS": address,
            "POLY_SIGNATURE": hmac_sig2,
            "POLY_TIMESTAMP": timestamp2,
            "POLY_API_KEY": creds.api_key,
            "POLY_PASSPHRASE": creds.api_passphrase,
        },
        timeout=8,
    ))

    # Test E: Empty markets list (to confirm this is what causes 1006)
    tests.append(await test_connection(
        "E: Empty markets list (expect 1006 - confirms root cause)",
        USER_WS_URL,
        {
            "auth": {
                "apiKey": creds.api_key,
                "secret": creds.api_secret,
                "passphrase": creds.api_passphrase,
            },
            "markets": [],
            "type": "user",
        },
        timeout=8,
    ))

    # Test F: Delayed subscription (connect first, wait 1s, then subscribe)
    logger.info(f"\n{'='*60}")
    logger.info(f"TEST: F: Delayed subscription (1s delay after connect)")
    logger.info(f"{'='*60}")
    test_f_result = {
        "test": "F: Delayed subscription (1s delay after connect)",
        "success": False,
        "duration": 0,
        "messages_received": 0,
        "close_code": None,
        "close_reason": None,
        "error": None,
        "first_message": None,
    }
    try:
        import websockets
        import inspect
        sig = inspect.signature(websockets.connect)
        header_kwarg = "additional_headers" if "additional_headers" in sig.parameters else "extra_headers"

        start = time.time()
        async with websockets.connect(
            USER_WS_URL,
            ping_interval=None,
            ping_timeout=None,
            close_timeout=5,
        ) as ws:
            logger.info(f"  ✓ TCP connected! Waiting 1s before subscribing...")
            await asyncio.sleep(1.0)

            sub_msg = {
                "auth": {
                    "apiKey": creds.api_key,
                    "secret": creds.api_secret,
                    "passphrase": creds.api_passphrase,
                },
                "markets": [condition_id],
                "type": "user",
            }
            await ws.send(json.dumps(sub_msg))
            logger.info(f"  ✓ Subscription sent after 1s delay!")

            try:
                async with asyncio.timeout(8):
                    while True:
                        msg = await ws.recv()
                        test_f_result["messages_received"] += 1
                        elapsed = time.time() - start
                        logger.info(f"  [{elapsed:.1f}s] Message: {str(msg)[:150]}")
            except asyncio.TimeoutError:
                elapsed = time.time() - start
                logger.info(f"  ✓ Connection stayed alive for {elapsed:.1f}s")
                test_f_result["success"] = True

        test_f_result["duration"] = time.time() - start
    except Exception as e:
        test_f_result["error"] = str(e)
        if hasattr(e, 'code'):
            test_f_result["close_code"] = e.code
        logger.error(f"  ✗ Failed: {str(e)[:200]}")

    tests.append(test_f_result)

    # ── Summary ──
    logger.info("\n" + "=" * 60)
    logger.info("RESULTS SUMMARY")
    logger.info("=" * 60)
    logger.info(f"  Baseline (market channel): {'✓ PASS' if baseline.get('success') else '✗ FAIL'}")
    logger.info("")

    for t in tests:
        status = "✓ PASS" if t["success"] else "✗ FAIL"
        code = f" (code={t['close_code']})" if t["close_code"] else ""
        msgs = f" [{t['messages_received']} msgs]" if t["messages_received"] else ""
        logger.info(f"  {status} | {t['test']}{code}{msgs}")

    logger.info("")
    logger.info("INTERPRETATION:")

    any_success = any(t["success"] for t in tests)
    if any_success:
        winners = [t["test"] for t in tests if t["success"]]
        logger.info(f"  ✓ Working approach(es): {', '.join(winners)}")
        logger.info(f"  → Use this format in ws_manager.py")
    else:
        all_1006 = all(t.get("close_code") == 1006 for t in tests)
        if all_1006:
            logger.info("  ✗ ALL tests got 1006 — likely an L2 credential issue.")
            logger.info("    Possible causes:")
            logger.info("    1. L2 creds expired — try regenerating: client.create_api_creds()")
            logger.info("    2. L2 creds are for wrong chain (should be Polygon 137)")
            logger.info("    3. Account doesn't have trading permissions")
            logger.info("    4. Polymarket user WS server is currently down/broken (see issue #303)")
        else:
            logger.info("  ✗ Mixed results — check individual test errors above")

    logger.info("\n" + "=" * 60)
    logger.info("DIAGNOSTIC COMPLETE")
    logger.info("=" * 60)

    return tests


if __name__ == "__main__":
    asyncio.run(run_diagnostics())
