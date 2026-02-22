#!/usr/bin/env python3
"""
test_wallet_rpc.py — Standalone wallet + RPC connectivity test
Run this BEFORE starting the bot to verify your .env is correct.

Usage:
    python test_wallet_rpc.py

Reads from .env:
    POLYGON_RPC_URL  (or POLYGON_RPC)   — your Polygon RPC endpoint
    POLY_PROXY_WALLET (or POLYMARKET_PROXY_ADDRESS or PROXY_WALLET) — your Polymarket proxy wallet
"""

import os
import sys
import time
import json

# ── Load .env ────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not required if env vars are already set

# ── Config ───────────────────────────────────────────────────────
POLYGON_RPC = os.getenv("POLYGON_RPC_URL",
                os.getenv("POLYGON_RPC", "https://polygon.drpc.org"))

PROXY_WALLET = os.getenv("POLY_PROXY_WALLET",
                 os.getenv("POLYMARKET_PROXY_ADDRESS",
                   os.getenv("PROXY_WALLET", "")))

USDC_E_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # USDC.e on Polygon

ERC20_ABI = json.loads(
    '[{"constant":true,"inputs":[{"name":"_owner","type":"address"}],'
    '"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],'
    '"type":"function"},'
    '{"constant":true,"inputs":[],"name":"decimals",'
    '"outputs":[{"name":"","type":"uint8"}],"type":"function"}]'
)

# Public fallback RPCs (free, no API key needed)
FALLBACK_RPCS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon.llamarpc.com",
    "https://rpc.ankr.com/polygon",
    "https://polygon-rpc.com",
]

# ── Colours ──────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def ok(msg):   print(f"  {GREEN}✓{RESET} {msg}")
def fail(msg): print(f"  {RED}✗{RESET} {msg}")
def warn(msg): print(f"  {YELLOW}⚠{RESET} {msg}")
def info(msg): print(f"  {CYAN}ℹ{RESET} {msg}")

# ── Tests ────────────────────────────────────────────────────────

def test_web3_import():
    """Test 1: Can we import web3?"""
    print(f"\n{BOLD}Test 1: web3 library{RESET}")
    try:
        from web3 import Web3
        ok(f"web3 imported successfully (version: {__import__('web3').__version__})")
        return True
    except ImportError:
        fail("web3 not installed. Run: pip install web3")
        return False


def test_env_vars():
    """Test 2: Are the required env vars set?"""
    print(f"\n{BOLD}Test 2: Environment variables{RESET}")
    all_ok = True

    if POLYGON_RPC == "https://polygon.drpc.org":
        info(f"POLYGON_RPC_URL not set — using default: drpc.org (free, usually reliable)")
        info(f"  For best reliability, add to .env:  POLYGON_RPC_URL=https://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY")
    else:
        ok(f"POLYGON_RPC_URL = {POLYGON_RPC[:60]}{'...' if len(POLYGON_RPC) > 60 else ''}")

    if not PROXY_WALLET:
        fail("PROXY_WALLET not set — cannot read wallet balance")
        fail("  Add to .env:  POLY_PROXY_WALLET=0xYourPolymarketProxyAddress")
        return False
    else:
        ok(f"PROXY_WALLET   = {PROXY_WALLET[:10]}...{PROXY_WALLET[-6:]}")

    return all_ok


def test_rpc_connection(rpc_url, label=""):
    """Test a single RPC endpoint: connect + read balance."""
    from web3 import Web3

    tag = f" ({label})" if label else ""
    start = time.time()
    try:
        w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 10}))
        connected = w3.is_connected()
        elapsed_connect = time.time() - start

        if not connected:
            fail(f"{rpc_url[:50]}{tag} — not connected ({elapsed_connect:.1f}s)")
            return None

        # Read USDC.e balance
        start2 = time.time()
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(USDC_E_ADDRESS), abi=ERC20_ABI)
        raw_balance = contract.functions.balanceOf(
            Web3.to_checksum_address(PROXY_WALLET)).call()
        balance = raw_balance / (10 ** 6)  # USDC.e has 6 decimals
        elapsed_read = time.time() - start2
        total = time.time() - start

        ok(f"{rpc_url[:50]}{tag}")
        info(f"  Connected in {elapsed_connect:.2f}s, balance read in {elapsed_read:.2f}s (total {total:.2f}s)")
        info(f"  USDC.e balance: ${balance:,.2f}")
        return balance

    except Exception as e:
        elapsed = time.time() - start
        fail(f"{rpc_url[:50]}{tag} — {str(e)[:80]} ({elapsed:.1f}s)")
        return None


def test_primary_rpc():
    """Test 3: Primary RPC connectivity + balance read."""
    print(f"\n{BOLD}Test 3: Primary RPC connection{RESET}")
    return test_rpc_connection(POLYGON_RPC, "primary")


def test_fallback_rpcs():
    """Test 4: Fallback RPC connectivity."""
    print(f"\n{BOLD}Test 4: Fallback RPCs{RESET}")
    results = {}
    for rpc in FALLBACK_RPCS:
        if rpc == POLYGON_RPC:
            info(f"Skipping {rpc[:40]} (same as primary)")
            continue
        bal = test_rpc_connection(rpc, "fallback")
        results[rpc] = bal
    return results


def test_block_number():
    """Test 5: Read latest block number (confirms full RPC functionality)."""
    from web3 import Web3
    print(f"\n{BOLD}Test 5: Block number read (full RPC test){RESET}")
    try:
        w3 = Web3(Web3.HTTPProvider(POLYGON_RPC, request_kwargs={"timeout": 10}))
        block = w3.eth.block_number
        ok(f"Latest Polygon block: {block:,}")
        return True
    except Exception as e:
        fail(f"Block read failed: {str(e)[:80]}")
        return False


def test_checksum_address():
    """Test 6: Validate wallet address format."""
    from web3 import Web3
    print(f"\n{BOLD}Test 6: Wallet address validation{RESET}")
    try:
        checksummed = Web3.to_checksum_address(PROXY_WALLET)
        ok(f"Valid checksummed address: {checksummed}")
        return True
    except Exception as e:
        fail(f"Invalid wallet address: {str(e)[:80]}")
        fail(f"  Make sure PROXY_WALLET is a valid Ethereum/Polygon address (0x...)")
        return False


# ── Main ─────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*60}")
    print(f"{BOLD}{CYAN}  PolyMaker Wallet & RPC Connection Test{RESET}")
    print(f"{'='*60}")

    # Test 1: web3 import
    if not test_web3_import():
        print(f"\n{RED}{BOLD}BLOCKED:{RESET} Install web3 first: pip install web3")
        sys.exit(1)

    # Test 2: env vars
    env_ok = test_env_vars()

    if not PROXY_WALLET:
        print(f"\n{RED}{BOLD}BLOCKED:{RESET} Set PROXY_WALLET in .env to continue")
        sys.exit(1)

    # Test 6: address format (before RPC calls)
    if not test_checksum_address():
        print(f"\n{RED}{BOLD}BLOCKED:{RESET} Fix wallet address format")
        sys.exit(1)

    # Test 3: primary RPC
    primary_balance = test_primary_rpc()

    # Test 5: block number
    test_block_number()

    # Test 4: fallback RPCs
    fallback_results = test_fallback_rpcs()

    # ── Summary ──────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"{BOLD}  Summary{RESET}")
    print(f"{'='*60}")

    working_rpcs = []
    if primary_balance is not None:
        working_rpcs.append(("PRIMARY", POLYGON_RPC, primary_balance))
    for rpc, bal in fallback_results.items():
        if bal is not None:
            working_rpcs.append(("FALLBACK", rpc, bal))

    if working_rpcs:
        print(f"\n  {GREEN}{BOLD}{len(working_rpcs)} RPC(s) working:{RESET}")
        for role, url, bal in working_rpcs:
            print(f"    {role:10s} {url[:50]:50s}  ${bal:,.2f}")

        # Check balance consistency
        balances = [b for _, _, b in working_rpcs]
        if len(set(balances)) > 1:
            warn(f"Balance mismatch across RPCs — may be due to caching lag")
        else:
            ok(f"All RPCs report consistent balance: ${balances[0]:,.2f}")
    else:
        print(f"\n  {RED}{BOLD}No working RPCs!{RESET}")
        fail("All RPC endpoints failed. Check your network and .env settings.")

    if primary_balance is None and not env_ok:
        print(f"\n  {YELLOW}{BOLD}Recommendation:{RESET}")
        print(f"  Add a dedicated RPC to your .env file:")
        print(f"  {CYAN}POLYGON_RPC_URL=https://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY{RESET}")
        print(f"  Free tier: https://dashboard.alchemy.com (300M compute units/month)")
        print(f"  Alternative: https://www.quicknode.com (free tier available)")

    if primary_balance is not None:
        print(f"\n  {GREEN}{BOLD}✓ Ready for bot integration{RESET}")
        print(f"  Wallet: {PROXY_WALLET[:10]}...{PROXY_WALLET[-6:]}")
        print(f"  Balance: ${primary_balance:,.2f} USDC.e")
    elif working_rpcs:
        print(f"\n  {YELLOW}{BOLD}⚠ Primary RPC failed but fallbacks work{RESET}")
        print(f"  Bot will use fallback RPCs. Consider adding a dedicated RPC for reliability.")
    else:
        print(f"\n  {RED}{BOLD}✗ Not ready — fix RPC connectivity first{RESET}")

    print()


if __name__ == "__main__":
    main()
