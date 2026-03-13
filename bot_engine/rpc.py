"""RPC rate limiting and wallet balance checking."""

import time
import logging

try:
    from web3 import Web3
except ImportError:
    pass

from bot_engine.constants import USDC_E_ADDRESS


class RPCRateLimiter:
    def __init__(self, min_interval=1.5):
        self.min_interval = min_interval
        self._last_call = 0
        self._backoff_until = 0
        self._gas_cache = None
        self._gas_cache_time = 0
        self._gas_cache_ttl = 30.0

    def wait(self):
        now = time.time()
        if now < self._backoff_until:
            time.sleep(self._backoff_until - now)
        elapsed = time.time() - self._last_call
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last_call = time.time()

    def report_rate_limit(self, retry_after=10):
        self._backoff_until = time.time() + retry_after + 2

    def get_gas_price(self, w3):
        now = time.time()
        if self._gas_cache and now - self._gas_cache_time < self._gas_cache_ttl:
            return self._gas_cache
        self.wait()
        try:
            price = w3.eth.gas_price
            self._gas_cache = price
            self._gas_cache_time = now
            return price
        except Exception:
            return self._gas_cache or 50_000_000_000

    @property
    def is_backed_off(self):
        return time.time() < self._backoff_until


# -----------------------------------------------------------------
# Chainlink Direct Price Feed


class WalletBalanceChecker:
    USDC_E = USDC_E_ADDRESS
    ERC20_ABI = json.loads('[{"constant":true,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"type":"function"}]')
    # V15.1-7: Fallback RPCs for wallet balance reads
    FALLBACK_RPCS = [
        "https://polygon-bor-rpc.publicnode.com",
        "https://polygon.llamarpc.com",
        "https://rpc.ankr.com/polygon",
        "https://polygon-rpc.com",
    ]

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.w3 = None
        self.contract = None
        self.decimals = 6
        self._cache = None
        self._cache_time = 0
        self._fallback_providers = []
        self._init_web3()

    def _init_web3(self):
        if not HAS_WEB3:
            return
        try:
            self.w3 = Web3(Web3.HTTPProvider(
                self.config.polygon_rpc, request_kwargs={"timeout": 10}))
            self.contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(self.USDC_E), abi=self.ERC20_ABI)
            self.logger.info("  Wallet balance checker initialized (primary: {})".format(
                self.config.polygon_rpc[:40]))
        except Exception as e:
            self.logger.warning(f"  Wallet balance checker init failed: {e}")
            self.w3 = None
        # Pre-init fallback providers
        for rpc_url in self.FALLBACK_RPCS:
            if rpc_url == self.config.polygon_rpc:
                continue
            try:
                fb_w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 8}))
                fb_contract = fb_w3.eth.contract(
                    address=Web3.to_checksum_address(self.USDC_E), abi=self.ERC20_ABI)
                self._fallback_providers.append((rpc_url, fb_w3, fb_contract))
            except Exception:
                pass
        if self._fallback_providers:
            self.logger.info("  Wallet balance: {} fallback RPCs ready".format(
                len(self._fallback_providers)))

    def _read_balance_from(self, contract, wallet):
        raw = contract.functions.balanceOf(
            Web3.to_checksum_address(wallet)).call()
        return raw / (10 ** self.decimals)

    def get_balance(self):
        wallet = self.config.proxy_wallet
        if not wallet:
            return None
        now = time.time()
        if self._cache is not None and now - self._cache_time < self.config.wallet_balance_cache_ttl:
            return self._cache
        # Try primary RPC
        if self.w3 and self.contract:
            try:
                balance = self._read_balance_from(self.contract, wallet)
                self._cache = balance
                self._cache_time = now
                return balance
            except Exception as e:
                self.logger.info("  Wallet read failed (primary): {}".format(
                    str(e)[:80]))
        # Try fallback RPCs
        for rpc_url, fb_w3, fb_contract in self._fallback_providers:
            try:
                balance = self._read_balance_from(fb_contract, wallet)
                self._cache = balance
                self._cache_time = now
                self.logger.info("  Wallet read OK via fallback: {}".format(
                    rpc_url[:40]))
                return balance
            except Exception as e:
                self.logger.debug("  Wallet fallback {} failed: {}".format(
                    rpc_url[:30], str(e)[:60]))
                continue
        self.logger.warning("  Wallet read failed on all RPCs (primary + {} fallbacks)".format(
            len(self._fallback_providers)))
        return self._cache


# -----------------------------------------------------------------
# V14.1-1: Auto Merger
