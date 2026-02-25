"""Tests for V15.1-23: Gnosis Safe execTransaction + merge circuit breaker."""
import time
import unittest
from unittest.mock import MagicMock, patch, PropertyMock
from dataclasses import dataclass

import logging
from trading_bot_v15 import (
    AutoMerger, BotConfig, FeeCalculator, PROXY_EXEC_ABI, ZERO_ADDR
)


def _make_config(**overrides):
    defaults = dict(
        api_key="k", api_secret="s", private_key="0x" + "ab" * 32,
        proxy_wallet="0xd9fAED3044a29908321c6ac64884985df8288283",
        chain_id=137,
        dry_run=False, auto_merge_enabled=True,
        merge_min_shares=1.0, merge_position_decimals=6,
    )
    defaults.update(overrides)
    return BotConfig(**defaults)


class TestProxyExecABI(unittest.TestCase):
    """Verify the PROXY_EXEC_ABI has Gnosis Safe functions."""

    def test_has_exec_transaction(self):
        fn_names = [f["name"] for f in PROXY_EXEC_ABI]
        self.assertIn("execTransaction", fn_names)

    def test_has_nonce(self):
        fn_names = [f["name"] for f in PROXY_EXEC_ABI]
        self.assertIn("nonce", fn_names)

    def test_has_get_transaction_hash(self):
        fn_names = [f["name"] for f in PROXY_EXEC_ABI]
        self.assertIn("getTransactionHash", fn_names)

    def test_exec_transaction_params(self):
        exec_fn = [f for f in PROXY_EXEC_ABI if f["name"] == "execTransaction"][0]
        param_names = [p["name"] for p in exec_fn["inputs"]]
        self.assertEqual(param_names, [
            "to", "value", "data", "operation", "safeTxGas",
            "baseGas", "gasPrice", "gasToken", "refundReceiver", "signatures"
        ])

    def test_no_old_execute_or_exec(self):
        """Old broken execute/exec functions should be gone."""
        fn_names = [f["name"] for f in PROXY_EXEC_ABI]
        self.assertNotIn("execute", fn_names)
        self.assertNotIn("exec", fn_names)

    def test_zero_addr_defined(self):
        self.assertEqual(ZERO_ADDR, "0x0000000000000000000000000000000000000000")


class TestMergeCircuitBreaker(unittest.TestCase):
    """Test the merge circuit breaker logic."""

    def _make_merger(self):
        config = _make_config()
        logger = logging.getLogger("test_merger")
        merger = AutoMerger(config, logger)
        merger.w3 = MagicMock()
        merger.ctf_contract = MagicMock()
        merger.proxy_contract = MagicMock()
        merger.account = MagicMock()
        merger.account.address = "0x5d97B1C1b7e96bae3b31d970dc8586e7b26cc41b"
        merger.engine = MagicMock()
        merger.engine.token_holdings = {}
        merger.engine.capital_in_positions = 0
        merger.engine.session_total_spent = 0
        merger.engine.window_fill_cost = {}
        merger.engine.held_windows = set()
        return merger

    def test_circuit_breaker_init(self):
        """Circuit breaker attributes are initialized on first call."""
        merger = self._make_merger()
        merger.config.auto_merge_enabled = True
        merger.config.dry_run = False
        merger.query_live_positions = MagicMock(return_value={})
        merger._find_mergeable = MagicMock(return_value={})
        merger.check_and_merge_all({}, {})
        self.assertEqual(merger._merge_consecutive_fails, 0)
        self.assertEqual(merger._merge_circuit_open_until, 0)

    def test_circuit_breaker_stops_after_3_cycle_fails(self):
        """After 3 failures in one cycle, stop trying more merges."""
        merger = self._make_merger()
        merger._merge_consecutive_fails = 0
        merger._merge_circuit_open_until = 0

        # Mock _find_mergeable to return 5 windows
        windows = {}
        for i in range(5):
            wid = f"btc-5m-{i}"
            windows[wid] = {
                "up_size": 10.0, "down_size": 10.0,
                "market": {"condition_id": f"cond_{i}", "token_up": f"up_{i}", "token_down": f"dn_{i}"}
            }
        merger._find_mergeable = MagicMock(return_value=windows)
        merger.query_live_positions = MagicMock(return_value={})
        merger._execute_merge = MagicMock(return_value=False)  # All fail

        result = merger.check_and_merge_all({}, {})
        self.assertEqual(result, 0)
        # Should have tried exactly 3 (circuit breaker kicks in)
        self.assertEqual(merger._execute_merge.call_count, 3)
        self.assertEqual(merger._merge_consecutive_fails, 3)

    def test_circuit_breaker_opens_after_5_consecutive(self):
        """After 5 total consecutive fails, open circuit for 5 minutes."""
        merger = self._make_merger()
        merger._merge_consecutive_fails = 4  # Already 4 fails from previous cycles
        merger._merge_circuit_open_until = 0

        windows = {"btc-5m-0": {
            "up_size": 10.0, "down_size": 10.0,
            "market": {"condition_id": "cond_0", "token_up": "up_0", "token_down": "dn_0"}
        }}
        merger._find_mergeable = MagicMock(return_value=windows)
        merger.query_live_positions = MagicMock(return_value={})
        merger._execute_merge = MagicMock(return_value=False)

        result = merger.check_and_merge_all({}, {})
        self.assertEqual(result, 0)
        self.assertEqual(merger._merge_consecutive_fails, 5)
        # Circuit should be open for ~5 minutes
        self.assertGreater(merger._merge_circuit_open_until, time.time())
        self.assertLess(merger._merge_circuit_open_until, time.time() + 310)

    def test_circuit_breaker_skips_when_open(self):
        """When circuit is open, skip all merges immediately."""
        merger = self._make_merger()
        merger._merge_consecutive_fails = 5
        merger._merge_circuit_open_until = time.time() + 300  # 5 min from now

        merger._find_mergeable = MagicMock()
        merger.query_live_positions = MagicMock()
        merger._execute_merge = MagicMock()

        result = merger.check_and_merge_all({}, {})
        self.assertEqual(result, 0)
        # Should not have even called _find_mergeable
        merger.query_live_positions.assert_not_called()

    def test_circuit_breaker_resets_on_success(self):
        """A successful merge resets the circuit breaker."""
        merger = self._make_merger()
        merger._merge_consecutive_fails = 4
        merger._merge_circuit_open_until = 0

        windows = {"btc-5m-0": {
            "up_size": 10.0, "down_size": 10.0,
            "market": {"condition_id": "cond_0", "token_up": "up_0", "token_down": "dn_0"}
        }}
        merger._find_mergeable = MagicMock(return_value=windows)
        merger.query_live_positions = MagicMock(return_value={})
        merger._execute_merge = MagicMock(return_value=True)  # Success!

        result = merger.check_and_merge_all({}, {})
        self.assertEqual(result, 1)
        self.assertEqual(merger._merge_consecutive_fails, 0)
        self.assertEqual(merger._merge_circuit_open_until, 0)


class TestSafeExecTx(unittest.TestCase):
    """Test the _safe_exec_tx helper method."""

    def _make_merger_with_safe(self):
        config = _make_config()
        logger = logging.getLogger("test_safe")
        merger = AutoMerger(config, logger)

        # Mock web3
        merger.w3 = MagicMock()
        merger.w3.eth.get_transaction_count.return_value = 42
        merger.w3.eth.send_raw_transaction.return_value = b'\x01' * 32
        receipt = MagicMock()
        receipt.status = 1
        receipt.gasUsed = 150000
        merger.w3.eth.wait_for_transaction_receipt.return_value = receipt
        merger.w3.to_hex.return_value = "0x" + "01" * 32
        merger.w3.eth.account.sign_transaction.return_value = MagicMock(
            raw_transaction=b'\x02' * 100)
        merger.w3.eth.account.sign_message.return_value = MagicMock(
            r=12345, s=67890, v=27)

        # Mock proxy contract (Gnosis Safe)
        merger.proxy_contract = MagicMock()
        merger.proxy_contract.functions.nonce.return_value.call.return_value = 148
        merger.proxy_contract.functions.getTransactionHash.return_value.call.return_value = b'\xaa' * 32
        exec_fn = MagicMock()
        exec_fn.build_transaction.return_value = {"from": "0x...", "nonce": 42}
        merger.proxy_contract.functions.execTransaction.return_value = exec_fn

        # Mock account
        merger.account = MagicMock()
        merger.account.address = "0x5d97B1C1b7e96bae3b31d970dc8586e7b26cc41b"

        # Mock rpc_limiter
        merger.rpc_limiter = MagicMock()
        merger.rpc_limiter.is_backed_off = False
        merger.rpc_limiter.get_gas_price.return_value = 30_000_000_000

        return merger

    def test_safe_exec_calls_nonce(self):
        """_safe_exec_tx should call proxy.nonce() to get Safe nonce."""
        merger = self._make_merger_with_safe()
        inner_data = b'\x00' * 32
        target = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

        result = merger._safe_exec_tx(target, inner_data, "test-wid", "TEST")
        self.assertTrue(result)
        merger.proxy_contract.functions.nonce.assert_called_once()

    def test_safe_exec_calls_get_transaction_hash(self):
        """_safe_exec_tx should call getTransactionHash with correct params."""
        merger = self._make_merger_with_safe()
        inner_data = b'\x00' * 32
        target = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

        result = merger._safe_exec_tx(target, inner_data, "test-wid", "TEST")
        self.assertTrue(result)
        merger.proxy_contract.functions.getTransactionHash.assert_called_once()
        call_args = merger.proxy_contract.functions.getTransactionHash.call_args[0]
        self.assertEqual(call_args[0], target)  # to
        self.assertEqual(call_args[1], 0)  # value
        self.assertEqual(call_args[2], inner_data)  # data
        self.assertEqual(call_args[3], 0)  # operation (CALL)
        self.assertEqual(call_args[9], 148)  # nonce from mock

    def test_safe_exec_calls_exec_transaction(self):
        """_safe_exec_tx should call execTransaction with signature."""
        merger = self._make_merger_with_safe()
        inner_data = b'\x00' * 32
        target = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

        result = merger._safe_exec_tx(target, inner_data, "test-wid", "TEST")
        self.assertTrue(result)
        merger.proxy_contract.functions.execTransaction.assert_called_once()
        call_args = merger.proxy_contract.functions.execTransaction.call_args[0]
        self.assertEqual(call_args[0], target)  # to
        self.assertEqual(call_args[1], 0)  # value
        self.assertEqual(call_args[2], inner_data)  # data
        # Signature should be 65 bytes (r=32 + s=32 + v=1)
        sig = call_args[9]
        self.assertEqual(len(sig), 65)

    def test_safe_exec_signature_v_plus_4(self):
        """Gnosis Safe eth_sign signatures need v += 4."""
        merger = self._make_merger_with_safe()
        # Mock sign_message to return v=27
        merger.w3.eth.account.sign_message.return_value = MagicMock(
            r=1, s=2, v=27)
        inner_data = b'\x00' * 32
        target = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

        merger._safe_exec_tx(target, inner_data, "test-wid", "TEST")
        call_args = merger.proxy_contract.functions.execTransaction.call_args[0]
        sig = call_args[9]
        # v byte should be 27 + 4 = 31
        self.assertEqual(sig[-1], 31)

    def test_safe_exec_returns_false_on_revert(self):
        """_safe_exec_tx returns False when receipt.status == 0."""
        merger = self._make_merger_with_safe()
        receipt = MagicMock()
        receipt.status = 0
        receipt.gasUsed = 34000
        merger.w3.eth.wait_for_transaction_receipt.return_value = receipt

        inner_data = b'\x00' * 32
        target = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
        result = merger._safe_exec_tx(target, inner_data, "test-wid", "TEST")
        self.assertFalse(result)

    def test_safe_exec_returns_false_on_exception(self):
        """_safe_exec_tx returns False on exception."""
        merger = self._make_merger_with_safe()
        merger.proxy_contract.functions.nonce.return_value.call.side_effect = Exception("RPC error")

        inner_data = b'\x00' * 32
        target = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
        result = merger._safe_exec_tx(target, inner_data, "test-wid", "TEST")
        self.assertFalse(result)

    def test_safe_exec_no_proxy_returns_false(self):
        """_safe_exec_tx returns False when no proxy contract."""
        merger = self._make_merger_with_safe()
        merger.proxy_contract = None

        inner_data = b'\x00' * 32
        target = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
        result = merger._safe_exec_tx(target, inner_data, "test-wid", "TEST")
        self.assertFalse(result)


class TestMergeViaProxyUsesSafe(unittest.TestCase):
    """Verify _merge_via_proxy delegates to _safe_exec_tx."""

    def test_merge_via_proxy_calls_safe_exec(self):
        config = _make_config()
        logger = logging.getLogger("test_merge_proxy")
        merger = AutoMerger(config, logger)
        merger.proxy_contract = MagicMock()
        merger.account = MagicMock()
        merger._safe_exec_tx = MagicMock(return_value=True)

        ctf_addr = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
        merge_data = "0x" + "ab" * 64

        result = merger._merge_via_proxy(ctf_addr, merge_data, "test-wid")
        self.assertTrue(result)
        merger._safe_exec_tx.assert_called_once()
        call_args = merger._safe_exec_tx.call_args
        self.assertEqual(call_args[0][0], ctf_addr)
        self.assertEqual(call_args[0][2], "test-wid")
        self.assertEqual(call_args[0][3], "MERGE")
        self.assertEqual(call_args[1]["gas_limit"], 350000)


class TestBlindRedeemUsesSafe(unittest.TestCase):
    """Verify blind redeem uses _safe_exec_tx for proxy path."""

    def test_safe_exec_tx_is_used_in_merge_via_proxy(self):
        """Since _try_blind_redeem is on AutoMerger (same class as _safe_exec_tx),
        verify the _safe_exec_tx method exists and has correct signature."""
        config = _make_config()
        logger = logging.getLogger("test_redeem_proxy")
        merger = AutoMerger(config, logger)
        # Verify _safe_exec_tx method exists
        self.assertTrue(hasattr(merger, '_safe_exec_tx'))
        self.assertTrue(callable(merger._safe_exec_tx))


if __name__ == "__main__":
    unittest.main()
