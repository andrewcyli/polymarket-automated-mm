"""Test _encode_abi compatibility with web3.py v7.

Verifies that the fix for the 'fn_name' keyword argument error works correctly.
The bug was: encode_abi(fn_name='mergePositions', ...) fails in web3 v7 because
the parameter was renamed to 'abi_element_identifier'. Fix: use positional args.
"""
import unittest
import json
import sys

try:
    from web3 import Web3
    HAS_WEB3 = True
except ImportError:
    HAS_WEB3 = False

# Import the _encode_abi function from trading_bot_v15
sys.path.insert(0, '.')
from trading_bot_v15 import _encode_abi, CTF_FULL_ABI

@unittest.skipUnless(HAS_WEB3, "web3 not installed")
class TestEncodeABI(unittest.TestCase):
    
    def setUp(self):
        self.w3 = Web3()
        self.ctf = self.w3.eth.contract(
            address='0x4D97DCd97eC945f40cF65F87097ACe5EA0476045',
            abi=CTF_FULL_ABI
        )
        self.usdc_addr = Web3.to_checksum_address('0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174')
        self.parent = bytes(32)
        self.cid_bytes = bytes.fromhex('aa' * 32)
    
    def test_merge_positions_encode(self):
        """mergePositions should encode without 'fn_name' keyword error."""
        amount_raw = 31600000
        result = _encode_abi(
            self.ctf, "mergePositions",
            [self.usdc_addr, self.parent, self.cid_bytes, [1, 2], amount_raw]
        )
        self.assertIsInstance(result, str)
        self.assertTrue(result.startswith('0x'))
        # mergePositions selector
        self.assertTrue(result.startswith('0x9e7212ad'))
    
    def test_redeem_positions_encode(self):
        """redeemPositions should encode without 'fn_name' keyword error."""
        result = _encode_abi(
            self.ctf, "redeemPositions",
            [self.usdc_addr, self.parent, self.cid_bytes, [1, 2]]
        )
        self.assertIsInstance(result, str)
        self.assertTrue(result.startswith('0x'))
        # redeemPositions selector
        self.assertTrue(result.startswith('0x01b7037c'))
    
    def test_balance_of_encode(self):
        """balanceOf should also work (used in position queries)."""
        owner = Web3.to_checksum_address('0x0000000000000000000000000000000000000001')
        token_id = 12345
        result = _encode_abi(
            self.ctf, "balanceOf",
            [owner, token_id]
        )
        self.assertIsInstance(result, str)
        self.assertTrue(result.startswith('0x'))
    
    def test_no_fn_name_keyword_error(self):
        """Regression test: must NOT raise 'unexpected keyword argument fn_name'."""
        try:
            _encode_abi(
                self.ctf, "mergePositions",
                [self.usdc_addr, self.parent, self.cid_bytes, [1, 2], 1000000]
            )
        except TypeError as e:
            if "fn_name" in str(e):
                self.fail(f"fn_name keyword error still present: {e}")
            raise  # re-raise other TypeErrors

if __name__ == '__main__':
    unittest.main(verbosity=2)
