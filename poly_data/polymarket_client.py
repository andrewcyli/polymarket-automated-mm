from dotenv import load_dotenv
import os
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, BalanceAllowanceParams, AssetType, PartialCreateOrderOptions
from py_clob_client.constants import POLYGON
from web3 import Web3
try:
    from web3.middleware import ExtraDataToPOAMiddleware
except ImportError:
    # For web3.py v6+, geth_poa_middleware is used instead
    from web3.middleware import geth_poa_middleware
    ExtraDataToPOAMiddleware = geth_poa_middleware
from eth_account import Account
import requests
import pandas as pd
import json
import subprocess
from poly_data.abis import NegRiskAdapterABI, ConditionalTokenABI, erc20_abi

load_dotenv()

class PolymarketClient:
    def __init__(self, pk='default') -> None:
        self.host = "https://clob.polymarket.com"
        self.key = os.getenv("PK")
        self.browser_address = os.getenv("BROWSER_ADDRESS")

        # Validate environment variables
        if not self.key:
            raise ValueError("PK environment variable is not set. Please check your .env file.")
        if not self.browser_address:
            raise ValueError("BROWSER_ADDRESS environment variable is not set. Please check your .env file.")

        print("Initializing Polymarket client...")
        self.chain_id = POLYGON

        try:
            # Handle both old and new web3.py versions
            if hasattr(Web3, 'to_checksum_address'):
                self.browser_wallet = Web3.to_checksum_address(self.browser_address)
            else:
                self.browser_wallet = Web3.toChecksumAddress(self.browser_address)
        except Exception as e:
            raise ValueError(f"Invalid BROWSER_ADDRESS format: {self.browser_address}. Error: {e}")

        try:
            self.client = ClobClient(
                host=self.host,
                key=self.key,
                chain_id=self.chain_id,
                funder=self.browser_wallet,
                signature_type=2
            )
        except Exception as e:
            raise RuntimeError(f"Failed to initialize ClobClient. Check your PK and network connection. Error: {e}")

        # Create or derive API credentials with error handling
        try:
            self.creds = self.client.create_or_derive_api_creds()
            print(f"✓ API credentials created successfully (API Key: {self.creds.api_key[:8]}...)")
        except Exception as e:
            raise RuntimeError(f"Failed to create API credentials. Your private key may be invalid. Error: {e}")

        # Set API credentials with error handling
        try:
            self.client.set_api_creds(creds=self.creds)
            print("✓ API credentials authenticated successfully")
        except Exception as e:
            raise RuntimeError(f"Failed to set API credentials. Authentication rejected. Error: {e}")

        # Initialize Web3 connection to Polygon
        self.web3 = Web3(Web3.HTTPProvider(os.getenv("POLYGON_RPC_URL", "https://polygon-rpc.com")))
        # Add POA middleware for Polygon
        try:
            self.web3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        except AttributeError:
            # For web3.py v6+, middleware is added differently
            self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)

        # Set up USDC contract for balance checks
        self.usdc_contract = self.web3.eth.contract(
            address="0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
            abi=erc20_abi
        )

        # Store key contract addresses
        self.addresses = {
            'neg_risk_adapter': '0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296',
            'collateral': '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174',
            'conditional_tokens': '0x4D97DCd97eC945f40cF65F87097ACe5EA0476045'
        }

        self.neg_risk_adapter = self.web3.eth.contract(
            address=self.addresses['neg_risk_adapter'],
            abi=NegRiskAdapterABI
        )
        self.conditional_tokens = self.web3.eth.contract(
            address=self.addresses['conditional_tokens'],
            abi=ConditionalTokenABI
        )

    # ... rest of the class unchanged (create_order, get_order_book, etc.) ...

    def create_order(self, marketId, action, price, size, neg_risk=False):
        """
        Create and submit a new order to the Polymarket order book.

        Args:
            marketId (str): ID of the market token to trade
            action (str): "BUY" or "SELL"
            price (float): Order price (0-1 range for prediction markets)
            size (float): Order size in USDC
            neg_risk (bool, optional): Whether this is a negative risk market. Defaults to False.

        Returns:
            dict: Response from the API containing order details, or empty dict on error
        """
        order_args = OrderArgs(
            token_id=str(marketId),
            price=price,
            size=size,
            side=action
        )
        signed_order = None
        try:
            if neg_risk == False:
                signed_order = self.client.create_order(order_args)
            else:
                signed_order = self.client.create_order(order_args, options=PartialCreateOrderOptions(neg_risk=True))
        except Exception as ex:
            print(f"❌ Failed to create signed order for {action} {marketId} at {price}: {ex}")
            return {}

        try:
            resp = self.client.post_order(signed_order)
            return resp
        except Exception as ex:
            error_str = str(ex)
            # Check for common authentication errors
            if 'auth' in error_str.lower() or 'unauthorized' in error_str.lower() or '401' in error_str:
                print(f"❌ AUTHENTICATION ERROR when posting order: {ex}")
                print("   Your API credentials may have expired or are invalid.")
            elif 'insufficient' in error_str.lower() or 'balance' in error_str.lower():
                print(f"❌ INSUFFICIENT BALANCE when posting order: {ex}")
            else:
                print(f"❌ Failed to post order for {action} {marketId} at {price}: {ex}")
            return {}

    def get_order_book(self, market):
        orderBook = self.client.get_order_book(market)
        return pd.DataFrame(orderBook.bids).astype(float), pd.DataFrame(orderBook.asks).astype(float)

    def get_usdc_balance(self):
        return self.usdc_contract.functions.balanceOf(self.browser_wallet).call() / 10 ** 6

    def get_pos_balance(self):
        res = requests.get(f'https://data-api.polymarket.com/value?user={self.browser_wallet}')
        return float(res.json()['value'])

    def get_total_balance(self):
        return self.get_usdc_balance() + self.get_pos_balance()

    def get_all_positions(self):
        res = requests.get(f'https://data-api.polymarket.com/positions?user={self.browser_wallet}')
        return pd.DataFrame(res.json())

    def get_raw_position(self, tokenId):
        return int(self.conditional_tokens.functions.balanceOf(self.browser_wallet, int(tokenId)).call())

    def get_position(self, tokenId):
        raw_position = self.get_raw_position(tokenId)
        shares = float(raw_position / 1e6)
        if shares < 1:
            shares = 0
        return raw_position, shares

    def get_all_orders(self):
        orders_df = pd.DataFrame(self.client.get_orders())
        for col in ['original_size', 'size_matched', 'price']:
            if col in orders_df.columns:
                orders_df[col] = orders_df[col].astype(float)
        return orders_df

    def get_market_orders(self, market):
        orders_df = pd.DataFrame(self.client.get_orders(OpenOrderParams(market=market)))
        for col in ['original_size', 'size_matched', 'price']:
            if col in orders_df.columns:
                orders_df[col] = orders_df[col].astype(float)
        return orders_df

    def cancel_all_asset(self, asset_id):
        self.client.cancel_market_orders(asset_id=str(asset_id))

    def cancel_all_market(self, marketId):
        self.client.cancel_market_orders(market=marketId)

    def merge_positions(self, amount_to_merge, condition_id, is_neg_risk_market):
        amount_to_merge_str = str(amount_to_merge)
        neg_risk_flag = "true" if is_neg_risk_market else "false"
        node_command = ['node', 'poly_merger/merge.js', amount_to_merge_str, str(condition_id), neg_risk_flag]
        print(' '.join(node_command))
        result = subprocess.run(node_command, capture_output=True, text=True)
        if result.returncode != 0:
            print("Error:", result.stderr)
            raise Exception(f"Error in merging positions: {result.stderr}")
        print("Done merging")
        return result.stdout