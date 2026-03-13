"""Automatic position merging and claim management."""

import time
import logging
import requests

try:
    from web3 import Web3
    from eth_account import Account
except ImportError:
    pass

from bot_engine.constants import (
    PROXY_EXEC_ABI, ZERO_ADDR, CTF_FULL_ABI,
    CTF_ADDRESS, USDC_E_ADDRESS, _encode_abi,
)
from bot_engine.rpc import RPCRateLimiter
from bot_engine.logging_setup import AuditLogger


class AutoMerger:
    def __init__(self, config, logger, engine=None):
        self.config = config
        self.logger = logger
        self.engine = engine
        self.audit = AuditLogger()
        self.w3 = None
        self.ctf_contract = None
        self.account = None
        self.proxy_contract = None
        self.rpc_limiter = RPCRateLimiter(min_interval=config.rpc_min_call_interval)
        self.merges_completed = 0
        self.merges_failed = 0
        self.total_merged_usd = 0.0
        self._merged_windows = set()
        # V15.9: Gas cost tracking per window
        self._window_gas_costs = {}  # {window_id: total_gas_matic}
        self._session_total_gas = 0.0  # Total gas spent this session (MATIC)
        self._init_web3()

    def set_engine(self, engine):
        self.engine = engine

    def _init_web3(self):
        if not HAS_WEB3 or not self.config.private_key:
            return
        try:
            self.w3 = Web3(Web3.HTTPProvider(
                self.config.polygon_rpc, request_kwargs={"timeout": 15}))
            self.ctf_contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(CTF_ADDRESS), abi=CTF_FULL_ABI)
            self.account = self.w3.eth.account.from_key(self.config.private_key)
            if self.config.proxy_wallet:
                try:
                    self.proxy_contract = self.w3.eth.contract(
                        address=Web3.to_checksum_address(self.config.proxy_wallet),
                        abi=PROXY_EXEC_ABI)
                except Exception:
                    self.proxy_contract = None
            self.logger.info("  AutoMerger initialized | CTF: {}...".format(CTF_ADDRESS[:10]))
        except Exception as e:
            self.logger.warning(f"  AutoMerger init failed: {e}")
            self.w3 = None

    def _to_bytes32(self, hex_str):
        clean = hex_str.replace("0x", "")
        raw = bytes.fromhex(clean)
        if len(raw) < 32:
            raw = raw.rjust(32, b'\x00')
        return raw[:32]

    def query_live_positions(self, market_cache):
        """Query on-chain CTF balanceOf for tokens we care about.
        V15.1-27: Only queries tokens for windows with fills, positions,
        or pending claims — not ALL discovered tokens. This reduces RPC
        calls from 160+ to ~20, cutting cycle time by 3-4 minutes.
        Falls back to engine.token_holdings if RPC is unavailable."""
        if not self.w3 or not self.ctf_contract:
            if self.engine:
                return dict(self.engine.token_holdings)
            return {}
        wallet = self.config.proxy_wallet
        if not wallet:
            if self.engine:
                return dict(self.engine.token_holdings)
            return {}
        wallet_addr = Web3.to_checksum_address(wallet)
        live = {}
        token_ids = set()
        # V15.1-27: Smart token selection — only query tokens we actually need:
        # 1. Tokens with known holdings (from fills/hedges)
        # 2. Tokens in windows pending claim/merge
        # 3. Tokens in windows with active fills
        # This avoids querying 160+ tokens when only ~20 matter.
        relevant_wids = set()
        if self.engine:
            # Windows with fills
            relevant_wids.update(self.engine.window_fill_sides.keys())
            # Windows pending claim
            relevant_wids.update(self.engine.expired_windows_pending_claim.keys())
            # Windows with held positions
            relevant_wids.update(self.engine.held_windows)
            # Paired windows (have both sides)
            relevant_wids.update(self.engine.paired_windows)
            # Also include tokens already in holdings (catch orphans)
            for tid in self.engine.token_holdings:
                if self.engine.token_holdings[tid].get("size", 0) >= 1.0:
                    token_ids.add(tid)
        # Build combined cache for relevant windows only
        combined_cache = {}
        for wid in relevant_wids:
            if wid in market_cache:
                combined_cache[wid] = market_cache[wid]
            elif self.engine:
                if wid in self.engine.expired_windows_pending_claim:
                    combined_cache[wid] = self.engine.expired_windows_pending_claim[wid]
                elif wid in self.engine.window_metadata:
                    combined_cache[wid] = self.engine.window_metadata[wid]
        for wid, market in combined_cache.items():
            for key in ("token_up", "token_down"):
                tid = market.get(key, "")
                if tid:
                    token_ids.add(tid)
        queried = 0
        errors = 0
        for tid in token_ids:
            try:
                self.rpc_limiter.wait()
                raw = self.ctf_contract.functions.balanceOf(
                    wallet_addr, int(tid, 16) if tid.startswith("0x") else int(tid)
                ).call()
                shares = float(raw) / 1e6
                queried += 1
                if shares >= 1.0:  # Ignore dust
                    live[tid] = {"size": shares, "cost": 0}
            except Exception as e:
                errors += 1
                err = str(e).lower()
                if "rate limit" in err or "-32090" in err:
                    self.rpc_limiter.report_rate_limit(10)
                    self.logger.info("  LIVE POS | RPC rate limited after {} queries".format(queried))
                    break
        if queried > 0:
            self.logger.info("  LIVE POS | Queried {} tokens | {} with balance | {} errors".format(
                queried, len(live), errors))
        # Sync back to engine.token_holdings so PnL calc uses live data
        if self.engine and live:
            for tid, pos in live.items():
                if tid in self.engine.token_holdings:
                    self.engine.token_holdings[tid]["size"] = pos["size"]
                else:
                    self.engine.token_holdings[tid] = {"size": pos["size"], "cost": 0}
            # Also clean up stale holdings that are no longer on-chain
            for tid in list(self.engine.token_holdings.keys()):
                if tid in token_ids and tid not in live:
                    self.engine.token_holdings[tid]["size"] = 0
        if errors > 0 and queried == 0:
            # Total RPC failure — fall back to engine data
            if self.engine:
                return dict(self.engine.token_holdings)
        return live

    def check_and_merge_all(self, market_cache, token_holdings):
        if not self.config.auto_merge_enabled or not self.w3:
            return 0
        if self.config.dry_run:
            return self._simulate_merges(market_cache, token_holdings)
        # V15.1-23: Circuit breaker — skip all merges if last N consecutive
        # attempts all failed (likely systemic issue like no gas).
        # Reset when a merge succeeds or after a cooldown period.
        if not hasattr(self, '_merge_consecutive_fails'):
            self._merge_consecutive_fails = 0
            self._merge_circuit_open_until = 0
        if time.time() < self._merge_circuit_open_until:
            return 0  # Circuit breaker is open, skip merges this cycle
        # Use live on-chain positions instead of fill-recorded token_holdings
        live_holdings = self.query_live_positions(market_cache)
        merged_count = 0
        cycle_fails = 0
        window_holdings = self._find_mergeable(market_cache, live_holdings)
        for wid, info in window_holdings.items():
            mergeable = min(info["up_size"], info["down_size"])
            if mergeable < self.config.merge_min_shares:
                continue
            merge_key = "{}:{:.1f}".format(wid, mergeable)
            if merge_key in self._merged_windows:
                continue
            market = info["market"]
            condition_id = market.get("condition_id", "")
            if not condition_id:
                continue
            self.logger.info(
                "  MERGE ATTEMPT | {} | {:.1f} shares (UP:{:.1f} DN:{:.1f}) | cond: {}...".format(
                    wid, mergeable, info["up_size"], info["down_size"], condition_id[:16]))
            success = self._execute_merge(condition_id, mergeable, wid)
            if success:
                self._merged_windows.add(merge_key)
                merged_count += 1
                self.merges_completed += 1
                self.total_merged_usd += mergeable
                token_up = market.get("token_up", "")
                token_down = market.get("token_down", "")
                # Update both the live_holdings and the passed-in token_holdings
                for holdings in (live_holdings, token_holdings):
                    if token_up in holdings:
                        holdings[token_up]["size"] = max(
                            0, holdings[token_up]["size"] - mergeable)
                    if token_down in holdings:
                        holdings[token_down]["size"] = max(
                            0, holdings[token_down]["size"] - mergeable)
                if self.engine:
                    self.engine.capital_in_positions = max(
                        0, self.engine.capital_in_positions - mergeable)
                    self.engine.session_total_spent = max(
                        0, self.engine.session_total_spent - mergeable)
                    self.engine._update_total_capital()
                    # V15.1-17b: After merge returns USDC, clean up accounting.
                    # window_fill_cost must be reduced so reconcile doesn't
                    # inflate capital_in_positions back up. filled_windows
                    # guard remains until cleanup_expired_windows releases it
                    # (the window is still blocked from re-entry, which is correct
                    # since the position is gone and we don't want to re-enter).
                    old_fill = self.engine.window_fill_cost.get(wid, 0)
                    if old_fill > 0:
                        new_fill = max(0, old_fill - mergeable)
                        if new_fill > 0:
                            self.engine.window_fill_cost[wid] = new_fill
                        else:
                            self.engine.window_fill_cost.pop(wid, None)
                # V15.1-P5: Track realized PnL from merge.
                # Merge returns $1 per pair of shares. The cost was what we paid
                # for those shares (up_price + down_price per pair).
                if self.engine:
                    # Estimate cost: use window_fill_cost if available, else use
                    # the merged amount as a rough cost estimate (conservative)
                    merge_cost = min(old_fill, mergeable) if old_fill > 0 else mergeable
                    self.engine.session_realized_returns += mergeable
                    self.engine.session_realized_cost += merge_cost
                    # V15.1-20: Release held_windows when capital is recovered.
                    # If window_fill_cost is now 0, all capital from this window
                    # has been returned via merge — safe to release the slot.
                    remaining_fill = self.engine.window_fill_cost.get(wid, 0)
                    if remaining_fill <= 0:
                        self.engine.held_windows.discard(wid)
                # V15.2: Clear fill_tokens so cleanup_expired_windows
                # doesn't queue this window for claim (prevents double-counting)
                if self.engine:
                    self.engine.window_fill_tokens.pop(wid, None)
                self.logger.info(
                    "  MERGED OK | {} | {:.1f} shares -> ~${:.2f} USDC returned".format(
                        wid, mergeable, mergeable))
                # V15.5-FIX2: Mark window as closed after merge to prevent phantom hedges
                if self.engine:
                    self.engine.closed_windows.add(wid)
                # V15.1-25: Track merge analytics
                if self.engine:
                    self.engine.hedge_analytics["resolved_by_merge"] += 1
                    asset = wid.split("-")[0] if "-" in wid else "unknown"
                    if asset not in self.engine.hedge_analytics["per_asset"]:
                        self.engine.hedge_analytics["per_asset"][asset] = {
                            "one_sided": 0, "hedges": 0, "exits": 0,
                            "merges": 0, "abandoned": 0, "t4_sells": 0
                        }
                    self.engine.hedge_analytics["per_asset"][asset]["merges"] += 1
                    # V15.9: Log resolution for CC Analytics
                    self.engine._resolution_log[wid] = {
                        "resolution": "merge",
                        "time": time.time(),
                        "window_pnl": mergeable,  # shares returned = value for merge
                    }
            else:
                self.merges_failed += 1
                cycle_fails += 1
                self._merge_consecutive_fails += 1
                self.logger.warning("  MERGE FAILED | {} | Consecutive fails: {}".format(
                    wid, self._merge_consecutive_fails))
                # V15.1-23: Circuit breaker — if 3 consecutive fails in this
                # cycle, stop trying. If 5+ total consecutive fails, open
                # circuit for 5 minutes (likely systemic issue).
                if cycle_fails >= 3:
                    self.logger.warning(
                        "  MERGE CIRCUIT BREAKER | {} consecutive fails this cycle — "
                        "stopping merges".format(cycle_fails))
                    break
                if self._merge_consecutive_fails >= 5:
                    cooldown = 300  # 5 minutes
                    self._merge_circuit_open_until = time.time() + cooldown
                    self.logger.warning(
                        "  MERGE CIRCUIT OPEN | {} consecutive fails total — "
                        "pausing merges for {}s".format(
                            self._merge_consecutive_fails, cooldown))
                    break
        if merged_count > 0:
            # Reset circuit breaker on any success
            self._merge_consecutive_fails = 0
            self._merge_circuit_open_until = 0
        return merged_count

    def _simulate_merges(self, market_cache, token_holdings):
        merged = 0
        window_holdings = self._find_mergeable(market_cache, token_holdings)
        for wid, info in window_holdings.items():
            mergeable = min(info["up_size"], info["down_size"])
            if mergeable < self.config.merge_min_shares:
                continue
            merge_key = "{}:{:.1f}".format(wid, mergeable)
            if merge_key in self._merged_windows:
                continue
            self._merged_windows.add(merge_key)
            self.merges_completed += 1
            self.total_merged_usd += mergeable
            market = info["market"]
            token_up = market.get("token_up", "")
            token_down = market.get("token_down", "")
            if token_up in token_holdings:
                token_holdings[token_up]["size"] = max(
                    0, token_holdings[token_up]["size"] - mergeable)
            if token_down in token_holdings:
                token_holdings[token_down]["size"] = max(
                    0, token_holdings[token_down]["size"] - mergeable)
            if self.engine:
                self.engine.capital_in_positions = max(
                    0, self.engine.capital_in_positions - mergeable)
                self.engine.session_total_spent = max(
                    0, self.engine.session_total_spent - mergeable)
                self.engine._update_total_capital()
                # V15.1-17b: Clean up window_fill_cost after simulated merge
                old_fill = self.engine.window_fill_cost.get(wid, 0)
                if old_fill > 0:
                    new_fill = max(0, old_fill - mergeable)
                    if new_fill > 0:
                        self.engine.window_fill_cost[wid] = new_fill
                    else:
                        self.engine.window_fill_cost.pop(wid, None)
            # V15.2: Clear fill_tokens to prevent double claim
            if self.engine:
                self.engine.window_fill_tokens.pop(wid, None)
            self.logger.info(
                "  MERGED (sim) | {} | {:.1f} shares -> ~${:.2f} returned".format(
                    wid, mergeable, mergeable))
            merged += 1
        return merged

    def _find_mergeable(self, market_cache, token_holdings):
        window_holdings = {}
        # V15.1-17: Build combined lookup from market_cache + expired_windows_pending_claim
        # so we can still match tokens after windows expire from market_cache.
        combined_cache = dict(market_cache)  # start with active windows
        if self.engine:
            for wid, info in self.engine.expired_windows_pending_claim.items():
                if wid not in combined_cache:
                    combined_cache[wid] = {
                        "token_up": info.get("token_up", ""),
                        "token_down": info.get("token_down", ""),
                        "condition_id": info.get("condition_id", ""),
                        "slug": info.get("slug", ""),
                    }
            # Also check window_metadata for windows that have been registered
            for wid, meta in self.engine.window_metadata.items():
                if wid not in combined_cache:
                    combined_cache[wid] = {
                        "token_up": meta.get("token_up", ""),
                        "token_down": meta.get("token_down", ""),
                        "condition_id": meta.get("condition_id", ""),
                        "slug": meta.get("slug", ""),
                    }
            # Also check window_fill_tokens for token mapping
            for wid, tokens in self.engine.window_fill_tokens.items():
                if wid not in combined_cache:
                    # Reconstruct from fill data + _is_up_token_cache
                    token_up = token_down = ""
                    for t in tokens:
                        tid = t.get("token_id", "")
                        if t.get("is_up"):
                            token_up = tid
                        elif t.get("is_up") is False:
                            token_down = tid
                    if token_up or token_down:
                        combined_cache[wid] = {
                            "token_up": token_up, "token_down": token_down,
                            "condition_id": "", "slug": "",
                        }
        for token_id, holding in token_holdings.items():
            size = holding.get("size", 0)
            if size < self.config.merge_min_shares:
                continue
            for wid, market in combined_cache.items():
                if token_id == market.get("token_up"):
                    if wid not in window_holdings:
                        window_holdings[wid] = {"up_size": 0, "down_size": 0, "market": market}
                    window_holdings[wid]["up_size"] = size
                elif token_id == market.get("token_down"):
                    if wid not in window_holdings:
                        window_holdings[wid] = {"up_size": 0, "down_size": 0, "market": market}
                    window_holdings[wid]["down_size"] = size
        # V15.1-17: Log merge scan results for debugging
        if window_holdings:
            for wid, info in window_holdings.items():
                self.logger.info(
                    "  MERGE SCAN | {} | UP:{:.1f} DN:{:.1f} | mergeable:{:.1f}".format(
                        wid, info["up_size"], info["down_size"],
                        min(info["up_size"], info["down_size"])))
        return window_holdings

    def _execute_merge(self, condition_id, shares, window_id):
        if self.rpc_limiter.is_backed_off:
            return False
        try:
            usdc_addr = Web3.to_checksum_address(USDC_E_ADDRESS)
            parent = bytes(32)
            cid_bytes = self._to_bytes32(condition_id)
            decimals = self.config.merge_position_decimals
            amount_raw = int(shares * (10 ** decimals))
            merge_data = _encode_abi(
                self.ctf_contract, "mergePositions",
                [usdc_addr, parent, cid_bytes, [1, 2], amount_raw])
            ctf_addr = Web3.to_checksum_address(CTF_ADDRESS)
            if self.proxy_contract:
                result = self._merge_via_proxy(ctf_addr, merge_data, window_id)
            else:
                result = self._merge_direct(ctf_addr, merge_data, window_id)
            self.audit.merge_executed(condition_id, shares, window_id, result)
            return result
        except Exception as e:
            err = str(e).lower()
            if "rate limit" in err or "-32090" in err:
                self.rpc_limiter.report_rate_limit(15)
            self.logger.warning("  MERGE ERROR | {} | {}".format(window_id, e))
            self.audit.merge_executed(condition_id, shares, window_id, False)
            return False

    def _merge_via_proxy(self, ctf_addr, merge_data, window_id):
        """Execute mergePositions through the Gnosis Safe proxy wallet.
        V15.1-23: Rewritten to use Safe's execTransaction instead of broken execute/exec."""
        if not self.proxy_contract or not self.account:
            self.logger.warning("  MERGE SKIP | {} | No proxy_contract or account".format(window_id))
            return False
        proxy_addr = Web3.to_checksum_address(self.config.proxy_wallet)
        inner_data = bytes.fromhex(merge_data[2:]) if merge_data.startswith("0x") else bytes.fromhex(merge_data)
        return self._safe_exec_tx(ctf_addr, inner_data, window_id, "MERGE", gas_limit=350000)

    def _proxy_exec_tx(self, target_addr, call_data, window_id, label):
        """Execute a transaction through the Gnosis Safe proxy wallet.
        V15.1-23: Rewritten to use Safe's execTransaction."""
        if not self.proxy_contract or not self.account:
            return False
        inner_data = bytes.fromhex(call_data[2:]) if call_data.startswith("0x") else bytes.fromhex(call_data)
        return self._safe_exec_tx(
            Web3.to_checksum_address(target_addr), inner_data,
            window_id, label, gas_limit=200000)

    def _safe_exec_tx(self, target_addr, inner_data, window_id, label, gas_limit=350000):
        """Execute a call through the Gnosis Safe proxy using execTransaction.
        V15.1-23: Core Safe execution helper. Signs the Safe tx hash with the
        owner key and submits via execTransaction.
        V15.9: Returns (success, gas_cost_matic) tuple instead of just bool.
        gas_cost_matic is the actual MATIC spent on gas (gasUsed * effectiveGasPrice).
        
        The Safe's execTransaction signature:
          execTransaction(to, value, data, operation, safeTxGas, baseGas,
                          gasPrice, gasToken, refundReceiver, signatures)
        For a single-owner Safe with threshold=1, we sign the Safe tx hash
        and pack it as r+s+v (65 bytes)."""
        if not self.proxy_contract or not self.account:
            return False
        proxy_addr = Web3.to_checksum_address(self.config.proxy_wallet)
        zero = Web3.to_checksum_address(ZERO_ADDR)
        try:
            # 1. Get the Safe's internal nonce
            self.rpc_limiter.wait()
            safe_nonce = self.proxy_contract.functions.nonce().call()
            # 2. Compute the Safe transaction hash on-chain
            self.rpc_limiter.wait()
            safe_tx_hash = self.proxy_contract.functions.getTransactionHash(
                target_addr,   # to
                0,             # value
                inner_data,    # data
                0,             # operation (CALL)
                0,             # safeTxGas
                0,             # baseGas
                0,             # gasPrice (no refund)
                zero,          # gasToken
                zero,          # refundReceiver
                safe_nonce     # _nonce
            ).call()
            # 3. Sign the hash with the owner's private key (eth_sign style)
            #    Gnosis Safe expects an eth_sign signature: sign("\x19Ethereum Signed Message:\n32" + hash)
            from eth_account.messages import encode_defunct
            # safe_tx_hash is HexBytes — convert to raw bytes for encode_defunct
            msg = encode_defunct(primitive=bytes(safe_tx_hash))
            signed_msg = self.w3.eth.account.sign_message(
                msg, private_key=self.config.private_key)
            # Pack signature as r(32) + s(32) + v(1) with v += 4 for eth_sign
            sig = (signed_msg.r.to_bytes(32, 'big') +
                   signed_msg.s.to_bytes(32, 'big') +
                   bytes([signed_msg.v + 4]))
            # 4. Build and send the outer transaction calling execTransaction
            self.rpc_limiter.wait()
            eth_nonce = self.w3.eth.get_transaction_count(self.account.address)
            gas_price = self.rpc_limiter.get_gas_price(self.w3)
            txn = self.proxy_contract.functions.execTransaction(
                target_addr, 0, inner_data, 0, 0, 0, 0, zero, zero, sig
            ).build_transaction({
                "from": self.account.address,
                "nonce": eth_nonce,
                "gas": gas_limit,
                "gasPrice": int(gas_price * 1.3),
                "chainId": self.config.chain_id,
            })
            signed_tx = self.w3.eth.account.sign_transaction(
                txn, self.config.private_key)
            self.rpc_limiter.wait()
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=90)
            # V15.9: Compute actual gas cost in MATIC from receipt
            effective_gas_price = getattr(receipt, 'effectiveGasPrice', None)
            if effective_gas_price is None:
                effective_gas_price = int(gas_price * 1.3)
            gas_cost_wei = receipt.gasUsed * effective_gas_price
            gas_cost_matic = gas_cost_wei / 1e18
            # Accumulate gas cost per window and session total
            self._window_gas_costs[window_id] = self._window_gas_costs.get(window_id, 0.0) + gas_cost_matic
            self._session_total_gas += gas_cost_matic
            if receipt.status == 1:
                self.logger.info("  SAFE TX OK | {} | {} | Tx: {} | Gas: {} | Cost: {:.6f} MATIC".format(
                    label, window_id, self.w3.to_hex(tx_hash)[:20], receipt.gasUsed, gas_cost_matic))
                return True
            else:
                self.logger.warning(
                    "  SAFE TX REVERTED | {} | {} | Tx: {} | Gas: {} | Cost: {:.6f} MATIC | "
                    "Signer: {} | Proxy: {}".format(
                        label, window_id,
                        self.w3.to_hex(tx_hash), receipt.gasUsed, gas_cost_matic,
                        self.account.address[:10] + "...",
                        proxy_addr[:10] + "..."))
                return False
        except Exception as e:
            err = str(e)
            err_lower = err.lower()
            self.logger.warning(
                "  SAFE TX ERROR | {} | {} | Signer: {} | Proxy: {} | Error: {}".format(
                    label, window_id,
                    self.account.address[:10] + "...",
                    proxy_addr[:10] + "...",
                    err[:200]))
            if "rate limit" in err_lower or "-32090" in err_lower:
                self.rpc_limiter.report_rate_limit(15)
            return False

    def _merge_direct(self, ctf_addr, merge_data, window_id):
        if not self.account:
            self.logger.warning("  MERGE SKIP (direct) | {} | No account".format(window_id))
            return False
        try:
            self.rpc_limiter.wait()
            nonce = self.w3.eth.get_transaction_count(self.account.address)
            gas_price = self.rpc_limiter.get_gas_price(self.w3)
            txn = {
                "to": ctf_addr, "data": merge_data,
                "from": self.account.address, "nonce": nonce,
                "gas": 300000, "gasPrice": int(gas_price * 1.3),
                "chainId": self.config.chain_id, "value": 0,
            }
            signed = self.w3.eth.account.sign_transaction(txn, self.config.private_key)
            self.rpc_limiter.wait()
            tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=90)
            # V15.9: Track gas cost for direct merge
            effective_gas_price = getattr(receipt, 'effectiveGasPrice', None)
            if effective_gas_price is None:
                effective_gas_price = int(gas_price * 1.3)
            gas_cost_wei = receipt.gasUsed * effective_gas_price
            gas_cost_matic = gas_cost_wei / 1e18
            self._window_gas_costs[window_id] = self._window_gas_costs.get(window_id, 0.0) + gas_cost_matic
            self._session_total_gas += gas_cost_matic
            if receipt.status == 1:
                self.logger.info("  MERGE TX OK (direct) | {} | Tx: {}... | Cost: {:.6f} MATIC".format(
                    window_id, self.w3.to_hex(tx_hash)[:20], gas_cost_matic))
                return True
            else:
                # V15.1-21: Verbose revert logging for direct merge
                self.logger.warning(
                    "  MERGE TX REVERTED (direct) | {} | Tx: {} | Gas used: {} | Cost: {:.6f} MATIC | "
                    "Signer: {}".format(
                        window_id, self.w3.to_hex(tx_hash),
                        receipt.gasUsed, gas_cost_matic, self.account.address[:10] + "..."))
        except Exception as e:
            err = str(e)
            err_lower = err.lower()
            # V15.1-21: Verbose error logging
            self.logger.warning(
                "  MERGE TX ERROR (direct) | {} | Signer: {} | Error: {}".format(
                    window_id, self.account.address[:10] + "...", err[:200]))
            if "rate limit" in err_lower or "-32090" in err_lower:
                self.rpc_limiter.report_rate_limit(15)
        return False

    def get_gas_cost(self, window_id):
        """V15.9: Get accumulated gas cost for a specific window."""
        return self._window_gas_costs.get(window_id, 0.0)

    def get_gas_stats(self):
        """V15.9: Get session-level gas cost statistics."""
        window_count = len(self._window_gas_costs)
        return {
            "session_total_gas_matic": self._session_total_gas,
            "windows_with_gas": window_count,
            "avg_gas_per_window": self._session_total_gas / window_count if window_count > 0 else 0.0,
            "window_gas_costs": dict(self._window_gas_costs),
        }

    def get_stats(self):
        return {
            "merges_completed": self.merges_completed,
            "merges_failed": self.merges_failed,
            "total_merged_usd": self.total_merged_usd,
            "session_total_gas_matic": self._session_total_gas,
        }


# -----------------------------------------------------------------
# Auto-Claim Manager
# -----------------------------------------------------------------

class AutoClaimManager:
    GAMMA_BASE = "https://gamma-api.polymarket.com"

    def __init__(self, config, logger, engine=None):
        self.config = config
        self.logger = logger
        self.engine = engine
        self.w3 = None
        self.ctf_contract = None
        self.account = None
        self.proxy_contract = None
        self.claimed_conditions = set()
        self._pending_claims = {}
        self._claim_attempts = {}
        self._claim_results = []
        self.total_claimed_usd = 0.0
        self.blind_redeem_attempts = 0
        self.blind_redeem_successes = 0
        self.rpc_limiter = RPCRateLimiter(min_interval=config.rpc_min_call_interval)
        # V15.9: Gas cost tracking for claim transactions
        self._window_gas_costs = {}  # {window_id: total_gas_matic}
        self._session_total_gas = 0.0
        self._init_web3()

    def set_engine(self, engine):
        self.engine = engine

    def _init_web3(self):
        if not HAS_WEB3 or not self.config.private_key:
            self.logger.info("  Auto-claim: web3 not available or no private key")
            return
        try:
            self.w3 = Web3(Web3.HTTPProvider(
                self.config.polygon_rpc, request_kwargs={"timeout": 15}))
            self.ctf_contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(CTF_ADDRESS), abi=CTF_FULL_ABI)
            self.account = self.w3.eth.account.from_key(self.config.private_key)
            if self.config.proxy_wallet:
                try:
                    self.proxy_contract = self.w3.eth.contract(
                        address=Web3.to_checksum_address(self.config.proxy_wallet),
                        abi=PROXY_EXEC_ABI)
                except Exception:
                    self.proxy_contract = None
            self.logger.info("  Auto-claim initialized | Signer: {}...{} | Proxy: {}".format(
                self.account.address[:8], self.account.address[-4:],
                "{}...{}".format(self.config.proxy_wallet[:8], self.config.proxy_wallet[-4:])
                if self.config.proxy_wallet else "NONE"))
        except Exception as e:
            self.logger.warning(f"  Auto-claim init failed: {e}")
            self.w3 = None

    def _to_bytes32(self, hex_str):
        clean = hex_str.replace("0x", "")
        raw = bytes.fromhex(clean)
        if len(raw) < 32:
            raw = raw.rjust(32, b'\x00')
        return raw[:32]

    def _parse_rate_limit_delay(self, error_msg):
        msg = str(error_msg).lower()
        if "retry in" in msg:
            try:
                parts = msg.split("retry in")[1].strip().split("s")[0].strip()
                return int(parts) + 2
            except (ValueError, IndexError):
                pass
        return 15

    def check_resolution_gamma(self, slug):
        try:
            resp = api_retry(lambda: requests.get(
                f"{self.GAMMA_BASE}/events", params={"slug": slug}, timeout=10,
            ), logger=self.logger)
            if resp and resp.status_code == 200:
                events = resp.json()
                if events:
                    event = events[0]
                    event_markets = event.get("markets", [])
                    if event_markets:
                        m = event_markets[0]
                        resolved = m.get("resolved", False)
                        if resolved:
                            outcomes_raw = m.get("outcomes", "[]")
                            if isinstance(outcomes_raw, str):
                                outcomes = json.loads(outcomes_raw)
                            else:
                                outcomes = outcomes_raw
                            outcome = m.get("outcome", "")
                            clob_ids_raw = m.get("clobTokenIds", "[]")
                            if isinstance(clob_ids_raw, str):
                                clob_ids = json.loads(clob_ids_raw)
                            else:
                                clob_ids = clob_ids_raw
                            winning_token = ""
                            if outcome and clob_ids and outcomes:
                                try:
                                    idx = outcomes.index(outcome)
                                    winning_token = clob_ids[idx] if idx < len(clob_ids) else ""
                                except (ValueError, IndexError):
                                    pass
                            return {
                                "resolved": True, "outcome": outcome,
                                "winning_token": winning_token,
                                "condition_id": m.get("conditionId", ""),
                            }
            return {"resolved": False}
        except Exception as e:
            self.logger.debug(f"  Gamma resolution check failed: {e}")
            return {"resolved": False}

    def check_resolution_onchain(self, condition_id):
        if not self.w3 or not self.ctf_contract or not condition_id:
            return False
        if self.rpc_limiter.is_backed_off:
            return False
        try:
            self.rpc_limiter.wait()
            cid_bytes = self._to_bytes32(condition_id)
            denominator = self.ctf_contract.functions.payoutDenominator(cid_bytes).call()
            return denominator > 0
        except Exception as e:
            err_msg = str(e)
            if "rate limit" in err_msg.lower() or "-32090" in err_msg:
                self.rpc_limiter.report_rate_limit(self._parse_rate_limit_delay(err_msg))
            return False

    def schedule_claim(self, condition_id, window_id, end_time, slug="",
                       tokens=None, token_up="", token_down=""):
        if not condition_id or condition_id in self.claimed_conditions:
            return
        if window_id in self._pending_claims:
            return
        self._pending_claims[window_id] = {
            "condition_id": condition_id, "end_time": end_time,
            "scheduled": time.time(), "slug": slug,
            "tokens": tokens or [], "token_up": token_up,
            "token_down": token_down, "last_check": 0,
            "resolved": False, "winning_token": "", "outcome": "",
        }
        self._claim_attempts[window_id] = 0
        self.logger.info("  CLAIM SCHEDULED | {} | cond: {}...".format(
            window_id, condition_id[:16]))

    def process_claims(self):
        if not self.config.auto_claim_enabled:
            return 0
        now = time.time()
        claimed = 0
        for wid in list(self._pending_claims.keys()):
            info = self._pending_claims[wid]
            condition_id = info.get("condition_id", "")
            if condition_id in self.claimed_conditions:
                del self._pending_claims[wid]
                continue
            if now < info["end_time"] + self.config.claim_delay_seconds:
                continue
            attempts = self._claim_attempts.get(wid, 0)
            backoff_interval = self.config.claim_check_interval * (1.2 ** min(attempts, 10))
            if now - info.get("last_check", 0) < backoff_interval:
                continue
            info["last_check"] = now
            if attempts >= self.config.claim_max_attempts:
                self.logger.warning("  CLAIM MAX ATTEMPTS | {} | {} tries".format(wid, attempts))
                self._log_manual_claim_instructions(wid, info)
                del self._pending_claims[wid]
                continue
            if now - info["end_time"] > self.config.claim_timeout_seconds:
                self.logger.warning("  CLAIM TIMEOUT | {}".format(wid))
                self._log_manual_claim_instructions(wid, info)
                del self._pending_claims[wid]
                continue
            self._claim_attempts[wid] = attempts + 1
            # V15.1-17: Check resolution FIRST before any redeem attempt.
            # Previous logic tried blind redeem before checking resolution,
            # wasting RPC calls on unresolved markets.
            if not info.get("resolved", False):
                resolved_info = self._check_if_resolved(wid, info)
                if not resolved_info:
                    # Not resolved yet — skip all redeem attempts
                    if attempts % 10 == 0:
                        self.logger.info("  CLAIM WAIT | {} | Not resolved yet ({} checks)".format(
                            wid, attempts + 1))
                    continue
                info["resolved"] = True
                info["winning_token"] = resolved_info.get("winning_token", "")
                info["outcome"] = resolved_info.get("outcome", "")
                self.logger.info("  RESOLVED | {} | Winner: {}".format(wid, info["outcome"]))
            # Market is resolved — now try to claim/redeem
            # Priority: 1) CLOB-SELL (fastest, no gas) 2) CTF-DIRECT 3) CTF-PROXY 4) BLIND-REDEEM
            if self.config.claim_fallback_sell and info.get("winning_token"):
                success = self._fallback_sell(wid, info)
                if success:
                    self._mark_claimed(wid, condition_id, "CLOB-SELL")
                    claimed += 1
                    continue
            if not self.rpc_limiter.is_backed_off:
                success = self._redeem_direct(condition_id, wid)
                if success:
                    self._mark_claimed(wid, condition_id, "CTF-DIRECT")
                    claimed += 1
                    continue
            if self.proxy_contract and self.config.proxy_wallet:
                if not self.rpc_limiter.is_backed_off:
                    success = self._redeem_via_proxy(condition_id, wid)
                    if success:
                        self._mark_claimed(wid, condition_id, "CTF-PROXY")
                        claimed += 1
                        continue
            # V15.1-17: Blind redeem as last resort (market already confirmed resolved)
            if self.config.blind_redeem_enabled and not self.rpc_limiter.is_backed_off:
                success = self._try_blind_redeem(condition_id, wid)
                if success:
                    self._mark_claimed(wid, condition_id, "BLIND-REDEEM")
                    claimed += 1
                    continue
            next_interval = self.config.claim_check_interval * (1.2 ** min(attempts + 1, 10))
            if attempts % 10 == 0:
                self.logger.info("  CLAIM RETRY | {} | {}/{} | Next {:.0f}s".format(
                    wid, attempts + 1, self.config.claim_max_attempts, next_interval))
        return claimed

    def _try_blind_redeem(self, condition_id, window_id):
        if not self.w3 or not self.ctf_contract or not self.account:
            self.logger.debug("  BLIND REDEEM SKIP | {} | No web3/contract/account".format(window_id))
            return False
        self.blind_redeem_attempts += 1
        try:
            usdc_addr = Web3.to_checksum_address(USDC_E_ADDRESS)
            parent = bytes(32)
            cid_bytes = self._to_bytes32(condition_id)
            # V15.1-16: Check payoutDenominator first to avoid wasting gas on
            # unresolved markets. If denom==0, market hasn't resolved yet.
            try:
                self.rpc_limiter.wait()
                denom = self.ctf_contract.functions.payoutDenominator(cid_bytes).call()
                if denom == 0:
                    self.logger.info("  BLIND REDEEM SKIP | {} | Not resolved (denom=0)".format(window_id))
                    return False
            except Exception as e_denom:
                err_d = str(e_denom).lower()
                if "rate limit" in err_d or "-32090" in err_d:
                    self.rpc_limiter.report_rate_limit(self._parse_rate_limit_delay(e_denom))
                    return False
                self.logger.info("  BLIND REDEEM | {} | payoutDenominator check failed: {}".format(
                    window_id, str(e_denom)[:80]))
                # Continue anyway — blind redeem is meant to try even without confirmation
            # V15.1-23: Try proxy wallet first using Safe execTransaction
            if self.proxy_contract and self.config.proxy_wallet:
                redeem_data = _encode_abi(
                    self.ctf_contract, "redeemPositions",
                    [usdc_addr, parent, cid_bytes, [1, 2]])
                ctf_addr = Web3.to_checksum_address(CTF_ADDRESS)
                inner_data = bytes.fromhex(redeem_data[2:]) if redeem_data.startswith("0x") else bytes.fromhex(redeem_data)
                success = self._safe_exec_tx(
                    ctf_addr, inner_data, window_id, "BLIND-REDEEM", gas_limit=300000)
                if success:
                    self.blind_redeem_successes += 1
                    self.logger.info("  BLIND REDEEM OK (safe) | {}".format(window_id))
                    return True
            # V15.1-16: Fall back to direct redeem if no proxy or proxy failed
            return self._redeem_direct(condition_id, window_id)
        except Exception as e:
            err = str(e).lower()
            self.logger.info("  BLIND REDEEM ERROR | {} | {}".format(window_id, str(e)[:100]))
            if "rate limit" in err or "-32090" in err:
                self.rpc_limiter.report_rate_limit(self._parse_rate_limit_delay(e))
            return False

    def _check_if_resolved(self, wid, info):
        slug = info.get("slug", "")
        if slug:
            res = self.check_resolution_gamma(slug)
            if res.get("resolved"):
                return res
        condition_id = info.get("condition_id", "")
        if condition_id and self.check_resolution_onchain(condition_id):
            token_up = info.get("token_up", "")
            token_down = info.get("token_down", "")
            winning_token = ""
            outcome = "UNKNOWN"
            if self.engine:
                held_up = self.engine.token_holdings.get(token_up, {}).get("size", 0)
                held_down = self.engine.token_holdings.get(token_down, {}).get("size", 0)
                if held_up > held_down:
                    winning_token = token_up
                    outcome = "Up (inferred)"
                elif held_down > 0:
                    winning_token = token_down
                    outcome = "Down (inferred)"
            return {"resolved": True, "outcome": outcome, "winning_token": winning_token}
        return None

    def _redeem_direct(self, condition_id, window_id):
        if not self.w3 or not self.ctf_contract or not self.account:
            return False
        if self.rpc_limiter.is_backed_off:
            return False
        try:
            usdc_addr = Web3.to_checksum_address(USDC_E_ADDRESS)
            parent = bytes(32)
            cid_bytes = self._to_bytes32(condition_id)
            self.rpc_limiter.wait()
            try:
                denom = self.ctf_contract.functions.payoutDenominator(cid_bytes).call()
                if denom == 0:
                    return False
            except Exception as e:
                if "rate limit" in str(e).lower() or "-32090" in str(e):
                    self.rpc_limiter.report_rate_limit(self._parse_rate_limit_delay(e))
                    return False
            self.rpc_limiter.wait()
            nonce = self.w3.eth.get_transaction_count(self.account.address)
            gas_price = self.rpc_limiter.get_gas_price(self.w3)
            txn = self.ctf_contract.functions.redeemPositions(
                usdc_addr, parent, cid_bytes, [1, 2],
            ).build_transaction({
                "from": self.account.address, "nonce": nonce,
                "gas": 250000, "gasPrice": int(gas_price * 1.3),
                "chainId": self.config.chain_id,
            })
            signed = self.w3.eth.account.sign_transaction(txn, self.config.private_key)
            self.rpc_limiter.wait()
            tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=90)
            # V15.9: Track gas cost for claim transactions
            effective_gas_price = getattr(receipt, 'effectiveGasPrice', None)
            if effective_gas_price is None:
                effective_gas_price = int(gas_price * 1.3)
            gas_cost_wei = receipt.gasUsed * effective_gas_price
            gas_cost_matic = gas_cost_wei / 1e18
            self._track_gas_cost(window_id, gas_cost_matic)
            if receipt.status == 1:
                self.logger.info("  CLAIMED (direct) | {} | Tx: {} | Gas: {:.6f} MATIC".format(
                    window_id, self.w3.to_hex(tx_hash)[:20], gas_cost_matic))
                return True
            return False
        except Exception as e:
            err = str(e).lower()
            if "rate limit" in err or "-32090" in err:
                self.rpc_limiter.report_rate_limit(self._parse_rate_limit_delay(e))
            return False

    def _redeem_via_proxy(self, condition_id, window_id):
        """Redeem positions through the Gnosis Safe proxy wallet.
        V15.1-23: Rewritten to use Safe's execTransaction."""
        if not self.w3 or not self.proxy_contract or not self.account:
            return False
        if self.rpc_limiter.is_backed_off:
            return False
        try:
            usdc_addr = Web3.to_checksum_address(USDC_E_ADDRESS)
            parent = bytes(32)
            cid_bytes = self._to_bytes32(condition_id)
            redeem_data = _encode_abi(
                self.ctf_contract, "redeemPositions",
                [usdc_addr, parent, cid_bytes, [1, 2]])
            ctf_addr = Web3.to_checksum_address(CTF_ADDRESS)
            inner_data = bytes.fromhex(redeem_data[2:]) if redeem_data.startswith("0x") else bytes.fromhex(redeem_data)
            success = self._safe_exec_tx(
                ctf_addr, inner_data, window_id, "REDEEM", gas_limit=350000)
            if success:
                self.logger.info("  CLAIMED (proxy/safe) | {}".format(window_id))
            return success
        except Exception as e:
            err = str(e).lower()
            if "rate limit" in err or "-32090" in err:
                self.rpc_limiter.report_rate_limit(self._parse_rate_limit_delay(e))
            self.logger.warning("  REDEEM PROXY ERROR | {} | {}".format(window_id, str(e)[:100]))
            return False

    def _fallback_sell(self, wid, info):
        if not self.engine or not self.engine.client:
            return False
        winning_token = info.get("winning_token", "")
        if not winning_token:
            token_up = info.get("token_up", "")
            token_down = info.get("token_down", "")
            outcome = info.get("outcome", "").lower()
            if "up" in outcome:
                winning_token = token_up
            elif "down" in outcome:
                winning_token = token_down
            else:
                for tok in [token_up, token_down]:
                    if tok and tok in self.engine.token_holdings:
                        held = self.engine.token_holdings[tok].get("size", 0)
                        if held > 0:
                            winning_token = tok
                            break
        if not winning_token:
            return False
        held = self.engine.token_holdings.get(winning_token, {}).get("size", 0)
        if held < 1.0:
            return False
        try:
            result = self.engine.place_order(
                winning_token, "SELL", self.config.claim_sell_min_price, held,
                wid, "CLAIM-SELL", "mm", is_taker=True)
            if result:
                self.logger.info("  CLAIM-SELL | {} | {:.1f} @ ${:.2f}".format(
                    wid, held, self.config.claim_sell_min_price))
                return True
        except Exception as e:
            self.logger.debug(f"  CLOB sell failed for {wid}: {e}")
        return False

    def _mark_claimed(self, wid, condition_id, method):
        self.claimed_conditions.add(condition_id)
        info = self._pending_claims.pop(wid, {})
        tokens = info.get("tokens", [])
        total_size = sum(t.get("size", 0) for t in tokens)
        self.total_claimed_usd += total_size
        self._claim_results.append({
            "window_id": wid, "method": method, "time": time.time(),
            "condition_id": condition_id, "est_amount": total_size,
        })
        if self.engine:
            self.engine.record_claim(total_size)
            # V15.1-P5: Track realized PnL from claim.
            # Claim returns the winning side's shares as USDC.
            # Cost was what we paid for those shares.
            fill_cost = info.get("fill_cost", 0)
            self.engine.session_realized_returns += total_size
            self.engine.session_realized_cost += fill_cost
            # V15.1-20: Release held_windows — capital recovered via claim
            self.engine.held_windows.discard(wid)
        self.logger.info("  CLAIMED | {} | {} | Est: ${:.2f}".format(wid, method, total_size))

    def _log_manual_claim_instructions(self, wid, info):
        cid = info.get("condition_id", "")
        self.logger.warning("  ---- MANUAL CLAIM REQUIRED ----")
        self.logger.warning("  Window: {} | Condition: {}".format(wid, cid))
        self.logger.warning("  Go to polymarket.com -> Portfolio -> Claim")
        self.logger.warning("  ---------------------------------")

    def execute_pre_exits(self, markets, price_feed, book_reader):
        if not self.config.pre_exit_enabled or not self.engine:
            return 0
        exits = 0
        now = time.time()
        for market in markets:
            wid = market["window_id"]
            time_left = market["end_time"] - now
            # V15.6: Timeframe-aware pre-exit timing
            # 5m windows: exit 60s before end (was 30s)
            # 15m windows: exit 120s before end (was 30s)
            # Adverse selection peaks in the final minute; earlier exit protects held positions
            tf = market.get("timeframe", "5m")
            if tf == "15m":
                pre_exit_window = getattr(self.config, 'pre_exit_time_15m', self.config.pre_exit_time_seconds)
            else:
                pre_exit_window = getattr(self.config, 'pre_exit_time_5m', self.config.pre_exit_time_seconds)
            if time_left > pre_exit_window or time_left < 5:
                continue
            token_up = market["token_up"]
            token_down = market["token_down"]
            held_up = self.engine.token_holdings.get(token_up, {}).get("size", 0)
            held_down = self.engine.token_holdings.get(token_down, {}).get("size", 0)
            if held_up < 1.0 and held_down < 1.0:
                continue
            prediction = price_feed.predict_resolution(
                market["asset"], market["timestamp"], market["end_time"])
            if not prediction or prediction["confidence"] < self.config.pre_exit_min_confidence:
                continue
            if prediction["direction"] == "UP" and held_up >= 1.0:
                spread = book_reader.get_spread(token_up)
                if spread and spread["bid"] >= self.config.pre_exit_min_price:
                    result = self.engine.place_order(
                        token_up, "SELL", spread["bid"], held_up,
                        wid, "PRE-EXIT-UP", "mm", is_taker=True)
                    if result:
                        exits += 1
            elif prediction["direction"] == "DOWN" and held_down >= 1.0:
                spread = book_reader.get_spread(token_down)
                if spread and spread["bid"] >= self.config.pre_exit_min_price:
                    result = self.engine.place_order(
                        token_down, "SELL", spread["bid"], held_down,
                        wid, "PRE-EXIT-DN", "mm", is_taker=True)
                    if result:
                        exits += 1
        return exits

    def _track_gas_cost(self, window_id, gas_cost_matic):
        """V15.9: Track gas cost for a window's claim transaction."""
        self._window_gas_costs[window_id] = self._window_gas_costs.get(window_id, 0.0) + gas_cost_matic
        self._session_total_gas += gas_cost_matic

    def get_gas_cost(self, window_id):
        """V15.9: Get accumulated gas cost for a specific window."""
        return self._window_gas_costs.get(window_id, 0.0)

    def get_claim_stats(self):
        return {
            "pending_claims": len(self._pending_claims),
            "claimed_total": len(self.claimed_conditions),
            "total_claimed_usd": self.total_claimed_usd,
            "claim_results": list(self._claim_results[-10:]),
            "rpc_backed_off": self.rpc_limiter.is_backed_off,
            "blind_attempts": self.blind_redeem_attempts,
            "blind_successes": self.blind_redeem_successes,
            "session_total_gas_matic": self._session_total_gas,
        }


# -----------------------------------------------------------------
# Trading Engine (V15.1-4: verbose order rejection)
