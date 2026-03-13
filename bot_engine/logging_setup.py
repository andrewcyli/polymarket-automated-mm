"""Logging setup and structured audit logger."""

import os
import json
import logging
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler


LOG_DIR = "logs"


def setup_logging(level="INFO"):
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = logging.getLogger("polybot_v15_1")
    logger.setLevel(getattr(logging, level, logging.INFO))
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    fh = RotatingFileHandler("bot_v15_1.log", maxBytes=10 * 1024 * 1024, backupCount=5)
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    session_path = os.path.join(LOG_DIR, "bot_v15_1_{}".format(ts) + ".log")
    sh = logging.FileHandler(session_path, mode="w")
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.info("  Session log: {}".format(session_path))
    return logger


# -----------------------------------------------------------------
# Structured Audit Logger — JSONL for trade/fill/merge events
# -----------------------------------------------------------------

class AuditLogger:
    """Writes structured JSON events to a JSONL file for audit trail.
    Each line is a self-contained JSON object with event type, timestamp,
    and relevant fields for post-hoc analysis."""

    def __init__(self, log_dir=LOG_DIR):
        os.makedirs(log_dir, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self._path = os.path.join(log_dir, "audit_{}.jsonl".format(ts))
        self._fh = open(self._path, "a")

    def _write(self, event):
        event["ts"] = datetime.now(timezone.utc).isoformat()
        try:
            self._fh.write(json.dumps(event, default=str) + "\n")
            self._fh.flush()
        except Exception:
            pass  # Never crash the bot due to audit logging

    def order_placed(self, order_id, token_id, side, price, size,
                     window_id, strategy, is_taker=False, dry_run=False):
        self._write({
            "event": "order_placed",
            "order_id": order_id,
            "token_id": token_id,
            "side": side,
            "price": price,
            "size": size,
            "cost": round(price * size, 4),
            "window_id": window_id,
            "strategy": strategy,
            "is_taker": is_taker,
            "dry_run": dry_run,
        })

    def order_rejected(self, reason, token_id, side, price, size,
                       window_id, strategy, details=None):
        self._write({
            "event": "order_rejected",
            "reason": reason,
            "token_id": token_id,
            "side": side,
            "price": price,
            "size": size,
            "window_id": window_id,
            "strategy": strategy,
            "details": details or {},
        })

    def fill_recorded(self, token_id, side, price, size, fee, window_id=""):
        self._write({
            "event": "fill",
            "token_id": token_id,
            "side": side,
            "price": price,
            "size": size,
            "fee": fee,
            "cost": round(price * size + fee, 4),
            "window_id": window_id,
        })

    def merge_executed(self, condition_id, shares, window_id, success):
        self._write({
            "event": "merge",
            "condition_id": condition_id,
            "shares": shares,
            "window_id": window_id,
            "success": success,
        })

    def close(self):
        try:
            self._fh.close()
        except Exception:
            pass


