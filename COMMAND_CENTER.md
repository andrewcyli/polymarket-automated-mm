# PolyMaker Command Center Integration

This document explains how to connect the trading bot to the **PolyMaker Command Center** — a web-based dashboard for monitoring, configuring, and optimizing your market-making bot.

## Architecture

```
┌──────────────────────────┐     HTTPS/REST      ┌──────────────────────────┐
│   Your Machine (Local)   │ ──────────────────►  │   Command Center (Cloud) │
│                          │                      │                          │
│  • Python trading bot    │  Trade logs          │  • Dashboard monitoring  │
│  • Private keys (.env)   │  Run metrics         │  • Config management     │
│  • Polymarket WebSocket  │  Market reports       │  • Feedback loop         │
│                          │ ◄──────────────────  │  • Performance analysis  │
│                          │  Config fetch         │  • Wallet balance check  │
└──────────────────────────┘                      └──────────────────────────┘
```

**Key principle:** Your private keys never leave your local machine. The Command Center only receives trade logs and metrics.

## Setup

### 1. Add Environment Variables

Add these three lines to your `.env` file (alongside your existing PK, BROWSER_ADDRESS, etc.):

```bash
# PolyMaker Command Center
POLYMAKER_URL=https://your-app.manus.space    # Your published Command Center URL
POLYMAKER_API_KEY=your-secure-api-key          # Must match the key set in Command Center
POLYMAKER_ENABLED=true                         # Set to 'false' to disable without removing
```

### 2. Set the API Key in the Command Center

In the Command Center web UI:
1. Go to **Settings > Secrets**
2. Set `BOT_API_KEY` to the same value as `POLYMAKER_API_KEY` in your `.env`

### 3. Run the Bot

```bash
python main.py
```

On startup, the bot will:
1. Connect to the Command Center and fetch the active config
2. Start a new "run" session
3. Log all trades (buy/sell orders) to the Command Center in real-time
4. Push metrics (P&L, fill rate, cycle count) every 30 seconds
5. Report active markets for the Markets page watchlist

On shutdown (Ctrl+C or SIGTERM):
1. The bot gracefully stops the run in the Command Center
2. Auto-analysis runs and generates optimization suggestions
3. You receive a notification with the run summary

## What Gets Sent to the Command Center

| Data | When | Purpose |
|------|------|---------|
| Run start/stop | Bot start/shutdown | Track bot sessions |
| Trade orders | Each buy/sell order | Trade history & fill tracking |
| Metrics (P&L, fills, cycles) | Every 30 seconds | Real-time dashboard |
| Active markets | Every 60 seconds | Markets page watchlist |

## What Does NOT Get Sent

- Private keys (PK)
- API credentials (API_KEY, API_SECRET, PASSPHRASE)
- Google Sheets credentials
- Wallet addresses (only used locally for RPC balance checks)

## Disabling the Integration

To disable without removing code:

```bash
POLYMAKER_ENABLED=false
```

Or simply remove the `POLYMAKER_URL` and `POLYMAKER_API_KEY` from your `.env`. The bot will continue trading normally — all Command Center calls are wrapped in try/except and silently fail.

## Module Reference

### `polymaker_client.py`

The main client module. Import the singleton:

```python
from polymaker_client import cc

# Check connection
status = cc.check_status()

# Start a run
run_id = cc.start_run(config_id=1, mode="live", bankroll=500.0)

# Log a trade
trade_id = cc.log_trade(
    cycle=1,
    asset="BTC",
    window="15m",
    side="buy",
    outcome="Up",
    price=0.52,
    size=10.0,
    token_id="123456789",
)

# Update metrics
cc.update_run(total_pnl=12.5, total_fills=50, total_orders=100)

# Stop run (triggers auto-analysis)
cc.stop_run(status="completed", total_pnl=12.5)
```

### Integration Points in the Bot

| File | What's Added |
|------|-------------|
| `main.py` | Run lifecycle (start/stop), periodic metrics push, market reporting |
| `trading.py` | Trade logging for buy/sell orders |

### Fault Tolerance

All Command Center calls are:
- Wrapped in `try/except` — never crash the bot
- Timeout-limited (10 seconds default)
- Logged as warnings on failure
- Completely skipped if `POLYMAKER_ENABLED=false` or credentials are missing

The bot continues trading normally even if the Command Center is completely unreachable.
