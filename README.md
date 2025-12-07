# Poly-Maker

Disclaimer, I forked and recreated these codes from the original creator @defiance_cr and continued to add features on top.
A market making bot for Polymarket prediction markets. This bot automates the process of providing liquidity to markets on Polymarket by maintaining orders on both sides of the book with configurable parameters.

## Overview

Poly-Maker is a comprehensive solution for automated market making on Polymarket. It includes:

- **Real-time order book monitoring** via WebSockets
- **Two-sided market making** - Places buy and sell orders simultaneously
- **Reward-optimized pricing** - Calculates optimal order placement based on Polymarket's maker reward formula
- **Automated market selection** - Data-driven market selection by profitability or daily rewards
- **Position management** with risk controls and automated position merging
- **Customizable trade parameters** fetched from Google Sheets
- **Maker reward tracking** - Real-time visibility into estimated rewards
- **Reduced order churn** - Intelligent cancellation thresholds to minimize gas fees

## Structure

The repository consists of several interconnected modules:

- `poly_data`: Core data management and market making logic
- `poly_merger`: Utility for merging positions (based on open-source Polymarket code)
- `poly_stats`: Account statistics tracking
- `poly_utils`: Shared utility functions
- `data_updater`: Separate module for collecting market information

## Requirements

- Python 3.9 with latest setuptools
- Node.js (for poly_merger)
- Google Sheets API credentials
- Polymarket account and API credentials

## Installation

1. **Clone the repository**:
```
git clone https://github.com/yourusername/poly-maker.git
cd poly-maker
```

2. **Install Python dependencies**:
```
pip install -r requirements.txt
```

3. **Install Node.js dependencies for the merger**:
```
cd poly_merger
npm install
cd ..
```

4. **Set up environment variables**:
```
cp .env.example .env
```

5. **Configure your credentials in `.env`**:
- `PK`: Your private key for Polymarket
- `BROWSER_ADDRESS`: Your wallet address

Make sure your wallet has done at least one trade thru the UI so that the permissions are proper.

6. **Set up Google Sheets integration**:
   - Create a Google Service Account and download credentials to the main directory
   - Add your Google service account to the sheet with edit permissions
   - Update `SPREADSHEET_URL` in your `.env` file

7. **Update market data**:
   ```bash
   # Run the data updater to fetch all available markets
   python data_updater/data_updater.py
   ```
   - This fetches all available markets and calculates rewards/volatility metrics
   - Should run continuously in the background (preferably on a different IP than your trading bot)

8. **Select markets to trade**:
   ```bash
   # Option 1: Automated selection by profitability (default)
   python update_selected_markets.py
   
   # Option 2: High reward mode (markets with >= $100/day)
   python update_selected_markets.py --min-reward 100 --max-markets 10
   
   # Option 3: Manual selection - Add markets to "Selected Markets" sheet
   # Select from "Volatility Markets" sheet for best results
   ```

9. **Configure trading parameters**:
   - Edit the "Hyperparameters" sheet in Google Sheets
   - Default parameters that worked well are included
   - Each market can have custom parameters via the "Selected Markets" sheet

10. **Start the market making bot**:
```bash
python main.py
```

## Configuration

The bot is configured via a Google Spreadsheet with several worksheets:

- **Selected Markets**: Markets you want to trade (can be auto-populated)
- **All Markets**: Database of all markets on Polymarket with metrics
- **Volatility Markets**: Filtered markets with volatility_sum < 20
- **Hyperparameters**: Configuration parameters for the trading logic
- **Trade Log**: Automatic logging of all trades (auto-created)
- **Maker Rewards**: Estimated reward tracking (auto-created)

## Key Features

### 1. Reward-Optimized Pricing
The bot calculates optimal order placement based on Polymarket's maker reward formula:
- Formula: `S = ((v - s) / v)^2` where `v` is max_spread and `s` is distance from mid-price
- Places orders at ~15% of max_spread distance for optimal reward/fill balance
- Maximizes maker rewards while maintaining fill probability

### 2. Automated Market Selection
Two selection modes available:

**Profitability Mode** (default):
```bash
python update_selected_markets.py
```
- Selects markets by `profitability_score = gm_reward_per_100 / (volatility_sum + 1)`
- Filters: reward >= 1.0%, volatility < 20, spread < 0.1, price 0.1-0.9
- Targets 5-6 markets for optimal diversification

**High Reward Mode**:
```bash
python update_selected_markets.py --min-reward 100 --max-markets 10
```
- Filters markets by minimum daily reward (e.g., >= $100/day)
- Automatically assigns trade sizes based on reward level
- Great for focusing on high-reward opportunities

### 3. Reduced Order Churn
- **30-second cooldown** between trading actions on price changes
- **Wider cancellation thresholds**: 1.5% price diff, 25% size diff (vs. previous 0.5% and 10%)
- **Result**: ~95% reduction in unnecessary order cancellations, saving gas fees

### 4. Two-Sided Market Making
- Places buy and sell orders simultaneously, even without existing positions
- Enabled via `TWO_SIDED_MARKET_MAKING=true` in `.env`
- Earns from both maker rewards and spread capture

### 5. Position Merging
The `poly_merger` module automatically merges opposing YES/NO positions:
- Frees up locked capital when you have both sides of a market
- Triggers when mergeable amount > $20 (configurable)
- Built on open-source Polymarket code
- Reduces gas fees and improves capital efficiency

### 6. Maker Reward Tracking
- Automatically logs order snapshots every 5 minutes to "Maker Rewards" sheet
- Estimates hourly/daily rewards based on order placement
- Tracks position sizes, order prices, and distance from mid-price
- Provides real-time visibility into reward earnings


## Environment Variables

Configure in `.env` file:

**Required:**
- `PK` - Your private key for Polymarket
- `BROWSER_ADDRESS` - Your wallet address
- `SPREADSHEET_URL` - Google Sheets URL

**Optional:**
- `POLYGON_RPC_URL` - Polygon RPC endpoint (default: https://polygon-rpc.com)
- `TWO_SIDED_MARKET_MAKING` - Enable two-sided market making (default: false)
- `AGGRESSIVE_MODE` - Skip safety checks (use with caution, default: false)

## Usage Examples

### Basic Workflow
```bash
# 1. Update market data (run in background)
python update_markets.py

# 2. Auto-select profitable markets
python update_selected_markets.py

# 3. Start trading bot
python main.py
```

### Automated Workflow
```bash
# Terminal 1: Data updater (run continuously in background)
python data_updater/data_updater.py

# Terminal 2: Trading bot
python main.py
```

### High Reward Focus
```bash
# Select markets with >= $150/day rewards
python update_selected_markets.py --min-reward 150 --max-markets 15 --replace

# Start bot
python main.py
```

### Utility Scripts
```bash
# Cancel all orders
python cancel_all_orders.py

# Check current positions
python check_positions.py

# Update hyperparameters in Google Sheets
python update_hyperparameters.py

# Validate bot configuration
python validate_polymarket_bot.py
```

## Monitoring

### Log Files
- `main.log` - Main bot activity and trading decisions
- `data_updater.log` - Market data update logs
- `websocket_handlers.log` - WebSocket connection events
- `data_processing.log` - Order book processing

### Google Sheets
- **Trade Log** - Every order placed/filled/cancelled with timestamps
- **Maker Rewards** - Estimated rewards updated every 5 minutes
- **Position Snapshots** - Periodic position snapshots (every 5 minutes)

## Performance Improvements

| Feature | Improvement |
|---------|-------------|
| Order Cancellations | ~95% reduction (30s cooldown + wider thresholds) |
| Reward Optimization | Orders placed at optimal distance for max rewards |
| Market Selection | Automated data-driven selection vs. manual |
| Capital Efficiency | Automatic position merging frees locked capital |
| Gas Fees | Significantly reduced through smarter cancellation logic |

## Important Notes

- ⚠️ **This code interacts with real markets and can potentially lose real money**
- 🧪 **Test thoroughly with small amounts before deploying with significant capital**
- 📊 **Monitor logs and Google Sheets regularly to ensure proper operation**
- 🔒 **Never commit `.env` or `credentials.json` to version control**
- 💡 **The `data_updater` should ideally run on a different IP than the trading bot**

## Troubleshooting

### Orders Cancelling Too Often
- Increase cooldown in `data_processing.py` (default: 30s)
- Increase thresholds in `trading.py` (price: 1.5%, size: 25%)

### No Rewards Showing
- Ensure bot has been running for at least 5 minutes
- Check "Maker Rewards" tab exists in Google Sheets
- Verify orders are being placed (check "Trade Log" tab)

### Market Selection Not Working
- Run `python update_markets.py` first to populate data
- Check "Volatility Markets" tab has data
- Try lowering `--min-reward` threshold

## License

MIT
