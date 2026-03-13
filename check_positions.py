#!/usr/bin/env python3
"""
Script to check existing and historical positions on Polymarket.

This script helps you:
1. View current positions with P&L
2. Check USDC balance
3. View active orders
4. See recent trading history
"""

import os
import sys
from dotenv import load_dotenv
from poly_data.polymarket_client import PolymarketClient
import pandas as pd
import requests
from datetime import datetime
from poly_data.gspread import get_spreadsheet
import gspread

load_dotenv()

def print_section(title):
    """Print a formatted section header."""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)

def check_balances(client):
    """Check USDC and position balances."""
    print_section("ACCOUNT BALANCES")

    try:
        usdc_balance = client.get_usdc_balance()
        print(f"💰 USDC Balance: ${usdc_balance:,.2f}")
    except Exception as e:
        print(f"❌ Error getting USDC balance: {e}")

    try:
        pos_balance = client.get_pos_balance()
        print(f"📊 Position Value: ${pos_balance:,.2f}")
    except Exception as e:
        print(f"❌ Error getting position balance: {e}")

    try:
        total_balance = client.get_total_balance()
        print(f"💵 Total Balance: ${total_balance:,.2f}")
    except Exception as e:
        print(f"❌ Error getting total balance: {e}")

def check_positions(client):
    """Check current positions."""
    print_section("CURRENT POSITIONS")

    try:
        positions = client.get_all_positions()

        if positions.empty:
            print("📭 No open positions")
            return

        # Filter to only positions with non-zero size
        positions = positions[positions['size'].astype(float) > 0]

        if positions.empty:
            print("📭 No open positions")
            return

        print(f"\n📈 You have {len(positions)} open position(s):\n")

        for idx, pos in positions.iterrows():
            size = float(pos.get('size', 0))
            avg_price = float(pos.get('averagePrice', 0))
            market_price = float(pos.get('marketPrice', 0))

            # Calculate P&L
            pnl_per_share = market_price - avg_price
            total_pnl = pnl_per_share * size
            pnl_percent = (pnl_per_share / avg_price * 100) if avg_price > 0 else 0

            outcome = pos.get('outcome', 'Unknown')
            market = pos.get('market', 'Unknown')

            print(f"  Market: {market[:60]}")
            print(f"  Outcome: {outcome}")
            print(f"  Size: {size:.2f} shares")
            print(f"  Avg Price: ${avg_price:.4f}")
            print(f"  Market Price: ${market_price:.4f}")

            pnl_symbol = "📈" if total_pnl >= 0 else "📉"
            print(f"  {pnl_symbol} P&L: ${total_pnl:+.2f} ({pnl_percent:+.2f}%)")
            print(f"  Position Value: ${size * market_price:.2f}")
            print()

    except Exception as e:
        print(f"❌ Error getting positions: {e}")
        import traceback
        traceback.print_exc()

def check_orders(client):
    """Check active orders."""
    print_section("ACTIVE ORDERS")

    try:
        orders = client.get_all_orders()

        if orders.empty:
            print("📭 No active orders")
            return

        print(f"\n📋 You have {len(orders)} active order(s):\n")

        for idx, order in orders.iterrows():
            side = order.get('side', 'Unknown')
            price = float(order.get('price', 0))
            original_size = float(order.get('original_size', 0))
            size_matched = float(order.get('size_matched', 0))
            remaining = original_size - size_matched

            token_id = order.get('asset_id', 'Unknown')
            order_id = order.get('id', 'Unknown')

            side_symbol = "🟢" if side == "BUY" else "🔴"
            print(f"  {side_symbol} {side} Order")
            print(f"  Token ID: {token_id[:20]}...")
            print(f"  Price: ${price:.4f}")
            print(f"  Size: {original_size:.2f} (Matched: {size_matched:.2f}, Remaining: {remaining:.2f})")
            print(f"  Order ID: {order_id}")
            print()

    except Exception as e:
        print(f"❌ Error getting orders: {e}")
        import traceback
        traceback.print_exc()

def check_trade_history(wallet_address):
    """Check recent trade history from Polymarket API."""
    print_section("RECENT TRADE HISTORY")

    try:
        # Get recent trades from Polymarket data API
        url = f"https://data-api.polymarket.com/trades?user={wallet_address}&limit=20"
        response = requests.get(url, timeout=10)

        if response.status_code != 200:
            print(f"❌ Could not fetch trade history (status {response.status_code})")
            return

        trades = response.json()

        if not trades:
            print("📭 No recent trades found")
            return

        print(f"\n📜 Showing last {len(trades)} trade(s):\n")

        for trade in trades[:10]:  # Show last 10 trades
            side = trade.get('side', 'Unknown')
            size = float(trade.get('size', 0))
            price = float(trade.get('price', 0))
            timestamp = trade.get('timestamp', '')
            market = trade.get('market', 'Unknown')

            # Convert timestamp to readable format
            if timestamp:
                try:
                    dt = datetime.fromtimestamp(int(timestamp) / 1000)
                    time_str = dt.strftime('%Y-%m-%d %H:%M:%S')
                except (ValueError, TypeError, OverflowError):
                    time_str = timestamp
            else:
                time_str = 'Unknown'

            side_symbol = "🟢" if side == "BUY" else "🔴"
            print(f"  {side_symbol} {side} - {size:.2f} @ ${price:.4f} ({time_str})")
            print(f"     Market: {market[:60]}")
            print()

    except Exception as e:
        print(f"❌ Error getting trade history: {e}")


def export_to_sheets(client, wallet_address):
    """Export position data to Google Sheets."""
    print_section("EXPORTING TO GOOGLE SHEETS")

    try:
        # Get spreadsheet
        sheet = get_spreadsheet()

        # Try to get or create "Position Snapshots" worksheet
        try:
            worksheet = sheet.worksheet("Position Snapshots")
            print("✓ Found existing 'Position Snapshots' tab")
        except gspread.exceptions.WorksheetNotFound:
            # Create new worksheet with headers
            worksheet = sheet.add_worksheet(title="Position Snapshots", rows=1000, cols=15)
            headers = [
                "Timestamp", "Wallet", "USDC Balance", "Position Value", "Total Balance",
                "Market", "Outcome", "Token ID", "Size", "Avg Price", "Market Price",
                "P&L ($)", "P&L (%)", "Position Value", "Order Count"
            ]
            worksheet.append_row(headers)
            print("✓ Created new 'Position Snapshots' tab")

        # Collect current snapshot data
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Get balances
        try:
            usdc_balance = client.get_usdc_balance()
            pos_balance = client.get_pos_balance()
            total_balance = client.get_total_balance()
        except Exception as e:
            print(f"⚠️  Warning: Could not get balances: {e}")
            usdc_balance = pos_balance = total_balance = 0

        # Get positions
        positions = client.get_all_positions()
        positions = positions[positions['size'].astype(float) > 0] if not positions.empty else pd.DataFrame()

        # Get active orders count
        try:
            orders = client.get_all_orders()
            order_count = len(orders) if not orders.empty else 0
        except Exception:
            order_count = 0

        # Prepare rows to append
        rows_to_add = []

        if positions.empty:
            # Add summary row even if no positions
            rows_to_add.append([
                timestamp, wallet_address, f"{usdc_balance:.2f}", f"{pos_balance:.2f}",
                f"{total_balance:.2f}", "No Positions", "", "", "", "", "", "", "", "", order_count
            ])
        else:
            # Add row for each position
            for idx, pos in positions.iterrows():
                size = float(pos.get('size', 0))
                avg_price = float(pos.get('averagePrice', 0))
                market_price = float(pos.get('marketPrice', 0))

                # Calculate P&L
                pnl_per_share = market_price - avg_price
                total_pnl = pnl_per_share * size
                pnl_percent = (pnl_per_share / avg_price * 100) if avg_price > 0 else 0

                outcome = pos.get('outcome', 'Unknown')
                market = pos.get('market', 'Unknown')
                token_id = pos.get('asset_id', 'Unknown')
                position_value = size * market_price

                rows_to_add.append([
                    timestamp, wallet_address, f"{usdc_balance:.2f}", f"{pos_balance:.2f}",
                    f"{total_balance:.2f}", market[:100], outcome, token_id, f"{size:.2f}",
                    f"{avg_price:.4f}", f"{market_price:.4f}", f"{total_pnl:.2f}",
                    f"{pnl_percent:.2f}", f"{position_value:.2f}", order_count
                ])

        # Append all rows
        if rows_to_add:
            worksheet.append_rows(rows_to_add)
            print(f"✅ Exported {len(rows_to_add)} row(s) to Google Sheets")
            print(f"   Spreadsheet: {sheet.url}")
            print(f"   Tab: Position Snapshots")

    except Exception as e:
        print(f"❌ Error exporting to Google Sheets: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main function to check all positions and balances."""
    print("\n🔍 POLYMARKET POSITION CHECKER")
    print(f"   Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Initialize client
    try:
        client = PolymarketClient()
        wallet_address = os.getenv('BROWSER_ADDRESS')
        print(f"   Wallet: {wallet_address}")
    except Exception as e:
        print(f"\n❌ Failed to initialize client: {e}")
        sys.exit(1)

    # Run all checks
    check_balances(client)
    check_positions(client)
    check_orders(client)
    check_trade_history(wallet_address)

    # Export to Google Sheets
    export_to_sheets(client, wallet_address)

    print_section("DONE")
    print()

if __name__ == "__main__":
    main()
