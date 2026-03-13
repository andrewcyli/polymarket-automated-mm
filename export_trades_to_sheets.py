#!/usr/bin/env python3
"""
Export Trade History to Google Sheets

This script fetches your complete trade history from Polymarket
and exports it to a new "Trade History" tab in your Google Sheets.
"""
from dotenv import load_dotenv
from poly_data.polymarket_client import PolymarketClient
from poly_data.gspread import get_spreadsheet
import pandas as pd
from datetime import datetime
import time

load_dotenv()

def export_trades_to_sheets():
    print("\n" + "=" * 80)
    print("EXPORT TRADE HISTORY TO GOOGLE SHEETS")
    print("=" * 80)

    # Initialize client
    print("\n1. Initializing Polymarket client...")
    client = PolymarketClient()
    print("   ✓ Client initialized")

    # Get trade history
    print("\n2. Fetching trade history...")
    try:
        # Get all historical orders (filled and canceled)
        all_orders = client.get_all_orders()

        print(f"   Found {len(all_orders)} order(s)")

        if len(all_orders) == 0:
            print("   No orders found. Trying to fetch from positions...")

        # Get positions for additional context
        positions = client.get_all_positions()

    except Exception as e:
        print(f"   ❌ Error fetching data: {e}")
        return

    # Get account balances
    print("\n3. Getting account summary...")
    try:
        usdc = client.get_usdc_balance()
        pos_value = client.get_pos_balance()
        total = usdc + pos_value

        print(f"   USDC Balance: ${usdc:.2f}")
        print(f"   Position Value: ${pos_value:.2f}")
        print(f"   Total: ${total:.2f}")

    except Exception as e:
        print(f"   ⚠️  Could not get balances: {e}")
        usdc = 0
        pos_value = 0
        total = 0

    # Prepare trade data
    print("\n4. Preparing trade data for export...")

    trade_records = []

    # Add position data
    if len(positions) > 0:
        print(f"   Processing {len(positions)} position(s)...")

        for idx, pos in positions.iterrows():
            if pos.get('size', 0) > 0:
                trade_records.append({
                    'Type': 'POSITION',
                    'Market': pos.get('title', 'Unknown'),
                    'Outcome': pos.get('outcome', 'N/A'),
                    'Shares': pos.get('size', 0),
                    'Avg Price': pos.get('avgPrice', 0),
                    'Current Price': pos.get('curPrice', 0),
                    'Initial Value': pos.get('initialValue', 0),
                    'Current Value': pos.get('currentValue', 0),
                    'Cash P&L': pos.get('cashPnl', 0),
                    'Percent P&L': pos.get('percentPnl', 0),
                    'Realized P&L': pos.get('realizedPnl', 0),
                    'Total Bought': pos.get('totalBought', 0),
                    'Redeemable': pos.get('redeemable', False),
                    'End Date': pos.get('endDate', 'N/A'),
                    'Token ID': pos.get('asset', 'N/A'),
                })

    # Add order data if available
    if len(all_orders) > 0:
        print(f"   Processing {len(all_orders)} order(s)...")

        for idx, order in all_orders.iterrows():
            trade_records.append({
                'Type': 'ORDER',
                'Market': order.get('market', 'Unknown'),
                'Outcome': 'N/A',
                'Shares': order.get('original_size', 0),
                'Avg Price': order.get('price', 0),
                'Current Price': 'N/A',
                'Initial Value': 'N/A',
                'Current Value': 'N/A',
                'Cash P&L': 'N/A',
                'Percent P&L': 'N/A',
                'Realized P&L': 'N/A',
                'Total Bought': 'N/A',
                'Redeemable': 'N/A',
                'End Date': 'N/A',
                'Token ID': order.get('asset_id', 'N/A'),
                'Order ID': order.get('id', 'N/A'),
                'Side': order.get('side', 'N/A'),
                'Status': order.get('status', 'N/A'),
            })

    print(f"   ✓ Prepared {len(trade_records)} record(s)")

    # Export to Google Sheets
    print("\n5. Exporting to Google Sheets...")
    try:
        spreadsheet = get_spreadsheet()

        # Create or get Trade History worksheet
        try:
            worksheet = spreadsheet.worksheet('Trade History')
            print("   Found existing 'Trade History' tab")
            # Clear existing data
            worksheet.clear()
            print("   Cleared old data")
        except Exception:
            worksheet = spreadsheet.add_worksheet(title='Trade History', rows=1000, cols=20)
            print("   ✓ Created new 'Trade History' tab")

        # Add summary header
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        summary_header = [
            ['POLYMARKET TRADE HISTORY'],
            [f'Export Date: {timestamp}'],
            [f'USDC Balance: ${usdc:.2f}'],
            [f'Position Value: ${pos_value:.2f}'],
            [f'Total Balance: ${total:.2f}'],
            [''],
        ]

        # Convert trade records to DataFrame
        if len(trade_records) > 0:
            df = pd.DataFrame(trade_records)

            # Prepare data for sheets (header + rows)
            headers = [df.columns.tolist()]
            data_rows = df.values.tolist()

            # Combine all data
            all_data = summary_header + headers + data_rows

            # Update worksheet
            worksheet.update('A1', all_data)

            print(f"   ✓ Exported {len(trade_records)} record(s) to 'Trade History' tab")

            # Format header row (make it bold)
            # Row 7 is where the data headers start (after summary)
            worksheet.format('A7:Z7', {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
            })

            # Format summary section
            worksheet.format('A1:A5', {
                'textFormat': {'bold': True},
            })

            print("   ✓ Applied formatting")
        else:
            all_data = summary_header + [['No trade records found']]
            worksheet.update('A1', all_data)
            print("   ✓ No trades to export")

        print(f"\n   📊 Trade history exported successfully!")
        print(f"   View at: {spreadsheet.url}")

    except Exception as e:
        print(f"   ❌ Error exporting to sheets: {e}")
        import traceback
        traceback.print_exc()
        return

    print("\n" + "=" * 80)
    print("✅ EXPORT COMPLETE")
    print("=" * 80)

    # Print summary
    print(f"\n📈 SUMMARY:")
    print(f"   Total Records Exported: {len(trade_records)}")
    print(f"   Active Positions: {len([r for r in trade_records if r['Type'] == 'POSITION'])}")
    print(f"   Active Orders: {len([r for r in trade_records if r['Type'] == 'ORDER'])}")
    print(f"\n   View your data in Google Sheets 'Trade History' tab")
    print()


if __name__ == "__main__":
    export_trades_to_sheets()
