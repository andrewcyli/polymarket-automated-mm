import time
import pandas as pd
import os
import requests
import warnings
import json
import re
import traceback
from io import StringIO
from dotenv import load_dotenv
import concurrent.futures
import numpy as np
from google.oauth2.service_account import Credentials
import gspread
from gspread_dataframe import set_with_dataframe
import urllib.parse
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('data_updater.log')
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()
warnings.filterwarnings("ignore")

def get_clob_client():
    logger.info("Attempting to create ClobClient")
    host = "https://clob.polymarket.com"
    key = os.getenv("PK")
    chain_id = 137  # Polygon Mainnet chain ID
    if key is None:
        logger.error("Environment variable 'PK' not found")
        return None
    try:
        from py_clob_client.client import ClobClient
        client = ClobClient(host, key=key, chain_id=chain_id)
        api_creds = client.create_or_derive_api_creds()
        client.set_api_creds(api_creds)
        logger.info(f"ClobClient created successfully with chain_id: {chain_id}")
        return client
    except Exception as ex:
        logger.error(f"Error creating ClobClient: {ex}", exc_info=True)
        return None

def get_spreadsheet(read_only=False):
    logger.info("Attempting to access spreadsheet")
    spreadsheet_url = os.getenv("SPREADSHEET_URL")
    if not spreadsheet_url:
        logger.error("SPREADSHEET_URL environment variable not set")
        raise ValueError("SPREADSHEET_URL environment variable is not set")
    creds_file = 'credentials.json' if os.path.exists('credentials.json') else '../credentials.json'
    logger.info(f"Checking for credentials file at: {creds_file}")
    if not read_only and os.path.exists(creds_file):
        try:
            with open(creds_file, 'r') as f:
                creds_data = json.load(f)
                service_account_email = creds_data.get('client_email', 'unknown')
                logger.info(f"Using Service Account: {service_account_email}")
            scope = ["https://www.googleapis.com/auth/spreadsheets"]
            credentials = Credentials.from_service_account_file(creds_file, scopes=scope)
            client = gspread.authorize(credentials)
            spreadsheet = client.open_by_url(spreadsheet_url)
            sheet_id = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', spreadsheet_url).group(1)
            logger.info(f"Authenticated access to Sheets enabled. Spreadsheet ID: {sheet_id}, Title: {spreadsheet.title}")
            return spreadsheet
        except Exception as e:
            logger.error(f"Error authenticating Sheets: {e}. Falling back to read-only mode.", exc_info=True)
            return ReadOnlySpreadsheet(spreadsheet_url)
    else:
        logger.warning(f"No credentials found at {creds_file} or read_only=True. Using read-only mode.")
        return ReadOnlySpreadsheet(spreadsheet_url)

class ReadOnlySpreadsheet:
    def __init__(self, spreadsheet_url):
        self.spreadsheet_url = spreadsheet_url
        self.sheet_id = self._extract_sheet_id(spreadsheet_url)
        logger.info(f"Initialized ReadOnlySpreadsheet with ID: {self.sheet_id}")

    def _extract_sheet_id(self, url):
        match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', url)
        if not match:
            logger.error("Invalid Google Sheets URL")
            raise ValueError("Invalid Google Sheets URL")
        return match.group(1)

    def worksheet(self, title):
        logger.info(f"Accessing read-only worksheet: {title}")
        return ReadOnlyWorksheet(self.sheet_id, title)

class ReadOnlyWorksheet:
    def __init__(self, sheet_id, title):
        self.sheet_id = sheet_id
        self.title = title
        logger.info(f"Initialized ReadOnlyWorksheet: {title}")

    def get_all_records(self):
        logger.info(f"Fetching records from sheet: {self.title}")
        try:
            encoded_title = urllib.parse.quote(self.title)
            csv_url = f"https://docs.google.com/spreadsheets/d/{self.sheet_id}/gviz/tq?tqx=out:csv&sheet={encoded_title}"
            response = requests.get(csv_url, timeout=30)
            response.raise_for_status()
            df = pd.read_csv(StringIO(response.text))
            if not df.empty and len(df.columns) > 0:
                logger.info(f"Successfully fetched {len(df)} records from sheet: {self.title}")
                return df.to_dict('records')
            logger.warning(f"Empty data for sheet: {self.title}")
            return []
        except Exception as e:
            logger.error(f"Could not fetch data from sheet '{self.title}': {e}", exc_info=True)
            return []

if not os.path.exists('data'):
    os.makedirs('data')
    logger.info("Created data directory")

def get_sel_df(spreadsheet, sheet_name='Selected Markets'):
    if spreadsheet is None:
        logger.error("No spreadsheet access. Returning empty selected_df.")
        return pd.DataFrame()
    try:
        logger.info(f"Loading selected markets from sheet: {sheet_name}")
        wk2 = spreadsheet.worksheet(sheet_name)
        sel_df = pd.DataFrame(wk2.get_all_records())
        sel_df = sel_df[sel_df['question'] != ""].reset_index(drop=True)
        logger.info(f"Loaded {len(sel_df)} selected markets from sheet: {sheet_name}")
        return sel_df
    except Exception as e:
        logger.error(f"Error loading selected markets from {sheet_name}: {e}", exc_info=True)
        return pd.DataFrame()

def get_all_markets(client):
    logger.info("Fetching all markets")
    cursor = ""
    all_markets = []
    while True:
        try:
            markets = client.get_sampling_markets(next_cursor=cursor)
            markets_df = pd.DataFrame(markets['data'])
            cursor = markets['next_cursor']
            all_markets.append(markets_df)
            logger.info(f"Fetched market page with {len(markets_df)} markets, next_cursor: {cursor}")
            if cursor is None:
                break
        except Exception as e:
            logger.error(f"Error fetching markets page: {e}", exc_info=True)
            break
    if not all_markets:
        logger.error("No markets fetched. Check client/API.")
        raise ValueError("No markets fetched. Check client/API.")
    all_df = pd.concat(all_markets, ignore_index=True)
    all_df = all_df.reset_index(drop=True)
    logger.info(f"Fetched {len(all_df)} total markets")
    return all_df

def get_bid_ask_range(ret, TICK_SIZE):
    bid_from = ret['midpoint'] - ret['max_spread'] / 100
    bid_to = ret['best_ask']
    if bid_to == 0:
        bid_to = ret['midpoint']
    if bid_to - TICK_SIZE > ret['midpoint']:
        bid_to = ret['best_bid'] + (TICK_SIZE + 0.1 * TICK_SIZE)
    if bid_from > bid_to:
        bid_from = bid_to - (TICK_SIZE + 0.1 * TICK_SIZE)
    ask_to = ret['midpoint'] + ret['max_spread'] / 100
    ask_from = ret['best_bid']
    if ask_from == 0:
        ask_from = ret['midpoint']
    if ask_from + TICK_SIZE < ret['midpoint']:
        ask_from = ret['best_ask'] - (TICK_SIZE + 0.1 * TICK_SIZE)
    if ask_from > ask_to:
        ask_to = ask_from + (TICK_SIZE + 0.1 * TICK_SIZE)
    bid_from = round(bid_from, 3)
    bid_to = round(bid_to, 3)
    ask_from = round(ask_from, 3)
    ask_to = round(ask_to, 3)
    if bid_from < 0:
        bid_from = 0
    if ask_from < 0:
        ask_from = 0
    return bid_from, bid_to, ask_from, ask_to

def generate_numbers(start, end, TICK_SIZE):
    rounded_start = (int(start * 100) + 1) / 100 if start * 100 % 1 != 0 else start + TICK_SIZE
    rounded_end = int(end * 100) / 100
    numbers = []
    current = rounded_start
    while current < end:
        numbers.append(current)
        current += TICK_SIZE
        current = round(current, len(str(TICK_SIZE).split('.')[1]) if '.' in str(TICK_SIZE) else 0)
    return numbers

def add_formula_params(curr_df, midpoint, v, daily_reward):
    if curr_df.empty:
        return curr_df
    curr_df = curr_df.copy()
    curr_df['s'] = (curr_df['price'] - midpoint).abs()
    curr_df['S'] = ((v - curr_df['s']) / v) ** 2
    curr_df['100'] = 1 / curr_df['price'] * 100
    curr_df['size'] = curr_df['size'] + curr_df['100']
    curr_df['Q'] = curr_df['S'] * curr_df['size']
    total_Q = curr_df['Q'].sum()
    if total_Q > 0:
        curr_df['reward_per_100'] = (curr_df['Q'] / total_Q) * daily_reward / 2 / curr_df['size'] * curr_df['100']
    else:
        num_rows = len(curr_df)
        curr_df['reward_per_100'] = (daily_reward / 2) / num_rows if num_rows > 0 else 0
    curr_df['reward_per_100'] = curr_df['reward_per_100'].replace([np.inf, -np.inf], 0).fillna(0)
    return curr_df

def process_single_row(row, client):
    ret = {}
    ret['question'] = row['question']
    ret['neg_risk'] = row['neg_risk']
    ret['answer1'] = row['tokens'][0]['outcome']
    ret['answer2'] = row['tokens'][1]['outcome']
    ret['min_size'] = row['rewards']['min_size']
    ret['max_spread'] = row['rewards']['max_spread']
    token1 = row['tokens'][0]['token_id']
    token2 = row['tokens'][1]['token_id']
    rate = 0
    for rate_info in row['rewards']['rates']:
        if rate_info['asset_address'].lower() == '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174'.lower():
            rate = rate_info['rewards_daily_rate']
            break
    ret['rewards_daily_rate'] = rate
    try:
        book = client.get_order_book(token1)
    except Exception:
        book = type('obj', (object,), {'bids': [], 'asks': []})()
    bids = pd.DataFrame()
    asks = pd.DataFrame()
    try:
        bids = pd.DataFrame(book.bids).astype(float)
    except (ValueError, TypeError, KeyError):
        pass
    try:
        asks = pd.DataFrame(book.asks).astype(float)
    except (ValueError, TypeError, KeyError):
        pass
    try:
        ret['best_bid'] = bids.iloc[-1]['price'] if not bids.empty else 0
    except (IndexError, KeyError):
        ret['best_bid'] = 0
    try:
        ret['best_ask'] = asks.iloc[-1]['price'] if not asks.empty else 0
    except (IndexError, KeyError):
        ret['best_ask'] = 0
    ret['midpoint'] = (ret['best_bid'] + ret['best_ask']) / 2
    if ret['midpoint'] == 0 or pd.isna(ret['midpoint']):
        ret['midpoint'] = 0.5
        ret['best_bid'] = 0.49
        ret['best_ask'] = 0.51
    TICK_SIZE = row['minimum_tick_size']
    ret['tick_size'] = TICK_SIZE
    bid_from, bid_to, ask_from, ask_to = get_bid_ask_range(ret, TICK_SIZE)
    v = round((ret['max_spread'] / 100), 2)
    bids_df = pd.DataFrame({'price': generate_numbers(bid_from, bid_to, TICK_SIZE), 'size': 0})
    asks_df = pd.DataFrame({'price': generate_numbers(ask_from, ask_to, TICK_SIZE), 'size': 0})
    try:
        bids_df = bids_df.merge(bids, on='price', how='left', suffixes=('', '_book')).fillna(0)
        if 'size_book' in bids_df.columns:
            bids_df['size'] = bids_df['size'].fillna(0) + bids_df['size_book'].fillna(0)
            bids_df.drop(columns=['size_book'], inplace=True)
    except Exception as merge_err:
        logger.error(f"Merge error for bids: {merge_err}", exc_info=True)
    try:
        asks_df = asks_df.merge(asks, on='price', how='left', suffixes=('', '_book')).fillna(0)
        if 'size_book' in asks_df.columns:
            asks_df['size'] = asks_df['size'].fillna(0) + asks_df['size_book'].fillna(0)
            asks_df.drop(columns=['size_book'], inplace=True)
    except Exception as merge_err:
        logger.error(f"Merge error for asks: {merge_err}", exc_info=True)
    best_bid_reward = 0
    try:
        ret_bid = add_formula_params(bids_df, ret['midpoint'], v, rate)
        best_bid_reward = round(ret_bid['reward_per_100'].max(), 2) if not ret_bid.empty else 0
    except Exception:
        pass
    best_ask_reward = 0
    try:
        ret_ask = add_formula_params(asks_df, ret['midpoint'], v, rate)
        best_ask_reward = round(ret_ask['reward_per_100'].max(), 2) if not ret_ask.empty else 0
    except Exception:
        pass
    ret['bid_reward_per_100'] = best_bid_reward
    ret['ask_reward_per_100'] = best_ask_reward
    ret['sm_reward_per_100'] = round((best_bid_reward + best_ask_reward) / 2, 2)
    ret['gm_reward_per_100'] = round((best_bid_reward * best_ask_reward) ** 0.5, 2)
    ret['end_date_iso'] = row['end_date_iso']
    ret['market_slug'] = row['market_slug']
    ret['token1'] = token1
    ret['token2'] = token2
    ret['condition_id'] = row['condition_id']
    return ret

def get_all_results(all_df, client, max_workers=5):
    logger.info("Processing all market results")
    all_results = []
    def process_with_progress(args):
        idx, row = args
        try:
            return process_single_row(row, client)
        except Exception as e:
            logger.error(f"Error processing row {idx}: {e}", exc_info=True)
            return None
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_with_progress, (idx, row)) for idx, row in all_df.iterrows()]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result is not None:
                all_results.append(result)
            if len(all_results) % (max_workers * 2) == 0:
                logger.info(f'Processed {len(all_results)} of {len(all_df)} markets')
    logger.info(f"Processed {len(all_results)} results")
    return all_results

def get_combined_markets(new_df, new_markets, sel_df):
    if len(sel_df) > 0:
        old_markets = new_df[new_df['question'].isin(sel_df['question'])]
        all_markets = pd.concat([old_markets, new_markets])
    else:
        all_markets = new_markets
    all_markets = all_markets.drop_duplicates('question')
    all_markets = all_markets.sort_values('gm_reward_per_100', ascending=False)
    return all_markets

def calculate_annualized_volatility(df, hours):
    if df.empty:
        return 0
    end_time = df['t'].max()
    start_time = end_time - pd.Timedelta(hours=hours)
    window_df = df[df['t'] >= start_time]
    if window_df.empty:
        return 0
    volatility = window_df['log_return'].std()
    annualized_volatility = volatility * np.sqrt(60 * 24 * 252)
    return round(annualized_volatility, 2)

def add_volatility(row):
    logger.info(f"Adding volatility for token: {row.get('token1', 'unknown')}")
    try:
        res = requests.get(f'https://clob.polymarket.com/prices-history?interval=1m&market={row["token1"]}&fidelity=10', timeout=10)
        price_df = pd.DataFrame(res.json()['history'])
        price_df['t'] = pd.to_datetime(price_df['t'], unit='s')
        price_df['p'] = price_df['p'].round(2)
        price_df.to_csv(f'data/{row["token1"]}.csv', index=False)
        price_df['log_return'] = np.log(price_df['p'] / price_df['p'].shift(1))
        row_dict = row.copy()
        stats = {
            '1_hour': calculate_annualized_volatility(price_df, 1),
            '3_hour': calculate_annualized_volatility(price_df, 3),
            '6_hour': calculate_annualized_volatility(price_df, 6),
            '12_hour': calculate_annualized_volatility(price_df, 12),
            '24_hour': calculate_annualized_volatility(price_df, 24),
            '7_day': calculate_annualized_volatility(price_df, 24 * 7),
            '30_day': calculate_annualized_volatility(price_df, 24 * 30),
            'volatility_price': price_df['p'].iloc[-1] if not price_df.empty else 0
        }
        new_dict = {**row_dict, **stats}
        logger.info(f"Volatility calculated for token: {row['token1']}")
        return new_dict
    except Exception as e:
        logger.error(f"Error adding volatility for token {row.get('token1', 'unknown')}: {e}", exc_info=True)
        return row

def add_volatility_to_df(df, max_workers=3):
    if df.empty:
        logger.warning("Empty DataFrame, skipping volatility calculation")
        return df
    logger.info(f"Adding volatility to {len(df)} markets")
    results = []
    df = df.reset_index(drop=True)
    def process_volatility_with_progress(args):
        idx, row = args
        try:
            ret = add_volatility(row.to_dict())
            return ret
        except Exception:
            logger.error(f"Error fetching volatility for row {idx}", exc_info=True)
            return None
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_volatility_with_progress, (idx, row)) for idx, row in df.iterrows()]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result is not None:
                results.append(result)
            if len(results) % (max_workers * 2) == 0:
                logger.info(f'Processed volatility for {len(results)} of {len(df)} markets')
    new_df = pd.DataFrame(results)
    logger.info(f"Added volatility to {len(new_df)} markets")
    return new_df

def get_markets(all_results, sel_df, maker_reward=0.75):
    logger.info("Processing market data")
    new_df = pd.DataFrame(all_results)
    if new_df.empty:
        logger.error("No market results to process")
        raise ValueError("No market results to process.")
    new_df['spread'] = abs(new_df['best_ask'] - new_df['best_bid'])
    new_df = new_df.sort_values('rewards_daily_rate', ascending=False)
    new_df[' '] = ''
    new_df = new_df[
        ['question', 'answer1', 'answer2', 'neg_risk', 'spread', 'best_bid', 'best_ask', 'rewards_daily_rate',
         'bid_reward_per_100', 'ask_reward_per_100', 'gm_reward_per_100', 'sm_reward_per_100', 'min_size', 'max_spread',
         'tick_size', 'market_slug', 'token1', 'token2', 'condition_id']]
    new_df = new_df.replace([np.inf, -np.inf], 0)
    all_data = new_df.copy()
    s_df = new_df.copy()
    exclude_questions = sel_df['question'].tolist() if not sel_df.empty and 'question' in sel_df.columns else []
    making_markets = s_df[~s_df['question'].isin(exclude_questions)]
    making_markets = making_markets.sort_values('gm_reward_per_100', ascending=False)
    making_markets = making_markets[making_markets['gm_reward_per_100'] >= maker_reward]
    all_markets = get_combined_markets(new_df, making_markets, sel_df)
    logger.info(f"Processed {len(all_markets)} markets for output")
    return all_data, all_markets

def update_sheet(data, worksheet, filename):
    if data.empty:
        logger.warning(f"Empty data for {filename}, skipping save")
        return
    if worksheet is None:
        logger.error(f"Worksheet is None for {filename}, saving to CSV instead")
        data.to_csv(filename, index=False)
        logger.info(f"Saved {len(data)} rows to {filename}")
        return
    try:
        logger.info(f"Preparing to update sheet: {worksheet.title}")
        logger.info(f"DataFrame shape: {data.shape}, columns: {list(data.columns)}")
        all_values = worksheet.get_all_values()
        existing_num_rows = len(all_values)
        existing_num_cols = len(all_values[0]) if all_values else 0
        logger.info(f"Existing sheet dimensions: {existing_num_rows} rows, {existing_num_cols} columns")
        num_rows, num_cols = data.shape
        max_rows = max(num_rows, existing_num_rows)
        max_cols = max(num_cols, existing_num_cols)
        padded_data = pd.DataFrame('', index=range(max_rows), columns=range(max_cols))
        padded_data.iloc[:num_rows, :num_cols] = data.values
        padded_data.columns = list(data.columns) + [''] * (max_cols - num_cols)
        logger.info(f"Attempting to write {num_rows} rows and {num_cols} columns to sheet: {worksheet.title}")
        set_with_dataframe(worksheet, padded_data, include_index=False, include_column_header=True, resize=True)
        logger.info(f"Successfully updated sheet: {worksheet.title} with {num_rows} rows and {num_cols} columns")
    except Exception as e:
        logger.error(f"Failed to update sheet {worksheet.title}: {e}. Saving to {filename} instead.", exc_info=True)
        data.to_csv(filename, index=False)
        logger.info(f"Saved {len(data)} rows to {filename}")

def sort_df(df):
    if df.empty or 'gm_reward_per_100' not in df.columns:
        logger.warning("Empty DataFrame or missing gm_reward_per_100, skipping sort")
        return df
    logger.info("Sorting DataFrame by composite score")
    mean_gm = df['gm_reward_per_100'].mean()
    std_gm = df['gm_reward_per_100'].std()
    mean_volatility = df['volatility_sum'].mean() if 'volatility_sum' in df.columns else 0
    std_volatility = df['volatility_sum'].std() if 'volatility_sum' in df.columns else 1
    df = df.copy()
    df['std_gm_reward_per_100'] = (df['gm_reward_per_100'] - mean_gm) / std_gm
    df['std_volatility_sum'] = (df['volatility_sum'] - mean_volatility) / std_volatility if std_volatility > 0 else 0
    def proximity_score(value):
        if pd.isna(value):
            return 0
        if 0.1 <= value <= 0.25:
            return (0.25 - value) / 0.15
        elif 0.75 <= value <= 0.9:
            return (value - 0.75) / 0.15
        else:
            return 0
    df['bid_score'] = df['best_bid'].apply(proximity_score)
    df['ask_score'] = df['best_ask'].apply(proximity_score)
    df['composite_score'] = (
        df['std_gm_reward_per_100'] -
        df['std_volatility_sum'] +
        df['bid_score'] +
        df['ask_score']
    )
    sorted_df = df.sort_values(by='composite_score', ascending=False)
    sorted_df = sorted_df.drop(
        columns=['std_gm_reward_per_100', 'std_volatility_sum', 'bid_score', 'ask_score', 'composite_score'],
        errors='ignore')
    logger.info("DataFrame sorted successfully")
    return sorted_df

def fetch_and_process_data():
    try:
        logger.info("Starting fetch_and_process_data")
        logger.info("Environment variables loaded: SPREADSHEET_URL, PK, API_KEY, API_SECRET, API_PASSPHRASE, DRY_RUN")
        spreadsheet = get_spreadsheet(read_only=False)
        client = get_clob_client()
        if client is None:
            logger.error("Failed to create ClobClient")
            raise ValueError("Failed to create ClobClient. Check PK and API credentials.")
        logger.info("Initializing worksheets")
        wk_all = spreadsheet.worksheet("All Markets") if spreadsheet else None
        wk_vol = spreadsheet.worksheet("Volatility Markets") if spreadsheet else None
        wk_full = spreadsheet.worksheet("Full Markets") if spreadsheet else None
        logger.info(f"Worksheet status - All Markets: {'Initialized' if wk_all else 'None'}, "
                    f"Volatility Markets: {'Initialized' if wk_vol else 'None'}, "
                    f"Full Markets: {'Initialized' if wk_full else 'None'}")
        sel_df = get_sel_df(spreadsheet, "Selected Markets")
        all_df = get_all_markets(client)
        logger.info("Got all markets")
        all_results = get_all_results(all_df, client)
        logger.info("Got all results")
        m_data, all_markets = get_markets(all_results, sel_df, maker_reward=0.75)
        logger.info("Got all orderbook data")
        logger.info(f"Fetched all markets data of length {len(all_markets)}")
        new_df = add_volatility_to_df(all_markets)
        if '24_hour' in new_df.columns and '7_day' in new_df.columns and '30_day' in new_df.columns:
            new_df['volatility_sum'] = new_df['24_hour'] + new_df['7_day'] + new_df['30_day']
        else:
            new_df['volatility_sum'] = 0
            logger.warning("Volatility columns missing, set volatility_sum to 0")
        new_df = new_df.sort_values('volatility_sum', ascending=True)
        new_df['volatility/reward'] = ((new_df['gm_reward_per_100'] / new_df['volatility_sum']).round(2)).astype(str).replace('inf', 'N/A')
        cols = ['question', 'answer1', 'answer2', 'spread', 'rewards_daily_rate', 'gm_reward_per_100',
                'sm_reward_per_100', 'bid_reward_per_100', 'ask_reward_per_100', 'volatility_sum', 'volatility/reward',
                'min_size', '1_hour', '3_hour', '6_hour', '12_hour', '24_hour', '7_day', '30_day',
                'best_bid', 'best_ask', 'volatility_price', 'max_spread', 'tick_size',
                'neg_risk', 'market_slug', 'token1', 'token2', 'condition_id']
        new_df = new_df[[col for col in cols if col in new_df.columns]]
        volatility_df = new_df.copy()
        volatility_df = volatility_df[volatility_df['volatility_sum'] < 20] if 'volatility_sum' in volatility_df.columns else volatility_df
        volatility_df = volatility_df.sort_values('gm_reward_per_100', ascending=False)
        new_df = sort_df(new_df)
        logger.info(f"Fetched select market of length {len(new_df)}")
        update_sheet(new_df, wk_all, 'data/all_markets.csv')
        update_sheet(volatility_df, wk_vol, 'data/volatility_markets.csv')
        update_sheet(m_data, wk_full, 'data/full_markets.csv')
        logger.info("Top 10 Markets (by gm_reward_per_100):")
        logger.info("\n" + new_df.head(10).to_string(index=False))
    except Exception as e:
        logger.error(f"Error in fetch_and_process_data: {e}", exc_info=True)

if __name__ == "__main__":
    while True:
        try:
            logger.info("Starting data fetch loop")
            fetch_and_process_data()
            logger.info("Data fetch complete. Check Google Sheets or 'data/*.csv' files for results.")
            time.sleep(60 * 60)  # Sleep for 1 hour
        except Exception as e:
            logger.error(f"Error in main loop: {e}", exc_info=True)
            time.sleep(60)  # Retry after 1 minute