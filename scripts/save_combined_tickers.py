#!/usr/bin/env python
"""Script to save a combined list of tickers to the data directory."""

import os
import sys
import logging
import pandas as pd
from yfinance_scraper.config import load_config, ensure_data_dir
from yfinance_scraper.utils import save_tickers_to_file
from yahoo_fin import stock_info as si

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Define list of tickers
def get_combined_tickers():
    tickers = set()
    tickers.update(si.tickers_sp500())
    # tickers.update(si.tickers_nasdaq())
    # tickers.update(si.tickers_dow())
    return sorted(tickers)

def main():
    """Main function to save combined tickers."""
    # Load configuration
    config = load_config()
    
    # Ensure data directory exists
    data_dir = config["data_dir"]
    ensure_data_dir(data_dir)
    
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    tables = pd.read_html(url)
    # The first table should have the list of S&P 500 companies.
    sp500_df = tables[0]

    # Often the column with tickers is labeled 'Symbol'. Adjust if needed.
    sp500_tickers = sp500_df["Symbol"].tolist()
    tickers = sorted(set(sp500_tickers))
    # tickers = get_combined_tickers()
    print(tickers)
    # Save tickers to file
    success = save_tickers_to_file(tickers, data_dir)
    
    if success:
        print(f"Successfully saved {len(tickers)} tickers to {os.path.join(data_dir, 'tickers.txt')}")
        return 0
    else:
        print("Failed to save tickers")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 