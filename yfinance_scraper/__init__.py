"""YFinance Scraper package for retrieving and storing Yahoo Finance data."""

__version__ = "0.1.0"

# Import and expose key functions for ease of use
from yfinance_scraper.loader import (
    get_available_tickers,
    get_available_data_types,
    load_ticker_history,
    load_ticker_financials,
    load_portfolio_history,
    load_all_ticker_data,
    load_field_for_all_tickers,
    get_data_summary
)

from yfinance_scraper.fetcher import (
    fetch_data_for_tickers,
    fetch_data_from_date
)

from yfinance_scraper.storage import (
    save_ticker_data,
    load_ticker_data,
    load_data_for_tickers
)

from yfinance_scraper.config import (
    load_config,
    update_config
) 