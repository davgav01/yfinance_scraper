"""Module for updating existing ticker data."""

import os
import pandas as pd
from typing import List, Dict, Any, Optional, Union
import logging
from datetime import datetime, timedelta

from yfinance_scraper.fetcher import fetch_data_from_date
from yfinance_scraper.storage import (
    load_ticker_data,
    save_ticker_data,
    get_latest_date
)

logger = logging.getLogger(__name__)


def update_ticker_data(
    ticker: str,
    data_dir: str,
    end_date: Optional[Union[str, datetime]] = None,
    interval: str = "1d"
) -> bool:
    """Update data for a single ticker.
    
    Args:
        ticker: Ticker symbol
        data_dir: Base data directory
        end_date: End date for the update (defaults to current date)
        interval: Data interval
        
    Returns:
        True if update was successful, False otherwise
    """
    logger.info(f"Updating data for {ticker}")
    
    # Get the latest date we have data for
    latest_date = get_latest_date(ticker, data_dir)
    
    if latest_date is None:
        logger.warning(f"No existing data found for {ticker}, skipping update")
        return False
    
    # Add one day to the latest date to avoid duplication
    start_date = latest_date + timedelta(days=1)
    
    # If the start date is in the future, no update needed
    if start_date > datetime.now():
        logger.info(f"Data for {ticker} is already up to date")
        return True
    
    # Set end date to today if not specified
    if end_date is None:
        end_date = datetime.now()
    
    # Fetch new data from the latest date
    new_data = fetch_data_from_date(
        tickers=[ticker],
        start_date=start_date,
        end_date=end_date,
        interval=interval
    )
    
    if not new_data or ticker not in new_data:
        logger.warning(f"No new data available for {ticker}")
        return False
    
    # Load existing data
    existing_data = load_ticker_data(ticker, data_dir)
    
    # Merge new data with existing data
    merged_data = {}
    
    for data_type in set(list(existing_data.keys()) + list(new_data[ticker].keys())):
        if data_type in new_data[ticker] and data_type in existing_data:
            # Concatenate the DataFrames
            merged_df = pd.concat([existing_data[data_type], new_data[ticker][data_type]])
            # Remove duplicates if any
            merged_df = merged_df[~merged_df.index.duplicated(keep='last')]
            # Sort the index
            merged_df = merged_df.sort_index()
            merged_data[data_type] = merged_df
        elif data_type in new_data[ticker]:
            merged_data[data_type] = new_data[ticker][data_type]
        else:
            merged_data[data_type] = existing_data[data_type]
    
    # Save the merged data
    success = save_ticker_data(ticker, merged_data, data_dir)
    
    if success:
        logger.info(f"Successfully updated data for {ticker}")
    else:
        logger.error(f"Failed to save updated data for {ticker}")
    
    return success


def update_data_for_tickers(
    tickers: List[str],
    data_dir: str,
    end_date: Optional[Union[str, datetime]] = None,
    interval: str = "1d"
) -> Dict[str, bool]:
    """Update data for multiple tickers.
    
    Args:
        tickers: List of ticker symbols
        data_dir: Base data directory
        end_date: End date for the update (defaults to current date)
        interval: Data interval
        
    Returns:
        Dictionary with ticker symbols as keys and success status as values
    """
    results = {}
    
    for ticker in tickers:
        results[ticker] = update_ticker_data(
            ticker=ticker,
            data_dir=data_dir,
            end_date=end_date,
            interval=interval
        )
    
    success_count = sum(results.values())
    logger.info(f"Updated data for {success_count}/{len(tickers)} tickers")
    
    if success_count < len(tickers):
        failed_tickers = [t for t, s in results.items() if not s]
        logger.warning(f"Failed to update data for tickers: {', '.join(failed_tickers)}")
    
    return results 