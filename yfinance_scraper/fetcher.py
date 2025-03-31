"""Module for fetching data from Yahoo Finance with rate limit handling."""

import yfinance as yf
import pandas as pd
from typing import List, Dict, Any, Optional, Union, Tuple
import logging
from datetime import datetime, timedelta
import time
import random
import os

from yfinance_scraper.utils import (
    is_cache_valid,
    prioritize_tickers,
    chunk_list
)
from yfinance_scraper.storage import (
    save_ticker_data,
    load_ticker_data,
    get_latest_date
)

logger = logging.getLogger(__name__)

# Rate limit settings
RATE_LIMIT_DEFAULT_TIMEOUT = 30  # seconds
MAX_RETRIES = 5
BATCH_SIZE = 50
DAILY_LIMIT = 400  # Tickers per day
REQUEST_DELAY = 2  # seconds between requests
BATCH_DELAY = 5  # seconds between batches
MAX_AGE_DAYS = 1  # Data older than this will be refreshed


def fetch_with_retry(
    ticker: str,
    period: str = "max",
    interval: str = "1d",
    prepost: bool = False,
    actions: bool = True,
    data_dir: Optional[str] = None,
    max_retries: int = MAX_RETRIES,
    force_refresh: bool = False
) -> Dict[str, pd.DataFrame]:
    """Fetch data for a single ticker with retry logic and caching.
    
    Args:
        ticker: Ticker symbol
        period: Data period to fetch
        interval: Data interval
        prepost: Include pre and post market data
        actions: Include dividends and stock splits
        data_dir: Base data directory for caching
        max_retries: Maximum number of retry attempts
        force_refresh: Force refresh even if cache is valid
        
    Returns:
        Dictionary with data frames
    """
    # Check cache first if data_dir is provided
    if data_dir and not force_refresh and is_cache_valid(ticker, data_dir, max_age_days=MAX_AGE_DAYS):
        logger.info(f"Using cached data for {ticker}")
        return load_ticker_data(ticker, data_dir)
    
    # Not in cache or forced refresh, fetch with retries
    for attempt in range(max_retries):
        try:
            logger.info(f"Fetching data for {ticker} (attempt {attempt+1}/{max_retries})")
            
            # Get ticker object
            ticker_obj = yf.Ticker(ticker)
            
            # Fetch historical data
            hist_data = ticker_obj.history(
                period=period,
                interval=interval,
                prepost=prepost,
                actions=actions,
                auto_adjust=False
            )
            
            if hist_data.empty:
                logger.warning(f"No data returned for {ticker}")
                time.sleep(REQUEST_DELAY)  # Sleep even on empty result
                continue
            
            # Prepare result
            result = {"ohlcv": hist_data}
            
            # Extract dividends and splits if available
            if 'Dividends' in hist_data.columns and hist_data['Dividends'].any():
                dividends = hist_data[hist_data['Dividends'] > 0][['Dividends']]
                result["dividends"] = dividends
                
            if 'Stock Splits' in hist_data.columns and hist_data['Stock Splits'].any():
                splits = hist_data[hist_data['Stock Splits'] > 0][['Stock Splits']]
                result["splits"] = splits
            
            # Get fundamental data with delays between requests
            try:
                # Add information about the company
                info = ticker_obj.info
                if info:
                    company_info = pd.DataFrame([info])
                    result["info"] = company_info
                time.sleep(REQUEST_DELAY)
            except Exception as e:
                logger.warning(f"Could not fetch info for {ticker}: {e}")
            
            # Try to get other fundamental data with delays
            try:
                financials = ticker_obj.financials
                if not financials.empty:
                    result["financials"] = financials
                time.sleep(REQUEST_DELAY)
            except Exception as e:
                logger.debug(f"Could not fetch financials for {ticker}: {e}")
                
            try:
                balance_sheet = ticker_obj.balance_sheet
                if not balance_sheet.empty:
                    result["balance_sheet"] = balance_sheet
                time.sleep(REQUEST_DELAY)
            except Exception as e:
                logger.debug(f"Could not fetch balance sheet for {ticker}: {e}")
                
            try:
                cashflow = ticker_obj.cashflow
                if not cashflow.empty:
                    result["cashflow"] = cashflow
                time.sleep(REQUEST_DELAY)
            except Exception as e:
                logger.debug(f"Could not fetch cashflow for {ticker}: {e}")
                
            try:
                # Using income_stmt instead of the deprecated earnings attribute
                income_stmt = ticker_obj.income_stmt
                if not income_stmt.empty and "Net Income" in income_stmt.index:
                    result["earnings"] = income_stmt.loc[["Net Income"]].T
                time.sleep(REQUEST_DELAY)
            except Exception as e:
                logger.debug(f"Could not fetch earnings data for {ticker}: {e}")
            
            # Save to cache if data_dir is provided
            if data_dir and result:
                save_ticker_data(ticker, result, data_dir)
                logger.info(f"Cached data for {ticker}")
            
            return result
                
        except Exception as e:
            if "Too Many Requests" in str(e):
                # Rate limited, use exponential backoff
                wait_time = RATE_LIMIT_DEFAULT_TIMEOUT * (2 ** attempt) + random.uniform(0, 3)
                logger.warning(f"Rate limited for {ticker}, waiting {wait_time:.2f}s before retry")
                time.sleep(wait_time)
            else:
                logger.error(f"Error fetching data for {ticker}: {e}")
                time.sleep(REQUEST_DELAY)  # Still add delay on errors
    
    logger.error(f"Failed to fetch {ticker} after {max_retries} retries")
    return {}


def fetch_batch_price_data(
    tickers: List[str],
    period: str = "max",
    interval: str = "1d",
    prepost: bool = False,
    actions: bool = True,
    max_retries: int = MAX_RETRIES
) -> Dict[str, pd.DataFrame]:
    """Fetch price data for multiple tickers in a single batch request.
    
    Args:
        tickers: List of ticker symbols
        period: Data period to fetch
        interval: Data interval
        prepost: Include pre and post market data
        actions: Include dividends and stock splits
        max_retries: Maximum number of retry attempts
        
    Returns:
        Dictionary with ticker symbols as keys and price data as values
    """
    if not tickers:
        return {}
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Fetching batch price data for {len(tickers)} tickers (attempt {attempt+1}/{max_retries})")
            
            # Use yf.download for more efficient batch request
            data = yf.download(
                tickers=tickers,
                period=period,
                interval=interval,
                prepost=prepost,
                actions=actions,
                group_by='ticker',
                auto_adjust=False
            )
            
            if data.empty:
                logger.warning(f"No data returned for batch of {len(tickers)} tickers")
                time.sleep(BATCH_DELAY)
                continue
                
            result = {}
            
            # Handle single ticker case differently (yfinance returns different format)
            if len(tickers) == 1:
                ticker = tickers[0]
                result[ticker] = data
            else:
                # Process multi-ticker response
                for ticker in tickers:
                    if ticker in data.columns.levels[0]:
                        ticker_data = data[ticker].copy()
                        if not ticker_data.empty:
                            result[ticker] = ticker_data
                    
            logger.info(f"Successfully fetched batch price data for {len(result)}/{len(tickers)} tickers")
            return result
                
        except Exception as e:
            if "Too Many Requests" in str(e):
                # Rate limited, use exponential backoff
                wait_time = RATE_LIMIT_DEFAULT_TIMEOUT * (2 ** attempt) + random.uniform(0, 5)
                logger.warning(f"Rate limited for batch request, waiting {wait_time:.2f}s before retry")
                time.sleep(wait_time)
            else:
                logger.error(f"Error fetching batch data: {e}")
                time.sleep(BATCH_DELAY)
    
    logger.error(f"Failed to fetch batch data after {max_retries} retries")
    return {}


def fetch_data_for_tickers(
    tickers: List[str],
    period: str = "max",
    interval: str = "1d",
    prepost: bool = False,
    actions: bool = True,
    data_dir: Optional[str] = None,
    max_retries: int = MAX_RETRIES,
    force_refresh: bool = False,
    batch_size: int = BATCH_SIZE,
    daily_limit: int = DAILY_LIMIT
) -> Dict[str, Dict[str, pd.DataFrame]]:
    """Fetch data for multiple tickers with rate limit handling, batching, and caching.
    
    Args:
        tickers: List of ticker symbols
        period: Data period to fetch
        interval: Data interval
        prepost: Include pre and post market data
        actions: Include dividends and stock splits
        data_dir: Base data directory for caching
        max_retries: Maximum number of retry attempts
        force_refresh: Force refresh even if cache is valid
        batch_size: Number of tickers to process in a batch
        daily_limit: Maximum number of tickers to process per day
        
    Returns:
        Dictionary with ticker symbols as keys and data dictionaries as values
    """
    if not tickers:
        logger.warning("No tickers provided")
        return {}
    
    # Check for excessive number of tickers
    if len(tickers) > 1000:
        logger.warning(f"Large number of tickers ({len(tickers)}). This operation might take a long time.")
    
    # Prioritize tickers if using cache
    if data_dir and not force_refresh:
        tickers = prioritize_tickers(tickers, data_dir, max_age_days=MAX_AGE_DAYS)
        logger.info(f"Prioritized {len(tickers)} tickers based on cache status")
    
    # Split into daily chunks to respect daily limits
    daily_chunks = chunk_list(tickers, daily_limit)
    logger.info(f"Split {len(tickers)} tickers into {len(daily_chunks)} daily chunks")
    
    result = {}
    
    for day, daily_tickers in enumerate(daily_chunks):
        logger.info(f"Processing day {day+1}/{len(daily_chunks)}, {len(daily_tickers)} tickers")
        
        # Process tickers in batches for price data
        price_batches = chunk_list(daily_tickers, batch_size)
        
        batch_results = {}
        for batch_idx, batch in enumerate(price_batches):
            logger.info(f"Processing batch {batch_idx+1}/{len(price_batches)}, {len(batch)} tickers")
            
            # Skip batch if all tickers have valid cache
            if data_dir and not force_refresh and all(is_cache_valid(t, data_dir, max_age_days=MAX_AGE_DAYS) for t in batch):
                logger.info(f"Batch {batch_idx+1} entirely in cache, loading cached data")
                for ticker in batch:
                    ticker_data = load_ticker_data(ticker, data_dir)
                    if ticker_data:
                        batch_results[ticker] = ticker_data
                continue
            
            # Attempt batch download for price data first
            try:
                price_data = fetch_batch_price_data(
                    tickers=batch,
                    period=period,
                    interval=interval,
                    prepost=prepost,
                    actions=actions,
                    max_retries=max_retries
                )
                
                # Add delay between batches
                time.sleep(BATCH_DELAY)
                
                # Fetch additional data (fundamentals) for each ticker individually
                for ticker in batch:
                    try:
                        logger.info(f"Fetching fundamentals for {ticker}")
                        
                        # Check if we have price data for this ticker
                        if ticker in price_data and not price_data[ticker].empty:
                            # Initialize with price data
                            ticker_result = {"ohlcv": price_data[ticker]}
                            
                            # Extract dividends and splits if available
                            if 'Dividends' in price_data[ticker].columns and price_data[ticker]['Dividends'].any():
                                dividends = price_data[ticker][price_data[ticker]['Dividends'] > 0][['Dividends']]
                                ticker_result["dividends"] = dividends
                                
                            if 'Stock Splits' in price_data[ticker].columns and price_data[ticker]['Stock Splits'].any():
                                splits = price_data[ticker][price_data[ticker]['Stock Splits'] > 0][['Stock Splits']]
                                ticker_result["splits"] = splits
                            
                            # If cache is valid for fundamentals but we need fresh price data
                            if data_dir and not force_refresh and is_cache_valid(ticker, data_dir, max_age_days=MAX_AGE_DAYS):
                                cached_data = load_ticker_data(ticker, data_dir)
                                # Merge fundamental data from cache
                                for key, df in cached_data.items():
                                    if key not in ["ohlcv", "dividends", "splits"]:
                                        ticker_result[key] = df
                            else:
                                # Fetch fundamentals with retries
                                ticker_obj = yf.Ticker(ticker)
                                
                                # Get fundamental data with delays between requests
                                try:
                                    # Add information about the company
                                    info = ticker_obj.info
                                    if info:
                                        company_info = pd.DataFrame([info])
                                        ticker_result["info"] = company_info
                                    time.sleep(REQUEST_DELAY)
                                except Exception as e:
                                    logger.warning(f"Could not fetch info for {ticker}: {e}")
                                
                                # Try to get other fundamental data with delays
                                try:
                                    financials = ticker_obj.financials
                                    if not financials.empty:
                                        ticker_result["financials"] = financials
                                    time.sleep(REQUEST_DELAY)
                                except Exception as e:
                                    logger.debug(f"Could not fetch financials for {ticker}: {e}")
                                    
                                try:
                                    balance_sheet = ticker_obj.balance_sheet
                                    if not balance_sheet.empty:
                                        ticker_result["balance_sheet"] = balance_sheet
                                    time.sleep(REQUEST_DELAY)
                                except Exception as e:
                                    logger.debug(f"Could not fetch balance sheet for {ticker}: {e}")
                                    
                                try:
                                    cashflow = ticker_obj.cashflow
                                    if not cashflow.empty:
                                        ticker_result["cashflow"] = cashflow
                                    time.sleep(REQUEST_DELAY)
                                except Exception as e:
                                    logger.debug(f"Could not fetch cashflow for {ticker}: {e}")
                                    
                                try:
                                    # Using income_stmt instead of the deprecated earnings attribute
                                    income_stmt = ticker_obj.income_stmt
                                    if not income_stmt.empty and "Net Income" in income_stmt.index:
                                        ticker_result["earnings"] = income_stmt.loc[["Net Income"]].T
                                    time.sleep(REQUEST_DELAY)
                                except Exception as e:
                                    logger.debug(f"Could not fetch earnings data for {ticker}: {e}")
                            
                            # Save to cache
                            if data_dir:
                                save_ticker_data(ticker, ticker_result, data_dir)
                                logger.info(f"Cached data for {ticker}")
                            
                            batch_results[ticker] = ticker_result
                        else:
                            # If batch request didn't return data for this ticker, try individual fetch
                            ticker_data = fetch_with_retry(
                                ticker=ticker,
                                period=period,
                                interval=interval,
                                prepost=prepost,
                                actions=actions,
                                data_dir=data_dir,
                                max_retries=max_retries,
                                force_refresh=force_refresh
                            )
                            
                            if ticker_data:
                                batch_results[ticker] = ticker_data
                    
                    except Exception as e:
                        if "Too Many Requests" in str(e):
                            logger.warning(f"Rate limited during fundamentals fetch for {ticker}, skipping for now")
                            # Don't immediately retry, continue with next ticker
                        else:
                            logger.error(f"Error processing ticker {ticker}: {e}")
            
            except Exception as e:
                logger.error(f"Error processing batch {batch_idx+1}: {e}")
                # Fall back to individual fetching for all tickers in the batch
                for ticker in batch:
                    ticker_data = fetch_with_retry(
                        ticker=ticker,
                        period=period,
                        interval=interval,
                        prepost=prepost,
                        actions=actions,
                        data_dir=data_dir,
                        max_retries=max_retries,
                        force_refresh=force_refresh
                    )
                    
                    if ticker_data:
                        batch_results[ticker] = ticker_data
        
        # Update overall results with this day's batch
        result.update(batch_results)
        
        # If there are more days, add a delay between days
        if day < len(daily_chunks) - 1:
            logger.info(f"Day {day+1} complete. Sleeping before starting next day.")
            # In production code, you might want to use a job scheduler instead
            day_delay = 60  # Just a token delay for testing
            time.sleep(day_delay)
    
    logger.info(f"Total fetched: {len(result)}/{len(tickers)} tickers")
    return result


def fetch_data_from_date(
    tickers: List[str],
    start_date: Union[str, datetime],
    end_date: Optional[Union[str, datetime]] = None,
    interval: str = "1d",
    prepost: bool = False,
    actions: bool = True,
    data_dir: Optional[str] = None,
    max_retries: int = MAX_RETRIES,
    batch_size: int = BATCH_SIZE,
    daily_limit: int = DAILY_LIMIT
) -> Dict[str, Dict[str, pd.DataFrame]]:
    """Fetch data for tickers from a specific start date with rate limit handling.
    
    Args:
        tickers: List of ticker symbols
        start_date: Start date for data
        end_date: End date for data (defaults to current date)
        interval: Data interval
        prepost: Include pre and post market data
        actions: Include dividends and stock splits
        data_dir: Base data directory for caching
        max_retries: Maximum number of retry attempts
        batch_size: Number of tickers to process in a batch
        daily_limit: Maximum number of tickers to process per day
        
    Returns:
        Dictionary with ticker symbols as keys and data dictionaries as values
    """
    if end_date is None:
        end_date = datetime.now()
    
    logger.info(f"Fetching data for {len(tickers)} tickers from {start_date} to {end_date}")
    
    # Check for excessive number of tickers
    if len(tickers) > 1000:
        logger.warning(f"Large number of tickers ({len(tickers)}). This operation might take a long time.")
    
    # Split into daily chunks to respect daily limits
    daily_chunks = chunk_list(tickers, daily_limit)
    logger.info(f"Split {len(tickers)} tickers into {len(daily_chunks)} daily chunks")
    
    result = {}
    
    for day, daily_tickers in enumerate(daily_chunks):
        logger.info(f"Processing day {day+1}/{len(daily_chunks)}, {len(daily_tickers)} tickers")
        
        # Process tickers in batches
        batches = chunk_list(daily_tickers, batch_size)
        
        for batch_idx, batch in enumerate(batches):
            logger.info(f"Processing batch {batch_idx+1}/{len(batches)}, {len(batch)} tickers")
            
            try:
                # Use yf.download for more efficient batch download
                hist_data = yf.download(
                    tickers=batch,
                    start=start_date,
                    end=end_date,
                    interval=interval,
                    prepost=prepost,
                    actions=actions,
                    group_by='ticker',
                    auto_adjust=False
                )
                
                # Add delay between batches
                time.sleep(BATCH_DELAY)
                
                # Process the results
                for ticker in batch:
                    try:
                        # Handle single ticker case
                        if len(batch) == 1:
                            ticker_hist = hist_data
                        else:
                            # Get this ticker's data slice
                            if ticker in hist_data.columns.levels[0]:
                                ticker_hist = hist_data[ticker].copy()
                            else:
                                logger.warning(f"No data returned for {ticker}")
                                continue
                        
                        if ticker_hist.empty:
                            logger.warning(f"Empty data returned for {ticker}")
                            continue
                            
                        # Store the results
                        ticker_result = {"ohlcv": ticker_hist}
                        
                        # Extract dividends and splits if available
                        if 'Dividends' in ticker_hist.columns and ticker_hist['Dividends'].any():
                            dividends = ticker_hist[ticker_hist['Dividends'] > 0][['Dividends']]
                            ticker_result["dividends"] = dividends
                            
                        if 'Stock Splits' in ticker_hist.columns and ticker_hist['Stock Splits'].any():
                            splits = ticker_hist[ticker_hist['Stock Splits'] > 0][['Stock Splits']]
                            ticker_result["splits"] = splits
                        
                        # Save to cache if data_dir is provided
                        if data_dir:
                            save_ticker_data(ticker, ticker_result, data_dir)
                            logger.info(f"Cached data for {ticker}")
                        
                        result[ticker] = ticker_result
                        
                    except Exception as e:
                        logger.error(f"Error processing ticker {ticker}: {e}")
            
            except Exception as e:
                if "Too Many Requests" in str(e):
                    # Rate limited, use exponential backoff
                    wait_time = RATE_LIMIT_DEFAULT_TIMEOUT * (2 ** batch_idx % 5) + random.uniform(0, 5)
                    logger.warning(f"Rate limited for batch request, waiting {wait_time:.2f}s before trying individual requests")
                    time.sleep(wait_time)
                    
                    # Fall back to individual requests
                    for ticker in batch:
                        for attempt in range(max_retries):
                            try:
                                ticker_obj = yf.Ticker(ticker)
                                
                                hist_data = ticker_obj.history(
                                    start=start_date,
                                    end=end_date,
                                    interval=interval,
                                    prepost=prepost,
                                    actions=actions,
                                    auto_adjust=False
                                )
                                
                                if hist_data.empty:
                                    logger.warning(f"No data returned for {ticker}")
                                    break
                                    
                                ticker_result = {"ohlcv": hist_data}
                                
                                # Extract dividends and splits if available
                                if 'Dividends' in hist_data.columns and hist_data['Dividends'].any():
                                    dividends = hist_data[hist_data['Dividends'] > 0][['Dividends']]
                                    ticker_result["dividends"] = dividends
                                    
                                if 'Stock Splits' in hist_data.columns and hist_data['Stock Splits'].any():
                                    splits = hist_data[hist_data['Stock Splits'] > 0][['Stock Splits']]
                                    ticker_result["splits"] = splits
                                
                                # Save to cache if data_dir is provided
                                if data_dir:
                                    save_ticker_data(ticker, ticker_result, data_dir)
                                    logger.info(f"Cached data for {ticker}")
                                
                                result[ticker] = ticker_result
                                break
                                
                            except Exception as inner_e:
                                if "Too Many Requests" in str(inner_e):
                                    # Rate limited, use exponential backoff
                                    wait_time = RATE_LIMIT_DEFAULT_TIMEOUT * (2 ** attempt) + random.uniform(0, 3)
                                    logger.warning(f"Rate limited for {ticker}, waiting {wait_time:.2f}s before retry")
                                    time.sleep(wait_time)
                                else:
                                    logger.error(f"Error fetching data for {ticker}: {inner_e}")
                                    break
                        
                        # Add delay between individual requests
                        time.sleep(REQUEST_DELAY)
                else:
                    logger.error(f"Error processing batch {batch_idx+1}: {e}")
        
        # If there are more days, add a delay between days  
        if day < len(daily_chunks) - 1:
            logger.info(f"Day {day+1} complete. Sleeping before starting next day.")
            # In production code, you might want to use a job scheduler instead
            day_delay = 60  # Just a token delay for testing
            time.sleep(day_delay)
    
    logger.info(f"Total fetched: {len(result)}/{len(tickers)} tickers")
    return result 