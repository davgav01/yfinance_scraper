"""Utility functions for YFinance Scraper."""

import os
import logging
from typing import List, Optional, Union, Any
from datetime import datetime, date, timedelta

logger = logging.getLogger(__name__)


def parse_date(date_str: str) -> Optional[datetime]:
    """Parse a date string into a datetime object.
    
    Args:
        date_str: Date string in format YYYY-MM-DD
        
    Returns:
        Datetime object or None if parsing fails
    """
    formats = ["%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d-%m-%Y", "%d/%m/%Y"]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    logger.error(f"Could not parse date string: {date_str}")
    return None


def format_date(dt: Union[datetime, date]) -> str:
    """Format a datetime or date object as a string.
    
    Args:
        dt: Datetime or date object
        
    Returns:
        Formatted date string in YYYY-MM-DD format
    """
    if isinstance(dt, datetime):
        return dt.strftime("%Y-%m-%d")
    else:
        return dt.strftime("%Y-%m-%d")


def validate_ticker_symbol(ticker: str) -> bool:
    """Validate a ticker symbol.
    
    Args:
        ticker: Ticker symbol to validate
        
    Returns:
        True if the ticker symbol is valid, False otherwise
    """
    # Basic validation - alphanumeric, dot, or dash, length <= 10
    if not ticker or len(ticker) > 10:
        return False
    
    valid_chars = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-^")
    return all(c in valid_chars for c in ticker.upper())


def validate_tickers(tickers: List[str]) -> List[str]:
    """Validate a list of ticker symbols.
    
    Args:
        tickers: List of ticker symbols to validate
        
    Returns:
        List of valid ticker symbols
    """
    valid_tickers = []
    
    for ticker in tickers:
        if validate_ticker_symbol(ticker):
            valid_tickers.append(ticker.upper())
        else:
            logger.warning(f"Invalid ticker symbol: {ticker}")
    
    return valid_tickers


def get_valid_intervals() -> List[str]:
    """Get list of valid intervals for Yahoo Finance.
    
    Returns:
        List of valid interval strings
    """
    return [
        "1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h",
        "1d", "5d", "1wk", "1mo", "3mo"
    ]


def get_valid_periods() -> List[str]:
    """Get list of valid periods for Yahoo Finance.
    
    Returns:
        List of valid period strings
    """
    return [
        "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"
    ]


def save_tickers_to_file(tickers: List[str], data_dir: str, filename: str = "tickers.txt") -> bool:
    """Save a list of tickers to a file.
    
    Args:
        tickers: List of ticker symbols to save
        data_dir: Directory to save the file
        filename: Name of the file
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Ensure the directory exists
        os.makedirs(data_dir, exist_ok=True)
        
        # Validate tickers
        valid_tickers = validate_tickers(tickers)
        
        # Write to file
        file_path = os.path.join(data_dir, filename)
        with open(file_path, 'w') as f:
            for ticker in valid_tickers:
                f.write(f"{ticker}\n")
                
        logger.info(f"Saved {len(valid_tickers)} tickers to {file_path}")
        return True
    except Exception as e:
        logger.error(f"Error saving tickers to file: {e}")
        return False


def load_tickers_from_file(data_dir: str, filename: str = "tickers.txt") -> List[str]:
    """Load a list of tickers from a file.
    
    Args:
        data_dir: Directory where the file is stored
        filename: Name of the file
        
    Returns:
        List of ticker symbols
    """
    file_path = os.path.join(data_dir, filename)
    
    if not os.path.exists(file_path):
        logger.warning(f"Tickers file does not exist: {file_path}")
        return []
    
    try:
        with open(file_path, 'r') as f:
            tickers = [line.strip() for line in f if line.strip()]
        
        # Validate tickers
        valid_tickers = validate_tickers(tickers)
        
        logger.info(f"Loaded {len(valid_tickers)} tickers from {file_path}")
        return valid_tickers
    except Exception as e:
        logger.error(f"Error loading tickers from file: {e}")
        return []


def is_cache_valid(ticker: str, data_dir: str, data_type: str = "ohlcv", max_age_days: int = 1) -> bool:
    """Check if cached data for a ticker is valid and recent enough.
    
    Args:
        ticker: Ticker symbol
        data_dir: Base data directory
        data_type: Type of data to check
        max_age_days: Maximum age of data in days
        
    Returns:
        True if data exists and is recent enough, False otherwise
    """
    from yfinance_scraper.storage import get_latest_date
    import os
    from datetime import datetime, timedelta
    
    # Check if the file exists
    ticker_dir = os.path.join(data_dir, ticker)
    file_path = os.path.join(ticker_dir, f"{data_type}.parquet")
    if not os.path.exists(file_path):
        return False
    
    # Check if the data is recent enough
    latest_date = get_latest_date(ticker, data_dir, data_type)
    if latest_date is None:
        return False
    
    cutoff_date = datetime.now() - timedelta(days=max_age_days)
    return latest_date >= cutoff_date


def prioritize_tickers(tickers: List[str], data_dir: str, max_age_days: int = 1) -> List[str]:
    """Prioritize tickers based on cache validity and importance.
    
    Args:
        tickers: List of ticker symbols
        data_dir: Base data directory
        max_age_days: Maximum age of data in days
        
    Returns:
        Prioritized list of tickers
    """
    import os
    
    # Separate tickers into those with and without cached data
    uncached = []
    cached_outdated = []
    cached_valid = []
    
    for ticker in tickers:
        if not os.path.exists(os.path.join(data_dir, ticker)):
            uncached.append(ticker)
        elif not is_cache_valid(ticker, data_dir, max_age_days=max_age_days):
            cached_outdated.append(ticker)
        else:
            cached_valid.append(ticker)
    
    # Prioritize uncached tickers and outdated cache
    return uncached + cached_outdated + cached_valid


def chunk_list(items: List[Any], chunk_size: int) -> List[List[Any]]:
    """Split a list into chunks of specified size.
    
    Args:
        items: List to split
        chunk_size: Maximum size of each chunk
        
    Returns:
        List of chunks
    """
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)] 