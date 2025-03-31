#!/usr/bin/env python
"""Example script demonstrating how to use yfinance_scraper's data loading capabilities."""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# Import functions from yfinance_scraper
from yfinance_scraper import (
    get_available_tickers,
    load_ticker_history,
    load_portfolio_history,
    load_ticker_financials,
    get_data_summary
)

# Set the data directory - adjust this to your actual data path
DATA_DIR = os.path.expanduser("~/data/yfinance")

def main():
    """Main entry point for example script."""
    print("YFinance Scraper Data Analysis Example")
    print("======================================")
    
    # 1. Check available tickers
    tickers = get_available_tickers(DATA_DIR)
    print(f"Found {len(tickers)} tickers in {DATA_DIR}")
    
    if not tickers:
        print("No tickers found. Make sure you've fetched data first.")
        return
    
    # 2. Get summary of available data
    print("\nGenerating data summary...")
    summary = get_data_summary(DATA_DIR)
    print(f"Data summary:\n{summary[['ticker', 'start_date', 'end_date', 'trading_days']].head()}")
    
    # 3. Load historical data for a single ticker
    example_ticker = tickers[0]
    print(f"\nLoading historical data for {example_ticker}")
    
    # Get last year of data
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    
    history = load_ticker_history(
        ticker=example_ticker,
        data_dir=DATA_DIR,
        start_date=start_date,
        end_date=end_date,
        fields=['Close', 'Volume']
    )
    
    if history is not None:
        print(f"Loaded {len(history)} days of data for {example_ticker}")
        print(history.tail())
        
        # Calculate some stats
        daily_returns = history['Close'].pct_change().dropna()
        print(f"\nDaily returns statistics for {example_ticker}:")
        print(f"Mean: {daily_returns.mean():.4f}")
        print(f"Std Dev: {daily_returns.std():.4f}")
        print(f"Min: {daily_returns.min():.4f}")
        print(f"Max: {daily_returns.max():.4f}")
    
    # 4. Load portfolio data for multiple tickers
    if len(tickers) >= 3:
        portfolio_tickers = tickers[:3]  # Take first 3 tickers
        
        print(f"\nLoading portfolio data for {portfolio_tickers}")
        portfolio = load_portfolio_history(
            tickers=portfolio_tickers,
            data_dir=DATA_DIR,
            field='Close',
            start_date=start_date,
            end_date=end_date,
            fill_method='ffill'
        )
        
        if not portfolio.empty:
            print(f"Portfolio data shape: {portfolio.shape}")
            print(portfolio.tail())
            
            # Calculate correlations
            corr = portfolio.pct_change().corr()
            print("\nCorrelation matrix:")
            print(corr)
    
    # 5. Load financial statement data
    print(f"\nLoading income statement data for {example_ticker}")
    financials = load_ticker_financials(
        ticker=example_ticker,
        data_dir=DATA_DIR,
        statement_type='financials'
    )
    
    if financials is not None and not financials.empty:
        print(f"Financial data available for {len(financials.columns)} periods")
        print(financials.head())


if __name__ == "__main__":
    main() 