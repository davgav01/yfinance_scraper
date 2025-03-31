#!/usr/bin/env python
"""Example script demonstrating ticker management functionality."""

import os
from yfinance_scraper.config import load_config, ensure_data_dir, update_config
from yfinance_scraper.utils import save_tickers_to_file, load_tickers_from_file
from yfinance_scraper.fetcher import fetch_data_for_tickers
from yfinance_scraper.storage import save_data_for_tickers

def main():
    """Run the example."""
    print("YFinance Scraper - Ticker Management Example")
    print("-------------------------------------------")
    
    # Load configuration
    config = load_config()
    print(f"Default data directory: {config['data_dir']}")
    
    # Ensure data directory exists
    ensure_data_dir(config['data_dir'])
    
    # Define a list of technology company tickers
    tech_tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "INTC", "CRM", "AMD"]
    print(f"\nSaving {len(tech_tickers)} technology company tickers:")
    for ticker in tech_tickers:
        print(f"  {ticker}")
    
    # Save tickers to file
    save_tickers_to_file(tech_tickers, config['data_dir'])
    print(f"Tickers saved to {os.path.join(config['data_dir'], 'tickers.txt')}")
    
    # Load tickers from file
    loaded_tickers = load_tickers_from_file(config['data_dir'])
    print(f"\nLoaded {len(loaded_tickers)} tickers from file:")
    for ticker in loaded_tickers:
        print(f"  {ticker}")
    
    # Update configuration with tickers from file
    update_config({"tickers": loaded_tickers})
    print("\nConfiguration updated with tickers from file")
    
    # Fetch data for the first 2 tickers as a demonstration
    demo_tickers = loaded_tickers[:2]
    print(f"\nFetching data for demo tickers: {', '.join(demo_tickers)}")
    
    try:
        data = fetch_data_for_tickers(
            tickers=demo_tickers,
            period="1mo",  # Just fetch 1 month of data for the demo
            interval="1d"
        )
        
        if data:
            save_data_for_tickers(data, config['data_dir'])
            print(f"Data saved to {config['data_dir']}")
            
            # Show what was saved
            for ticker in data:
                print(f"\n{ticker} data:")
                for data_type, df in data[ticker].items():
                    print(f"  {data_type}: {df.shape[0]} rows")
    except Exception as e:
        print(f"Error fetching data: {e}")
    
    print("\nDemo complete!")
    print("You can now try the CLI commands:")
    print("  yfinance-scraper tickers --load")
    print("  yfinance-scraper fetch --from-file")
    print("  yfinance-scraper update --from-file")

if __name__ == "__main__":
    main() 