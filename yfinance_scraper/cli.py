"""Command Line Interface for yfinance_scraper."""

import argparse
import logging
import sys
from typing import List, Optional
import json
import os
import pandas as pd

from yfinance_scraper.config import (
    load_config,
    update_config,
    DEFAULT_CONFIG_PATH
)
from yfinance_scraper.fetcher import fetch_data_for_tickers
from yfinance_scraper.storage import save_data_for_tickers
from yfinance_scraper.updater import update_data_for_tickers
from yfinance_scraper.utils import save_tickers_to_file, load_tickers_from_file
from yfinance_scraper.loader import (
    get_available_tickers,
    get_available_data_types,
    get_data_summary,
    load_ticker_history,
    load_ticker_financials
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def setup_fetch_parser(subparsers):
    """Set up the fetch subcommand parser."""
    parser = subparsers.add_parser(
        "fetch",
        help="Fetch data for tickers and save to parquet files"
    )
    
    parser.add_argument(
        "--tickers",
        type=str,
        nargs="+",
        help="List of ticker symbols to fetch"
    )
    
    parser.add_argument(
        "--period",
        type=str,
        help="Period to fetch data for (e.g., max, 1y, 6mo)"
    )
    
    parser.add_argument(
        "--interval",
        type=str,
        help="Interval for data (e.g., 1d, 1h)"
    )
    
    parser.add_argument(
        "--data-dir",
        type=str,
        help="Directory to save data to"
    )
    
    parser.add_argument(
        "--config",
        type=str,
        default=DEFAULT_CONFIG_PATH,
        help="Path to config file"
    )
    
    parser.add_argument(
        "--from-file",
        type=str,
        help="Load tickers from specified file"
    )
    
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Force refresh of all data, ignoring cache"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Number of tickers to process in a batch"
    )
    
    parser.add_argument(
        "--daily-limit",
        type=int,
        help="Maximum number of tickers to process per day"
    )
    
    parser.add_argument(
        "--max-retries",
        type=int,
        help="Maximum number of retry attempts for rate limiting"
    )


def setup_update_parser(subparsers):
    """Set up the update subcommand parser."""
    parser = subparsers.add_parser(
        "update",
        help="Update existing data with latest information"
    )
    
    parser.add_argument(
        "--tickers",
        type=str,
        nargs="+",
        help="List of ticker symbols to update"
    )
    
    parser.add_argument(
        "--interval",
        type=str,
        help="Interval for data (e.g., 1d, 1h)"
    )
    
    parser.add_argument(
        "--data-dir",
        type=str,
        help="Directory where data is stored"
    )
    
    parser.add_argument(
        "--config",
        type=str,
        default=DEFAULT_CONFIG_PATH,
        help="Path to config file"
    )
    
    parser.add_argument(
        "--from-file",
        action="store_true",
        help="Load tickers from tickers.txt file in data directory"
    )


def setup_config_parser(subparsers):
    """Set up the config subcommand parser."""
    parser = subparsers.add_parser(
        "config",
        help="View or modify configuration"
    )
    
    parser.add_argument(
        "--show",
        action="store_true",
        help="Show current configuration"
    )
    
    parser.add_argument(
        "--set",
        type=str,
        nargs=2,
        action="append",
        metavar=("KEY", "VALUE"),
        help="Set a configuration option"
    )
    
    parser.add_argument(
        "--config",
        type=str,
        default=DEFAULT_CONFIG_PATH,
        help="Path to config file"
    )


def setup_tickers_parser(subparsers):
    """Set up the tickers subcommand parser."""
    parser = subparsers.add_parser(
        "tickers",
        help="Manage ticker symbols"
    )
    
    parser.add_argument(
        "--save",
        type=str,
        nargs="+",
        metavar="TICKER",
        help="Save tickers to tickers.txt file"
    )
    
    parser.add_argument(
        "--load",
        action="store_true",
        help="Load tickers from tickers.txt file and show them"
    )
    
    parser.add_argument(
        "--update-config",
        action="store_true",
        help="Update config with tickers from tickers.txt file"
    )
    
    parser.add_argument(
        "--data-dir",
        type=str,
        help="Directory where data is stored"
    )
    
    parser.add_argument(
        "--config",
        type=str,
        default=DEFAULT_CONFIG_PATH,
        help="Path to config file"
    )


def setup_load_parser(subparsers):
    """Set up the load subcommand parser."""
    parser = subparsers.add_parser(
        "load",
        help="Load and summarize stored ticker data"
    )
    
    parser.add_argument(
        "--data-dir",
        type=str,
        help="Directory where data is stored"
    )
    
    parser.add_argument(
        "--config",
        type=str,
        default=DEFAULT_CONFIG_PATH,
        help="Path to config file"
    )
    
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Generate a summary of available data"
    )
    
    parser.add_argument(
        "--list-tickers",
        action="store_true",
        help="List all available tickers"
    )
    
    parser.add_argument(
        "--ticker-info",
        type=str,
        help="Get detailed information for a specific ticker"
    )
    
    parser.add_argument(
        "--output",
        type=str,
        help="Path to save output (default is to print to console)"
    )
    
    parser.add_argument(
        "--format",
        choices=["csv", "json", "parquet", "excel"],
        default="csv",
        help="Output format for saved data"
    )


def handle_fetch(args):
    """Handle the fetch subcommand."""
    config = load_config(args.config)
    
    # Override config with command line arguments
    if args.data_dir:
        config["data_dir"] = args.data_dir
    
    # Load tickers from file if requested
    if args.from_file:
        from yfinance_scraper.utils import load_tickers_from_file
        file_path = args.from_file
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                file_tickers = [line.strip() for line in f if line.strip()]
            if file_tickers:
                config["tickers"] = file_tickers
                logger.info(f"Loaded {len(file_tickers)} tickers from {file_path}")
            else:
                logger.error(f"No tickers found in {file_path}")
                return 1
        else:
            logger.error(f"Ticker file not found: {file_path}")
            return 1
    elif args.tickers:
        config["tickers"] = args.tickers
        
    if args.period:
        config["period"] = args.period
    if args.interval:
        config["interval"] = args.interval
    
    # Additional optimization parameters
    force_refresh = args.force_refresh if args.force_refresh else False
    batch_size = args.batch_size if args.batch_size else 50
    daily_limit = args.daily_limit if args.daily_limit else 400
    max_retries = args.max_retries if args.max_retries else 5
    
    # Set log level
    logging.getLogger().setLevel(config["log_level"])
    
    # Fetch data with optimizations
    logger.info(f"Fetching data for {len(config['tickers'])} tickers")
    data = fetch_data_for_tickers(
        tickers=config["tickers"],
        period=config["period"],
        interval=config["interval"],
        data_dir=config["data_dir"],
        force_refresh=force_refresh,
        batch_size=batch_size,
        daily_limit=daily_limit,
        max_retries=max_retries
    )
    
    # No need to save data separately as it's cached during fetching
    if data:
        logger.info(f"Successfully processed {len(data)}/{len(config['tickers'])} tickers")
    else:
        logger.error("No data fetched")
        return 1
    
    return 0


def handle_update(args):
    """Handle the update subcommand."""
    config = load_config(args.config)
    
    # Override config with command line arguments
    if args.data_dir:
        config["data_dir"] = args.data_dir
        
    # Load tickers from file if requested
    if args.from_file:
        file_tickers = load_tickers_from_file(config["data_dir"])
        if file_tickers:
            config["tickers"] = file_tickers
    elif args.tickers:
        config["tickers"] = args.tickers
        
    if args.interval:
        config["interval"] = args.interval
    
    # Set log level
    logging.getLogger().setLevel(config["log_level"])
    
    # Update data
    logger.info(f"Updating data for {len(config['tickers'])} tickers")
    results = update_data_for_tickers(
        tickers=config["tickers"],
        data_dir=config["data_dir"],
        interval=config["interval"]
    )
    
    # Check results
    success_count = sum(results.values())
    if success_count < len(config["tickers"]):
        logger.warning(f"Updated {success_count}/{len(config['tickers'])} tickers")
        return 1
    else:
        logger.info(f"All {len(config['tickers'])} tickers updated successfully")
        return 0


def handle_config(args):
    """Handle the config subcommand."""
    config = load_config(args.config)
    
    # Show current configuration
    if args.show:
        print(json.dumps(config, indent=4))
    
    # Set configuration options
    if args.set:
        updates = {}
        for key, value in args.set:
            # Try to convert value to appropriate type
            try:
                if value.lower() == "true":
                    value = True
                elif value.lower() == "false":
                    value = False
                elif value.isdigit():
                    value = int(value)
                elif "." in value and all(p.isdigit() for p in value.split(".")):
                    value = float(value)
                elif value.startswith("[") and value.endswith("]"):
                    value = json.loads(value)
            except (ValueError, json.JSONDecodeError):
                pass
                
            updates[key] = value
        
        if updates:
            update_config(updates, args.config)
            logger.info(f"Configuration updated: {updates}")
    
    return 0


def handle_tickers(args):
    """Handle the tickers subcommand."""
    config = load_config(args.config)
    
    # Use data directory from command line or config
    data_dir = args.data_dir if args.data_dir else config["data_dir"]
    
    # Save tickers to file
    if args.save:
        save_tickers_to_file(args.save, data_dir)
        print(f"Saved {len(args.save)} tickers to {os.path.join(data_dir, 'tickers.txt')}")
    
    # Load tickers from file
    if args.load or args.update_config:
        tickers = load_tickers_from_file(data_dir)
        
        if not tickers:
            print(f"No tickers found in {os.path.join(data_dir, 'tickers.txt')}")
            return 1
            
        if args.load:
            print("Tickers:")
            for ticker in tickers:
                print(f"  {ticker}")
        
        # Update config with tickers from file
        if args.update_config:
            update_config({"tickers": tickers}, args.config)
            print(f"Updated configuration with {len(tickers)} tickers from file")
    
    return 0


def handle_load(args):
    """Handle the load subcommand."""
    config = load_config(args.config)
    
    # Override config with command line arguments
    if args.data_dir:
        config["data_dir"] = args.data_dir
    
    data_dir = config["data_dir"]
    
    # List all available tickers
    if args.list_tickers:
        tickers = get_available_tickers(data_dir)
        if tickers:
            print(f"Found {len(tickers)} tickers in {data_dir}:")
            for ticker in tickers:
                print(f"  {ticker}")
        else:
            print(f"No tickers found in {data_dir}")
        return 0
    
    # Generate data summary
    if args.summary:
        summary = get_data_summary(data_dir)
        
        if summary.empty:
            print(f"No data found in {data_dir}")
            return 1
            
        # Save or print summary
        if args.output:
            output_path = args.output
            if args.format == "csv":
                summary.to_csv(output_path, index=False)
            elif args.format == "json":
                summary.to_json(output_path, orient="records", indent=4)
            elif args.format == "parquet":
                summary.to_parquet(output_path, index=False)
            elif args.format == "excel":
                summary.to_excel(output_path, index=False)
            print(f"Data summary saved to {output_path}")
        else:
            # Print summary to console
            pd.set_option('display.max_rows', None)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            print(summary)
            
        return 0
    
    # Get detailed info for a specific ticker
    if args.ticker_info:
        ticker = args.ticker_info
        
        # Check if ticker exists
        available_tickers = get_available_tickers(data_dir)
        if ticker not in available_tickers:
            print(f"Ticker {ticker} not found in {data_dir}")
            return 1
            
        # Get available data types
        data_types = get_available_data_types(data_dir, ticker)
        
        print(f"Data available for {ticker}:")
        for data_type in sorted(data_types.get(ticker, [])):
            print(f"  - {data_type}")
            
        # Print some sample data
        if 'ohlcv' in data_types.get(ticker, []):
            print("\nMost recent OHLCV data:")
            history = load_ticker_history(ticker, data_dir)
            if history is not None:
                print(history.tail().to_string())
            
        if 'financials' in data_types.get(ticker, []):
            print("\nMost recent financial data:")
            financials = load_ticker_financials(ticker, data_dir)
            if financials is not None:
                # Show just a few rows for readability
                if len(financials) > 5:
                    print(financials.head(5).to_string())
                else:
                    print(financials.to_string())
        
        return 0
    
    # If no specific command is given, show help
    print("Please specify an action (--summary, --list-tickers, or --ticker-info)")
    return 1


def parse_args(args: Optional[List[str]] = None):
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Retrieve and store Yahoo Finance data"
    )
    
    parser.add_argument(
        "--version",
        action="store_true",
        help="Show version and exit"
    )
    
    subparsers = parser.add_subparsers(dest="command")
    setup_fetch_parser(subparsers)
    setup_update_parser(subparsers)
    setup_config_parser(subparsers)
    setup_tickers_parser(subparsers)
    setup_load_parser(subparsers)
    
    return parser.parse_args(args)


def main(args: Optional[List[str]] = None) -> int:
    """Main entry point for the CLI."""
    parsed_args = parse_args(args)
    
    if parsed_args.version:
        from yfinance_scraper import __version__
        print(f"yfinance_scraper version {__version__}")
        return 0
    
    if parsed_args.command == "fetch":
        return handle_fetch(parsed_args)
    elif parsed_args.command == "update":
        return handle_update(parsed_args)
    elif parsed_args.command == "config":
        return handle_config(parsed_args)
    elif parsed_args.command == "tickers":
        return handle_tickers(parsed_args)
    elif parsed_args.command == "load":
        return handle_load(parsed_args)
    else:
        parse_args(["--help"])
        return 1


if __name__ == "__main__":
    sys.exit(main()) 