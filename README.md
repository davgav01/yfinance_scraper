# YFinance Scraper

A Python package for retrieving, storing, and updating financial data from Yahoo Finance. This package allows you to:

1. Fetch extended OHLCV (open, high, low, close, adjusted close, volume) data along with dividends, splits, and other fundamental data from Yahoo Finance
2. Store the data in Apache Parquet format for efficient storage and retrieval
3. Update the saved data with the latest available information

## Installation

```bash
# From PyPI (when published)
pip install yfinance-scraper

# From source
git clone https://github.com/yourusername/yfinance-scraper.git
cd yfinance-scraper
pip install -e .
```

## Usage

### Command Line Interface

The package provides a command-line interface (CLI) for easy interaction:

#### Initial Data Fetching

```bash
# Using default configuration (fetches AAPL, MSFT, GOOGL, AMZN, META)
yfinance-scraper fetch

# Specifying tickers
yfinance-scraper fetch --tickers AAPL MSFT GOOGL

# Customizing period and interval
yfinance-scraper fetch --tickers AAPL MSFT --period 5y --interval 1d

# Specifying data directory
yfinance-scraper fetch --data-dir /custom/data/path

# Fetch data for tickers in tickers.txt file
yfinance-scraper fetch --from-file
```

#### Managing Tickers

```bash
# Save a list of tickers to tickers.txt in the data directory
yfinance-scraper tickers --save AAPL MSFT GOOGL AMZN META

# Load and display tickers from tickers.txt
yfinance-scraper tickers --load

# Update the configuration with tickers from tickers.txt
yfinance-scraper tickers --update-config

# Using a custom data directory
yfinance-scraper tickers --save AAPL MSFT --data-dir /custom/data/path
```

#### Updating Existing Data

```bash
# Update all tickers in the configuration
yfinance-scraper update

# Update specific tickers
yfinance-scraper update --tickers AAPL MSFT

# Update using tickers from tickers.txt file
yfinance-scraper update --from-file
```

#### Configuration Management

```bash
# View current configuration
yfinance-scraper config --show

# Set configuration options
yfinance-scraper config --set tickers "[\"AAPL\", \"MSFT\", \"GOOGL\"]"
yfinance-scraper config --set data_dir "/custom/data/path"
yfinance-scraper config --set interval "1d"
yfinance-scraper config --set period "max"
```

### Using as a Python Package

You can also use the package programmatically in your Python code:

```python
from yfinance_scraper.fetcher import fetch_data_for_tickers
from yfinance_scraper.storage import save_data_for_tickers
from yfinance_scraper.updater import update_data_for_tickers
from yfinance_scraper.utils import save_tickers_to_file, load_tickers_from_file

# Fetch data
data = fetch_data_for_tickers(
    tickers=["AAPL", "MSFT", "GOOGL"],
    period="5y",
    interval="1d"
)

# Save data
save_data_for_tickers(data, data_dir="/data/yfinance")

# Update data
update_data_for_tickers(
    tickers=["AAPL", "MSFT", "GOOGL"],
    data_dir="/data/yfinance"
)

# Save tickers to file
save_tickers_to_file(["AAPL", "MSFT", "GOOGL"], "/data/yfinance")

# Load tickers from file
tickers = load_tickers_from_file("/data/yfinance")
```

## Setting Up a Daily Update

You can set up a cron job to automatically update the data daily:

```bash
# Open crontab editor
crontab -e

# Add a line to run the update at 5:00 PM every day
0 17 * * * /usr/local/bin/yfinance-scraper update --from-file
```

## Data Directory Structure

The data is stored in the following structure:

```
/data/yfinance/
├── tickers.txt           # List of tickers to fetch
├── AAPL/
│   ├── ohlcv.parquet
│   ├── dividends.parquet
│   ├── splits.parquet
│   ├── info.parquet
│   ├── financials.parquet
│   ├── balance_sheet.parquet
│   ├── cashflow.parquet
│   └── earnings.parquet
├── MSFT/
│   └── ...
└── ...
```

## Included Scripts

The package includes helpful scripts:

- `save_combined_tickers.py`: Saves a comprehensive list of tickers to the data directory

```bash
# Run the script to save a predefined list of tickers
python -m yfinance_scraper.scripts.save_combined_tickers
```

## License

This project is licensed under the MIT License - see the LICENSE file for details. 