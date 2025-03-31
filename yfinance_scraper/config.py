"""Configuration module for YFinance Scraper."""

import os
import json
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

DEFAULT_DATA_DIR = os.path.join(str(Path.home()), "data/yfinance")
DEFAULT_CONFIG_PATH = os.path.join(str(Path.home()), ".yfinance_scraper_config.json")
DEFAULT_TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]
DEFAULT_CONFIG = {
    "data_dir": DEFAULT_DATA_DIR,
    "tickers": DEFAULT_TICKERS,
    "period": "max",  # Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
    "interval": "1d",  # Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
    "log_level": "INFO"
}


def ensure_data_dir(data_dir: str) -> str:
    """Ensure the data directory exists.
    
    Args:
        data_dir: Path to the data directory
        
    Returns:
        The path to the data directory
    """
    os.makedirs(data_dir, exist_ok=True)
    logger.info(f"Data directory ensured: {data_dir}")
    return data_dir


def load_config(config_path: str = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    """Load configuration from file.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Configuration dictionary
    """
    config = DEFAULT_CONFIG.copy()
    
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                user_config = json.load(f)
                config.update(user_config)
                logger.info(f"Loaded configuration from {config_path}")
        except Exception as e:
            logger.error(f"Error loading config from {config_path}: {e}")
    else:
        logger.info(f"No config file found at {config_path}, using defaults")
        save_config(config, config_path)
    
    # Always ensure the data directory exists
    ensure_data_dir(config['data_dir'])
    
    return config


def save_config(config: Dict[str, Any], config_path: str = DEFAULT_CONFIG_PATH) -> None:
    """Save configuration to file.
    
    Args:
        config: Configuration dictionary
        config_path: Path to save the configuration file
    """
    try:
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=4)
        logger.info(f"Saved configuration to {config_path}")
    except Exception as e:
        logger.error(f"Error saving config to {config_path}: {e}")


def update_config(updates: Dict[str, Any], config_path: str = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    """Update configuration with new values and save.
    
    Args:
        updates: Dictionary of configuration updates
        config_path: Path to the configuration file
        
    Returns:
        Updated configuration dictionary
    """
    config = load_config(config_path)
    config.update(updates)
    save_config(config, config_path)
    return config 