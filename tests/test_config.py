"""Tests for the configuration module."""

import os
import tempfile
import unittest
import json
from unittest.mock import patch

from yfinance_scraper.config import (
    load_config,
    save_config,
    update_config,
    ensure_data_dir,
    DEFAULT_CONFIG
)


class TestConfig(unittest.TestCase):
    """Test the configuration module."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = os.path.join(self.temp_dir.name, "test_config.json")
        self.data_dir = os.path.join(self.temp_dir.name, "data")
    
    def tearDown(self):
        """Tear down test fixtures."""
        self.temp_dir.cleanup()
    
    def test_ensure_data_dir(self):
        """Test that ensure_data_dir creates directory if it doesn't exist."""
        # Directory shouldn't exist yet
        self.assertFalse(os.path.exists(self.data_dir))
        
        # Function should create it
        result = ensure_data_dir(self.data_dir)
        
        # Directory should now exist
        self.assertTrue(os.path.exists(self.data_dir))
        self.assertEqual(result, self.data_dir)
        
        # Test with existing directory
        result = ensure_data_dir(self.data_dir)
        self.assertEqual(result, self.data_dir)
    
    def test_save_config(self):
        """Test saving configuration to a file."""
        config = DEFAULT_CONFIG.copy()
        config["test_key"] = "test_value"
        
        # Save config
        save_config(config, self.config_path)
        
        # Check if file exists
        self.assertTrue(os.path.exists(self.config_path))
        
        # Check file contents
        with open(self.config_path, "r") as f:
            saved_config = json.load(f)
        
        self.assertEqual(saved_config, config)
    
    def test_load_config_existing(self):
        """Test loading configuration from an existing file."""
        config = DEFAULT_CONFIG.copy()
        config["test_key"] = "test_value"
        
        # Save config
        with open(self.config_path, "w") as f:
            json.dump(config, f)
        
        # Load config
        loaded_config = load_config(self.config_path)
        
        # Check loaded config
        self.assertEqual(loaded_config, config)
    
    def test_load_config_nonexistent(self):
        """Test loading configuration from a non-existent file."""
        # Load config from a file that doesn't exist
        with patch("yfinance_scraper.config.save_config") as mock_save:
            loaded_config = load_config(self.config_path)
            
            # Check that default config was returned
            self.assertEqual(loaded_config, DEFAULT_CONFIG)
            
            # Check that save_config was called to create the file
            mock_save.assert_called_once_with(DEFAULT_CONFIG, self.config_path)
    
    def test_update_config(self):
        """Test updating configuration."""
        # Save initial config
        initial_config = DEFAULT_CONFIG.copy()
        with open(self.config_path, "w") as f:
            json.dump(initial_config, f)
        
        # Update config
        updates = {
            "tickers": ["AAPL", "MSFT"],
            "period": "1y"
        }
        
        updated_config = update_config(updates, self.config_path)
        
        # Check updated config
        expected_config = initial_config.copy()
        expected_config.update(updates)
        self.assertEqual(updated_config, expected_config)
        
        # Check saved file
        with open(self.config_path, "r") as f:
            saved_config = json.load(f)
        
        self.assertEqual(saved_config, expected_config)


if __name__ == "__main__":
    unittest.main() 