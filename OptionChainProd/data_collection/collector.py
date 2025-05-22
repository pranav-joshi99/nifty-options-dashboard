"""
Data collection module for option chain data.
"""
import os
import requests
import pandas as pd
import logging
from datetime import datetime
from io import StringIO
import time
from config.settings import API_ENDPOINTS, PATHS
from data_collection.auth import get_auth_token

logger = logging.getLogger(__name__)

class OptionChainCollector:
    """
    Collects option chain data from the TrueData API and saves it to Excel files.
    """
    
    def __init__(self):
        """Initialize the collector with default values."""
        self.token = None
        self.last_collection_time = None
    
    def refresh_token(self):
        """Refresh the authentication token."""
        self.token = get_auth_token()
        return self.token is not None
    
    def collect_data(self, symbol, expiry):
        """
        Collect option chain data for the specified symbol and expiry.
        
        Args:
            symbol (str): Symbol name (e.g., 'NIFTY')
            expiry (str): Expiry date in DD-MM-YYYY format
            
        Returns:
            pandas.DataFrame or None: Collected data or None if failed
        """
        if not self.token and not self.refresh_token():
            logger.error("Cannot collect data: No valid authentication token")
            return None
        
        logger.info(f"Collecting data for {symbol} with expiry {expiry}")
        
        # Prepare request
        headers = {"Authorization": f"Bearer {self.token}"}
        params = {
            "symbol": symbol,
            "expiry": expiry,
            "response": "csv"
        }
        
        try:
            # Make API request
            response = requests.get(
                API_ENDPOINTS["option_chain"], 
                headers=headers, 
                params=params
            )
            response.raise_for_status()
            
            # Parse CSV response
            data = pd.read_csv(StringIO(response.text))
            
            # Save timestamp of collection
            self.last_collection_time = datetime.now()
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response: {e.response.text}")
                
                # If unauthorized, try refreshing token once
                if e.response.status_code == 401:
                    logger.info("Trying to refresh token and retry")
                    if self.refresh_token():
                        # Recursive call with new token (just once)
                        return self.collect_data(symbol, expiry)
            return None
        
        except Exception as e:
            logger.error(f"Error processing option chain data: {str(e)}")
            return None
    
    def save_data(self, data, symbol, expiry):
        """
        Save the collected data to an Excel file.
        
        Args:
            data (pandas.DataFrame): Data to save
            symbol (str): Symbol name
            expiry (str): Expiry date
            
        Returns:
            str or None: Path to saved file or None if failed
        """
        if data is None or data.empty:
            logger.error("No data to save")
            return None
        
        try:
            # Create directory structure
            current_date = datetime.now().strftime(PATHS["date_format"])
            current_time = datetime.now().strftime(PATHS["time_format"])
            
            # data/DD-MM-YYYY/SYMBOL/SYMBOL_EXPIRY_TIME.xlsx
            dir_path = os.path.join(PATHS["data_folder"], current_date, symbol)
            os.makedirs(dir_path, exist_ok=True)
            
            # Format filename
            filename = f"{symbol}_{expiry}_{current_time}.xlsx"
            filepath = os.path.join(dir_path, filename)
            
            # Save to Excel
            data.to_excel(filepath, index=False)
            logger.info(f"Data saved to {filepath}")
            
            return filepath
            
        except Exception as e:
            logger.error(f"Error saving data: {str(e)}")
            return None

    def collect_and_save(self, symbol, expiry):
        """
        Collect and save option chain data.
        
        Args:
            symbol (str): Symbol name
            expiry (str): Expiry date
            
        Returns:
            tuple: (data DataFrame, file path) or (None, None) if failed
        """
        data = self.collect_data(symbol, expiry)
        if data is not None:
            filepath = self.save_data(data, symbol, expiry)
            return data, filepath
        return None, None