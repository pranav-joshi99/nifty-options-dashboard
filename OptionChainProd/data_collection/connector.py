"""
Connector for linking data collection with database storage.
"""
import os
import pandas as pd
import logging
from datetime import datetime
from data_collection.collector import OptionChainCollector
from database.db_manager import DatabaseManager

logger = logging.getLogger(__name__)

class DataCollectionConnector:
    """
    Connects the data collection with database storage.
    """
    
    def __init__(self):
        """Initialize the connector."""
        self.collector = OptionChainCollector()
        self.db = DatabaseManager()
    
    def collect_and_store(self, symbol, expiry):
        """
        Collect data and store in both file system and database.
        
        Args:
            symbol (str): Symbol name
            expiry (str): Expiry date
            
        Returns:
            tuple: (data, filepath, success)
        """
        # Refresh token if needed
        if not self.collector.token:
            success = self.collector.refresh_token()
            if not success:
                logger.error("Failed to refresh authentication token")
                return None, None, False
        
        # Collect data
        data, filepath = self.collector.collect_and_save(symbol, expiry)
        
        if data is None:
            return None, None, False
        
        # Store in database
        timestamp = datetime.now()
        success = self.db.save_option_data(data, symbol, expiry, timestamp)
        
        return data, filepath, success
    
    def process_existing_files(self, data_dir, symbol, expiry):
        """
        Process existing data files and store in database.
        
        Args:
            data_dir (str): Base directory for data files
            symbol (str): Symbol name
            expiry (str): Expiry date
            
        Returns:
            int: Number of files processed
        """
        # Get current date
        current_date = datetime.now().strftime('%d-%m-%Y')
        
        # Construct symbol directory
        symbol_dir = os.path.join(data_dir, current_date, symbol)
        
        if not os.path.exists(symbol_dir):
            logger.warning(f"Directory not found: {symbol_dir}")
            return 0
        
        # Get all Excel files
        files = [f for f in os.listdir(symbol_dir) if f.endswith('.xlsx')]
        
        if not files:
            logger.warning(f"No Excel files found in {symbol_dir}")
            return 0
        
        # Process each file
        processed_count = 0
        
        for file in files:
            filepath = os.path.join(symbol_dir, file)
            
            try:
                # Extract timestamp from filename
                # Format: SYMBOL_EXPIRY_TIME.xlsx
                time_str = file.split('_')[-1].split('.')[0]
                
                # Convert to datetime
                file_time = datetime.strptime(time_str, '%H%M')
                
                # Add current date
                timestamp = datetime.combine(
                    datetime.now().date(),
                    file_time.time()
                )
                
                # Read data
                data = pd.read_excel(filepath)
                
                # Store in database
                success = self.db.save_option_data(data, symbol, expiry, timestamp)
                
                if success:
                    processed_count += 1
                    logger.info(f"Processed file: {file}")
                
            except Exception as e:
                logger.error(f"Error processing file {file}: {str(e)}")
        
        return processed_count