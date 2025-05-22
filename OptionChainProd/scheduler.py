"""
Scheduler for regular data collection and dashboard updates.
"""
import time
import logging
import os
from datetime import datetime
import pandas as pd
from config.settings import DATA_COLLECTION, PATHS
from data_collection.collector import OptionChainCollector
from utils.helpers import setup_logging, is_trading_hours, time_until_next_collection

# Setup logging
logger = setup_logging()

def run_scheduler(symbol, expiry):
    """
    Run the data collection scheduler.
    
    Args:
        symbol (str): Symbol to collect data for (e.g., 'NIFTY')
        expiry (str): Expiry date in DD-MM-YYYY format
    """
    logger.info(f"Starting scheduler for {symbol} with expiry {expiry}")
    
    # Create collector
    collector = OptionChainCollector()
    
    # Ensure token is valid
    if not collector.refresh_token():
        logger.error("Failed to authenticate. Exiting scheduler.")
        return
    
    try:
        # Main scheduling loop
        while True:
            # Check if within trading hours
            if is_trading_hours():
                logger.info("Collecting data...")
                
                # Collect and save data
                data, filepath = collector.collect_and_save(symbol, expiry)
                
                if data is not None:
                    logger.info(f"Successfully collected data with {len(data)} rows")
                else:
                    logger.error("Failed to collect or save data")
            else:
                logger.info("Outside trading hours. Waiting for next trading day.")
                # Sleep until next check (check every hour outside trading hours)
                time.sleep(3600)  # 1 hour
                continue
            
            # Calculate time until next collection
            sleep_seconds = time_until_next_collection()
            next_collection_time = datetime.now() + pd.Timedelta(seconds=sleep_seconds)
            
            logger.info(f"Next collection at {next_collection_time.strftime('%H:%M:%S')}")
            
            # Sleep until next collection time
            time.sleep(sleep_seconds)
    
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")
    except Exception as e:
        logger.critical(f"Scheduler crashed: {str(e)}", exc_info=True)
        # In production, you might want to add notification here
        raise

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="NIFTY Options Data Collector")
    parser.add_argument("--symbol", default="NIFTY", help="Symbol to collect data for")
    parser.add_argument("--expiry", required=True, help="Expiry date (DD-MM-YYYY)")
    
    args = parser.parse_args()
    
    # Create base data directory
    os.makedirs(PATHS["data_folder"], exist_ok=True)
    
    # Run scheduler
    run_scheduler(args.symbol, args.expiry)