"""
Main entry point for the NIFTY Options Dashboard application.
"""
import os
import logging
import argparse
import time
from threading import Thread
from datetime import datetime

import streamlit.web.bootstrap as bootstrap
from streamlit.config import get_config_options

# Import our modules
from data_collection.connector import DataCollectionConnector
from database.db_manager import DatabaseManager
from utils.helpers import setup_logging, is_trading_hours
from config.settings import PATHS, DATA_COLLECTION

# Set up logging
logger = setup_logging()

def run_data_collection(symbol, expiry, interval_minutes=None, end_time=None):
    """
    Run data collection process.
    
    Args:
        symbol (str): Symbol to collect data for
        expiry (str): Expiry date
        interval_minutes (int, optional): Collection interval in minutes
        end_time (str, optional): End time in HH:MM format
    """
    logger.info(f"Starting data collection for {symbol} with expiry {expiry}")
    
    # Use default interval if not provided
    if interval_minutes is None:
        interval_minutes = DATA_COLLECTION["interval_minutes"]
    
    # Create connector
    connector = DataCollectionConnector()
    
    # Create data directory
    os.makedirs(PATHS["data_folder"], exist_ok=True)
    
    try:
        while True:
            # Check if within trading hours
            if is_trading_hours():
                logger.info("Collecting data...")
                
                # Collect and store data
                data, filepath, success = connector.collect_and_store(symbol, expiry)
                
                if success and data is not None:
                    logger.info(f"Successfully collected data with {len(data)} rows")
                else:
                    logger.error("Failed to collect or save data")
            else:
                logger.info("Outside trading hours. Waiting for next check...")
                # Sleep for a longer period outside trading hours
                time.sleep(3600)  # 1 hour
                continue
            
            # Check if we should stop
            if end_time:
                now = datetime.now()
                end_hour, end_minute = map(int, end_time.split(':'))
                
                if now.hour > end_hour or (now.hour == end_hour and now.minute >= end_minute):
                    logger.info(f"Reached end time {end_time}. Stopping collection.")
                    break
            
            # Sleep until next collection
            logger.info(f"Sleeping for {interval_minutes} minutes until next collection...")
            time.sleep(interval_minutes * 60)
    
    except KeyboardInterrupt:
        logger.info("Data collection stopped by user")
    except Exception as e:
        logger.critical(f"Data collection crashed: {str(e)}", exc_info=True)
        # In production, you might want to add notification here
        raise

def run_dashboard():
    """Run the Streamlit dashboard."""
    logger.info("Starting dashboard...")
    
    # Get the directory of this file
    file_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Path to the dashboard file
    dashboard_path = os.path.join(file_dir, "ui", "dashboard.py")
    
    # Run the dashboard
    bootstrap.run(dashboard_path, "", [], flag_options={})

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="NIFTY Options Dashboard")
    
    parser.add_argument(
        "--mode",
        choices=["dashboard", "collection", "both"],
        default="both",
        help="Run mode (dashboard, collection, or both)"
    )
    
    parser.add_argument(
        "--symbol",
        default="NIFTY",
        help="Symbol to collect data for"
    )
    
    parser.add_argument(
        "--expiry",
        help="Expiry date (DD-MM-YYYY)"
    )
    
    parser.add_argument(
        "--interval",
        type=int,
        help=f"Collection interval in minutes (default: {DATA_COLLECTION['interval_minutes']})"
    )
    
    parser.add_argument(
        "--end-time",
        help="End time for data collection (HH:MM)"
    )
    
    args = parser.parse_args()
    
    # Run in the specified mode
    if args.mode in ["collection", "both"]:
        if not args.expiry:
            parser.error("--expiry is required for data collection")
        
        if args.mode == "both":
            # Start data collection in a separate thread
            collection_thread = Thread(
                target=run_data_collection,
                args=(args.symbol, args.expiry, args.interval, args.end_time),
                daemon=True
            )
            collection_thread.start()
            
            # Run dashboard in main thread
            run_dashboard()
        else:
            # Run data collection in main thread
            run_data_collection(args.symbol, args.expiry, args.interval, args.end_time)
    
    elif args.mode == "dashboard":
        # Run dashboard only
        run_dashboard()

if __name__ == "__main__":
    main()