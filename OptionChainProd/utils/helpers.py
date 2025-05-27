"""
Helper utilities for the NIFTY Options Dashboard.
"""
import logging
import os
from datetime import datetime, time
from config.settings import LOGGING, DATA_COLLECTION
import pytz

def setup_logging():
    """Configure the logging system."""
    log_folder = "logs"
    os.makedirs(log_folder, exist_ok=True)
    
    log_file = os.path.join(log_folder, LOGGING["filename"])
    
    logging.basicConfig(
        filename=log_file,
        level=getattr(logging, LOGGING["level"]),
        format=LOGGING["format"]
    )
    
    # Also log to console
    console = logging.StreamHandler()
    console.setLevel(getattr(logging, LOGGING["level"]))
    console.setFormatter(logging.Formatter(LOGGING["format"]))
    logging.getLogger('').addHandler(console)
    
    return logging.getLogger(__name__)

def is_trading_hours():
    """
    Check if current time is within trading hours.
    
    Returns:
        bool: True if current time is within trading hours, False otherwise
    """
    # Get current time
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist).time()
    
    # Parse trading hours from settings
    start_hour, start_minute = map(int, DATA_COLLECTION["trading_hours"]["start"].split(':'))
    end_hour, end_minute = map(int, DATA_COLLECTION["trading_hours"]["end"].split(':'))
    
    trading_start = time(start_hour, start_minute)
    trading_end = time(end_hour, end_minute)
    
    # Check if current time is within trading hours
    return trading_start <= now <= trading_end

def time_until_next_collection(interval_minutes=None):
    """
    Calculate seconds until next data collection.
    
    Args:
        interval_minutes (int, optional): Collection interval in minutes.
            If None, use the value from settings.
    
    Returns:
        int: Seconds until next collection
    """
    if interval_minutes is None:
        interval_minutes = DATA_COLLECTION["interval_minutes"]
        
    now = datetime.now()
    
    # Calculate next collection time
    minutes_to_add = interval_minutes - (now.minute % interval_minutes)
    if minutes_to_add == interval_minutes and now.second == 0 and now.microsecond == 0:
        minutes_to_add = 0
        
    next_collection = now.replace(
        second=0, 
        microsecond=0
    ) + pd.Timedelta(minutes=minutes_to_add)
    
    # Return seconds until next collection
    return (next_collection - now).total_seconds()
