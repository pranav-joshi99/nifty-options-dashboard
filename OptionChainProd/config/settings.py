"""
Configuration settings for the NIFTY Options Dashboard application.
"""

# API credentials
API_CREDENTIALS = {
    "username": "tdwsp681",
    "password": "niraj@681",
}

# API endpoints
API_ENDPOINTS = {
    "auth": "https://auth.truedata.in/token",
    "option_chain": "https://greeks.truedata.in/api/getOptionChainwithGreeks"
}

# Data collection settings
DATA_COLLECTION = {
    "interval_minutes": 5,  # Data collection interval in minutes
    "trading_hours": {
        "start": "09:15",  # Trading day start time (24h format)
        "end": "15:30"      # Trading day end time (24h format)
    },
    "analysis_intervals": [5, 10, 15, 30]  # Time intervals for OI change analysis (in minutes)
}

# File paths and naming
PATHS = {
    "data_folder": "data",
    "date_format": "%d-%m-%Y",
    "time_format": "%H%M"
}

# Database settings
DATABASE = {
    "filename": "option_metrics.db",
    "backup_interval_hours": 24  # How often to backup the database
}

# Logging settings
LOGGING = {
    "filename": "app.log",
    "level": "INFO",  # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
}
