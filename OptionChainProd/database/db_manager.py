"""
Database management module for NIFTY Options Dashboard.
"""
import os
import sqlite3
import logging
import pandas as pd
from datetime import datetime
import json
from config.settings import DATABASE

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Manages SQLite database operations for the NIFTY Options Dashboard.
    """
    
    def __init__(self, db_file=None):
        """
        Initialize database manager.
        
        Args:
            db_file (str, optional): Path to database file.
                If None, use the filename from settings.
        """
        if db_file is None:
            db_file = DATABASE["filename"]
            
        self.db_file = db_file
        self._ensure_db_exists()
    
    def _ensure_db_exists(self):
        """Ensure database file and tables exist."""
        conn = None
        try:
            # Create database directory if needed
            os.makedirs(os.path.dirname(os.path.abspath(self.db_file)), exist_ok=True)
            
            # Connect to database
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            # Create tables if they don't exist
            
            # Options data table - store raw metrics
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS option_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                symbol TEXT NOT NULL,
                expiry TEXT NOT NULL,
                strike REAL NOT NULL,
                call_oi INTEGER,
                call_prev_oi INTEGER,
                put_oi INTEGER,
                put_prev_oi INTEGER,
                UNIQUE(timestamp, symbol, expiry, strike)
            )
            ''')
            
            # Calculated metrics table - store derived values
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS oi_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                symbol TEXT NOT NULL,
                expiry TEXT NOT NULL,
                strike REAL NOT NULL,
                interval INTEGER NOT NULL,  -- in minutes (5, 10, 15, etc.)
                ce_oi_change INTEGER,
                pe_oi_change INTEGER,
                UNIQUE(timestamp, symbol, expiry, strike, interval)
            )
            ''')
            
            # User settings table - store dashboard configuration
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_settings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                settings TEXT NOT NULL  -- JSON string of settings
            )
            ''')
            
            conn.commit()
            logger.info("Database initialized successfully")
            
        except sqlite3.Error as e:
            logger.error(f"Database initialization error: {str(e)}")
            raise
        
        finally:
            if conn:
                conn.close()
    
    def _get_connection(self):
        """Get database connection."""
        try:
            conn = sqlite3.connect(self.db_file)
            conn.row_factory = sqlite3.Row  # Enable row access by name
            return conn
        except sqlite3.Error as e:
            logger.error(f"Database connection error: {str(e)}")
            raise
    
    def save_option_data(self, data, symbol, expiry, timestamp=None):
        """
        Save option data to the database.
        
        Args:
            data (pandas.DataFrame): Option chain data
            symbol (str): Symbol name
            expiry (str): Expiry date
            timestamp (datetime, optional): Timestamp for the data.
                If None, use current time.
                
        Returns:
            bool: True if successful, False otherwise
        """
        if timestamp is None:
            timestamp = datetime.now()
            
        if data is None or data.empty:
            logger.error("No data to save to database")
            return False
            
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Convert timestamp to string format for SQLite
            ts_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            
            # Filter only needed columns
            if 'strike' in data.columns and 'callOI' in data.columns and 'putOI' in data.columns:
                # Extract needed data
                for _, row in data.iterrows():
                    # Insert data into option_data table
                    cursor.execute('''
                    INSERT OR REPLACE INTO option_data 
                    (timestamp, symbol, expiry, strike, call_oi, call_prev_oi, put_oi, put_prev_oi)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        ts_str, 
                        symbol,
                        expiry,
                        float(row['strike']),
                        int(row['callOI']) if 'callOI' in row and pd.notna(row['callOI']) else None,
                        int(row['callpOI']) if 'callpOI' in row and pd.notna(row['callpOI']) else None,
                        int(row['putOI']) if 'putOI' in row and pd.notna(row['putOI']) else None,
                        int(row['putPOI']) if 'putPOI' in row and pd.notna(row['putPOI']) else None
                    ))
                
                conn.commit()
                logger.info(f"Saved {len(data)} records to database")
                return True
            else:
                logger.error("Required columns not found in data")
                return False
                
        except sqlite3.Error as e:
            logger.error(f"Error saving option data to database: {str(e)}")
            if conn:
                conn.rollback()
            return False
            
        finally:
            if conn:
                conn.close()
    
    def save_oi_changes(self, changes_df):
        """
        Save calculated OI changes to the database.
    
        Args:
            changes_df (pandas.DataFrame): DataFrame with OI changes.
            Must have columns: timestamp, symbol, expiry, strike, 
            interval, ce_oi_change, pe_oi_change
            
        Returns:
        bool: True if successful, False otherwise
        """
        if changes_df is None or changes_df.empty:
            logger.error("No OI changes to save to database")
            return False
        
        # Fill NaN values with 0 before saving
        changes_df = changes_df.fillna(0)
        
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
        
            # Insert each row
            for _, row in changes_df.iterrows():
                cursor.execute('''
                INSERT OR REPLACE INTO oi_changes
                (timestamp, symbol, expiry, strike, interval, ce_oi_change, pe_oi_change)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    row['timestamp'].strftime('%Y-%m-%d %H:%M:%S') if isinstance(row['timestamp'], datetime) else row['timestamp'],
                    row['symbol'],
                    row['expiry'],
                    float(row['strike']),
                    int(row['interval']),
                    int(row['ce_oi_change']),
                    int(row['pe_oi_change'])
                ))
        
            conn.commit()
            logger.info(f"Saved {len(changes_df)} OI changes to database")
            return True
        
        except sqlite3.Error as e:
            logger.error(f"Error saving OI changes to database: {str(e)}")
            if conn:
                conn.rollback()
            return False
        
        finally:
            if conn:
                conn.close()
    
    def get_latest_option_data(self, symbol, expiry):
        """
        Get the latest option data for a symbol and expiry.
        
        Args:
            symbol (str): Symbol name
            expiry (str): Expiry date
            
        Returns:
            pandas.DataFrame: Latest option data
        """
        conn = None
        try:
            conn = self._get_connection()
            
            # Get the latest timestamp first
            latest_ts_query = '''
            SELECT MAX(timestamp) as max_ts FROM option_data 
            WHERE symbol = ? AND expiry = ?
            '''
            
            cursor = conn.cursor()
            cursor.execute(latest_ts_query, (symbol, expiry))
            latest_ts = cursor.fetchone()['max_ts']
            
            if not latest_ts:
                logger.warning(f"No data found for {symbol} {expiry}")
                return pd.DataFrame()
            
            # Now get data for that timestamp
            data_query = '''
            SELECT timestamp, symbol, expiry, strike, call_oi, call_prev_oi, put_oi, put_prev_oi
            FROM option_data
            WHERE symbol = ? AND expiry = ? AND timestamp = ?
            ORDER BY strike
            '''
            
            return pd.read_sql_query(
                data_query, 
                conn, 
                params=(symbol, expiry, latest_ts)
            )
            
        except sqlite3.Error as e:
            logger.error(f"Error getting latest option data: {str(e)}")
            return pd.DataFrame()
            
        finally:
            if conn:
                conn.close()
    
    def get_option_data_by_timestamp(self, symbol, expiry, timestamp):
        """
        Get option data for a specific timestamp.
        
        Args:
            symbol (str): Symbol name
            expiry (str): Expiry date
            timestamp (datetime or str): Timestamp to fetch
            
        Returns:
            pandas.DataFrame: Option data for the timestamp
        """
        conn = None
        try:
            conn = self._get_connection()
            
            # Format timestamp if it's a datetime object
            if isinstance(timestamp, datetime):
                timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            
            # Query data
            query = '''
            SELECT timestamp, symbol, expiry, strike, call_oi, call_prev_oi, put_oi, put_prev_oi
            FROM option_data
            WHERE symbol = ? AND expiry = ? AND timestamp = ?
            ORDER BY strike
            '''
            
            return pd.read_sql_query(
                query, 
                conn, 
                params=(symbol, expiry, timestamp)
            )
            
        except sqlite3.Error as e:
            logger.error(f"Error getting option data by timestamp: {str(e)}")
            return pd.DataFrame()
            
        finally:
            if conn:
                conn.close()
    
    def get_timestamps(self, symbol, expiry, limit=None):
        """
        Get available timestamps for a symbol and expiry.
        
        Args:
            symbol (str): Symbol name
            expiry (str): Expiry date
            limit (int, optional): Limit number of timestamps.
                If None, return all timestamps.
                
        Returns:
            list: List of timestamp strings
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            query = '''
            SELECT DISTINCT timestamp 
            FROM option_data
            WHERE symbol = ? AND expiry = ?
            ORDER BY timestamp DESC
            '''
            
            if limit:
                query += f" LIMIT {int(limit)}"
            
            cursor.execute(query, (symbol, expiry))
            
            # Extract timestamps from results
            return [row['timestamp'] for row in cursor.fetchall()]
            
        except sqlite3.Error as e:
            logger.error(f"Error getting timestamps: {str(e)}")
            return []
            
        finally:
            if conn:
                conn.close()
    
    def save_user_settings(self, settings_dict):
        """
        Save user settings to the database.
        
        Args:
            settings_dict (dict): Dictionary of user settings
            
        Returns:
            bool: True if successful, False otherwise
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Convert settings to JSON string
            settings_json = json.dumps(settings_dict)
            
            # Insert into database
            cursor.execute('''
            INSERT INTO user_settings (timestamp, settings)
            VALUES (?, ?)
            ''', (
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                settings_json
            ))
            
            conn.commit()
            logger.info("User settings saved to database")
            return True
            
        except sqlite3.Error as e:
            logger.error(f"Error saving user settings: {str(e)}")
            if conn:
                conn.rollback()
            return False
            
        finally:
            if conn:
                conn.close()
    
    def get_latest_user_settings(self):
        """
        Get the latest user settings from the database.
        
        Returns:
            dict: Latest user settings or empty dict if not found
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Get latest settings
            cursor.execute('''
            SELECT settings FROM user_settings
            ORDER BY timestamp DESC
            LIMIT 1
            ''')
            
            result = cursor.fetchone()
            
            if result:
                return json.loads(result['settings'])
            else:
                logger.warning("No user settings found")
                return {}
                
        except sqlite3.Error as e:
            logger.error(f"Error getting user settings: {str(e)}")
            return {}
            
        finally:
            if conn:
                conn.close()
    
    def backup_database(self, backup_dir="backups"):
        """
        Create a backup of the database.
        
        Args:
            backup_dir (str): Directory to store backups
            
        Returns:
            str or None: Path to backup file or None if failed
        """
        try:
            # Create backup directory
            os.makedirs(backup_dir, exist_ok=True)
            
            # Create backup filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = os.path.join(
                backup_dir,
                f"{os.path.splitext(os.path.basename(self.db_file))[0]}_{timestamp}.db"
            )
            
            # Connect to source database
            source_conn = sqlite3.connect(self.db_file)
            
            # Create backup
            with sqlite3.connect(backup_file) as backup_conn:
                source_conn.backup(backup_conn)
            
            source_conn.close()
            
            logger.info(f"Database backed up to {backup_file}")
            return backup_file
            
        except sqlite3.Error as e:
            logger.error(f"Database backup error: {str(e)}")
            return None