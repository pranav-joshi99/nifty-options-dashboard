"""
Calculation module for NIFTY Options Dashboard.
"""
import pandas as pd
import logging
from datetime import datetime, timedelta
from database.db_manager import DatabaseManager
from config.settings import DATA_COLLECTION

logger = logging.getLogger(__name__)

class OptionMetricsCalculator:
    """
    Calculator for option metrics and OI changes.
    """
    
    def __init__(self):
        """Initialize the calculator."""
        self.db = DatabaseManager()
    
    def calculate_oi_changes(self, symbol, expiry, current_data=None):
        """
        Calculate OI changes for all configured intervals.
        
        Args:
            symbol (str): Symbol name
            expiry (str): Expiry date
            current_data (pandas.DataFrame, optional): Current option data.
                If None, fetch latest data from database.
                
        Returns:
            pandas.DataFrame: Calculated OI changes
        """
        # Get current data if not provided
        if current_data is None:
            current_data = self.db.get_latest_option_data(symbol, expiry)
            
        if current_data.empty:
            logger.error("No current data available for OI change calculation")
            return pd.DataFrame()
        
        # Get current timestamp
        current_timestamp = pd.to_datetime(current_data['timestamp'].iloc[0])
        
        # Get all available timestamps
        all_timestamps = self.db.get_timestamps(symbol, expiry)
        
        if not all_timestamps:
            logger.error("No timestamps available for OI change calculation")
            return pd.DataFrame()
        
        # Convert string timestamps to datetime objects
        all_timestamps = [pd.to_datetime(ts) for ts in all_timestamps]
        
        # Initialize results dataframe
        results = []
        
        # Calculate OI changes for each interval
        for interval_minutes in DATA_COLLECTION["analysis_intervals"]:
            # Calculate target timestamp (approximately)
            target_timestamp = current_timestamp - timedelta(minutes=interval_minutes)
            
            # Find closest timestamp
            closest_timestamp = min(
                [ts for ts in all_timestamps if ts < current_timestamp],
                key=lambda x: abs((x - target_timestamp).total_seconds()),
                default=None
            )
            
            if closest_timestamp is None:
                logger.warning(f"No data found for {interval_minutes} minute interval")
                continue
            
            # Get data for the closest timestamp
            past_data = self.db.get_option_data_by_timestamp(
                symbol, 
                expiry, 
                closest_timestamp
            )
            
            if past_data.empty:
                logger.warning(f"Empty data for timestamp {closest_timestamp}")
                continue
            
            # Calculate actual interval in minutes
            actual_interval = round((current_timestamp - closest_timestamp).total_seconds() / 60)
            
            logger.info(f"Calculating changes for requested {interval_minutes}min interval "
                       f"(actual: {actual_interval}min, {closest_timestamp})")
            
            # Merge current and past data
            merged = pd.merge(
                current_data, 
                past_data,
                on='strike', 
                suffixes=('_current', '_past')
            )
            
            # Calculate OI changes
            for _, row in merged.iterrows():
                # Calculate CE OI change
                try:
                    ce_oi_change = row['call_oi_current'] - row['call_oi_past']
                    if pd.isna(ce_oi_change):
                        ce_oi_change = 0
                except (KeyError, TypeError):
                    ce_oi_change = 0
                
                # Calculate PE OI change
                try:
                    pe_oi_change = row['put_oi_current'] - row['put_oi_past']
                    if pd.isna(pe_oi_change):
                        pe_oi_change = 0
                except (KeyError, TypeError):
                    pe_oi_change = 0
                
                # Add to results
                results.append({
                    'timestamp': current_timestamp,
                    'symbol': symbol,
                    'expiry': expiry,
                    'strike': row['strike'],
                    'interval': actual_interval,
                    'ce_oi_change': ce_oi_change,
                    'pe_oi_change': pe_oi_change
                })
        
        # Convert to DataFrame
        return pd.DataFrame(results)
        
    def process_latest_data(self, symbol, expiry, currently_trading, range_limit, highlight_limit):
        """
        Process the latest data and prepare for dashboard.
        
        Args:
            symbol (str): Symbol name
            expiry (str): Expiry date
            currently_trading (float): Current trading value
            range_limit (float): Range limit for filtering
            highlight_limit (float): Highlight limit
            
        Returns:
            dict: Processed data for dashboard
        """
        # Get latest data
        latest_data = self.db.get_latest_option_data(symbol, expiry)
        
        if latest_data.empty:
            logger.error("No data available for processing")
            return {
                'success': False,
                'message': 'No data available',
                'data': None
            }
        
        # Calculate OI changes
        oi_changes = self.calculate_oi_changes(symbol, expiry, latest_data)
        
        # Save OI changes to database
        if not oi_changes.empty:
            self.db.save_oi_changes(oi_changes)
        
        # Filter strikes by range
        filtered_data = self.filter_strikes_by_range(
            latest_data, 
            currently_trading, 
            range_limit
        )
        
        filtered_changes = self.filter_strikes_by_range(
            oi_changes, 
            currently_trading, 
            range_limit
        )
        
        if filtered_data.empty:
            logger.warning(f"No data in range {currently_trading}±{range_limit}")
            return {
                'success': False,
                'message': f'No data in range {currently_trading}±{range_limit}',
                'data': None
            }
        
        # Calculate highlight range
        min_strike = filtered_data['strike'].min()
        max_strike = filtered_data['strike'].max()
        
        highlight_min, highlight_max = self.get_highlight_range(
            min_strike, 
            max_strike, 
            highlight_limit
        )
        
        # Save user settings
        self.db.save_user_settings({
            'symbol': symbol,
            'expiry': expiry,
            'currently_trading': currently_trading,
            'range_limit': range_limit,
            'highlight_limit': highlight_limit
        })
        
        # Prepare data for dashboard
        dashboard_data = self._prepare_dashboard_data(
            filtered_data,
            filtered_changes,
            highlight_min,
            highlight_max
        )
        
        return {
            'success': True,
            'message': 'Data processed successfully',
            'timestamp': latest_data['timestamp'].iloc[0] if 'timestamp' in latest_data.columns else datetime.now(),
            'data': dashboard_data
        }
        
    def filter_strikes_by_range(self, df, currently_trading, range_limit):
        """
        Filter strikes based on the currently trading value and range limit.
        
        Args:
            df (pandas.DataFrame): DataFrame with strike column
            currently_trading (float): Current trading value
            range_limit (float): Range limit for filtering
            
        Returns:
            pandas.DataFrame: Filtered DataFrame
        """
        if df.empty:
            return df
            
        # Calculate range
        range_min = currently_trading - range_limit
        range_max = currently_trading + range_limit
        
        # Filter dataframe
        return df[(df['strike'] >= range_min) & (df['strike'] <= range_max)]
    
    def get_highlight_range(self, min_strike, max_strike, highlight_limit):
        """
        Calculate the range to highlight in the dashboard.
        
        Args:
            min_strike (float): Minimum strike in the filtered range
            max_strike (float): Maximum strike in the filtered range
            highlight_limit (float): Highlight limit
            
        Returns:
            tuple: (highlight_min, highlight_max)
        """
        # Calculate midpoint
        midpoint = (min_strike + max_strike) / 2
        
        # Calculate highlight range
        highlight_min = midpoint - highlight_limit
        highlight_max = midpoint + highlight_limit
        
        return highlight_min, highlight_max
    
    def _prepare_dashboard_data(self, option_data, oi_changes, highlight_min, highlight_max):
        """
        Prepare data for dashboard display.
        
        Args:
            option_data (pandas.DataFrame): Option data
            oi_changes (pandas.DataFrame): OI changes
            highlight_min (float): Lower bound for highlighting
            highlight_max (float): Upper bound for highlighting
            
        Returns:
            dict: Dashboard data
        """
        # Start with strikes from option_data
        dashboard_df = option_data[['strike']].copy()
        
        # Add highlight flag
        dashboard_df['highlight'] = (
            (dashboard_df['strike'] >= highlight_min) & 
            (dashboard_df['strike'] <= highlight_max)
        )
        
        # Initialize OI change columns with N/A (for when market is closed or insufficient data)
        for interval in [5, 10, 15]:
            dashboard_df[f'ce_{interval}min'] = "N/A"
            dashboard_df[f'pe_{interval}min'] = "N/A"
        
        # If we have OI changes data, populate the columns
        if not oi_changes.empty:
            # Handle duplicate strike/interval combinations by aggregating first
            ce_pivot_df = oi_changes.groupby(['strike', 'interval'])['ce_oi_change'].mean().reset_index()
            pe_pivot_df = oi_changes.groupby(['strike', 'interval'])['pe_oi_change'].mean().reset_index()
            
            # Populate CE columns
            for _, row in ce_pivot_df.iterrows():
                strike = row['strike']
                interval = int(row['interval'])
                change = int(row['ce_oi_change'])
                
                # Only update if this interval is one we're tracking (5, 10, 15)
                if interval in [5, 10, 15]:
                    mask = dashboard_df['strike'] == strike
                    if mask.any():
                        dashboard_df.loc[mask, f'ce_{interval}min'] = change
            
            # Populate PE columns
            for _, row in pe_pivot_df.iterrows():
                strike = row['strike']
                interval = int(row['interval'])
                change = int(row['pe_oi_change'])
                
                # Only update if this interval is one we're tracking (5, 10, 15)
                if interval in [5, 10, 15]:
                    mask = dashboard_df['strike'] == strike
                    if mask.any():
                        dashboard_df.loc[mask, f'pe_{interval}min'] = change
        
        # Format for dashboard
        return dashboard_df.to_dict(orient='records')