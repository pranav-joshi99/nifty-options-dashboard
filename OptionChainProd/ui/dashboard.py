"""
Streamlit dashboard UI for NIFTY Options Dashboard.
"""
import streamlit as st
import pandas as pd
import numpy as np
import time
import logging
from datetime import datetime, timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import threading
import os
import pytz

# Import our modules
from database.db_manager import DatabaseManager
from processing.calculator import OptionMetricsCalculator
from data_collection.connector import DataCollectionConnector
from config.settings import DATA_COLLECTION, PATHS
from utils.helpers import is_trading_hours, time_until_next_collection

# Set up logging
logger = logging.getLogger(__name__)

# Initialize components
db = DatabaseManager()
calculator = OptionMetricsCalculator()
connector = DataCollectionConnector()

# Configure Streamlit page
st.set_page_config(
    page_title="NIFTY Options Analysis",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Apply dark theme and custom styling
st.markdown("""
<style>
    /* Dark theme for the whole app */
    .stApp {
        background-color: #0E1117;
        color: #FAFAFA;
    }
    
    /* Headers */
    h1, h2, h3, h4, h5 {
        color: #00CED1 !important;
        font-weight: 600 !important;
    }
    
    /* Status indicators */
    .positive {
        color: #00FF7F !important;
        font-weight: bold !important;
    }
    
    .negative {
        color: #FF4500 !important;
        font-weight: bold !important;
    }
    
    .neutral {
        color: #CCCCCC !important;
    }
    
    /* Data table styling */
    .dataframe {
        background-color: #1A1E24 !important;
        border: 1px solid #333 !important;
    }
    
    /* Sidebar styling */
    .css-1d391kg {
        background-color: #161A21 !important;
    }
    
    /* Input fields */
    .stTextInput, .stNumberInput, .stSelectbox {
        background-color: #1A1E24 !important;
        color: #FAFAFA !important;
    }
    
    /* Highlight row in table */
    .highlight {
         border-left: 4px solid #FFD700 !important;
         background-color: transparent !important;
    }
    
    /* Dashboard tiles */
    .metric-card {
        background-color: #1A1E24;
        border-radius: 5px;
        padding: 15px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.2);
        text-align: center;
    }
    
    .metric-title {
        font-size: 14px;
        color: #AAAAAA;
        margin-bottom: 10px;
    }
    
    .metric-value {
        font-size: 24px;
        font-weight: bold;
    }
    
    /* Table cell colors for OI changes */
    .cell-up {
        background-color: rgba(0, 255, 0, 0.2);
        color: #00FF7F !important;
    }
    
    .cell-down {
        background-color: rgba(255, 0, 0, 0.2);
        color: #FF4500 !important;
    }
    
    
    .cell-dark-green {
        background-color: rgba(0, 100, 0, 0.3);
        color: #006400 !important;
        font-weight: bold !important;
    }
    
    .cell-red {
        background-color: rgba(255, 0, 0, 0.2);
        color: #FF4500 !important;
    }
    
    .cell-dark-red {
        background-color: rgba(139, 0, 0, 0.3);
        color: #8B0000 !important;
        font-weight: bold !important;
    }
    
    .cell-green {
        background-color: rgba(0, 255, 0, 0.2);
        color: #00FF7F !important;
    }
    
    /* Status indicators */
    .status-indicator {
        display: inline-block;
        width: 10px;
        height: 10px;
        border-radius: 50%;
        margin-right: 5px;
    }
    
    .status-active {
        background-color: #00FF7F;
    }
    
    .status-inactive {
        background-color: #FF4500;
    }
    
    .status-pending {
        background-color: #FFA500;
    }
    
    /* Sidebar section headers */
    .sidebar-header {
        font-weight: bold;
        margin-top: 20px;
        margin-bottom: 10px;
        color: #00CED1;
    }
    
    /* Custom button styling */
    .stButton>button {
        background-color: #00CED1;
        color: #0E1117;
        font-weight: bold;
        border: none;
        border-radius: 4px;
    }
    
    .stButton>button:hover {
        background-color: #00A0A0;
    }
</style>
""", unsafe_allow_html=True)

# Function to format OI changes with colors
def format_oi_change(val, is_ce=True):
    """Format OI change value with colors based on interpretation."""
    if pd.isna(val) or val == "N/A":
        return "N/A"
        
    # Convert to int if it's not already
    try:
        val = int(val)
    except (ValueError, TypeError):
        return "N/A"
    
    # Apply new color coding
    if is_ce:
        # CE: DARK Green for decrease (bullish), Normal Red for increase (bearish)
        if val < 0:
            color = "cell-dark-green"  # DARK Green for bullish
        elif val > 0:
            color = "cell-red"  # Normal Red for bearish
        else:
            color = ""
    else:
        # PE: DARK Red for decrease (bearish), Normal Green for increase (bullish)
        if val < 0:
            color = "cell-dark-red"  # DARK Red for bearish
        elif val > 0:
            color = "cell-green"  # Normal Green for bullish
        else:
            color = ""
    
    # Format with commas for thousands
    formatted_val = f"{val:,}"
    
    # Add + sign for positive values
    if val > 0:
        formatted_val = f"+{formatted_val}"
    
    return f'<div class="{color}">{formatted_val}</div>'

# Function to create OI change table
def create_oi_change_table(data, selected_intervals=[5, 10, 15]):
    """Create OI change table with formatting."""
    if not data or not isinstance(data, list) or not data:
        return pd.DataFrame()
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Create column mapping for the new structure
    rename_dict = {'strike': 'Strike'}
    columns_to_display = ['Strike']
    
    # Add CE columns
    for interval in selected_intervals:
        ce_col = f'ce_{interval}min'
        if ce_col in df.columns:
            rename_dict[ce_col] = f'CE {interval}min'
            columns_to_display.append(f'CE {interval}min')
    
    # Add PE columns  
    for interval in selected_intervals:
        pe_col = f'pe_{interval}min'
        if pe_col in df.columns:
            rename_dict[pe_col] = f'PE {interval}min'
            columns_to_display.append(f'PE {interval}min')
    
    # Rename columns
    display_df = df.rename(columns=rename_dict)
    
    # Add highlight indicator column
    if 'highlight' in df.columns:
        display_df['highlight'] = df['highlight']
    else:
        display_df['highlight'] = False
    
    return display_df[columns_to_display + ['highlight']]

# Function to display OI change table with styling
def display_oi_table(df):
    """Display OI change table with proper styling."""
    if df.empty:
        st.warning("No data available for display")
        return
    
    # Get column names
    cols = df.columns.tolist()
    
    # Remove highlight column from display
    display_cols = [col for col in cols if col != 'highlight']
    
    # Create HTML output
    html_output = "<div style='overflow-x: auto;'><table class='dataframe'>"
    
    # Table header
    html_output += "<thead><tr>"
    for col in display_cols:
        html_output += f"<th>{col}</th>"
    html_output += "</tr></thead>"
    
    # Table body
    html_output += "<tbody>"
    for idx, row in df.iterrows():
        # Add highlight class if needed
        row_class = "highlight" if row.get('highlight', False) else ""
        html_output += f"<tr class='{row_class}'>"
        
        for col in display_cols:
            value = row[col]
            
            # Format OI change columns
            if col.startswith('CE ') and col != 'CE OI':
                cell_content = format_oi_change(value, is_ce=True)
            elif col.startswith('PE ') and col != 'PE OI':
                cell_content = format_oi_change(value, is_ce=False)
            # Format strike price
            elif col == 'Strike':
                cell_content = f"{value:,.0f}"
            # Format regular OI columns with commas
            elif col in ['CE OI', 'PE OI']:
                cell_content = f"{int(value):,}" if pd.notna(value) else ""
            else:
                cell_content = str(value)
            
            html_output += f"<td>{cell_content}</td>"
        
        html_output += "</tr>"
    
    html_output += "</tbody></table></div>"
    
    # Display the HTML table
    st.markdown(html_output, unsafe_allow_html=True)

# Function to start data collection process
def start_data_collection(symbol, expiry):
    """Start background data collection process."""
    # Store settings in session state
    st.session_state['collection_running'] = True
    st.session_state['collection_symbol'] = symbol
    st.session_state['collection_expiry'] = expiry
    ist = pytz.timezone('Asia/Kolkata')
    st.session_state['last_collection_time'] = datetime.now(ist)
    st.session_state['next_collection_time'] = datetime.now(ist) + timedelta(
        minutes=DATA_COLLECTION["interval_minutes"]
    )

# Function to stop data collection process
def stop_data_collection():
    """Stop background data collection process."""
    st.session_state['collection_running'] = False

# Function to collect data once
def collect_data_once(symbol, expiry):
    """Collect data once and store result."""
    with st.spinner("Collecting data..."):
        try:
            # Collect and store data
            data, filepath, success = connector.collect_and_store(symbol, expiry)
            
            if success and data is not None:
                ist = pytz.timezone('Asia/Kolkata')
                st.session_state['last_collection_time'] = datetime.now(ist)
                st.session_state['next_collection_time'] = datetime.now(ist) + timedelta(
                    minutes=DATA_COLLECTION["interval_minutes"]
                )
                st.session_state['last_collection_success'] = True
                st.session_state['last_collected_data'] = data
                st.session_state['last_collection_file'] = filepath
                
                # Success message
                st.success(f"Data collected successfully: {len(data)} rows")
                return True
            else:
                st.session_state['last_collection_success'] = False
                st.error("Failed to collect data")
                return False
        
        except Exception as e:
            st.session_state['last_collection_success'] = False
            st.error(f"Error collecting data: {str(e)}")
            logger.error(f"Error in manual data collection: {str(e)}", exc_info=True)
            return False

# Function to update dashboard data
def update_dashboard_data(symbol, expiry, currently_trading, range_limit, highlight_limit):
    """Update dashboard data based on latest collection."""
    try:
        # Process data for dashboard
        result = calculator.process_latest_data(
            symbol,
            expiry,
            currently_trading,
            range_limit,
            highlight_limit
        )
        
        # Store result in session state
        if result['success']:
            st.session_state['dashboard_data'] = result['data']
            
            # Ensure timestamp is in IST
            ts = result['timestamp']
            
            # Convert string to datetime if needed
            if isinstance(ts, str):
                ts = pd.to_datetime(ts)
            
            # Now handle timezone
            if ts.tzinfo is None:
                ist = pytz.timezone('Asia/Kolkata')
                ts = ist.localize(ts)
            else:
                ist = pytz.timezone('Asia/Kolkata')
                ts = ts.astimezone(ist)
            
            st.session_state['dashboard_timestamp'] = ts
            st.session_state['dashboard_success'] = True
            return True
        else:
            st.session_state['dashboard_success'] = False
            st.error(f"Failed to update dashboard: {result['message']}")
            return False
    
    except Exception as e:
        st.session_state['dashboard_success'] = False
        st.error(f"Error updating dashboard: {str(e)}")
        logger.error(f"Error updating dashboard: {str(e)}", exc_info=True)
        return False

# Initialize session state variables
if 'collection_running' not in st.session_state:
    st.session_state['collection_running'] = False

if 'last_collection_time' not in st.session_state:
    st.session_state['last_collection_time'] = None

if 'next_collection_time' not in st.session_state:
    st.session_state['next_collection_time'] = None

if 'last_collection_success' not in st.session_state:
    st.session_state['last_collection_success'] = None

if 'dashboard_data' not in st.session_state:
    st.session_state['dashboard_data'] = None

if 'dashboard_timestamp' not in st.session_state:
    st.session_state['dashboard_timestamp'] = None

if 'dashboard_success' not in st.session_state:
    st.session_state['dashboard_success'] = False

# Try to load user settings
user_settings = db.get_latest_user_settings()

# Header
st.title("📊 AccuNirvana Options Analysis Dashboard")

# Sidebar inputs
st.sidebar.markdown("<div class='sidebar-header'>Settings</div>", unsafe_allow_html=True)

# Symbol selection
symbol = st.sidebar.selectbox(
    "Symbol",
    ["NIFTY", "BANKNIFTY"],
    index=0,
    help="Select the symbol to analyze"
)

# Expiry date
default_expiry = user_settings.get('expiry', datetime.now().strftime('%d-%m-%Y'))
expiry = st.sidebar.text_input(
    "Expiry (DD-MM-YYYY)",
    value=default_expiry,
    help="Enter expiry date in DD-MM-YYYY format"
)

# Currently trading value
default_trading = user_settings.get('currently_trading', 22000)
currently_trading = st.sidebar.number_input(
    "Currently Trading",
    min_value=10000,
    max_value=50000,
    value=int(default_trading),
    step=50,
    help="Enter the current trading level"
)

# Range limit
default_range = user_settings.get('range_limit', 1000)
range_limit = st.sidebar.number_input(
    "Data Range Limit",
    min_value=500,
    max_value=5000,
    value=int(default_range),
    step=100,
    help="Range of strikes to include in analysis (Currently Trading ± Range)"
)

# Highlight limit
default_highlight = user_settings.get('highlight_limit', 400)
highlight_limit = st.sidebar.number_input(
    "Highlight Range Limit",
    min_value=100,
    max_value=1000,
    value=int(default_highlight),
    step=50,
    help="Range of strikes to highlight in the dashboard"
)

# Data Collection Controls
st.sidebar.markdown("<div class='sidebar-header'>Data Collection</div>", unsafe_allow_html=True)

# Status indicators
col1, col2 = st.sidebar.columns(2)

with col1:
    # Trading hours indicator
    trading_active = is_trading_hours()
    status_class = "status-active" if trading_active else "status-inactive"
    st.markdown(
        f"<div><span class='status-indicator {status_class}'></span> Trading Hours: "
        f"{'Active' if trading_active else 'Inactive'}</div>",
        unsafe_allow_html=True
    )

with col2:
    # Collection status indicator
    collection_status = "Active" if st.session_state['collection_running'] else "Inactive"
    status_class = "status-active" if st.session_state['collection_running'] else "status-inactive"
    st.markdown(
        f"<div><span class='status-indicator {status_class}'></span> Data Collection: "
        f"{collection_status}</div>",
        unsafe_allow_html=True
    )

# Collection controls
col1, col2 = st.sidebar.columns(2)

with col1:
    if st.button(
        "Start Collection" if not st.session_state['collection_running'] else "Collection Running",
        disabled=st.session_state['collection_running'],
        key="start_collection"
    ):
        start_data_collection(symbol, expiry)
        # Collect data immediately
        collect_data_once(symbol, expiry)

with col2:
    if st.button(
        "Stop Collection",
        disabled=not st.session_state['collection_running'],
        key="stop_collection"
    ):
        stop_data_collection()

# Manual collection
if st.sidebar.button("Collect Data Now", key="collect_now"):
    collect_data_once(symbol, expiry)

# Collection status info
if st.session_state['last_collection_time']:
    st.sidebar.markdown(
        f"Last collection: {st.session_state['last_collection_time'].strftime('%H:%M:%S')}"
    )

if st.session_state['next_collection_time'] and st.session_state['collection_running']:
    ist = pytz.timezone('Asia/Kolkata')
    time_diff = (st.session_state['next_collection_time'] - datetime.now(ist)).total_seconds()
    
    if time_diff > 0:
        st.sidebar.progress(
            1 - (time_diff / (DATA_COLLECTION["interval_minutes"] * 60)),
            text=f"Next in: {int(time_diff // 60):02d}:{int(time_diff % 60):02d}"
        )

# Dashboard Controls
st.sidebar.markdown("<div class='sidebar-header'>Dashboard</div>", unsafe_allow_html=True)

# Refresh dashboard button
if st.sidebar.button("Refresh Dashboard", key="refresh_dashboard"):
    update_dashboard_data(
        symbol, 
        expiry, 
        currently_trading, 
        range_limit, 
        highlight_limit
    )

# Auto-refresh toggle
auto_refresh = st.sidebar.checkbox(
    "Auto-refresh Dashboard",
    value=True,
    help="Automatically refresh dashboard when new data is collected"
)

# OI Change intervals to display
st.sidebar.markdown("<div class='sidebar-header'>Display Options</div>", unsafe_allow_html=True)
selected_intervals = st.sidebar.multiselect(
    "OI Change Intervals",
    options=DATA_COLLECTION["analysis_intervals"],
    default=[5, 10, 15],
    help="Select which time intervals to display in the dashboard"
)

# Main dashboard area
st.header("Options Open Interest Analysis")

# Status metrics row
col1, col2, col3, col4 = st.columns(4)

with col1:
    # Last updated time
    last_updated = st.session_state.get('dashboard_timestamp', "Not updated yet")
    last_updated_display = "Not updated yet"
    
    if last_updated and not isinstance(last_updated, str):
        try:
            ist = pytz.timezone('Asia/Kolkata')
            if last_updated.tzinfo is None:
                last_updated = ist.localize(last_updated)

            else:
                last_updated = last_updated.astimezone(ist)
            last_updated_display = last_updated.strftime('%H:%M:%S')
        except:
            pass
    elif isinstance(last_updated, str):
        last_updated_display = last_updated
    
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-title">Last Updated</div>
            <div class="metric-value">{last_updated_display}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

with col2:
    # Current Symbol & Expiry
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-title">Symbol & Expiry</div>
            <div class="metric-value">{symbol} {expiry}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

with col3:
    # Data Range
    range_from = currently_trading - range_limit
    range_to = currently_trading + range_limit
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-title">Data Range</div>
            <div class="metric-value">{range_from:,.0f} - {range_to:,.0f}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

with col4:
    # Highlight Range
    highlight_from = (range_from + range_to) / 2 - highlight_limit
    highlight_to = (range_from + range_to) / 2 + highlight_limit
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-title">Focus Range</div>
            <div class="metric-value">{highlight_from:,.0f} - {highlight_to:,.0f}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

# OI Change Analysis Table
st.subheader("Open Interest Changes")

# Legend
legend_col1, legend_col2, legend_col3, legend_col4 = st.columns(4)

with legend_col1:
    st.markdown('<div class="cell-down">🔴 CE OI Increase (Bearish)</div>', unsafe_allow_html=True)

with legend_col2:
    st.markdown('<div class="cell-up">🟢 CE OI Decrease (Bullish)</div>', unsafe_allow_html=True)

with legend_col3:
    st.markdown('<div class="cell-down">🔴 PE OI Decrease (Bearish)</div>', unsafe_allow_html=True)

with legend_col4:
    st.markdown('<div class="cell-up">🟢 PE OI Increase (Bullish)</div>', unsafe_allow_html=True)

# Dashboard data
dashboard_data = st.session_state.get('dashboard_data')

if dashboard_data:
    # Convert to DataFrame for display
    display_df = create_oi_change_table(dashboard_data, selected_intervals)
    
    # Display table
    display_oi_table(display_df)
else:
    st.info("Click 'Refresh Dashboard' to view data")

# Auto-collection and refresh logic
# Auto-collection logic - simplified
if st.session_state['collection_running'] and trading_active:
    if 'last_auto_check' not in st.session_state:
        st.session_state['last_auto_check'] = datetime.now(pytz.timezone('Asia/Kolkata'))
    
    current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
    time_since_last = (current_time - st.session_state['last_auto_check']).total_seconds()
    
    if time_since_last >= 300:  # 5 minutes
        collect_data_once(
            st.session_state['collection_symbol'],
            st.session_state['collection_expiry']
        )
        if auto_refresh:
            update_dashboard_data(
                symbol,
                expiry,
                currently_trading,
                range_limit,
                highlight_limit
            )
        st.session_state['last_auto_check'] = current_time

# Add periodic refresh for countdown
#if st.session_state['collection_running'] and st.session_state['next_collection_time']:
    # Add auto-refresh every 10 seconds to update countdown
    #ist = pytz.timezone('Asia/Kolkata')
    #time_to_wait = min(10, max(1, int((st.session_state['next_collection_time'] - datetime.now(ist)).total_seconds())))
    #time.sleep(time_to_wait)
    #st.rerun()
