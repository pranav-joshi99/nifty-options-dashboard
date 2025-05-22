"""
Entry point for NIFTY Options Dashboard.
"""
import streamlit as st
import os
import sys

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Execute the dashboard directly
exec(open(os.path.join(os.path.dirname(__file__), 'ui', 'dashboard.py')).read())
