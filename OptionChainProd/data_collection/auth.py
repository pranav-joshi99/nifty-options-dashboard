"""
Authentication module for TrueData API.
"""
import requests
import logging
from config.settings import API_CREDENTIALS, API_ENDPOINTS

logger = logging.getLogger(__name__)

def get_auth_token():
    """
    Authenticate with the TrueData API and get a bearer token.
    
    Returns:
        str: Bearer token if successful, None otherwise
    """
    auth_data = {
        "username": API_CREDENTIALS["username"],
        "password": API_CREDENTIALS["password"],
        "grant_type": "password"
    }
    
    try:
        response = requests.post(API_ENDPOINTS["auth"], data=auth_data)
        response.raise_for_status()  # Raise exception for HTTP errors
        
        token_data = response.json()
        logger.info("Authentication successful, token expires in %s seconds", 
                   token_data.get("expires_in", "unknown"))
        
        return token_data["access_token"]
    
    except requests.exceptions.RequestException as e:
        logger.error("Authentication failed: %s", str(e))
        if hasattr(e, 'response') and e.response:
            logger.error("Response: %s", e.response.text)
        return None