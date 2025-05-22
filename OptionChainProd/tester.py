# save as test_api.py
import requests
from io import StringIO
import pandas as pd

def test_auth():
    """Test authentication with TrueData API."""
    auth_url = "https://auth.truedata.in/token"
    auth_data = {
        "username": "Trial139",
        "password": "niraj139",
        "grant_type": "password"
    }
    
    print("Testing authentication...")
    response = requests.post(auth_url, data=auth_data)
    print(f"Auth status: {response.status_code}")
    
    if response.status_code == 200:
        token_data = response.json()
        token = token_data.get("access_token")
        print(f"Successfully obtained token (expires in {token_data.get('expires_in')} seconds)")
        return token
    else:
        print(f"Auth error: {response.text}")
        return None

def test_data_fetch(token, symbol, expiry):
    """Test fetching option chain data."""
    option_url = "https://greeks.truedata.in/api/getOptionChainwithGreeks"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "symbol": symbol,
        "expiry": expiry,
        "response": "csv"
    }
    
    print(f"\nTesting data fetch for {symbol} with expiry {expiry}...")
    response = requests.get(option_url, headers=headers, params=params)
    print(f"Data fetch status: {response.status_code}")
    
    if response.status_code == 200:
        if response.text:
            try:
                # Try to parse as CSV
                df = pd.read_csv(StringIO(response.text))
                print(f"Successfully parsed data - found {len(df)} rows")
                if len(df) > 0:
                    print("Column names:", df.columns.tolist())
                    return True
                else:
                    print("Data is empty (0 rows)")
                    return False
            except Exception as e:
                print(f"Error parsing CSV: {str(e)}")
                print(f"Response text sample: {response.text[:200]}")
                return False
        else:
            print("Empty response from API")
            return False
    else:
        print(f"Data fetch error: {response.text}")
        return False

if __name__ == "__main__":
    # Test authentication
    token = test_auth()
    
    if token:
        # Test with multiple expiry date formats
        expiryDates = ["22-05-2025", "22-05-25", "22052025", "2025-05-22"]
        
        success = False
        for expiry in expiryDates:
            if test_data_fetch(token, "NIFTY", expiry):
                print(f"\n✅ Successfully fetched data using expiry format: {expiry}")
                success = True
                break
        
        if not success:
            print("\n❌ Failed to fetch data with any expiry format")
    else:
        print("\n❌ Authentication failed")