import requests
import os
from dotenv import load_dotenv
from .data_storage import (
    store_timeframe_data,
    store_teams_data,
    store_players_data
)

load_dotenv()
SPORTSDATAIO_API_KEY = os.getenv('SPORTSDATAIO_API_KEY')

def ingest_timeframe_data():
    try:
        print('Fetching timeframe data from SportsDataIO API...')
        api_call = f"https://api.sportsdata.io/v3/nfl/scores/json/Timeframes/all?key={SPORTSDATAIO_API_KEY}"
        response = requests.get(api_call)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        raise Exception(f"Failed to fetch data from SportsDataIO API: {e}")

    try:
        print('Storing in PostgreSQL...')
        store_timeframe_data(data)
    except Exception as e:
        raise Exception(f"Failed to store data in PostgreSQL: {e}")

def ingest_teams_data():
    try:
        print('Fetching team data from SportsDataIO API...')
        api_call = f"https://api.sportsdata.io/v3/nfl/scores/json/TeamsBasic?key={SPORTSDATAIO_API_KEY}"
        response = requests.get(api_call)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        raise Exception(f"Failed to fetch data from SportsDataIO API: {e}")

    try:
        print('Storing in PostgreSQL...')
        store_teams_data(data)
    except Exception as e:
        raise Exception(f"Failed to store data in PostgreSQL: {e}")
    
def ingest_players_data():
    try:
        print('Fetching player data from SportsDataIO API...')
        api_call = f"https://api.sportsdata.io/v3/nfl/scores/json/PlayersByAvailable?key={SPORTSDATAIO_API_KEY}"
        response = requests.get(api_call)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        raise Exception(f"Failed to fetch data from SportsDataIO API: {e}")

    try:
        print('Storing in PostgreSQL...')
        store_players_data(data)
    except Exception as e:
        raise Exception(f"Failed to store data in PostgreSQL: {e}")