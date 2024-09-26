import requests
import os
from dotenv import load_dotenv
import time  # For handling rate limits if necessary
from .data_storage import (
    store_teams_data,
    store_players_data,
    store_games_data
)

load_dotenv()
NATSTAT_API = os.getenv('NATSTAT_API')

def ingest_teams_data():
    """"Potentially needs a for each on a list of seasons"""
    season = [2024]
    try:
        print('Fetching schedules data from NatStat API...')
        url = f"https://api3.natst.at/{NATSTAT_API}/teams/PFB/2024"
        response = requests.get(url)
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
    """
    Ingest paginated player data from the API and store it in the database.
    """
    url = f'https://api3.natst.at/{NATSTAT_API}/players/PFB/2024'

    while url:
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            # Check if the response is successful and contains player data
            if data.get('success') == '1' and 'players' in data:
                players_data = data['players']
                
                # Store players data using the store_players_data function
                store_players_data({'players': players_data})

                # Log success
                print(f"Processed page with URI: {data['query']['uri']}")
                
                # Get the next page URL, if available
                url = data['meta'].get('page-next', None)

            else:
                # Log if no data found or there is an error
                print(f"No more data or error encountered: {data.get('error', {}).get('message', 'Unknown Error')}")
                break

        except Exception as e:
            print(f"Failed to process data: {e}")
            break

    print("Players ingestion complete.")

def ingest_games_data():
    """
    Ingest paginated games data from the API and store it in the database.
    """
    url = f'https://api3.natst.at/{NATSTAT_API}/games/PFB/2001-03-23,2045-03-30'

    while url:
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            # Check if the response is successful and contains player data
            if data.get('success') == '1' and 'games' in data:
                games_data = data['games']
                
                # Store players data using the store_players_data function
                store_games_data({'games': games_data})

                # Log success
                print(f"Processed page with URI: {data['query']['uri']}")
                
                print(f"Next Page: {data['meta'].get('page-next', None)}")
                # Get the next page URL, if available
                url = data['meta'].get('page-next', None)


            else:
                # Log if no data found or there is an error
                print(f"No more data or error encountered: {data.get('error', {}).get('message', 'Unknown Error')}")
                break

        except Exception as e:
            print(f"Failed to process data: {e}")
            break

    print("Games ingestion complete.")
    
# def ingest_schedules_data():
#     """"Potentially needs a for each on a list of seasons"""
#     season = [2024]
#     try:
#         print('Fetching schedules data from SportsDataIO API...')
#         api_call = f"https://api.sportsdata.io/v3/nfl/scores/json/Schedules/{season}?key={SPORTSDATAIO_API_KEY}"
#         response = requests.get(api_call)
#         response.raise_for_status()
#         data = response.json()
#     except Exception as e:
#         raise Exception(f"Failed to fetch data from SportsDataIO API: {e}")

#     try:
#         print('Storing in PostgreSQL...')
#         store_schedules_data(data)
#     except Exception as e:
#         raise Exception(f"Failed to store data in PostgreSQL: {e}")
    
# def ingest_final_scores_data():
#     """"Potentially needs a for each on a list of seasons"""
#     season = [2024]
#     try:
#         print('Fetching schedules data from SportsDataIO API...')
#         api_call = f"https://api.sportsdata.io/v3/nfl/stats/json/ScoresFinal/{season}?key={SPORTSDATAIO_API_KEY}"
#         response = requests.get(api_call)
#         response.raise_for_status()
#         data = response.json()
#     except Exception as e:
#         raise Exception(f"Failed to fetch data from SportsDataIO API: {e}")

#     try:
#         print('Storing in PostgreSQL...')
#         store_final_scores_data(data)
#     except Exception as e:
#         raise Exception(f"Failed to store data in PostgreSQL: {e}")