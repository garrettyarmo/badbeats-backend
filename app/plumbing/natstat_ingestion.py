import pandas as pd
import requests, re
import asyncio, logging, aiohttp, nest_asyncio
from aiohttp import ClientSession
from typing import List, Dict
from tqdm.asyncio import tqdm_asyncio  # Ensure tqdm is installed: pip install tqdm

# Configure logging to write WARNING and above to a file
logging.basicConfig(
    filename='ingestion_pipelines.log',  # Log file name
    filemode='a',  # Append mode
    level=logging.DEBUG,  # Set to WARNING to suppress INFO and DEBUG messages
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# Example log messages
logger.debug("This DEBUG message will not be logged.")
logger.info("This INFO message will not be logged.")
logger.warning("This WARNING message will be logged to the file.")
logger.error("This ERROR message will be logged to the file.")
logger.critical("This CRITICAL message will be logged to the file.")


# Apply the nest_asyncio patch
nest_asyncio.apply()

def ingest_teams_data():
    """ Gets team data from the NatStat API and returns a DataFrame with team information.
    """
    url = f"https://interst.at/team/pfb/2024"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # Extract the 'teams' dictionary
    teams_data = data['teams']

    # Prepare a list to store team info
    team_list = []

    # Iterate over each team and extract relevant fields
    for team_key, team_info in teams_data.items():
        team_dict = {
            "id": team_info['id'],
            "name": team_info['name'],
            "nickname": team_info['nickname'],
            "fullname": team_info['fullname'],
            "code": team_info['code'],
            "api_url": team_info['meta']['apiurl'],
            "site_url": team_info['meta']['siteurl']
        }
        team_list.append(team_dict)

    # Convert list of dictionaries to pandas DataFrame
    teams_df = pd.DataFrame(team_list)

    return teams_df

def ingest_players_data(teams_df):
    """Probably should add list of seasons for easy iteration later"""
    player_list = []
    for team in teams_df.itertuples():
        url = f"https://interst.at/player/pfb/{team.code}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        players = data['players']

        for player_key, player_info in players.items():            
            player_dict = {
                "id": player_info.get('id'),
                "name": player_info.get('name'),
                "position": player_info.get('position'),
                "jersey_number": player_info.get('jersey'),
                "years_experience": player_info.get('experience'),
                "height": player_info.get('bio', {}).get('height_ftin'),
                "weight": player_info.get('bio', {}).get('weight_lbs'),
                "api_url": player_info.get('meta', {}).get('apiurl'),
            }
            player_list.append(player_dict)
            
    players_df = pd.DataFrame(player_list)
    return players_df

def ingest_games_data():
    """
    NOTE: only calling 2024 games. Gets game data from the NatStat API and returns a DataFrame with game information.
    Handles null values in the data.
    """
    # API endpoint for games
    url = f"https://interst.at/game/pfb/2024"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # Extract the 'games' dictionary
    games_data = data.get('games', {})

    # Prepare a list to store game info
    game_list = []

    # Iterate over each game and extract relevant fields
    for game_key, game_info in games_data.items():
        game_dict = {
            "game_id": game_info.get('id'),
            "gameday": game_info.get('gameday'),
            "starttime": game_info.get('starttime'),
            "status": game_info.get('status'),
            "visitor_id": game_info.get('visitor', {}).get('id'),
            "visitor_code": game_info.get('visitor', {}).get('code'),
            "visitor_team": game_info.get('visitor', {}).get('team'),
            "visitor_team_fullname": game_info.get('visitor', {}).get('team_fullname'),
            "visitor_score": game_info.get('visitor', {}).get('score'),
            "home_id": game_info.get('home', {}).get('id'),
            "home_code": game_info.get('home', {}).get('code'),
            "home_team": game_info.get('home', {}).get('team'),
            "home_team_fullname": game_info.get('home', {}).get('team_fullname'),
            "home_score": game_info.get('home', {}).get('score'),
            "winner_team": game_info.get('winner', {}).get('team'),
            "loser_team": game_info.get('loser', {}).get('team'),
            "venue_name": game_info.get('venue', {}).get('name'),
            "venue_citystate": game_info.get('venue', {}).get('citystate'),
            "venue_nation": game_info.get('venue', {}).get('nation'),
            "attendance": game_info.get('attendance'),  # Allow null values
            "game_api_url": game_info.get('meta', {}).get('apiurl'),
            "game_site_url": game_info.get('meta', {}).get('siteurl'),
            "player_statline_count": game_info.get('meta', {}).get('playerstatlines'),
            "play_by_play_count": game_info.get('meta', {}).get('playbyplay')
        }
        game_list.append(game_dict)

    # Convert list of dictionaries to pandas DataFrame
    games_df = pd.DataFrame(game_list)

    return games_df

def parse_game_data(game_url):
    game_id = extract_game_code(game_url)
    response = requests.get(game_url)
    response.raise_for_status()
    data = response.json()

    game = data['games'][f'game_{game_id}']

    # Create a dataframe for game details
    game_details = {
        'game_id': game['id'],
        'gameday': game['gameday'],
        'starttime': game['starttime'],
        'status': game['status'],
        'visitor_team': game['visitor']['team'],
        'visitor_score': game['visitor']['score'],
        'home_team': game['home']['team'],
        'home_score': game['home']['score'],
        'venue': game['venue']['name'],
        'attendance': game['attendance'],
    }

    # Extract player level information (example for one player, repeat for others if needed)
    player_data = []
    for player_id, player_info in game['players'].items():
        player_data.append({
            'player_id': player_info['id'],
            'player_name': player_info['name'],
            'team': player_info['team']['name'],
            'position': player_info.get('position', 'unknown'),
            'starter': player_info['starter']
        })

    # Convert both game details and player data to DataFrame
    game_df = pd.DataFrame([game_details])
    player_df = pd.DataFrame(player_data)
    return game_df, player_df

def extract_game_code(url):
    # Regular expression to capture the numeric game code after /pfb/
    pattern = re.compile(r'/pfb/(\d+)')
    
    # Search for the pattern in the given URL
    match = pattern.search(url)
    if match:
        # Return the numeric part of the match (the game code)
        return match.group(1)
    else:
        return None  # Return None if no game code is found
    
async def fetch_player_data(session: ClientSession, player: pd.Series, semaphore: asyncio.Semaphore) -> List[Dict]:
    """
    Asynchronously fetches and processes data for a single player.
    """
    url = player.api_url
    async with semaphore:
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                data = await response.json()
                logger.info(f"Parsing statistics for {player.name} at URL: {url}")

                # Validate JSON structure
                player_key = f'player_{player.id}'
                if 'players' not in data or player_key not in data['players']:
                    logger.error(f"Player data not found for {player.name} at URL: {url}")
                    return []

                player_data = data['players'][player_key]
                if 'stats' not in player_data:
                    logger.error(f"'stats' not found for {player.name} at URL: {url}")
                    return []

                stats = player_data['stats']

                # Extract player_statlines
                player_statlines = stats.get('playerstatline', {})
                if not isinstance(player_statlines, dict):
                    logger.error(f"'playerstatline' is not a dict for {player.name} at URL: {url}. Content: {player_statlines}")
                    return []

                # Initialize list to hold combined statlines for this player
                statline_dicts = []

                for key, value in player_statlines.items():
                    if not isinstance(value, dict):
                        logger.warning(f"Value for 'playerstatline' key '{key}' is not a dict for {player.name} at URL: {url}. Content: {value}")
                        continue  # Skip this statline

                    # Extract statline data
                    statline_dict = {
                        "statline_id": value.get('id'),
                        "player_name": player.name,
                        "player_id": player.id,
                        "position": value.get('position'),
                        "date": value.get('date'),
                        "season": value.get('season'),  # Fixed typo
                        "game_id": value.get('game', {}).get('id'),
                        "team_id": value.get('team', {}).get('id'),
                        "team_name": value.get('team', {}).get('name'),
                        "opponent_id": value.get('opponent', {}).get('id'),
                        "opponent_name": value.get('opponent', {}).get('name'),
                        "pass_attempts": value.get('passatt'),
                        "pass_completions": value.get('passcomp'),
                        "pass_yards": value.get('passyds'),
                        "pass_yards_per_attempt": value.get('passypa'),
                        "passing_touchdowns": value.get('passtd'),
                        "interceptions_thrown": value.get('passint'),
                        "rush_attempts": value.get('rushatt'),
                        "rush_yards": value.get('rushyds'),
                        "rush_yards_per_attempt": value.get('rushypa'),
                        "rushing_touchdowns": value.get('rushtd'),
                        "longest_run": value.get('rushlong'),
                        "performance_score": value.get('perfscore'),
                        "performance_score_season_average": value.get('perfscoreseasonavg'),  # Fixed key name
                        "presence_rate": value.get('presencerate'),
                        "presence_rate_adjusted": value.get('adjpresencerate'),
                        "short_statline": value.get('statline'),
                    }

                    # Extract PCR Stats safely
                    player_pcr_stats = stats.get('pcr', None)
                    if isinstance(player_pcr_stats, dict):
                        # PCR data exists; extract fields
                        player_pcr_dict = {
                            "pcr_season": player_pcr_stats.get('season'),
                            "pcr_efficiency": player_pcr_stats.get('efficiency'),
                            "pcr_efficiency_points": player_pcr_stats.get('efficiencypoints'),
                            "pcr_power": player_pcr_stats.get('power'),
                            "pcr_power_points": player_pcr_stats.get('powerpoints'),
                            "pcr_speed_agility": player_pcr_stats.get('speedagility'),
                            "pcr_speed_agility_points": player_pcr_stats.get('speedagilitypoints'),
                            "pcr_accuracy": player_pcr_stats.get('accuracy'),
                            "pcr_accuracy_points": player_pcr_stats.get('accuracypoints'),
                            "pcr_opponent_quality": player_pcr_stats.get('oppquality'),
                            "pcr_opponent_quality_points": player_pcr_stats.get('oppqualitypoints'),
                            "pcr_points": player_pcr_stats.get('pcrpoints'),
                            "pcr_points_adjusted": player_pcr_stats.get('pcradjusted'),
                            "pcr_rank": player_pcr_stats.get('pcrrank')
                        }
                    else:
                        # PCR data does not exist; assign None to all PCR fields
                        player_pcr_dict = {
                            "pcr_season": None,
                            "pcr_efficiency": None,
                            "pcr_efficiency_points": None,
                            "pcr_power": None,
                            "pcr_power_points": None,
                            "pcr_speed_agility": None,
                            "pcr_speed_agility_points": None,
                            "pcr_accuracy": None,
                            "pcr_accuracy_points": None,
                            "pcr_opponent_quality": None,
                            "pcr_opponent_quality_points": None,
                            "pcr_points": None,
                            "pcr_points_adjusted": None,
                            "pcr_rank": None
                        }

                    # Combine Dictionaries
                    combined_dict = {**statline_dict, **player_pcr_dict}
                    statline_dicts.append(combined_dict)

                return statline_dicts

        except aiohttp.ClientResponseError as e:
            logger.error(f"HTTP error for player {player.name} at URL: {url}. Status: {e.status}. Message: {e.message}")
            return []
        except aiohttp.ClientError as e:
            logger.error(f"Network error for player {player.name} at URL: {url}. Error: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error for player {player.name} at URL: {url}. Error: {e}")
            return []


async def get_player_statlines_async(players_df: pd.DataFrame, season: int = 2024) -> pd.DataFrame:
    """
    Asynchronously fetches and compiles player statlines into a DataFrame.
    """
    player_stat_list: List[Dict] = []
    semaphore = asyncio.Semaphore(100)  # Limit concurrent requests to 100

    connector = aiohttp.TCPConnector(limit=100)  # Adjust limit as needed
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            fetch_player_data(session, player, semaphore)
            for player in players_df.itertuples(index=False)
        ]

        # Using tqdm for progress bar (optional)
        for coroutine in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Processing Players"):
            result = await coroutine
            player_stat_list.extend(result)

    # Create DataFrame from list of dictionaries
    players_df_result = pd.DataFrame(player_stat_list)
    return players_df_result

def get_player_statlines(players_df: pd.DataFrame, season: int = 2024) -> pd.DataFrame:
    """
    Synchronous wrapper to execute the asynchronous statline fetching. NOTE: Unsure if season param does anything 
    """
    return asyncio.run(get_player_statlines_async(players_df, season))