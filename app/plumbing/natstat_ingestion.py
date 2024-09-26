import pandas as pd
import requests, re

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
            "team_id": team_info['id'],
            "team_name": team_info['name'],
            "team_nickname": team_info['nickname'],
            "team_fullname": team_info['fullname'],
            "team_code": team_info['code'],
            "api_url": team_info['meta']['apiurl'],
            "site_url": team_info['meta']['siteurl']
        }
        team_list.append(team_dict)

    # Convert list of dictionaries to pandas DataFrame
    teams_df = pd.DataFrame(team_list)

    return teams_df

def ingest_players_data(teams_df):
    team_code_list = teams_df['team_code'].tolist()
    player_list = []
    for team_code in team_code_list:
        url = f"https://interst.at/player/pfb/{team_code}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        players = data['players']

        for player_key, player_info in players.items():            
            player_dict = {
                "player_id": player_info.get('id'),
                "player_name": player_info.get('name'),
                "player_position": player_info.get('position'),
                "player_jersey": player_info.get('jersey'),
                "player_experience": player_info.get('experience'),
                "player_height": player_info.get('bio', {}).get('height_ftin'),
                "player_weight": player_info.get('bio', {}).get('weight_lbs'),
                "player_api_url": player_info.get('meta', {}).get('apiurl'),
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