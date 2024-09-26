from datetime import datetime

def parse_date(date_str):
    """Converts a date string to a datetime object."""
    if date_str and date_str != '':
        try:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except ValueError:
            try:
                return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
            except ValueError:
                print(f"Unable to parse date: {date_str}")
                return None
    else:
        return None
    

import json

def parse_players(json_data):
    """
    Parses the JSON data to extract code, name, team, and team-code for each player.

    Args:
        json_data (dict): The JSON data as a Python dictionary.

    Returns:
        list of dict: A list containing dictionaries with player details.
    """
    players = json_data.get('players', {})
    parsed_players = []

    for player_id, player_info in players.items():
        player_details = {
            'code': player_info.get('code'),
            'name': player_info.get('name'),
            'team': player_info.get('team'),
            'team_code': player_info.get('team-code')
        }
        parsed_players.append(player_details)

    return parsed_players
