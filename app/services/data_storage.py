import inspect
from psycopg2.extras import execute_values
from .db_connection import get_db_connection
from .utility import parse_date
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def store_teams_data(data):
    """
    Inserts or updates data in the 'teams' table. Assumes field names from the API match the database columns.
    """
    print(f"Running {inspect.currentframe().f_code.co_name}...")

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO teams (
            "Code", "Name", "Location"
        ) VALUES (
            %(Code)s, %(Name)s, %(Location)s
        )
        ON CONFLICT ("Code") DO UPDATE SET
            "Name" = EXCLUDED."Name",
            "Location" = EXCLUDED."Location";
        """
        for team_id, team_data in data['teams'].items():
            team_record = {
                "Code": team_data['code'],
                "Name": team_data['name'],
                "Location": team_data['location']
            }
            cursor.execute(insert_query, team_record)

        conn.commit()
        print("Data inserted/updated successfully.")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Database operation failed: {e}")
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()

def store_players_data(data):
    """
    Inserts or updates data in the 'players' table. Assumes field names from the API match the database columns.
    """
    print(f"Running {inspect.currentframe().f_code.co_name}...")

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO players (
            "Code", "Name", "Team", "TeamCode"
        ) VALUES (
            %(Code)s, %(Name)s, %(Team)s, %(TeamCode)s
        )
        ON CONFLICT ("Code") DO UPDATE SET
            "Name" = EXCLUDED."Name",
            "Team" = EXCLUDED."Team",
            "TeamCode" = EXCLUDED."TeamCode";
        """
        for player_id, player_data in data['players'].items():
            player_record = {
                "Code": player_data['code'],
                "Name": player_data['name'],
                "Team": player_data['team'],
                "TeamCode": player_data['team-code']
            }
            cursor.execute(insert_query, player_record)

        conn.commit()
        print("Data inserted/updated successfully.")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Database operation failed: {e}")
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()

def store_games_data(data):
    """
    Inserts or updates data in the 'games' table. Handles incomplete games by allowing NULL values.
    """
    print(f"Running {inspect.currentframe().f_code.co_name}...")

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO games (
            game_id, visitor, visitor_code, score_vis, home, home_code, score_home, 
            gamestatus, overtime, winner_code, loser_code, gameday, gameno, venue, venue_code
        ) VALUES (
            %(game_id)s, %(visitor)s, %(visitor_code)s, %(score_vis)s, %(home)s, %(home_code)s, %(score_home)s,
            %(gamestatus)s, %(overtime)s, %(winner_code)s, %(loser_code)s, %(gameday)s, %(gameno)s, %(venue)s, %(venue_code)s
        )
        ON CONFLICT (game_id) DO UPDATE SET
            visitor = EXCLUDED.visitor,
            visitor_code = EXCLUDED.visitor_code,
            score_vis = EXCLUDED.score_vis,
            home = EXCLUDED.home,
            home_code = EXCLUDED.home_code,
            score_home = EXCLUDED.score_home,
            gamestatus = EXCLUDED.gamestatus,
            overtime = EXCLUDED.overtime,
            winner_code = EXCLUDED.winner_code,
            loser_code = EXCLUDED.loser_code,
            gameday = EXCLUDED.gameday,
            gameno = EXCLUDED.gameno,
            venue = EXCLUDED.venue,
            venue_code = EXCLUDED.venue_code;
        """

        for game_id, game_data in data['games'].items():
            # Prepare the game record, handling cases where the value might be an empty dictionary
            game_record = {
                "game_id": game_data.get('id'),
                "visitor": game_data.get('visitor') if isinstance(game_data.get('visitor'), str) else None,
                "visitor_code": game_data.get('visitor-code'),
                "score_vis": game_data.get('score-vis'),
                "home": game_data.get('home') if isinstance(game_data.get('home'), str) else None,
                "home_code": game_data.get('home-code'),
                "score_home": game_data.get('score-home'),
                "gamestatus": game_data.get('gamestatus'),
                "overtime": game_data.get('overtime'),
                "winner_code": game_data.get('winner-code'),
                "loser_code": game_data.get('loser-code'),
                "gameday": game_data.get('gameday'),
                "gameno": game_data.get('gameno'),
                "venue": game_data.get('venue') if isinstance(game_data.get('venue'), str) else None,
                "venue_code": game_data.get('venue-code')
            }

            cursor.execute(insert_query, game_record)

        conn.commit()
        print("Data inserted/updated successfully.")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Database operation failed: {e}")
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()


# def store_schedules_data(data):
#     """
#     Inserts or updates data in the 'schedules' table. 
#     Assumes field names from the API match the database columns.
#     """
#     print(f"Running {inspect.currentframe().f_code.co_name}...")

#     conn = None
#     try:
#         conn = get_db_connection()
#         cursor = conn.cursor()

#         insert_query = """
#         INSERT INTO schedules (
#             "GameKey", "SeasonType", "Season", "Week", "Date",
#             "AwayTeam", "HomeTeam", "Channel", "PointSpread", "OverUnder",
#             "StadiumID", "Canceled", "GeoLat", "GeoLong", "ForecastTempLow",
#             "ForecastTempHigh", "ForecastDescription", "ForecastWindChill", "ForecastWindSpeed",
#             "AwayTeamMoneyLine", "HomeTeamMoneyLine", "Day", "DateTime", "GlobalGameID",
#             "GlobalAwayTeamID", "GlobalHomeTeamID", "ScoreID", "Status", "IsClosed",
#             "DateTimeUTC",
#             -- Flattened StadiumDetails
#             "StadiumDetails_StadiumID", "StadiumDetails_Name", "StadiumDetails_City",
#             "StadiumDetails_State", "StadiumDetails_Country", "StadiumDetails_Capacity",
#             "StadiumDetails_PlayingSurface", "StadiumDetails_GeoLat", "StadiumDetails_GeoLong",
#             "StadiumDetails_Type"
#         ) VALUES (
#             %(GameKey)s, %(SeasonType)s, %(Season)s, %(Week)s, %(Date)s,
#             %(AwayTeam)s, %(HomeTeam)s, %(Channel)s, %(PointSpread)s, %(OverUnder)s,
#             %(StadiumID)s, %(Canceled)s, %(GeoLat)s, %(GeoLong)s, %(ForecastTempLow)s,
#             %(ForecastTempHigh)s, %(ForecastDescription)s, %(ForecastWindChill)s, %(ForecastWindSpeed)s,
#             %(AwayTeamMoneyLine)s, %(HomeTeamMoneyLine)s, %(Day)s, %(DateTime)s, %(GlobalGameID)s,
#             %(GlobalAwayTeamID)s, %(GlobalHomeTeamID)s, %(ScoreID)s, %(Status)s, %(IsClosed)s,
#             %(DateTimeUTC)s,
#             %(StadiumDetails_StadiumID)s, %(StadiumDetails_Name)s, %(StadiumDetails_City)s,
#             %(StadiumDetails_State)s, %(StadiumDetails_Country)s, %(StadiumDetails_Capacity)s,
#             %(StadiumDetails_PlayingSurface)s, %(StadiumDetails_GeoLat)s, %(StadiumDetails_GeoLong)s,
#             %(StadiumDetails_Type)s
#         )
#         ON CONFLICT ("GameKey") DO UPDATE SET
#             "SeasonType" = EXCLUDED."SeasonType",
#             "Season" = EXCLUDED."Season",
#             "Week" = EXCLUDED."Week",
#             "Date" = EXCLUDED."Date",
#             "AwayTeam" = EXCLUDED."AwayTeam",
#             "HomeTeam" = EXCLUDED."HomeTeam",
#             "Channel" = EXCLUDED."Channel",
#             "PointSpread" = EXCLUDED."PointSpread",
#             "OverUnder" = EXCLUDED."OverUnder",
#             "StadiumID" = EXCLUDED."StadiumID",
#             "Canceled" = EXCLUDED."Canceled",
#             "GeoLat" = EXCLUDED."GeoLat",
#             "GeoLong" = EXCLUDED."GeoLong",
#             "ForecastTempLow" = EXCLUDED."ForecastTempLow",
#             "ForecastTempHigh" = EXCLUDED."ForecastTempHigh",
#             "ForecastDescription" = EXCLUDED."ForecastDescription",
#             "ForecastWindChill" = EXCLUDED."ForecastWindChill",
#             "ForecastWindSpeed" = EXCLUDED."ForecastWindSpeed",
#             "AwayTeamMoneyLine" = EXCLUDED."AwayTeamMoneyLine",
#             "HomeTeamMoneyLine" = EXCLUDED."HomeTeamMoneyLine",
#             "Day" = EXCLUDED."Day",
#             "DateTime" = EXCLUDED."DateTime",
#             "GlobalGameID" = EXCLUDED."GlobalGameID",
#             "GlobalAwayTeamID" = EXCLUDED."GlobalAwayTeamID",
#             "GlobalHomeTeamID" = EXCLUDED."GlobalHomeTeamID",
#             "ScoreID" = EXCLUDED."ScoreID",
#             "Status" = EXCLUDED."Status",
#             "IsClosed" = EXCLUDED."IsClosed",
#             "DateTimeUTC" = EXCLUDED."DateTimeUTC",
#             -- Flattened StadiumDetails
#             "StadiumDetails_StadiumID" = EXCLUDED."StadiumDetails_StadiumID",
#             "StadiumDetails_Name" = EXCLUDED."StadiumDetails_Name",
#             "StadiumDetails_City" = EXCLUDED."StadiumDetails_City",
#             "StadiumDetails_State" = EXCLUDED."StadiumDetails_State",
#             "StadiumDetails_Country" = EXCLUDED."StadiumDetails_Country",
#             "StadiumDetails_Capacity" = EXCLUDED."StadiumDetails_Capacity",
#             "StadiumDetails_PlayingSurface" = EXCLUDED."StadiumDetails_PlayingSurface",
#             "StadiumDetails_GeoLat" = EXCLUDED."StadiumDetails_GeoLat",
#             "StadiumDetails_GeoLong" = EXCLUDED."StadiumDetails_GeoLong",
#             "StadiumDetails_Type" = EXCLUDED."StadiumDetails_Type"
#         ;
#         """

#         required_fields = ["GameKey", "Season", "Date"]  # Add other required fields as necessary

#         for item in data:
#             # Data Validation
#             missing_fields = [field for field in required_fields if not item.get(field)]
#             if missing_fields:
#                 print(f"Skipping record due to missing fields: {missing_fields}")
#                 continue  # Skip this record

#             # Correct typographical errors
#             if 'Foreription' in item:
#                 item['ForecastDescription'] = item.pop('Foreription')

#             # Flatten StadiumDetails
#             stadium_details = item.pop('StadiumDetails', {})
#             if stadium_details is None:
#                 stadium_details = {}
#             for key, value in stadium_details.items():
#                 item[f'StadiumDetails_{key}'] = value

#             # Data Transformation
#             params = {}

#             # Normalize keys to match placeholders (capitalize first letter)
#             for key in item:
#                 if key.startswith('StadiumDetails_'):
#                     # Capitalize first letter after prefix
#                     suffix = key[len('StadiumDetails_'):]
#                     normalized_key = 'StadiumDetails_' + suffix[0].upper() + suffix[1:]
#                 else:
#                     normalized_key = key[0].upper() + key[1:]
#                 params[normalized_key] = item[key]

#             # Convert date strings to datetime objects
#             date_fields = ["Date", "Day", "DateTime", "DateTimeUTC"]
#             for field in date_fields:
#                 if params.get(field):
#                     params[field] = parse_date(params[field])
#                 else:
#                     params[field] = None

#             # Convert boolean fields to booleans
#             boolean_fields = ["Canceled", "IsClosed"]
#             for field in boolean_fields:
#                 if field in params:
#                     value = params[field]
#                     if isinstance(value, str):
#                         params[field] = value.lower() == 'true'
#                     elif value is None:
#                         params[field] = False  # Default to False or handle as needed

#             # Handle missing fields
#             expected_fields = [
#                 "GameKey", "SeasonType", "Season", "Week", "Date",
#                 "AwayTeam", "HomeTeam", "Channel", "PointSpread", "OverUnder",
#                 "StadiumID", "Canceled", "GeoLat", "GeoLong", "ForecastTempLow",
#                 "ForecastTempHigh", "ForecastDescription", "ForecastWindChill", "ForecastWindSpeed",
#                 "AwayTeamMoneyLine", "HomeTeamMoneyLine", "Day", "DateTime", "GlobalGameID",
#                 "GlobalAwayTeamID", "GlobalHomeTeamID", "ScoreID", "Status", "IsClosed",
#                 "DateTimeUTC",
#                 "StadiumDetails_StadiumID", "StadiumDetails_Name", "StadiumDetails_City",
#                 "StadiumDetails_State", "StadiumDetails_Country", "StadiumDetails_Capacity",
#                 "StadiumDetails_PlayingSurface", "StadiumDetails_GeoLat", "StadiumDetails_GeoLong",
#                 "StadiumDetails_Type"
#             ]
#             for field in expected_fields:
#                 if field not in params:
#                     params[field] = None  # or set a default value

#             try:
#                 cursor.execute(insert_query, params)
#             except Exception as e:
#                 print(f"Failed to insert/update record: {e}")
#                 print(f"Problematic data item: {item}")
#                 conn.rollback()
#                 continue  # Skip to the next record

#         conn.commit()
#         print("Data inserted/updated successfully.")
#     except Exception as e:
#         if conn:
#             conn.rollback()
#         print(f"Database operation failed: {e}")
#         raise
#     finally:
#         if conn:
#             cursor.close()
#             conn.close()

# def store_final_scores_data(data):
#     """
#     Inserts or updates data in the 'scores' table.
#     Assumes field names from the API match the database columns.
#     """
#     logger.info(f"Running {inspect.currentframe().f_code.co_name}...")

#     conn = None
#     try:
#         conn = get_db_connection()
#         cursor = conn.cursor()

#         insert_query = """
#         INSERT INTO scores (
#             "GameKey", "SeasonType", "Season", "Week", "Date",
#             "AwayTeam", "HomeTeam", "AwayScore", "HomeScore", "Channel",
#             "PointSpread", "OverUnder", "Quarter", "TimeRemaining", "Possession",
#             "Down", "Distance", "YardLine", "YardLineTerritory", "RedZone",
#             "AwayScoreQuarter1", "AwayScoreQuarter2", "AwayScoreQuarter3",
#             "AwayScoreQuarter4", "AwayScoreOvertime", "HomeScoreQuarter1",
#             "HomeScoreQuarter2", "HomeScoreQuarter3", "HomeScoreQuarter4",
#             "HomeScoreOvertime", "HasStarted", "IsInProgress", "IsOver",
#             "Has1stQuarterStarted", "Has2ndQuarterStarted", "Has3rdQuarterStarted",
#             "Has4thQuarterStarted", "IsOvertime", "DownAndDistance",
#             "QuarterDescription", "StadiumID", "LastUpdated", "GeoLat",
#             "GeoLong", "ForecastTempLow", "ForecastTempHigh",
#             "ForecastDescription", "ForecastWindChill", "ForecastWindSpeed",
#             "AwayTeamMoneyLine", "HomeTeamMoneyLine", "Canceled", "Closed",
#             "LastPlay", "Day", "DateTime", "AwayTeamID", "HomeTeamID",
#             "GlobalGameID", "GlobalAwayTeamID", "GlobalHomeTeamID",
#             "PointSpreadAwayTeamMoneyLine", "PointSpreadHomeTeamMoneyLine",
#             "ScoreID", "Status", "GameEndDateTime", "HomeRotationNumber",
#             "AwayRotationNumber", "NeutralVenue", "RefereeID", "OverPayout",
#             "UnderPayout", "HomeTimeouts", "AwayTimeouts", "DateTimeUTC",
#             "Attendance", "IsClosed",
#             -- Flattened StadiumDetails
#             "StadiumDetails_StadiumID", "StadiumDetails_Name", "StadiumDetails_City",
#             "StadiumDetails_State", "StadiumDetails_Country", "StadiumDetails_Capacity",
#             "StadiumDetails_PlayingSurface", "StadiumDetails_GeoLat", "StadiumDetails_GeoLong",
#             "StadiumDetails_Type"
#         ) VALUES (
#             %(GameKey)s, %(SeasonType)s, %(Season)s, %(Week)s, %(Date)s,
#             %(AwayTeam)s, %(HomeTeam)s, %(AwayScore)s, %(HomeScore)s, %(Channel)s,
#             %(PointSpread)s, %(OverUnder)s, %(Quarter)s, %(TimeRemaining)s, %(Possession)s,
#             %(Down)s, %(Distance)s, %(YardLine)s, %(YardLineTerritory)s, %(RedZone)s,
#             %(AwayScoreQuarter1)s, %(AwayScoreQuarter2)s, %(AwayScoreQuarter3)s,
#             %(AwayScoreQuarter4)s, %(AwayScoreOvertime)s, %(HomeScoreQuarter1)s,
#             %(HomeScoreQuarter2)s, %(HomeScoreQuarter3)s, %(HomeScoreQuarter4)s,
#             %(HomeScoreOvertime)s, %(HasStarted)s, %(IsInProgress)s, %(IsOver)s,
#             %(Has1stQuarterStarted)s, %(Has2ndQuarterStarted)s, %(Has3rdQuarterStarted)s,
#             %(Has4thQuarterStarted)s, %(IsOvertime)s, %(DownAndDistance)s,
#             %(QuarterDescription)s, %(StadiumID)s, %(LastUpdated)s, %(GeoLat)s,
#             %(GeoLong)s, %(ForecastTempLow)s, %(ForecastTempHigh)s,
#             %(ForecastDescription)s, %(ForecastWindChill)s, %(ForecastWindSpeed)s,
#             %(AwayTeamMoneyLine)s, %(HomeTeamMoneyLine)s, %(Canceled)s, %(Closed)s,
#             %(LastPlay)s, %(Day)s, %(DateTime)s, %(AwayTeamID)s, %(HomeTeamID)s,
#             %(GlobalGameID)s, %(GlobalAwayTeamID)s, %(GlobalHomeTeamID)s,
#             %(PointSpreadAwayTeamMoneyLine)s, %(PointSpreadHomeTeamMoneyLine)s,
#             %(ScoreID)s, %(Status)s, %(GameEndDateTime)s, %(HomeRotationNumber)s,
#             %(AwayRotationNumber)s, %(NeutralVenue)s, %(RefereeID)s, %(OverPayout)s,
#             %(UnderPayout)s, %(HomeTimeouts)s, %(AwayTimeouts)s, %(DateTimeUTC)s,
#             %(Attendance)s, %(IsClosed)s,
#             %(StadiumDetails_StadiumID)s, %(StadiumDetails_Name)s, %(StadiumDetails_City)s,
#             %(StadiumDetails_State)s, %(StadiumDetails_Country)s, %(StadiumDetails_Capacity)s,
#             %(StadiumDetails_PlayingSurface)s, %(StadiumDetails_GeoLat)s, %(StadiumDetails_GeoLong)s,
#             %(StadiumDetails_Type)s
#         )
#         ON CONFLICT ("GameKey") DO UPDATE SET
#             "SeasonType" = EXCLUDED."SeasonType",
#             "Season" = EXCLUDED."Season",
#             "Week" = EXCLUDED."Week",
#             "Date" = EXCLUDED."Date",
#             "AwayTeam" = EXCLUDED."AwayTeam",
#             "HomeTeam" = EXCLUDED."HomeTeam",
#             "AwayScore" = EXCLUDED."AwayScore",
#             "HomeScore" = EXCLUDED."HomeScore",
#             "Channel" = EXCLUDED."Channel",
#             "PointSpread" = EXCLUDED."PointSpread",
#             "OverUnder" = EXCLUDED."OverUnder",
#             "Quarter" = EXCLUDED."Quarter",
#             "TimeRemaining" = EXCLUDED."TimeRemaining",
#             "Possession" = EXCLUDED."Possession",
#             "Down" = EXCLUDED."Down",
#             "Distance" = EXCLUDED."Distance",
#             "YardLine" = EXCLUDED."YardLine",
#             "YardLineTerritory" = EXCLUDED."YardLineTerritory",
#             "RedZone" = EXCLUDED."RedZone",
#             "AwayScoreQuarter1" = EXCLUDED."AwayScoreQuarter1",
#             "AwayScoreQuarter2" = EXCLUDED."AwayScoreQuarter2",
#             "AwayScoreQuarter3" = EXCLUDED."AwayScoreQuarter3",
#             "AwayScoreQuarter4" = EXCLUDED."AwayScoreQuarter4",
#             "AwayScoreOvertime" = EXCLUDED."AwayScoreOvertime",
#             "HomeScoreQuarter1" = EXCLUDED."HomeScoreQuarter1",
#             "HomeScoreQuarter2" = EXCLUDED."HomeScoreQuarter2",
#             "HomeScoreQuarter3" = EXCLUDED."HomeScoreQuarter3",
#             "HomeScoreQuarter4" = EXCLUDED."HomeScoreQuarter4",
#             "HomeScoreOvertime" = EXCLUDED."HomeScoreOvertime",
#             "HasStarted" = EXCLUDED."HasStarted",
#             "IsInProgress" = EXCLUDED."IsInProgress",
#             "IsOver" = EXCLUDED."IsOver",
#             "Has1stQuarterStarted" = EXCLUDED."Has1stQuarterStarted",
#             "Has2ndQuarterStarted" = EXCLUDED."Has2ndQuarterStarted",
#             "Has3rdQuarterStarted" = EXCLUDED."Has3rdQuarterStarted",
#             "Has4thQuarterStarted" = EXCLUDED."Has4thQuarterStarted",
#             "IsOvertime" = EXCLUDED."IsOvertime",
#             "DownAndDistance" = EXCLUDED."DownAndDistance",
#             "QuarterDescription" = EXCLUDED."QuarterDescription",
#             "StadiumID" = EXCLUDED."StadiumID",
#             "LastUpdated" = EXCLUDED."LastUpdated",
#             "GeoLat" = EXCLUDED."GeoLat",
#             "GeoLong" = EXCLUDED."GeoLong",
#             "ForecastTempLow" = EXCLUDED."ForecastTempLow",
#             "ForecastTempHigh" = EXCLUDED."ForecastTempHigh",
#             "ForecastDescription" = EXCLUDED."ForecastDescription",
#             "ForecastWindChill" = EXCLUDED."ForecastWindChill",
#             "ForecastWindSpeed" = EXCLUDED."ForecastWindSpeed",
#             "AwayTeamMoneyLine" = EXCLUDED."AwayTeamMoneyLine",
#             "HomeTeamMoneyLine" = EXCLUDED."HomeTeamMoneyLine",
#             "Canceled" = EXCLUDED."Canceled",
#             "Closed" = EXCLUDED."Closed",
#             "LastPlay" = EXCLUDED."LastPlay",
#             "Day" = EXCLUDED."Day",
#             "DateTime" = EXCLUDED."DateTime",
#             "AwayTeamID" = EXCLUDED."AwayTeamID",
#             "HomeTeamID" = EXCLUDED."HomeTeamID",
#             "GlobalGameID" = EXCLUDED."GlobalGameID",
#             "GlobalAwayTeamID" = EXCLUDED."GlobalAwayTeamID",
#             "GlobalHomeTeamID" = EXCLUDED."GlobalHomeTeamID",
#             "PointSpreadAwayTeamMoneyLine" = EXCLUDED."PointSpreadAwayTeamMoneyLine",
#             "PointSpreadHomeTeamMoneyLine" = EXCLUDED."PointSpreadHomeTeamMoneyLine",
#             "ScoreID" = EXCLUDED."ScoreID",
#             "Status" = EXCLUDED."Status",
#             "GameEndDateTime" = EXCLUDED."GameEndDateTime",
#             "HomeRotationNumber" = EXCLUDED."HomeRotationNumber",
#             "AwayRotationNumber" = EXCLUDED."AwayRotationNumber",
#             "NeutralVenue" = EXCLUDED."NeutralVenue",
#             "RefereeID" = EXCLUDED."RefereeID",
#             "OverPayout" = EXCLUDED."OverPayout",
#             "UnderPayout" = EXCLUDED."UnderPayout",
#             "HomeTimeouts" = EXCLUDED."HomeTimeouts",
#             "AwayTimeouts" = EXCLUDED."AwayTimeouts",
#             "DateTimeUTC" = EXCLUDED."DateTimeUTC",
#             "Attendance" = EXCLUDED."Attendance",
#             "IsClosed" = EXCLUDED."IsClosed",
#             -- Flattened StadiumDetails
#             "StadiumDetails_StadiumID" = EXCLUDED."StadiumDetails_StadiumID",
#             "StadiumDetails_Name" = EXCLUDED."StadiumDetails_Name",
#             "StadiumDetails_City" = EXCLUDED."StadiumDetails_City",
#             "StadiumDetails_State" = EXCLUDED."StadiumDetails_State",
#             "StadiumDetails_Country" = EXCLUDED."StadiumDetails_Country",
#             "StadiumDetails_Capacity" = EXCLUDED."StadiumDetails_Capacity",
#             "StadiumDetails_PlayingSurface" = EXCLUDED."StadiumDetails_PlayingSurface",
#             "StadiumDetails_GeoLat" = EXCLUDED."StadiumDetails_GeoLat",
#             "StadiumDetails_GeoLong" = EXCLUDED."StadiumDetails_GeoLong",
#             "StadiumDetails_Type" = EXCLUDED."StadiumDetails_Type"
#         ;
#         """

#         # Define required fields
#         required_fields = ["GameKey", "Season", "Date"]  # Add other required fields as necessary

#         for item in data:
#             # Data Validation: Ensure required fields are present
#             missing_fields = [field for field in required_fields if not item.get(field)]
#             if missing_fields:
#                 logger.warning(f"Skipping record due to missing fields: {missing_fields}")
#                 continue  # Skip this record

#             # Correct typographical errors (if any)
#             # Example: if 'Foreription' in item:
#             #             item['ForecastDescription'] = item.pop('Foreription')

#             # Flatten StadiumDetails
#             stadium_details = item.pop('StadiumDetails', {})
#             if stadium_details is None:
#                 stadium_details = {}
#             for key, value in stadium_details.items():
#                 item[f'StadiumDetails_{key}'] = value

#             # Data Transformation
#             params = {}

#             # Normalize keys to match placeholders (capitalize first letter)
#             for key in item:
#                 if key.startswith('StadiumDetails_'):
#                     # Capitalize first letter after prefix
#                     suffix = key[len('StadiumDetails_'):]
#                     if suffix:
#                         normalized_key = 'StadiumDetails_' + suffix[0].upper() + suffix[1:]
#                     else:
#                         normalized_key = key  # Edge case where suffix is empty
#                 else:
#                     normalized_key = key[0].upper() + key[1:]
#                 params[normalized_key] = item[key]

#             # Convert date strings to datetime objects
#             date_fields = ["Date", "Day", "DateTime", "DateTimeUTC", "LastUpdated", "GameEndDateTime"]
#             for field in date_fields:
#                 if params.get(field):
#                     params[field] = parse_date(params[field])
#                 else:
#                     params[field] = None

#             # Convert boolean fields to booleans
#             boolean_fields = [
#                 "Canceled", "Closed", "HasStarted", "IsInProgress", "IsOver",
#                 "Has1stQuarterStarted", "Has2ndQuarterStarted", "Has3rdQuarterStarted",
#                 "Has4thQuarterStarted", "IsOvertime", "NeutralVenue", "IsClosed"
#             ]
#             for field in boolean_fields:
#                 if field in params:
#                     value = params[field]
#                     if isinstance(value, str):
#                         params[field] = value.lower() == 'true'
#                     elif value is None:
#                         # Decide on default behavior; here, we default to False
#                         params[field] = False

#             # Handle missing fields: Set to None if not present
#             expected_fields = [
#                 "GameKey", "SeasonType", "Season", "Week", "Date",
#                 "AwayTeam", "HomeTeam", "AwayScore", "HomeScore", "Channel",
#                 "PointSpread", "OverUnder", "Quarter", "TimeRemaining", "Possession",
#                 "Down", "Distance", "YardLine", "YardLineTerritory", "RedZone",
#                 "AwayScoreQuarter1", "AwayScoreQuarter2", "AwayScoreQuarter3",
#                 "AwayScoreQuarter4", "AwayScoreOvertime", "HomeScoreQuarter1",
#                 "HomeScoreQuarter2", "HomeScoreQuarter3", "HomeScoreQuarter4",
#                 "HomeScoreOvertime", "HasStarted", "IsInProgress", "IsOver",
#                 "Has1stQuarterStarted", "Has2ndQuarterStarted", "Has3rdQuarterStarted",
#                 "Has4thQuarterStarted", "IsOvertime", "DownAndDistance",
#                 "QuarterDescription", "StadiumID", "LastUpdated", "GeoLat",
#                 "GeoLong", "ForecastTempLow", "ForecastTempHigh",
#                 "ForecastDescription", "ForecastWindChill", "ForecastWindSpeed",
#                 "AwayTeamMoneyLine", "HomeTeamMoneyLine", "Canceled", "Closed",
#                 "LastPlay", "Day", "DateTime", "AwayTeamID", "HomeTeamID",
#                 "GlobalGameID", "GlobalAwayTeamID", "GlobalHomeTeamID",
#                 "PointSpreadAwayTeamMoneyLine", "PointSpreadHomeTeamMoneyLine",
#                 "ScoreID", "Status", "GameEndDateTime", "HomeRotationNumber",
#                 "AwayRotationNumber", "NeutralVenue", "RefereeID", "OverPayout",
#                 "UnderPayout", "HomeTimeouts", "AwayTimeouts", "DateTimeUTC",
#                 "Attendance", "IsClosed",
#                 "StadiumDetails_StadiumID", "StadiumDetails_Name", "StadiumDetails_City",
#                 "StadiumDetails_State", "StadiumDetails_Country", "StadiumDetails_Capacity",
#                 "StadiumDetails_PlayingSurface", "StadiumDetails_GeoLat", "StadiumDetails_GeoLong",
#                 "StadiumDetails_Type"
#             ]
#             for field in expected_fields:
#                 if field not in params:
#                     params[field] = None  # or set a default value

#             try:
#                 cursor.execute(insert_query, params)
#             except Exception as e:
#                 logger.error(f"Failed to insert/update record: {e}")
#                 logger.error(f"Problematic data item: {item}")
#                 conn.rollback()
#                 continue  # Skip to the next record

#         conn.commit()
#         logger.info("Data inserted/updated successfully.")
#     except Exception as e:
#         if conn:
#             conn.rollback()
#         logger.error(f"Database operation failed: {e}")
#         raise
#     finally:
#         if conn:
#             cursor.close()
#             conn.close()

# def store_timeframe_data(data):
#     """
#     SQL query to insert or update data in the 'timeframes' table. (assumes field names match)
#     """
#     print(f"Running {inspect.currentframe().f_code.co_name}...")
    
#     conn = None
#     try:
#         conn = get_db_connection()
#         cursor = conn.cursor()

#         insert_query = """
#         INSERT INTO timeframes (
#             "ApiSeason", "ApiWeek", "EndDate", "FirstGameEnd", "FirstGameStart",
#             "HasEnded", "HasFirstGameEnded", "HasFirstGameStarted", "HasGames",
#             "HasLastGameEnded", "HasStarted", "LastGameEnd", "Name", "Season",
#             "SeasonType", "ShortName", "StartDate", "Week"
#         ) VALUES (
#             %(ApiSeason)s, %(ApiWeek)s, %(EndDate)s, %(FirstGameEnd)s, %(FirstGameStart)s,
#             %(HasEnded)s, %(HasFirstGameEnded)s, %(HasFirstGameStarted)s, %(HasGames)s,
#             %(HasLastGameEnded)s, %(HasStarted)s, %(LastGameEnd)s, %(Name)s, %(Season)s,
#             %(SeasonType)s, %(ShortName)s, %(StartDate)s, %(Week)s
#         )
#         ON CONFLICT ("ApiSeason", "ShortName") DO UPDATE SET
#             "ApiWeek" = EXCLUDED."ApiWeek",
#             "EndDate" = EXCLUDED."EndDate",
#             "FirstGameEnd" = EXCLUDED."FirstGameEnd",
#             "FirstGameStart" = EXCLUDED."FirstGameStart",
#             "HasEnded" = EXCLUDED."HasEnded",
#             "HasFirstGameEnded" = EXCLUDED."HasFirstGameEnded",
#             "HasFirstGameStarted" = EXCLUDED."HasFirstGameStarted",
#             "HasGames" = EXCLUDED."HasGames",
#             "HasLastGameEnded" = EXCLUDED."HasLastGameEnded",
#             "HasStarted" = EXCLUDED."HasStarted",
#             "LastGameEnd" = EXCLUDED."LastGameEnd",
#             "Name" = EXCLUDED."Name",
#             "Season" = EXCLUDED."Season",
#             "SeasonType" = EXCLUDED."SeasonType",
#             "StartDate" = EXCLUDED."StartDate",
#             "Week" = EXCLUDED."Week";
#         """

#         for item in data:
#             cursor.execute(insert_query, item)

#         conn.commit()
#         print(f"Data inserted/updated successfully.")
#     except Exception as e:
#         if conn:
#             conn.rollback()
#         print(f"Database operation failed: {e}")
#         print(f"Problematic data item: {item}")
#         raise
#     finally:
#         if conn:
#             cursor.close()
#             conn.close()