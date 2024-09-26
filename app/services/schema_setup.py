from .db_connection import get_db_connection

# def setup_timeframes_table():
#     create_table_query = """
#     CREATE TABLE IF NOT EXISTS timeframes (
#         id SERIAL PRIMARY KEY,
#         "ApiSeason" VARCHAR(20) NOT NULL,
#         "ApiWeek" VARCHAR(10),
#         "EndDate" TIMESTAMP,
#         "FirstGameEnd" TIMESTAMP,
#         "FirstGameStart" TIMESTAMP,
#         "HasEnded" BOOLEAN,
#         "HasFirstGameEnded" BOOLEAN,
#         "HasFirstGameStarted" BOOLEAN,
#         "HasGames" BOOLEAN,
#         "HasLastGameEnded" BOOLEAN,
#         "HasStarted" BOOLEAN,
#         "LastGameEnd" TIMESTAMP,
#         "Name" VARCHAR(100),
#         "Season" INTEGER,
#         "SeasonType" INTEGER,
#         "ShortName" VARCHAR(50) NOT NULL,
#         "StartDate" TIMESTAMP,
#         "Week" INTEGER,
#         UNIQUE ("ApiSeason", "ShortName")
#     );
#     """
#     conn = None
#     try:
#         conn = get_db_connection()
#         cursor = conn.cursor()
#         cursor.execute(create_table_query)
#         conn.commit()
#         print("Table 'timeframes' is set up.")
#     except Exception as e:
#         if conn:
#             conn.rollback()
#         print(f"Failed to set up table 'timeframes': {e}")
#         raise
#     finally:
#         if conn:
#             cursor.close()
#             conn.close()

def setup_teams_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS teams (
        id SERIAL PRIMARY KEY,
        "Code" VARCHAR(10) NOT NULL,
        "Name" VARCHAR(50) NOT NULL,
        "Location" VARCHAR(100) NOT NULL,
        UNIQUE ("Code")
    );
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        print("Table 'teams' is set up.")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Failed to set up table 'teams': {e}")
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()

def setup_players_table():
    create_table_query = """
        CREATE TABLE IF NOT EXISTS players (
            "Code" VARCHAR(20) PRIMARY KEY,
            "Name" VARCHAR(100),
            "Team" VARCHAR(100),
            "TeamCode" VARCHAR(10)
        );
        """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        print("Table 'players' is set up.")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Failed to set up table 'players': {e}")
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()

def setup_games_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS games (
        id SERIAL PRIMARY KEY,
        game_id VARCHAR(10) NOT NULL,
        visitor VARCHAR(50),
        visitor_code VARCHAR(10),
        score_vis INTEGER NULL,  -- Score can be NULL if not available yet
        home VARCHAR(50),
        home_code VARCHAR(10),
        score_home INTEGER NULL, -- Score can be NULL if not available yet
        gamestatus VARCHAR(20) NULL,  -- Status can be NULL for future games
        overtime CHAR(10) NULL,  -- Overtime info can be NULL if not known yet
        winner_code VARCHAR(10) NULL,  -- Winner can be NULL for future games
        loser_code VARCHAR(10) NULL,   -- Loser can be NULL for future games 
        gameday DATE,
        gameno INTEGER,
        venue VARCHAR(100),
        venue_code VARCHAR(10),
        UNIQUE (game_id)
    );
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        print("Table 'games' is set up.")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Failed to set up table 'games': {e}")
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()


# def setup_schedules_table():
#     create_table_query = """
#     CREATE TABLE IF NOT EXISTS schedules (
#         "GameKey" VARCHAR(20) PRIMARY KEY,
#         "SeasonType" INTEGER,
#         "Season" INTEGER,
#         "Week" INTEGER,
#         "Date" TIMESTAMP,
#         "AwayTeam" VARCHAR(10),
#         "HomeTeam" VARCHAR(10),
#         "Channel" VARCHAR(50),
#         "PointSpread" FLOAT,
#         "OverUnder" FLOAT,
#         "StadiumID" INTEGER,
#         "Canceled" BOOLEAN,
#         "GeoLat" FLOAT,
#         "GeoLong" FLOAT,
#         "ForecastTempLow" INTEGER,
#         "ForecastTempHigh" INTEGER,
#         "ForecastDescription" VARCHAR(100),
#         "ForecastWindChill" INTEGER,
#         "ForecastWindSpeed" INTEGER,
#         "AwayTeamMoneyLine" INTEGER,
#         "HomeTeamMoneyLine" INTEGER,
#         "Day" TIMESTAMP,
#         "DateTime" TIMESTAMP,
#         "GlobalGameID" INTEGER,
#         "GlobalAwayTeamID" INTEGER,
#         "GlobalHomeTeamID" INTEGER,
#         "ScoreID" INTEGER,
#         "Status" VARCHAR(20),
#         "IsClosed" BOOLEAN,
#         "DateTimeUTC" TIMESTAMP,
#         -- Flattened StadiumDetails
#         "StadiumDetails_StadiumID" INTEGER,
#         "StadiumDetails_Name" VARCHAR(100),
#         "StadiumDetails_City" VARCHAR(50),
#         "StadiumDetails_State" VARCHAR(50),
#         "StadiumDetails_Country" VARCHAR(50),
#         "StadiumDetails_Capacity" INTEGER,
#         "StadiumDetails_PlayingSurface" VARCHAR(20),
#         "StadiumDetails_GeoLat" FLOAT,
#         "StadiumDetails_GeoLong" FLOAT,
#         "StadiumDetails_Type" VARCHAR(20)
#     );
#     """
#     conn = None
#     try:
#         conn = get_db_connection()
#         cursor = conn.cursor()
#         cursor.execute(create_table_query)
#         conn.commit()
#         print("Table 'schedules' is set up.")
#     except Exception as e:
#         if conn:
#             conn.rollback()
#         print(f"Failed to set up table 'schedules': {e}")
#         raise
#     finally:
#         if conn:
#             cursor.close()
#             conn.close()

# def setup_final_scores_table():
#     create_table_query = """
#     CREATE TABLE IF NOT EXISTS final_scores (
#         "GameKey" VARCHAR(20) PRIMARY KEY,
#         "SeasonType" INTEGER,
#         "Season" INTEGER,
#         "Week" INTEGER,
#         "Date" TIMESTAMP,
#         "AwayTeam" VARCHAR(10),
#         "HomeTeam" VARCHAR(10),
#         "AwayScore" INTEGER,
#         "HomeScore" INTEGER,
#         "Channel" VARCHAR(50),
#         "PointSpread" FLOAT,
#         "OverUnder" FLOAT,
#         "Quarter" VARCHAR(5),
#         "TimeRemaining" VARCHAR(10),
#         "Possession" VARCHAR(10),
#         "Down" INTEGER,
#         "Distance" VARCHAR(20),
#         "YardLine" INTEGER,
#         "YardLineTerritory" VARCHAR(20),
#         "RedZone" BOOLEAN,
#         "AwayScoreQuarter1" INTEGER,
#         "AwayScoreQuarter2" INTEGER,
#         "AwayScoreQuarter3" INTEGER,
#         "AwayScoreQuarter4" INTEGER,
#         "AwayScoreOvertime" INTEGER,
#         "HomeScoreQuarter1" INTEGER,
#         "HomeScoreQuarter2" INTEGER,
#         "HomeScoreQuarter3" INTEGER,
#         "HomeScoreQuarter4" INTEGER,
#         "HomeScoreOvertime" INTEGER,
#         "HasStarted" BOOLEAN,
#         "IsInProgress" BOOLEAN,
#         "IsOver" BOOLEAN,
#         "Has1stQuarterStarted" BOOLEAN,
#         "Has2ndQuarterStarted" BOOLEAN,
#         "Has3rdQuarterStarted" BOOLEAN,
#         "Has4thQuarterStarted" BOOLEAN,
#         "IsOvertime" BOOLEAN,
#         "DownAndDistance" VARCHAR(20),
#         "QuarterDescription" VARCHAR(50),
#         "StadiumID" INTEGER,
#         "LastUpdated" TIMESTAMP,
#         "GeoLat" FLOAT,
#         "GeoLong" FLOAT,
#         "ForecastTempLow" INTEGER,
#         "ForecastTempHigh" INTEGER,
#         "ForecastDescription" VARCHAR(100),
#         "ForecastWindChill" INTEGER,
#         "ForecastWindSpeed" INTEGER,
#         "AwayTeamMoneyLine" INTEGER,
#         "HomeTeamMoneyLine" INTEGER,
#         "Canceled" BOOLEAN,
#         "Closed" BOOLEAN,
#         "LastPlay" VARCHAR(100),
#         "Day" TIMESTAMP,
#         "DateTime" TIMESTAMP,
#         "AwayTeamID" INTEGER,
#         "HomeTeamID" INTEGER,
#         "GlobalGameID" INTEGER,
#         "GlobalAwayTeamID" INTEGER,
#         "GlobalHomeTeamID" INTEGER,
#         "PointSpreadAwayTeamMoneyLine" INTEGER,
#         "PointSpreadHomeTeamMoneyLine" INTEGER,
#         "ScoreID" INTEGER,
#         "Status" VARCHAR(20),
#         "GameEndDateTime" TIMESTAMP,
#         "HomeRotationNumber" INTEGER,
#         "AwayRotationNumber" INTEGER,
#         "NeutralVenue" BOOLEAN,
#         "RefereeID" INTEGER,
#         "OverPayout" INTEGER,
#         "UnderPayout" INTEGER,
#         "HomeTimeouts" INTEGER,
#         "AwayTimeouts" INTEGER,
#         "DateTimeUTC" TIMESTAMP,
#         "Attendance" INTEGER,
#         "IsClosed" BOOLEAN,
#         -- Flattened StadiumDetails
#         "StadiumDetails_StadiumID" INTEGER,
#         "StadiumDetails_Name" VARCHAR(100),
#         "StadiumDetails_City" VARCHAR(50),
#         "StadiumDetails_State" VARCHAR(50),
#         "StadiumDetails_Country" VARCHAR(50),
#         "StadiumDetails_Capacity" INTEGER,
#         "StadiumDetails_PlayingSurface" VARCHAR(20),
#         "StadiumDetails_GeoLat" FLOAT,
#         "StadiumDetails_GeoLong" FLOAT,
#         "StadiumDetails_Type" VARCHAR(20)
#     );
#     """
#     conn = None
#     try:
#         conn = get_db_connection()
#         cursor = conn.cursor()
#         cursor.execute(create_table_query)
#         conn.commit()
#         print("Table 'scores' is set up.")
#     except Exception as e:
#         if conn:
#             conn.rollback()
#         print(f"Failed to set up table 'scores': {e}")
#         raise
#     finally:
#         if conn:
#             cursor.close()
#             conn.close()
