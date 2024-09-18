from .db_connection import get_db_connection

from .db_connection import get_db_connection

def setup_timeframes_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS timeframes (
        id SERIAL PRIMARY KEY,
        "ApiSeason" VARCHAR(20) NOT NULL,
        "ApiWeek" VARCHAR(10),
        "EndDate" TIMESTAMP,
        "FirstGameEnd" TIMESTAMP,
        "FirstGameStart" TIMESTAMP,
        "HasEnded" BOOLEAN,
        "HasFirstGameEnded" BOOLEAN,
        "HasFirstGameStarted" BOOLEAN,
        "HasGames" BOOLEAN,
        "HasLastGameEnded" BOOLEAN,
        "HasStarted" BOOLEAN,
        "LastGameEnd" TIMESTAMP,
        "Name" VARCHAR(100),
        "Season" INTEGER,
        "SeasonType" INTEGER,
        "ShortName" VARCHAR(50) NOT NULL,
        "StartDate" TIMESTAMP,
        "Week" INTEGER,
        UNIQUE ("ApiSeason", "ShortName")
    );
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        print("Table 'timeframes' is set up.")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Failed to set up table 'timeframes': {e}")
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()

def setup_teams_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS teams (
        id SERIAL PRIMARY KEY,
        "Key" VARCHAR(10) NOT NULL,
        "TeamID" INTEGER,
        "PlayerID" INTEGER,
        "City" VARCHAR(50) NOT NULL,
        "Name" VARCHAR(50) NOT NULL,
        "Conference" VARCHAR(20) NOT NULL,
        "Division" VARCHAR(20) NOT NULL,
        "FullName" VARCHAR(100) NOT NULL,
        "StadiumID" INTEGER,
        "ByeWeek" INTEGER,
        "GlobalTeamID" INTEGER,
        "HeadCoach" VARCHAR(50),
        "PrimaryColor" VARCHAR(10),
        "SecondaryColor" VARCHAR(10),
        "TertiaryColor" VARCHAR(10),
        "QuaternaryColor" VARCHAR(10),
        "WikipediaLogoURL" VARCHAR(200),
        "WikipediaWordMarkURL" VARCHAR(200),
        "OffensiveCoordinator" VARCHAR(50),
        "DefensiveCoordinator" VARCHAR(50),
        "SpecialTeamsCoach" VARCHAR(50),
        "OffensiveScheme" VARCHAR(20),
        "DefensiveScheme" VARCHAR(20),
        UNIQUE ("Key")
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
        "PlayerID" INTEGER PRIMARY KEY,
        "Team" VARCHAR(10),
        "Number" INTEGER,
        "FirstName" VARCHAR(50),
        "LastName" VARCHAR(50),
        "Position" VARCHAR(5),
        "Status" VARCHAR(50),
        "Height" VARCHAR(10),
        "Weight" INTEGER,
        "BirthDate" TIMESTAMP,
        "College" VARCHAR(100),
        "Experience" INTEGER,
        "FantasyPosition" VARCHAR(10),
        "Active" BOOLEAN,
        "PositionCategory" VARCHAR(10),
        "Name" VARCHAR(100),
        "Age" INTEGER,
        "ShortName" VARCHAR(50),
        "HeightFeet" INTEGER,
        "HeightInches" INTEGER,
        "TeamID" INTEGER,
        "GlobalTeamID" INTEGER,
        "UsaTodayPlayerID" INTEGER,
        "UsaTodayHeadshotUrl" VARCHAR(200),
        "UsaTodayHeadshotNoBackgroundUrl" VARCHAR(200),
        "UsaTodayHeadshotUpdated" TIMESTAMP,
        "UsaTodayHeadshotNoBackgroundUpdated" TIMESTAMP
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