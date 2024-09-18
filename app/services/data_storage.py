import inspect
from .db_connection import get_db_connection

def store_timeframe_data(data):
    """
    SQL query to insert or update data in the 'timeframes' table. (assumes field names match)
    """
    print(f"Running {inspect.currentframe().f_code.co_name}...")
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO timeframes (
            "ApiSeason", "ApiWeek", "EndDate", "FirstGameEnd", "FirstGameStart",
            "HasEnded", "HasFirstGameEnded", "HasFirstGameStarted", "HasGames",
            "HasLastGameEnded", "HasStarted", "LastGameEnd", "Name", "Season",
            "SeasonType", "ShortName", "StartDate", "Week"
        ) VALUES (
            %(ApiSeason)s, %(ApiWeek)s, %(EndDate)s, %(FirstGameEnd)s, %(FirstGameStart)s,
            %(HasEnded)s, %(HasFirstGameEnded)s, %(HasFirstGameStarted)s, %(HasGames)s,
            %(HasLastGameEnded)s, %(HasStarted)s, %(LastGameEnd)s, %(Name)s, %(Season)s,
            %(SeasonType)s, %(ShortName)s, %(StartDate)s, %(Week)s
        )
        ON CONFLICT ("ApiSeason", "ShortName") DO UPDATE SET
            "ApiWeek" = EXCLUDED."ApiWeek",
            "EndDate" = EXCLUDED."EndDate",
            "FirstGameEnd" = EXCLUDED."FirstGameEnd",
            "FirstGameStart" = EXCLUDED."FirstGameStart",
            "HasEnded" = EXCLUDED."HasEnded",
            "HasFirstGameEnded" = EXCLUDED."HasFirstGameEnded",
            "HasFirstGameStarted" = EXCLUDED."HasFirstGameStarted",
            "HasGames" = EXCLUDED."HasGames",
            "HasLastGameEnded" = EXCLUDED."HasLastGameEnded",
            "HasStarted" = EXCLUDED."HasStarted",
            "LastGameEnd" = EXCLUDED."LastGameEnd",
            "Name" = EXCLUDED."Name",
            "Season" = EXCLUDED."Season",
            "SeasonType" = EXCLUDED."SeasonType",
            "StartDate" = EXCLUDED."StartDate",
            "Week" = EXCLUDED."Week";
        """

        for item in data:
            cursor.execute(insert_query, item)

        conn.commit()
        print(f"Data inserted/updated successfully.")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Database operation failed: {e}")
        print(f"Problematic data item: {item}")
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()

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
            "Key", "TeamID", "PlayerID", "City", "Name",
            "Conference", "Division", "FullName", "StadiumID", "ByeWeek",
            "GlobalTeamID", "HeadCoach", "PrimaryColor", "SecondaryColor", "TertiaryColor",
            "QuaternaryColor", "WikipediaLogoURL", "WikipediaWordMarkURL", "OffensiveCoordinator",
            "DefensiveCoordinator", "SpecialTeamsCoach", "OffensiveScheme", "DefensiveScheme"
        ) VALUES (
            %(Key)s, %(TeamID)s, %(PlayerID)s, %(City)s, %(Name)s,
            %(Conference)s, %(Division)s, %(FullName)s, %(StadiumID)s, %(ByeWeek)s,
            %(GlobalTeamID)s, %(HeadCoach)s, %(PrimaryColor)s, %(SecondaryColor)s, %(TertiaryColor)s,
            %(QuaternaryColor)s, %(WikipediaLogoURL)s, %(WikipediaWordMarkURL)s, %(OffensiveCoordinator)s,
            %(DefensiveCoordinator)s, %(SpecialTeamsCoach)s, %(OffensiveScheme)s, %(DefensiveScheme)s
        )
        ON CONFLICT ("Key") DO UPDATE SET
            "TeamID" = EXCLUDED."TeamID",
            "PlayerID" = EXCLUDED."PlayerID",
            "City" = EXCLUDED."City",
            "Name" = EXCLUDED."Name",
            "Conference" = EXCLUDED."Conference",
            "Division" = EXCLUDED."Division",
            "FullName" = EXCLUDED."FullName",
            "StadiumID" = EXCLUDED."StadiumID",
            "ByeWeek" = EXCLUDED."ByeWeek",
            "GlobalTeamID" = EXCLUDED."GlobalTeamID",
            "HeadCoach" = EXCLUDED."HeadCoach",
            "PrimaryColor" = EXCLUDED."PrimaryColor",
            "SecondaryColor" = EXCLUDED."SecondaryColor",
            "TertiaryColor" = EXCLUDED."TertiaryColor",
            "QuaternaryColor" = EXCLUDED."QuaternaryColor",
            "WikipediaLogoURL" = EXCLUDED."WikipediaLogoURL",
            "WikipediaWordMarkURL" = EXCLUDED."WikipediaWordMarkURL",
            "OffensiveCoordinator" = EXCLUDED."OffensiveCoordinator",
            "DefensiveCoordinator" = EXCLUDED."DefensiveCoordinator",
            "SpecialTeamsCoach" = EXCLUDED."SpecialTeamsCoach",
            "OffensiveScheme" = EXCLUDED."OffensiveScheme",
            "DefensiveScheme" = EXCLUDED."DefensiveScheme";
        """
        for item in data:
            cursor.execute(insert_query, item)

        conn.commit()
        print(f"Data inserted/updated successfully.")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Database operation failed: {e}")
        print(f"Problematic data item: {item}")
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()

def store_players_data(data):
    """
    Inserts or updates data in the 'players' table. Assumes field names from the API match the database columns.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO players (
            "PlayerID", "Team", "Number", "FirstName", "LastName",
            "Position", "Status", "Height", "Weight", "BirthDate",
            "College", "Experience", "FantasyPosition", "Active", "PositionCategory",
            "Name", "Age", "ShortName", "HeightFeet", "HeightInches",
            "TeamID", "GlobalTeamID", "UsaTodayPlayerID", "UsaTodayHeadshotUrl",
            "UsaTodayHeadshotNoBackgroundUrl", "UsaTodayHeadshotUpdated", "UsaTodayHeadshotNoBackgroundUpdated"
        ) VALUES (
            %(PlayerID)s, %(Team)s, %(Number)s, %(FirstName)s, %(LastName)s,
            %(Position)s, %(Status)s, %(Height)s, %(Weight)s, %(BirthDate)s,
            %(College)s, %(Experience)s, %(FantasyPosition)s, %(Active)s, %(PositionCategory)s,
            %(Name)s, %(Age)s, %(ShortName)s, %(HeightFeet)s, %(HeightInches)s,
            %(TeamID)s, %(GlobalTeamID)s, %(UsaTodayPlayerID)s, %(UsaTodayHeadshotUrl)s,
            %(UsaTodayHeadshotNoBackgroundUrl)s, %(UsaTodayHeadshotUpdated)s, %(UsaTodayHeadshotNoBackgroundUpdated)s
        )
        ON CONFLICT ("PlayerID") DO UPDATE SET
            "Team" = EXCLUDED."Team",
            "Number" = EXCLUDED."Number",
            "FirstName" = EXCLUDED."FirstName",
            "LastName" = EXCLUDED."LastName",
            "Position" = EXCLUDED."Position",
            "Status" = EXCLUDED."Status",
            "Height" = EXCLUDED."Height",
            "Weight" = EXCLUDED."Weight",
            "BirthDate" = EXCLUDED."BirthDate",
            "College" = EXCLUDED."College",
            "Experience" = EXCLUDED."Experience",
            "FantasyPosition" = EXCLUDED."FantasyPosition",
            "Active" = EXCLUDED."Active",
            "PositionCategory" = EXCLUDED."PositionCategory",
            "Name" = EXCLUDED."Name",
            "Age" = EXCLUDED."Age",
            "ShortName" = EXCLUDED."ShortName",
            "HeightFeet" = EXCLUDED."HeightFeet",
            "HeightInches" = EXCLUDED."HeightInches",
            "TeamID" = EXCLUDED."TeamID",
            "GlobalTeamID" = EXCLUDED."GlobalTeamID",
            "UsaTodayPlayerID" = EXCLUDED."UsaTodayPlayerID",
            "UsaTodayHeadshotUrl" = EXCLUDED."UsaTodayHeadshotUrl",
            "UsaTodayHeadshotNoBackgroundUrl" = EXCLUDED."UsaTodayHeadshotNoBackgroundUrl",
            "UsaTodayHeadshotUpdated" = EXCLUDED."UsaTodayHeadshotUpdated",
            "UsaTodayHeadshotNoBackgroundUpdated" = EXCLUDED."UsaTodayHeadshotNoBackgroundUpdated"
        ;
        """

        for item in data:
            cursor.execute(insert_query, item)

        conn.commit()
        print("Data inserted/updated successfully.")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Database operation failed: {e}")
        print(f"Problematic data item: {item}")
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()
