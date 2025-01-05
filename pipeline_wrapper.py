from utils import *
import pandas as pd
from sqlalchemy import create_engine
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from joblib import Parallel, delayed

import warnings

# Ignore all warnings
warnings.filterwarnings("ignore")

def load_seasons_table() :
    try:
        parent_url = "https://www.espncricinfo.com/ci/engine/series/index.html?view=season"
        engine = create_engine('postgresql://postgres:superuser@localhost:5432/postgres')
        with engine.connect() as conn:
            conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('temp_seasons', 'copy from local csv', 'start')")
            conn.connection.commit()
            print('connected')
            conn.execute("""
                CREATE TEMP TABLE temp_seasons (
                    period TEXT,
                    link_prefix TEXT
                );
            """)
            print('temp created in staging')
            df = seasons_and_urls(parent_url)
            temp_csv_path = 'staging_files/temp_seasons.csv'
            df.to_csv(temp_csv_path, index=False, header=False)
            print('tmp file created')
            with open(temp_csv_path, 'r') as f:
                conn.connection.cursor().copy_expert(
                    "COPY temp_seasons (period, link_prefix) FROM STDIN WITH CSV", f
                )
                conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('temp_seasons', 'copy from local csv', 'complete')")
                conn.connection.commit()
            print('temp table loaded')
            conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.seasons', 'merge', 'start')")
            conn.connection.commit()
            merge_statement = """
            MERGE INTO cricinfo.seasons s
        USING (SELECT period, link_prefix 
            FROM temp_seasons) AS T
        ON T.period = s.period --AND T.link_prefix = s.link_prefix
        WHEN MATCHED THEN 
            UPDATE SET 
                link_prefix = T.link_prefix,
                last_updated = NOW()
        WHEN NOT MATCHED THEN
            INSERT (period, link_prefix)
            VALUES (T.period, T.link_prefix);
            """
            conn.execute(merge_statement)
            conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.seasons', 'merge', 'complete')")
            conn.connection.commit()
            print('loaded seasons')
    except Exception as e:
        print(str(e).replace("'", ""))
        engine = create_engine('postgresql://postgres:superuser@localhost:5432/postgres')
        with engine.connect() as conn:
            conn.execute(f"""INSERT INTO cricinfo.operation_logs (table_name, operation, status, exception) VALUES ('cricinfo.seasons', 'load', 'failed', '{str(e).replace("'", "")}')""")
            conn.connection.commit()


def load_series_table(season_url = None) :

    # series_id,year,series_name,match_type,url

    try:
        #parent_url = "https://www.espncricinfo.com/ci/engine/series/index.html?view=season"
        engine = create_engine('postgresql://postgres:superuser@localhost:5432/postgres')
        with engine.connect() as conn:
            conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('temp_seasons', 'copy from local csv', 'start')")
            conn.connection.commit()
            print('connected')
            conn.execute("""
                CREATE TEMP TABLE temp_series (
                    series_id text,
                    start_year text,
                    end_year text,
                    series_name text,
                    match_type text,
                    url text
                );
            """)
            print('temp created in staging')
            #df = seasons_and_urls(extract_series_from_a_season)           
            query = "SELECT DISTINCT link_prefix FROM cricinfo.seasons"
            if season_url is None:
                seasons_links = pd.read_sql_query(query, engine)['link_prefix'].tolist()
            else :
                seasons_links = [season_url]
            #print(seasons_links)
            df = pd.DataFrame()
            for i, season in enumerate(seasons_links):
                # if i == 20 :
                #     break
                print(season)
                df = df.append(extract_series_from_a_season("https://www.espncricinfo.com"+season))

            df = (
                    df.groupby("series_id")
                    .agg(
                        start_year=("year", lambda x: extract_start_end(x)[0]),
                        end_year=("year", lambda x: extract_start_end(x)[1]),
                        series=("series_name", "first"),
                        type=("match_type", "first"),
                        url=("url", "first"),
                    )
                    .reset_index()
                )
            #print('getting df now')
            #print(df)
            temp_csv_path = 'staging_files/temp_seasons.csv'
            df.to_csv(temp_csv_path, index=False, header=False)
            print('tmp file created')
            with open(temp_csv_path, 'r') as f:
                conn.connection.cursor().copy_expert(
                    "COPY temp_series (series_id, start_year, end_year, series_name, match_type, url) FROM STDIN WITH CSV", f
                )
                conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('temp_series', 'copy from local csv', 'complete')")
                conn.connection.commit()
            print('temp table loaded')
            conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.series', 'merge', 'start')")
            conn.connection.commit()
            merge_statement = """
            MERGE INTO cricinfo.series s
        USING (SELECT series_id,start_year, end_year, series_name, match_type, url
            FROM temp_series) AS T
        ON T.series_id = s.series_id --AND T.link_prefix = s.link_prefix
        WHEN MATCHED THEN 
            UPDATE SET 
                start_year = T.start_year::INT, 
                end_year = T.end_year::INT, 
                series_name = T.series_name, 
                match_type = T.match_type, 
                url = T.url,
                last_updated = NOW()
        WHEN NOT MATCHED THEN
            INSERT (series_id, start_year, end_year, series_name,match_type,url)
            VALUES (T.series_id, T.start_year::INT, T.end_year::INT, T.series_name,T.match_type,T.url);
            """
            conn.execute(merge_statement)
            conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.series', 'merge', 'complete')")
            conn.connection.commit()
            print('loaded seasons')
    except Exception as e:
        print(str(e).replace("'", ""))
        engine = create_engine('postgresql://postgres:superuser@localhost:5432/postgres')
        with engine.connect() as conn:
            conn.execute(f"""INSERT INTO cricinfo.operation_logs (table_name, operation, status, exception) VALUES ('cricinfo.series', 'load', 'failed', '{str(e).replace("'", "")}')""")
            conn.connection.commit()


def process_season(series, series_id) :
    #series_id = '14588'
    #print(series_id)

    start_time = time.time()
    engine = create_engine('postgresql://postgres:superuser@localhost:5432/postgres')
    with engine.connect() as conn:
        try:

            

            # if i ==20 :
            #     print(i, series)
            #     break
            #print(series)
            #if series == "https://www.espncricinfo.com/ci/engine/match/index/series.html?series=13216":
            # series = "https://www.espncricinfo.com/ci/engine/match/index/series.html?series=14588"
            # series_id = '14588'

            match_temp, teams_temp, innings_temp, result_temp, squads_temp, players_temp, batting_innings_temp, bowling_innings_temp = scrape_series_matches(series)
            match_temp['series_id'] = series_id
            temp_match_path = 'staging_files/temp_match.csv'
            temp_teams_path = 'staging_files/temp_teams.csv'
            temp_innings_path = 'staging_files/temp_innings.csv'
            temp_result_path = 'staging_files/temp_result.csv'
            temp_squads_path = 'staging_files/temp_squads.csv'
            temp_players_path = 'staging_files/temp_players.csv'
            temp_batting_innings_path = 'staging_files/temp_batting_innings.csv'
            temp_bowling_innings_path = 'staging_files/temp_bowling_innings.csv'
            

            match_temp.to_csv(temp_match_path, index=False, header=False)
            teams_temp.to_csv(temp_teams_path, index=False, header=False)
            innings_temp.to_csv(temp_innings_path, index=False, header=False)
            result_temp.to_csv(temp_result_path, index=False, header=False)
            squads_temp.to_csv(temp_squads_path, index=False, header=False)
            players_temp.to_csv(temp_players_path, index=False, header=False)
            batting_innings_temp.to_csv(temp_batting_innings_path, index=False, header=False)
            bowling_innings_temp.to_csv(temp_bowling_innings_path, index=False, header=False)


            # match_temp.to_csv(temp_match_path, index=False, header=True)
            # teams_temp.to_csv(temp_teams_path, index=False, header=True)
            # innings_temp.to_csv(temp_innings_path, index=False, header=True)
            # result_temp.to_csv(temp_result_path, index=False, header=True)
            # squads_temp.to_csv(temp_squads_path, index=False, header=True)
            # players_temp.to_csv(temp_players_path, index=False, header=True)
            # batting_innings_temp.to_csv(temp_batting_innings_path, index=False, header=True)
            # bowling_innings_temp.to_csv(temp_bowling_innings_path, index=False, header=True)
            #print('csvs created')

            ##Table 1: Teams################################
            
            with open(temp_teams_path, 'r') as f:
                conn.execute("""
                    DROP TABLE IF EXISTS temp_teams;
                    CREATE TEMP TABLE temp_teams (
                        team_id TEXT,
                        team_name TEXT
                    );
                """)
                #print('temp created in staging')

                #print('temp_match_path found')
                conn.connection.cursor().copy_expert(
                    "COPY temp_teams (team_id,team_name) FROM STDIN WITH CSV", f
                )
                #print('temp_teams table loaded from csv')
                conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('temp_teams', 'copy from local csv', 'complete')")
                conn.connection.commit()
                #print('temp table loaded')
                conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.teams', 'merge', 'start')")
                conn.connection.commit()
                merge_statement = """
                MERGE INTO cricinfo.teams s
            USING (SELECT team_id,team_name
                FROM temp_teams) AS T
            ON T.team_id = s.team_id --AND T.link_prefix = s.link_prefix
            WHEN MATCHED THEN 
                UPDATE SET 
                    team_id = T.team_id,
                    team_name = T.team_name,
                    last_updated = NOW()
            WHEN NOT MATCHED THEN
                INSERT (team_id,team_name)
                VALUES (T.team_id,T.team_name);
                """
                conn.execute(merge_statement)
                conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.series', 'merge', 'complete')")
                conn.connection.commit()
        #         #print('loaded teams')



            ## Table 2: Match############################################################################################
            with open(temp_match_path, 'r') as f:
                #print('ins')
                
                conn.execute("""
                DROP TABLE IF EXISTS temp_match;
                    CREATE TEMP TABLE temp_match (
                        match_id TEXT, 
                        match_no TEXT, 
                        date TEXT, 
                        venue TEXT, 
                        team1 TEXT, 
                        team2 TEXT, 
                        result TEXT, 
                        match_url TEXT, 
                        team1_innings TEXT, 
                        team2_innings TEXT, 
                        winner TEXT, 
                        win_by TEXT, 
                        win_margin TEXT,
                        series_id TEXT
                    );
                """)
                #print('temp created in staging')




                #print('temp_match_path found')
                conn.connection.cursor().copy_expert(
                    "COPY temp_match (match_id,match_no,date,venue,team1,team2,result,match_url,team1_innings,team2_innings,  winner, win_by, win_margin, series_id) FROM STDIN WITH CSV", f
                )
                #print('temp_match table loaded from csv')
                conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('temp_match', 'copy from local csv', 'complete')")
                conn.connection.commit()
            #print('temp matcg loaded')
            conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.match', 'merge', 'start')")
            conn.connection.commit()
            merge_statement = """
            MERGE INTO cricinfo.match s
        USING (SELECT match_id,match_no,date,venue,team1,team2,result,match_url,team1_innings,team2_innings,winner, win_by,win_margin, series_id
            FROM temp_match) AS T
        ON T.match_id = s.match_id and T.series_id = s.series_id --AND T.link_prefix = s.link_prefix
        WHEN MATCHED THEN 
            UPDATE SET 
                match_id = T.match_id,
                match_no = T.match_no,
                date = T.date::DATE,
                venue = T.venue,
                team1 = T.team1,
                team2 = T.team2,
                result = T.result,
                match_url = T.match_url,
                team1_innings = T.team1_innings,
                team2_innings = T.team2_innings,
                win_by = T.win_by,
                winner = T.winner,
                win_margin = T.win_margin::INT,
                series_id = T.series_id,
                last_updated = NOW()
        WHEN NOT MATCHED THEN
            INSERT (match_id,match_no,date,venue,team1,team2,result,match_url,team1_innings,team2_innings,winner, win_by,win_margin, series_id)
            VALUES (T.match_id,T.match_no,T.date::DATE,T.venue,T.team1,T.team2,T.result,T.match_url,T.team1_innings,T.team2_innings,T.winner, T.win_by,T.win_margin::INT, T.series_id);
            """
            conn.execute(merge_statement)
            conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.series', 'merge', 'complete')")
            conn.connection.commit()
        #     #print('loaded matches')


            ## Table 3 - Innings
            
            with open(temp_innings_path, 'r') as f:
                
                conn.execute("""
                DROP TABLE IF EXISTS temp_innings;
                    CREATE TEMP TABLE temp_innings (
                        match_id TEXT,
                        team_id TEXT,
                        runs TEXT,
                        wickets TEXT,
                        overs_played TEXT,
                        inning_number TEXT
                    );
                """)
                #print('temp created in staging')



                #print('temp_match_path found')
                conn.connection.cursor().copy_expert(
                    "COPY temp_innings (match_id, team_id, runs, wickets, overs_played, inning_number) FROM STDIN WITH CSV", f
                )
                #print('temp_innings table loaded from csv')
                conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('temp_innings', 'copy from local csv', 'complete')")
                conn.connection.commit()
            #print('temp table loaded')
            conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.innings', 'merge', 'start')")
            conn.connection.commit()
            merge_statement = """
            MERGE INTO cricinfo.innings s
        USING (SELECT match_id, team_id, runs, wickets, overs_played, inning_number
            FROM temp_innings) AS T
        ON T.match_id = s.match_id AND T.inning_number::INT = s.inning_number AND T.team_id = s.team_id
        WHEN MATCHED THEN 
            UPDATE SET 
                match_id = T.match_id,
                team_id = T.team_id,
                runs = T.runs::INT,
                wickets = T.wickets::INT,
                overs_played = T.overs_played::NUMERIC(5,2),
                inning_number = T.inning_number::INT,
                last_updated = NOW()
        WHEN NOT MATCHED THEN
            INSERT (match_id, team_id, runs, wickets, overs_played, inning_number)
            VALUES (T.match_id, T.team_id, T.runs::INT, T.wickets::INT, T.overs_played::NUMERIC(5,2), T.inning_number::INT);
            """
            conn.execute(merge_statement)
            conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.innings', 'merge', 'complete')")
            conn.connection.commit()
            #print('loaded matches')



            ## Table 4: Players
            with open(temp_players_path, 'r') as f:
                conn.execute("""
                    DROP TABLE IF EXISTS temp_players;
                    CREATE TEMP TABLE temp_players (
                        player_id TEXT,
                        card_short TEXT,
                        known_as TEXT,
                        dob TEXT,
                        country_id TEXT,
                        batting_hand TEXT,
                        batting_style TEXT,
                        bowling_hand TEXT,
                        bowling_pacespin TEXT,
                        bowling_style TEXT
                    );
                """)
                conn.connection.cursor().copy_expert(
                    "COPY temp_players (player_id, card_short, known_as, dob, country_id, batting_hand, batting_style, bowling_hand, bowling_pacespin, bowling_style) FROM STDIN WITH CSV", f
                )
                conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('temp_players', 'copy from local csv', 'complete')")
                conn.connection.commit()
                conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.players', 'merge', 'start')")
                conn.connection.commit()
                merge_statement = """
                MERGE INTO cricinfo.players s
                USING (SELECT DISTINCT player_id, known_as, dob, country_id, batting_hand, batting_style, bowling_hand, bowling_pacespin, bowling_style
                    FROM temp_players) AS T
                ON T.player_id = s.player_id
                WHEN MATCHED THEN 
                    UPDATE SET 
                        known_as = T.known_as,
                        dob = T.dob::DATE,
                        country_id = T.country_id,
                        batting_hand = T.batting_hand,
                        batting_style = T.batting_style,
                        bowling_hand = T.bowling_hand,
                        bowling_pacespin = T.bowling_pacespin,
                        bowling_style = T.bowling_style,
                        last_updated = NOW()
                WHEN NOT MATCHED THEN
                    INSERT (player_id,  known_as, dob, country_id, batting_hand, batting_style, bowling_hand, bowling_pacespin, bowling_style)
                    VALUES (T.player_id,  T.known_as, T.dob::DATE, T.country_id, T.batting_hand, T.batting_style, T.bowling_hand, T.bowling_pacespin, T.bowling_style);
                """
                conn.execute(merge_statement)
                conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.players', 'merge', 'complete')")
                conn.connection.commit()


            ## Table 5: Squads
            with open(temp_squads_path, 'r') as f:
                conn.execute("""
                    DROP TABLE IF EXISTS temp_squads;
                    CREATE TEMP TABLE temp_squads (
                        player_id TEXT,
                        match_id TEXT,
                        team_id TEXT
                    );
                """)
                conn.connection.cursor().copy_expert(
                    "COPY temp_squads (player_id, match_id, team_id) FROM STDIN WITH CSV", f
                )
                conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('temp_squads', 'copy from local csv', 'complete')")
                conn.connection.commit()
                conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.squads', 'merge', 'start')")
                conn.connection.commit()
                merge_statement = """
                MERGE INTO cricinfo.squads s
                USING (SELECT DISTINCT player_id, match_id, team_id
                    FROM temp_squads) AS T
                ON T.player_id = s.player_id AND T.match_id = s.match_id AND T.team_id = s.team_id
                WHEN MATCHED THEN 
                    UPDATE SET 
                        player_id = T.player_id,
                        match_id = T.match_id,
                        team_id = T.team_id,
                        last_updated = NOW()
                WHEN NOT MATCHED THEN
                    INSERT (player_id, match_id, team_id)
                    VALUES (T.player_id, T.match_id, T.team_id);
                """
                conn.execute(merge_statement)
                conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.squads', 'merge', 'complete')")
                conn.connection.commit()




            ## Table 6: Batting Innings
            with open(temp_batting_innings_path, 'r') as f:
                conn.execute("""
                    DROP TABLE IF EXISTS temp_batting_innings;
                    CREATE TEMP TABLE temp_batting_innings (
                        match_id TEXT,
                        innings_number TEXT,
                        Batter TEXT,
                        runs TEXT,
                        balls TEXT,
                        fours TEXT,
                        sixes TEXT,
                        strike_rate TEXT,
                        in_at TEXT,
                        is_out BOOLEAN,
                        bowler TEXT,
                        wicket_number TEXT,
                        wicket_fell_at TEXT,
                        player_id TEXT,
                        bowler_id TEXT
                    );
                """)
                conn.connection.cursor().copy_expert(
                    """
                    COPY temp_batting_innings (
                        match_id, 
                        innings_number, 
                        Batter, 
                        runs, 
                        balls, 
                        fours, 
                        sixes, 
                        strike_rate, 
                        in_at, 
                        is_out, 
                        bowler, 
                        wicket_number, 
                        wicket_fell_at, 
                        player_id, 
                        bowler_id
                    ) FROM STDIN WITH CSV
                    """, 
                    f
                )
                conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('temp_batting_innings', 'copy from local csv', 'complete')")
                conn.connection.commit()
                conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.batting_innings', 'merge', 'start')")
                conn.connection.commit()
                merge_statement = """
                MERGE INTO cricinfo.batting_innings s
                USING (SELECT 
                            match_id, 
                            innings_number::INT, 
                            Batter, 
                            runs::INT, 
                            balls::INT, 
                            fours::INT, 
                            sixes::INT, 
                            strike_rate::NUMERIC, 
                            in_at, 
                            is_out, 
                            bowler, 
                            bowler_id,
                            wicket_number::INT, 
                            wicket_fell_at::INT, 
                            player_id
                        FROM temp_batting_innings
                        WHERE match_id IS NOT NULL AND innings_number IS NOT NULL AND player_id IS NOT NULL) AS T
                ON T.match_id = s.match_id AND T.innings_number = s.innings_number AND T.player_id = s.player_id
                
                WHEN MATCHED THEN 
                    UPDATE SET 
                        Batter = T.Batter,
                        runs = T.runs,
                        balls = T.balls,
                        fours = T.fours,
                        sixes = T.sixes,
                        strike_rate = T.strike_rate,
                        in_at = T.in_at,
                        is_out = T.is_out,
                        bowler = T.bowler,
                        bowler_id = T.bowler_id,
                        wicket_number = T.wicket_number,
                        wicket_fell_at = T.wicket_fell_at,
                        last_updated = NOW()
                WHEN NOT MATCHED THEN
                    INSERT (match_id, innings_number, Batter, runs, balls, fours, sixes, strike_rate, in_at, is_out, bowler, bowler_id, wicket_number, wicket_fell_at, player_id)
                    VALUES (T.match_id, T.innings_number, T.Batter, T.runs, T.balls, T.fours, T.sixes, T.strike_rate, T.in_at, T.is_out, T.bowler, T.bowler_id, T.wicket_number, T.wicket_fell_at, T.player_id)
                    ;
                """
                conn.execute(merge_statement)
                conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.batting_innings', 'merge', 'complete')")
                conn.connection.commit()

        

            ## Table 7: Bowling Innings
            with open(temp_bowling_innings_path, 'r') as f:
                # Drop and create the temporary table with updated column names
                conn.execute("""
                    DROP TABLE IF EXISTS temp_bowling_innings;
                    CREATE TEMP TABLE temp_bowling_innings (
                        match_id TEXT,
                        innings_number TEXT,
                        bowler_name TEXT, -- Renamed from 'Bowling'
                        overs TEXT,       -- Renamed from 'O'
                        maidens TEXT,     -- Renamed from 'M'
                        runs TEXT,        -- Renamed from 'R'
                        wickets TEXT,     -- Renamed from 'W'
                        economy TEXT,     -- Renamed from 'Econ'
                        dot_balls TEXT,   -- Added column for '0s'
                        fours_conceded TEXT, -- Added column for '4s'
                        sixes_conceded TEXT, -- Added column for '6s'
                        wides TEXT,       -- Added column for 'WD'
                        no_balls TEXT,    -- Added column for 'NB'
                        player_id TEXT
                    );
                """)

                # Copy data into the temporary table from the CSV file
                conn.connection.cursor().copy_expert(


                    """
                    COPY temp_bowling_innings (
                        bowler_name,
                        overs,
                        maidens,
                        runs,
                        wickets,
                        economy,
                        dot_balls,
                        fours_conceded,
                        sixes_conceded,
                        wides,
                        no_balls,
                        innings_number,
                        match_id,
                        player_id 
                    ) FROM STDIN WITH CSV
                    """, f
                )

                # Log the completion of data copy
                conn.execute(f"""
                    INSERT INTO cricinfo.operation_logs (table_name, operation, status)
                    VALUES ('temp_bowling_innings', 'copy from local csv', 'complete')
                """)
                conn.connection.commit()

                # Log the start of the merge operation
                conn.execute(f"""
                    INSERT INTO cricinfo.operation_logs (table_name, operation, status)
                    VALUES ('cricinfo.bowling_innings', 'merge', 'start')
                """)
                conn.connection.commit()

                # Updated MERGE statement with renamed columns
                merge_statement = """
                    MERGE INTO cricinfo.bowling_innings s
                    USING (
                        SELECT 
                            match_id,
                            innings_number::INT AS innings_number,
                            bowler_name,
                            overs::NUMERIC AS overs,
                            maidens::INT AS maidens,
                            runs::INT AS runs,
                            wickets::INT AS wickets,
                            economy::NUMERIC AS economy,
                            dot_balls::INT AS dot_balls,
                            fours_conceded::INT AS fours_conceded,
                            sixes_conceded::INT AS sixes_conceded,
                            wides::INT AS wides,
                            no_balls::INT AS no_balls,
                            player_id
                        FROM temp_bowling_innings
                        WHERE match_id IS NOT NULL AND innings_number IS NOT NULL AND player_id IS NOT NULL
                    ) AS T
                    ON T.match_id = s.match_id AND T.innings_number = s.innings_number AND T.player_id = s.player_id
                    
                    WHEN MATCHED THEN 
                        UPDATE SET 
                            bowler_name = T.bowler_name,
                            overs = T.overs,
                            maidens = T.maidens,
                            runs = T.runs,
                            wickets = T.wickets,
                            economy = T.economy,
                            dot_balls = T.dot_balls,
                            fours_conceded = T.fours_conceded,
                            sixes_conceded = T.sixes_conceded,
                            wides = T.wides,
                            no_balls = T.no_balls,
                            last_updated = NOW()
                    WHEN NOT MATCHED THEN
                        INSERT (match_id, innings_number, bowler_name, overs, maidens, runs, wickets, economy, dot_balls, fours_conceded, sixes_conceded, wides, no_balls, player_id)
                        VALUES (T.match_id, T.innings_number, T.bowler_name, T.overs, T.maidens, T.runs, T.wickets, T.economy, T.dot_balls, T.fours_conceded, T.sixes_conceded, T.wides, T.no_balls, T.player_id)
                        ;
                """
                
                # Execute the MERGE statement
                conn.execute(merge_statement)

                # Log the completion of the merge operation
                conn.execute(f"""
                    INSERT INTO cricinfo.operation_logs (table_name, operation, status)
                    VALUES ('cricinfo.bowling_innings', 'merge', 'complete')
                """)
                conn.connection.commit()
            query = """
            select end_year from cricinfo.series where series_id = '""" + str(series_id) + """'
            """
            #if series_url is None:
            end_year = pd.read_sql_query(query, engine)['end_year'].tolist()[0]
            #print(end_year)

            if end_year<datetime.now().year :
                query = """
            UPDATE cricinfo.series
                SET load_complete = 'Y'
            WHERE series_id = '""" + str(series_id) + """'
            """
                with engine.connect() as conn:
                    conn.execute(query)
                    conn.connection.commit()
            
        except Exception as e:
            print(str(e).replace("'", ""))
            print(str(e).replace("'", ""))
            with open("error_log.txt", "a") as file:
                file.write(f"Can't write this series: series_id {series_id} {e}\n")
            engine = create_engine('postgresql://postgres:superuser@localhost:5432/postgres')
            with engine.connect() as conn:
                conn.execute(f"""INSERT INTO cricinfo.operation_logs (table_name, operation, status, exception) VALUES ('cricinfo.series', 'load', 'failed', '{str(e).replace("'", "")}')""")
                conn.connection.commit()
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        mins, secs = divmod(elapsed_time, 60)
        # Print in minutes and seconds
        print(f"Time taken for series index something, series id {series_id}: {int(mins)} minutes and {secs:.2f} seconds")


from joblib import Parallel, delayed

# Sample DataFrame
data = {
    'col1': [1, 2, 3, 4, 5],
    'col2': ['A', 'B', 'C', 'D', 'E']
}
df = pd.DataFrame(data)

# Function to apply
# Function to fetch the country of a player
def get_country(player_id):
    try:
        url = f"http://core.espnuk.org/v2/sports/cricket/athletes/{player_id}"

        # Fetch the data
        response = requests.get(url)

        # Check if the request was successful
        if response.status_code == 200:
            # Parse the JSON data
            data = response.json()
            if data.get('country', 'Unknown') != 'Unknown':
                print(data.get('country', 'Unknown'))
            return data.get('country', 'Unknown')  # Return 'Unknown' if 'country' is not in the response
        else:
            print(f"Failed to fetch data for player {player_id}. Status code: {response.status_code}")
            return "Unknown"
    except Exception as e:
        print(f"Failed to fetch data for player {player_id}. Error: {e}")
        return "Unknown"

# Parallel apply function
def parallel_apply(df, func, column, n_jobs=-1):
    # Apply the function in parallel
    results = Parallel(n_jobs=n_jobs)(
        delayed(func)(value) for value in df[column]
    )
    return results

# Function to update countries
def update_countries():
    engine = create_engine('postgresql://postgres:superuser@localhost:5432/postgres')
    with engine.connect() as conn:
        conn.execute(
            f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('temp_seasons', 'copy from local csv', 'start')"
        )
        conn.connection.commit()
        print('Connected to the database')
        
        # Query to fetch player IDs
        query = '''
        SELECT DISTINCT player_id FROM cricinfo.players 
        '''
        players = pd.read_sql_query(query, engine)
        
        # Use parallel_apply to get countries
        players['country'] = parallel_apply(players, get_country, column='player_id', n_jobs=4)
        players.to_csv('players.csv')
        
        print(players)

        ##http://core.espnuk.org/v2/sports/cricket/athletes/277916
    



def load_teams_table(series_url = None, series_id = None) : 
    try:
    #parent_url = "https://www.espncricinfo.com/ci/engine/series/index.html?view=season"
        engine = create_engine('postgresql://postgres:superuser@localhost:5432/postgres')
        with engine.connect() as conn:
            conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('temp_seasons', 'copy from local csv', 'start')")
            conn.connection.commit()
            print('connected')
            
            #df = seasons_and_urls(extract_series_from_a_season)           
            query = '''
            select series_name, 'https://www.espncricinfo.com/ci/engine/match/index/series.html?series='|| series_id as url, series_id from cricinfo.series
            WHERE NOT EXISTS (
                SELECT 
                FROM regexp_split_to_table(series_name, '\s+') AS word
                WHERE LOWER(word) IN ('australia', 'india',  'africa', 'england', 
                                     'zealand', 'sri', 'lanka', 'pakistan', 
                                     'indies', 'bangladesh')
            )
            AND
            --EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER between start_year and end_year
            --2023 between start_year and end_year
            start_year>=2015 --and start_year<=2023
            AND match_type not in ('Tests', 'Women''s Tests', 'First-class')
            AND load_complete = 'N'
            '''
            if series_url is None:
                series_links = pd.read_sql_query(query, engine)['url'].tolist()
                series_ids = pd.read_sql_query(query, engine)['series_id'].tolist()
            else :
                series_links = [series_url]
                series_ids = [series_id]
            #print(pd.read_sql_query(query, engine))
            #print(seasons_links)
            match_df = pd.DataFrame()
            teams_df = pd.DataFrame()
            innings_df = pd.DataFrame()
            result_df = pd.DataFrame()
            squads_df = pd.DataFrame()
            players_df = pd.DataFrame()
            batting_innings_df = pd.DataFrame()
            bowling_innings_df = pd.DataFrame()
            #match_df = pd.DataFrame()

            overall_start_time = time.time()
            #print('above for')

            #for i, (series, series_id) in enumerate(zip(series_links, series_ids)):
                #start_time = time.time()
            with ThreadPoolExecutor(max_workers=4) as executor:  # Adjust 'max_workers' as needed
                # Submit tasks to the thread pool
                futures = [executor.submit(process_season, series, series_id) for series, series_id in zip(series_links, series_ids)]
                # try:

                    

                #     # if i ==20 :
                #     #     print(i, series)
                #     #     break
                #     #print(series)
                #     #if series == "https://www.espncricinfo.com/ci/engine/match/index/series.html?series=13216":
                #     #series = "https://www.espncricinfo.com/ci/engine/match/index/series.html?series=10727"

                #     match_temp, teams_temp, innings_temp, result_temp, squads_temp, players_temp, batting_innings_temp, bowling_innings_temp = scrape_series_matches(series)
                #     match_temp['series_id'] = series_id
                #     temp_match_path = 'staging_files/temp_match.csv'
                #     temp_teams_path = 'staging_files/temp_teams.csv'
                #     temp_innings_path = 'staging_files/temp_innings.csv'
                #     temp_result_path = 'staging_files/temp_result.csv'
                #     temp_squads_path = 'staging_files/temp_squads.csv'
                #     temp_players_path = 'staging_files/temp_players.csv'
                #     temp_batting_innings_path = 'staging_files/temp_batting_innings.csv'
                #     temp_bowling_innings_path = 'staging_files/temp_bowling_innings.csv'
                    

                #     match_temp.to_csv(temp_match_path, index=False, header=False)
                #     teams_temp.to_csv(temp_teams_path, index=False, header=False)
                #     innings_temp.to_csv(temp_innings_path, index=False, header=False)
                #     result_temp.to_csv(temp_result_path, index=False, header=False)
                #     squads_temp.to_csv(temp_squads_path, index=False, header=False)
                #     players_temp.to_csv(temp_players_path, index=False, header=False)
                #     batting_innings_temp.to_csv(temp_batting_innings_path, index=False, header=False)
                #     bowling_innings_temp.to_csv(temp_bowling_innings_path, index=False, header=False)


                #     # match_temp.to_csv(temp_match_path, index=False, header=True)
                #     # teams_temp.to_csv(temp_teams_path, index=False, header=True)
                #     # innings_temp.to_csv(temp_innings_path, index=False, header=True)
                #     # result_temp.to_csv(temp_result_path, index=False, header=True)
                #     # squads_temp.to_csv(temp_squads_path, index=False, header=True)
                #     # players_temp.to_csv(temp_players_path, index=False, header=True)
                #     # batting_innings_temp.to_csv(temp_batting_innings_path, index=False, header=True)
                #     # bowling_innings_temp.to_csv(temp_bowling_innings_path, index=False, header=True)
                #     #print('csvs created')

                #     ##Table 1: Teams################################
                    
                #     with open(temp_teams_path, 'r') as f:
                #         conn.execute("""
                #             DROP TABLE IF EXISTS temp_teams;
                #             CREATE TEMP TABLE temp_teams (
                #                 team_id TEXT,
                #                 team_name TEXT
                #             );
                #         """)
                #         #print('temp created in staging')

                #         #print('temp_match_path found')
                #         conn.connection.cursor().copy_expert(
                #             "COPY temp_teams (team_id,team_name) FROM STDIN WITH CSV", f
                #         )
                #         #print('temp_teams table loaded from csv')
                #         conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('temp_teams', 'copy from local csv', 'complete')")
                #         conn.connection.commit()
                #         #print('temp table loaded')
                #         conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.teams', 'merge', 'start')")
                #         conn.connection.commit()
                #         merge_statement = """
                #         MERGE INTO cricinfo.teams s
                #     USING (SELECT team_id,team_name
                #         FROM temp_teams) AS T
                #     ON T.team_id = s.team_id --AND T.link_prefix = s.link_prefix
                #     WHEN MATCHED THEN 
                #         UPDATE SET 
                #             team_id = T.team_id,
                #             team_name = T.team_name,
                #             last_updated = NOW()
                #     WHEN NOT MATCHED THEN
                #         INSERT (team_id,team_name)
                #         VALUES (T.team_id,T.team_name);
                #         """
                #         conn.execute(merge_statement)
                #         conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.series', 'merge', 'complete')")
                #         conn.connection.commit()
                # #         #print('loaded teams')



                #     ## Table 2: Match############################################################################################
                #     with open(temp_match_path, 'r') as f:
                #         #print('ins')
                        
                #         conn.execute("""
                #         DROP TABLE IF EXISTS temp_match;
                #             CREATE TEMP TABLE temp_match (
                #                 match_id TEXT, 
                #                 match_no TEXT, 
                #                 date TEXT, 
                #                 venue TEXT, 
                #                 team1 TEXT, 
                #                 team2 TEXT, 
                #                 result TEXT, 
                #                 match_url TEXT, 
                #                 team1_innings TEXT, 
                #                 team2_innings TEXT, 
                #                 winner TEXT, 
                #                 win_by TEXT, 
                #                 win_margin TEXT,
                #                 series_id TEXT
                #             );
                #         """)
                #         #print('temp created in staging')




                #         #print('temp_match_path found')
                #         conn.connection.cursor().copy_expert(
                #             "COPY temp_match (match_id,match_no,date,venue,team1,team2,result,match_url,team1_innings,team2_innings,  winner, win_by, win_margin, series_id) FROM STDIN WITH CSV", f
                #         )
                #         #print('temp_match table loaded from csv')
                #         conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('temp_match', 'copy from local csv', 'complete')")
                #         conn.connection.commit()
                #     #print('temp matcg loaded')
                #     conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.match', 'merge', 'start')")
                #     conn.connection.commit()
                #     merge_statement = """
                #     MERGE INTO cricinfo.match s
                # USING (SELECT match_id,match_no,date,venue,team1,team2,result,match_url,team1_innings,team2_innings,winner, win_by,win_margin, series_id
                #     FROM temp_match) AS T
                # ON T.match_id = s.match_id --AND T.link_prefix = s.link_prefix
                # WHEN MATCHED THEN 
                #     UPDATE SET 
                #         match_id = T.match_id,
                #         match_no = T.match_no,
                #         date = T.date::DATE,
                #         venue = T.venue,
                #         team1 = T.team1,
                #         team2 = T.team2,
                #         result = T.result,
                #         match_url = T.match_url,
                #         team1_innings = T.team1_innings,
                #         team2_innings = T.team2_innings,
                #         win_by = T.win_by,
                #         winner = T.winner,
                #         win_margin = T.win_margin::INT,
                #         series_id = T.series_id,
                #         last_updated = NOW()
                # WHEN NOT MATCHED THEN
                #     INSERT (match_id,match_no,date,venue,team1,team2,result,match_url,team1_innings,team2_innings,winner, win_by,win_margin, series_id)
                #     VALUES (T.match_id,T.match_no,T.date::DATE,T.venue,T.team1,T.team2,T.result,T.match_url,T.team1_innings,T.team2_innings,T.winner, T.win_by,T.win_margin::INT, T.series_id);
                #     """
                #     conn.execute(merge_statement)
                #     conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.series', 'merge', 'complete')")
                #     conn.connection.commit()
                # #     #print('loaded matches')


                #     ## Table 3 - Innings
                    
                #     with open(temp_innings_path, 'r') as f:
                        
                #         conn.execute("""
                #         DROP TABLE IF EXISTS temp_innings;
                #             CREATE TEMP TABLE temp_innings (
                #                 match_id TEXT,
                #                 team_id TEXT,
                #                 runs TEXT,
                #                 wickets TEXT,
                #                 overs_played TEXT,
                #                 inning_number TEXT
                #             );
                #         """)
                #         #print('temp created in staging')



                #         #print('temp_match_path found')
                #         conn.connection.cursor().copy_expert(
                #             "COPY temp_innings (match_id, team_id, runs, wickets, overs_played, inning_number) FROM STDIN WITH CSV", f
                #         )
                #         #print('temp_innings table loaded from csv')
                #         conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('temp_innings', 'copy from local csv', 'complete')")
                #         conn.connection.commit()
                #     #print('temp table loaded')
                #     conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.innings', 'merge', 'start')")
                #     conn.connection.commit()
                #     merge_statement = """
                #     MERGE INTO cricinfo.innings s
                # USING (SELECT match_id, team_id, runs, wickets, overs_played, inning_number
                #     FROM temp_innings) AS T
                # ON T.match_id = s.match_id AND T.inning_number::INT = s.inning_number AND T.team_id = s.team_id
                # WHEN MATCHED THEN 
                #     UPDATE SET 
                #         match_id = T.match_id,
                #         team_id = T.team_id,
                #         runs = T.runs::INT,
                #         wickets = T.wickets::INT,
                #         overs_played = T.overs_played::NUMERIC(5,2),
                #         inning_number = T.inning_number::INT,
                #         last_updated = NOW()
                # WHEN NOT MATCHED THEN
                #     INSERT (match_id, team_id, runs, wickets, overs_played, inning_number)
                #     VALUES (T.match_id, T.team_id, T.runs::INT, T.wickets::INT, T.overs_played::NUMERIC(5,2), T.inning_number::INT);
                #     """
                #     conn.execute(merge_statement)
                #     conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.innings', 'merge', 'complete')")
                #     conn.connection.commit()
                #     #print('loaded matches')



                #     ## Table 4: Players
                #     with open(temp_players_path, 'r') as f:
                #         conn.execute("""
                #             DROP TABLE IF EXISTS temp_players;
                #             CREATE TEMP TABLE temp_players (
                #                 player_id TEXT,
                #                 card_short TEXT,
                #                 known_as TEXT,
                #                 dob TEXT,
                #                 country_id TEXT,
                #                 batting_hand TEXT,
                #                 batting_style TEXT,
                #                 bowling_hand TEXT,
                #                 bowling_pacespin TEXT,
                #                 bowling_style TEXT
                #             );
                #         """)
                #         conn.connection.cursor().copy_expert(
                #             "COPY temp_players (player_id, card_short, known_as, dob, country_id, batting_hand, batting_style, bowling_hand, bowling_pacespin, bowling_style) FROM STDIN WITH CSV", f
                #         )
                #         conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('temp_players', 'copy from local csv', 'complete')")
                #         conn.connection.commit()
                #         conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.players', 'merge', 'start')")
                #         conn.connection.commit()
                #         merge_statement = """
                #         MERGE INTO cricinfo.players s
                #         USING (SELECT DISTINCT player_id, known_as, dob, country_id, batting_hand, batting_style, bowling_hand, bowling_pacespin, bowling_style
                #             FROM temp_players) AS T
                #         ON T.player_id = s.player_id
                #         WHEN MATCHED THEN 
                #             UPDATE SET 
                #                 known_as = T.known_as,
                #                 dob = T.dob::DATE,
                #                 country_id = T.country_id,
                #                 batting_hand = T.batting_hand,
                #                 batting_style = T.batting_style,
                #                 bowling_hand = T.bowling_hand,
                #                 bowling_pacespin = T.bowling_pacespin,
                #                 bowling_style = T.bowling_style,
                #                 last_updated = NOW()
                #         WHEN NOT MATCHED THEN
                #             INSERT (player_id,  known_as, dob, country_id, batting_hand, batting_style, bowling_hand, bowling_pacespin, bowling_style)
                #             VALUES (T.player_id,  T.known_as, T.dob::DATE, T.country_id, T.batting_hand, T.batting_style, T.bowling_hand, T.bowling_pacespin, T.bowling_style);
                #         """
                #         conn.execute(merge_statement)
                #         conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.players', 'merge', 'complete')")
                #         conn.connection.commit()


                #     ## Table 5: Squads
                #     with open(temp_squads_path, 'r') as f:
                #         conn.execute("""
                #             DROP TABLE IF EXISTS temp_squads;
                #             CREATE TEMP TABLE temp_squads (
                #                 player_id TEXT,
                #                 match_id TEXT,
                #                 team_id TEXT
                #             );
                #         """)
                #         conn.connection.cursor().copy_expert(
                #             "COPY temp_squads (player_id, match_id, team_id) FROM STDIN WITH CSV", f
                #         )
                #         conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('temp_squads', 'copy from local csv', 'complete')")
                #         conn.connection.commit()
                #         conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.squads', 'merge', 'start')")
                #         conn.connection.commit()
                #         merge_statement = """
                #         MERGE INTO cricinfo.squads s
                #         USING (SELECT DISTINCT player_id, match_id, team_id
                #             FROM temp_squads) AS T
                #         ON T.player_id = s.player_id AND T.match_id = s.match_id AND T.team_id = s.team_id
                #         WHEN MATCHED THEN 
                #             UPDATE SET 
                #                 player_id = T.player_id,
                #                 match_id = T.match_id,
                #                 team_id = T.team_id,
                #                 last_updated = NOW()
                #         WHEN NOT MATCHED THEN
                #             INSERT (player_id, match_id, team_id)
                #             VALUES (T.player_id, T.match_id, T.team_id);
                #         """
                #         conn.execute(merge_statement)
                #         conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.squads', 'merge', 'complete')")
                #         conn.connection.commit()




                #     ## Table 6: Batting Innings
                #     with open(temp_batting_innings_path, 'r') as f:
                #         conn.execute("""
                #             DROP TABLE IF EXISTS temp_batting_innings;
                #             CREATE TEMP TABLE temp_batting_innings (
                #                 match_id TEXT,
                #                 innings_number TEXT,
                #                 Batter TEXT,
                #                 runs TEXT,
                #                 balls TEXT,
                #                 fours TEXT,
                #                 sixes TEXT,
                #                 strike_rate TEXT,
                #                 in_at TEXT,
                #                 is_out BOOLEAN,
                #                 bowler TEXT,
                #                 wicket_number TEXT,
                #                 wicket_fell_at TEXT,
                #                 player_id TEXT,
                #                 bowler_id TEXT
                #             );
                #         """)
                #         conn.connection.cursor().copy_expert(
                #             """
                #             COPY temp_batting_innings (
                #                 match_id, 
                #                 innings_number, 
                #                 Batter, 
                #                 runs, 
                #                 balls, 
                #                 fours, 
                #                 sixes, 
                #                 strike_rate, 
                #                 in_at, 
                #                 is_out, 
                #                 bowler, 
                #                 wicket_number, 
                #                 wicket_fell_at, 
                #                 player_id, 
                #                 bowler_id
                #             ) FROM STDIN WITH CSV
                #             """, 
                #             f
                #         )
                #         conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('temp_batting_innings', 'copy from local csv', 'complete')")
                #         conn.connection.commit()
                #         conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.batting_innings', 'merge', 'start')")
                #         conn.connection.commit()
                #         merge_statement = """
                #         MERGE INTO cricinfo.batting_innings s
                #         USING (SELECT 
                #                     match_id, 
                #                     innings_number::INT, 
                #                     Batter, 
                #                     runs::INT, 
                #                     balls::INT, 
                #                     fours::INT, 
                #                     sixes::INT, 
                #                     strike_rate::NUMERIC, 
                #                     in_at, 
                #                     is_out, 
                #                     bowler, 
                #                     bowler_id,
                #                     wicket_number::INT, 
                #                     wicket_fell_at::INT, 
                #                     player_id
                #                 FROM temp_batting_innings
                #                 WHERE match_id IS NOT NULL AND innings_number IS NOT NULL AND player_id IS NOT NULL) AS T
                #         ON T.match_id = s.match_id AND T.innings_number = s.innings_number AND T.player_id = s.player_id
                        
                #         WHEN MATCHED THEN 
                #             UPDATE SET 
                #                 Batter = T.Batter,
                #                 runs = T.runs,
                #                 balls = T.balls,
                #                 fours = T.fours,
                #                 sixes = T.sixes,
                #                 strike_rate = T.strike_rate,
                #                 in_at = T.in_at,
                #                 is_out = T.is_out,
                #                 bowler = T.bowler,
                #                 bowler_id = T.bowler_id,
                #                 wicket_number = T.wicket_number,
                #                 wicket_fell_at = T.wicket_fell_at,
                #                 last_updated = NOW()
                #         WHEN NOT MATCHED THEN
                #             INSERT (match_id, innings_number, Batter, runs, balls, fours, sixes, strike_rate, in_at, is_out, bowler, bowler_id, wicket_number, wicket_fell_at, player_id)
                #             VALUES (T.match_id, T.innings_number, T.Batter, T.runs, T.balls, T.fours, T.sixes, T.strike_rate, T.in_at, T.is_out, T.bowler, T.bowler_id, T.wicket_number, T.wicket_fell_at, T.player_id)
                #             ;
                #         """
                #         conn.execute(merge_statement)
                #         conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.batting_innings', 'merge', 'complete')")
                #         conn.connection.commit()

                

                #     ## Table 7: Bowling Innings
                #     with open(temp_bowling_innings_path, 'r') as f:
                #         # Drop and create the temporary table with updated column names
                #         conn.execute("""
                #             DROP TABLE IF EXISTS temp_bowling_innings;
                #             CREATE TEMP TABLE temp_bowling_innings (
                #                 match_id TEXT,
                #                 innings_number TEXT,
                #                 bowler_name TEXT, -- Renamed from 'Bowling'
                #                 overs TEXT,       -- Renamed from 'O'
                #                 maidens TEXT,     -- Renamed from 'M'
                #                 runs TEXT,        -- Renamed from 'R'
                #                 wickets TEXT,     -- Renamed from 'W'
                #                 economy TEXT,     -- Renamed from 'Econ'
                #                 dot_balls TEXT,   -- Added column for '0s'
                #                 fours_conceded TEXT, -- Added column for '4s'
                #                 sixes_conceded TEXT, -- Added column for '6s'
                #                 wides TEXT,       -- Added column for 'WD'
                #                 no_balls TEXT,    -- Added column for 'NB'
                #                 player_id TEXT
                #             );
                #         """)

                #         # Copy data into the temporary table from the CSV file
                #         conn.connection.cursor().copy_expert(


                #             """
                #             COPY temp_bowling_innings (
                #                 bowler_name,
                #                 overs,
                #                 maidens,
                #                 runs,
                #                 wickets,
                #                 economy,
                #                 dot_balls,
                #                 fours_conceded,
                #                 sixes_conceded,
                #                 wides,
                #                 no_balls,
                #                 innings_number,
                #                 match_id,
                #                 player_id 
                #             ) FROM STDIN WITH CSV
                #             """, f
                #         )

                #         # Log the completion of data copy
                #         conn.execute(f"""
                #             INSERT INTO cricinfo.operation_logs (table_name, operation, status)
                #             VALUES ('temp_bowling_innings', 'copy from local csv', 'complete')
                #         """)
                #         conn.connection.commit()

                #         # Log the start of the merge operation
                #         conn.execute(f"""
                #             INSERT INTO cricinfo.operation_logs (table_name, operation, status)
                #             VALUES ('cricinfo.bowling_innings', 'merge', 'start')
                #         """)
                #         conn.connection.commit()

                #         # Updated MERGE statement with renamed columns
                #         merge_statement = """
                #             MERGE INTO cricinfo.bowling_innings s
                #             USING (
                #                 SELECT 
                #                     match_id,
                #                     innings_number::INT AS innings_number,
                #                     bowler_name,
                #                     overs::NUMERIC AS overs,
                #                     maidens::INT AS maidens,
                #                     runs::INT AS runs,
                #                     wickets::INT AS wickets,
                #                     economy::NUMERIC AS economy,
                #                     dot_balls::INT AS dot_balls,
                #                     fours_conceded::INT AS fours_conceded,
                #                     sixes_conceded::INT AS sixes_conceded,
                #                     wides::INT AS wides,
                #                     no_balls::INT AS no_balls,
                #                     player_id
                #                 FROM temp_bowling_innings
                #                 WHERE match_id IS NOT NULL AND innings_number IS NOT NULL AND player_id IS NOT NULL
                #             ) AS T
                #             ON T.match_id = s.match_id AND T.innings_number = s.innings_number AND T.player_id = s.player_id
                            
                #             WHEN MATCHED THEN 
                #                 UPDATE SET 
                #                     bowler_name = T.bowler_name,
                #                     overs = T.overs,
                #                     maidens = T.maidens,
                #                     runs = T.runs,
                #                     wickets = T.wickets,
                #                     economy = T.economy,
                #                     dot_balls = T.dot_balls,
                #                     fours_conceded = T.fours_conceded,
                #                     sixes_conceded = T.sixes_conceded,
                #                     wides = T.wides,
                #                     no_balls = T.no_balls,
                #                     last_updated = NOW()
                #             WHEN NOT MATCHED THEN
                #                 INSERT (match_id, innings_number, bowler_name, overs, maidens, runs, wickets, economy, dot_balls, fours_conceded, sixes_conceded, wides, no_balls, player_id)
                #                 VALUES (T.match_id, T.innings_number, T.bowler_name, T.overs, T.maidens, T.runs, T.wickets, T.economy, T.dot_balls, T.fours_conceded, T.sixes_conceded, T.wides, T.no_balls, T.player_id)
                #                 ;
                #         """
                        
                #         # Execute the MERGE statement
                #         conn.execute(merge_statement)

                #         # Log the completion of the merge operation
                #         conn.execute(f"""
                #             INSERT INTO cricinfo.operation_logs (table_name, operation, status)
                #             VALUES ('cricinfo.bowling_innings', 'merge', 'complete')
                #         """)
                #         conn.connection.commit()
                    
                # except Exception as e:
                #     print(str(e).replace("'", ""))
                #     print(str(e).replace("'", ""))
                #     with open("error_log.txt", "a") as file:
                #         file.write(f"Can't write this series: series_id {series_id} {e}\n")
                #     engine = create_engine('postgresql://postgres:superuser@localhost:5432/postgres')
                #     with engine.connect() as conn:
                #         conn.execute(f"""INSERT INTO cricinfo.operation_logs (table_name, operation, status, exception) VALUES ('cricinfo.series', 'load', 'failed', '{str(e).replace("'", "")}')""")
                #         conn.connection.commit()
                
                # end_time = time.time()
                # elapsed_time = end_time - start_time
                # mins, secs = divmod(elapsed_time, 60)
                # # Print in minutes and seconds
                # print(f"Time taken for series index {i}, series id {series_id}: {int(mins)} minutes and {secs:.2f} seconds")


                # query = """
                # select end_year from cricinfo.series where series_id = '""" + str(series_id) + """'
                # """
                # #if series_url is None:
                # end_year = pd.read_sql_query(query, engine)['end_year'].tolist()[0]
                # #print(end_year)

                # if end_year<datetime.now().year :
                #     query = """
                # UPDATE cricinfo.series
                #     SET load_complete = 'Y'
                # WHERE series_id = '""" + str(series_id) + """'
                # """
                #     conn.execute(query)
                #     conn.connection.commit()
                


            overall_end_time = time.time()
            overall_elapsed_time = overall_end_time - overall_start_time
            overall_mins, overall_secs = divmod(overall_elapsed_time, 60)
            print(f"Time taken for all series: {int(overall_mins)} minutes and {overall_secs:.2f} seconds")

            #     # print('hiya')
        #     # match_df = match_df.append(match_temp, ignore_index=True)
        #     # teams_df = teams_df.append(teams_temp, ignore_index=True)
        #     # innings_df = innings_df.append(innings_temp, ignore_index=True)
        #     # result_df = result_df.append(result_temp, ignore_index=True)
        #     #grounds_df = grounds_df.append(grounds_temp, ignore_index=True)


                


            
    except Exception as e:
        print(str(e).replace("'", ""))
        #with open("error_log.txt", "a") as file:
            #file.write(f"Can't write this series: series_id {series_id} {e}\n")
        engine = create_engine('postgresql://postgres:superuser@localhost:5432/postgres')
        with engine.connect() as conn:
            conn.execute(f"""INSERT INTO cricinfo.operation_logs (table_name, operation, status, exception) VALUES ('cricinfo.series', 'load', 'failed', '{str(e).replace("'", "")}')""")
            conn.connection.commit()



if __name__ == '__main__': 
    # load_seasons_table()
    # load_series_table()
    load_teams_table()
    update_countries()
    # engine = create_engine('postgresql://postgres:superuser@localhost:5432/postgres')
    # query = "SELECT DISTINCT link_prefix FROM cricinfo.seasons"
    # seasons_links = pd.read_sql_query(query, engine)['link_prefix'].tolist()
    # #print(seasons_links)
    # df = pd.DataFrame()
    # for i, season in enumerate(seasons_links):
    #     print(season)
    #     df = df.append(extract_series_from_a_season("https://www.espncricinfo.com"+season))

    #     #if i ==20:
    #     #    break
    # print('getting df now')
    # print(df)


