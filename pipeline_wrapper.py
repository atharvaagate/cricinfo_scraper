from utils import *
import pandas as pd
from sqlalchemy import create_engine
import time

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
            select 'https://www.espncricinfo.com/ci/engine/match/index/series.html?series='|| series_id as url, series_id from cricinfo.series
            WHERE NOT EXISTS (
                SELECT 1
                FROM regexp_split_to_table(series_name, '\s+') AS word
                WHERE LOWER(word) IN ('australia', 'india', 'south', 'africa', 'england', 
                                    'new', 'zealand', 'sri', 'lanka', 'pakistan', 
                                    'west', 'indies', 'bangladesh')
            )
            AND
            EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER between start_year and end_year
            AND match_type not in ('Tests', 'Women''s Tests', 'First-class')
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
            grounds_df = pd.DataFrame()
            #match_df = pd.DataFrame()

            overall_start_time = time.time()
            #print('above for')

            for i, (series, series_id) in enumerate(zip(series_links, series_ids)):

                start_time = time.time()

                if i ==20 :
                    print(i, series)
                    break
                #print(series)

                match_temp, teams_temp, innings_temp, result_temp = scrape_series_matches(series)
                match_temp['series_id'] = series_id
                temp_match_path = 'staging_files/temp_match.csv'
                temp_teams_path = 'staging_files/temp_teams.csv'
                temp_innings_path = 'staging_files/temp_innings.csv'
                temp_result_path = 'staging_files/temp_result.csv'
                
                match_temp.to_csv(temp_match_path, index=False, header=False)
                teams_temp.to_csv(temp_teams_path, index=False, header=False)
                innings_temp.to_csv(temp_innings_path, index=False, header=False)
                result_temp.to_csv(temp_result_path, index=False, header=False)
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
                    #print('loaded teams')



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
                        "COPY temp_match (match_id,match_no,date,venue,team1,team2,result,match_url,team1_innings,team2_innings, win_by, win_margin, winner, series_id) FROM STDIN WITH CSV", f
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
            ON T.match_id = s.match_id --AND T.link_prefix = s.link_prefix
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
                #print('loaded matches')


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



                
                #########################start here#############################

                # print('hiya')
                # match_df = match_df.append(match_temp, ignore_index=True)
                # teams_df = teams_df.append(teams_temp, ignore_index=True)
                # innings_df = innings_df.append(innings_temp, ignore_index=True)
                # result_df = result_df.append(result_temp, ignore_index=True)
                #grounds_df = grounds_df.append(grounds_temp, ignore_index=True)


                end_time = time.time()
                elapsed_time = end_time - start_time
                mins, secs = divmod(elapsed_time, 60)

                # Print in minutes and seconds
                print(f"Time taken for series {i}: {int(mins)} minutes and {secs:.2f} seconds")


                #df = df.append(extract_series_from_a_season("https://www.espncricinfo.com"+season))

            # df = (
            #         df.groupby("series_id")
            #         .agg(
            #             start_year=("year", lambda x: extract_start_end(x)[0]),
            #             end_year=("year", lambda x: extract_start_end(x)[1]),
            #             series=("series_name", "first"),
            #             type=("match_type", "first"),
            #             url=("url", "first"),
            #         )
            #         .reset_index()
            #     )

            #print('getting df now')
            #print(df)
            #temp_csv_path = 'staging_files/temp_seasons.csv'
            # temp_match_path = 'staging_files/temp_match.csv'
            # temp_teams_path = 'staging_files/temp_teams.csv'
            # temp_innings_path = 'staging_files/temp_innings.csv'
            # temp_result_path = 'staging_files/temp_result.csv'
            # #temp_grounds_path = 'staging_files/temp_grounds.csv'



            # #df.to_csv(temp_csv_path, index=False, header=False)

            # match_temp.to_csv(temp_match_path, index=False, header=False)
            # teams_temp.to_csv(temp_teams_path, index=False, header=False)
            # innings_temp.to_csv(temp_innings_path, index=False, header=False)
            # result_temp.to_csv(temp_result_path, index=False, header=False)
            # #grounds_temp.to_csv(temp_grounds_path, index=False, header=False)


            
            overall_end_time = time.time()
            overall_elapsed_time = overall_end_time - overall_start_time
            mins, secs = divmod(overall_elapsed_time, 60)

            # Print in minutes and seconds
            print(f"Time taken: {int(mins)} minutes and {secs:.2f} seconds")

            print('tmp file created')
        #     with open(temp_csv_path, 'r') as f:
        #         conn.connection.cursor().copy_expert(
        #             "COPY temp_series (series_id, start_year, end_year, series_name, match_type, url) FROM STDIN WITH CSV", f
        #         )
        #         conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('temp_series', 'copy from local csv', 'complete')")
        #         conn.connection.commit()
        #     print('temp table loaded')
        #     conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.series', 'merge', 'start')")
        #     conn.connection.commit()
        #     merge_statement = """
        #     MERGE INTO cricinfo.series s
        # USING (SELECT series_id,start_year, end_year, series_name, match_type, url
        #     FROM temp_series) AS T
        # ON T.series_id = s.series_id --AND T.link_prefix = s.link_prefix
        # WHEN MATCHED THEN 
        #     UPDATE SET 
        #         start_year = T.start_year::INT, 
        #         end_year = T.end_year::INT, 
        #         series_name = T.series_name, 
        #         match_type = T.match_type, 
        #         url = T.url,
        #         last_updated = NOW()
        # WHEN NOT MATCHED THEN
        #     INSERT (series_id, start_year, end_year, series_name,match_type,url)
        #     VALUES (T.series_id, T.start_year::INT, T.end_year::INT, T.series_name,T.match_type,T.url);
        #     """
        #     conn.execute(merge_statement)
        #     conn.execute(f"INSERT INTO cricinfo.operation_logs (table_name, operation, status) VALUES ('cricinfo.series', 'merge', 'complete')")
        #     conn.connection.commit()
        #     print('loaded seasons')
    except Exception as e:
        print(str(e).replace("'", ""))
        engine = create_engine('postgresql://postgres:superuser@localhost:5432/postgres')
        with engine.connect() as conn:
            conn.execute(f"""INSERT INTO cricinfo.operation_logs (table_name, operation, status, exception) VALUES ('cricinfo.series', 'load', 'failed', '{str(e).replace("'", "")}')""")
            conn.connection.commit()



if __name__ == '__main__': 
    #load_seasons_table()
    #load_series_table()
    load_teams_table()
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
    
    
