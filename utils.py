import requests
from bs4 import BeautifulSoup
import re
from espncricinfo.match import Match
import requests
from bs4 import BeautifulSoup
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from espncricinfo.match import Match

def seasons_and_urls(parent_url):
    # Fetch the HTML content
    response = requests.get(parent_url)
    response.raise_for_status()  # Check for any errors in the request
    
    # Create a BeautifulSoup object
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find all <a> tags within the <ul>
    links = soup.find_all('a')
    
    # Extract the href attribute and the year text
    season_links = {}
    for link in soup.find_all('a'):
        span = link.find('span', class_='year')
        if span:
            season_links[span.text] = link['href']

    data = []

    # Print the results
    for year, link in season_links.items():
        #print(f"{year}: {link}")
        data.append((year, link))
    df = pd.DataFrame(data, columns=['Year', 'Link'])
    #df['link_prefix'] = df['Link'].str.extract(r'season=([^;]+)').fillna("")
    #df = df.drop('Link', axis=1)
    return df


def extract_series_from_a_season(season_url):
    # Send a GET request to the series URL
    response = requests.get(season_url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')
        # Extract the active year
        calendar_section = soup.find('section', class_='slider-calendar-wrap season')
        active_year_element = calendar_section.find('li', class_='active') if calendar_section else None
        year = active_year_element.find('span', class_='year').text.strip() if active_year_element else 'Unknown'
        # Find all series summary blocks
        series_summary_blocks = soup.find_all('section', class_='series-summary-block')

        # Extract the URLs, match types, and series names for each series
        series_data = []
        for block in series_summary_blocks:
            # Find the <h2> tag above the current series_summary_block
            match_section_head = block.find_previous('div', class_='match-section-head')
            match_type = match_section_head.find('h2').text if match_section_head else 'Unknown'
            series_id = block['data-series-id']

            link = block.find('a', href=True)
            if link:
                series_data.append({
                    'url': link['href'],
                    'match_type': match_type,
                    'series_name': link.text.strip(),  # Extract the series name
                    'year' : year,
                    'series_id' : series_id
                })

            df = pd.DataFrame(series_data, columns=['series_id', 'year', 'series_name', 'match_type', 'url'])
        #print('got data')
        return df
    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")
        return []



def extract_start_end(years):
    start = float("inf")
    end = float("-inf")
    for year in years:
        if "/" in year:  # Split year ranges like 2015/16
            y1, y2 = map(int, year.split("/"))
            
            start = min(start, y1)
            current_start = y1
            if y2%100 < y1%100 :
                end = max(end, y1 + ((y2%100) + 100 - (y1%100)))
            else :
                end = max(end, y1 + ((y2%100) - (y1%100)))
            #end = max(end, y2 if y2 > 99 else y1 + y2)  # Handle cases like 2015/16 -> 2015 to 2016
        else:
            y = int(year)
            start = min(start, y)
            end = max(end, y)
        print(years, start, end)
    return start, end








def squads_df_and_players_df_from_a_match_id(match) :
    ## player id should be object_id, not player_id
    m = match

    # squads_df = pd.DataFrame(m.team_1_players)
    # players_df

    team_1_players = pd.DataFrame(m.team_1_players)
    team_1_players['team_id'] = m.team_1_id
    
    team_2_players = pd.DataFrame(m.team_2_players)
    team_2_players['team_id'] = m.team_2_id
    
    players_df = pd.concat([team_1_players, team_2_players], axis=0)
    players_df['match_id'] = m.match_id
    players_df['country_id'] = ''
    
    
    players_df_cols = ['object_id', 'match_id', 'team_id'] + [col for col in players_df.columns if col not in ['object_id', 'match_id', 'team_id']]
    players_df = players_df[players_df_cols]
    #print(df.columns)


    squads_df_cols = ['object_id', 'match_id', 'team_id']
    squads_df = players_df[squads_df_cols]

    players_df = players_df[['object_id', 'card_short', 'known_as', 'dob', 'country_id', 'batting_hand', 'batting_style', 'bowling_hand', 'bowling_pacespin', 'bowling_style']]
    players_df['known_as'] = players_df['known_as'].apply(clean_name)
    players_df['card_short'] = players_df['card_short'].apply(clean_name)

    players_df['dob'] = pd.to_datetime(players_df['dob'], format='%Y-%m-%d', errors='coerce')


    players_df = players_df.drop_duplicates()
    squads_df = squads_df.drop_duplicates()

    players_df.rename(columns={'object_id': 'player_id'}, inplace=True)
    squads_df.rename(columns={'object_id': 'player_id'}, inplace=True)

    

    
    #print(df.columns)
    #squads_df = squads_df[['player_id', 'card_short', 'known_as', 'dob', 'batting_hand', 'batting_style', 'bowling_hand', 'bowling_pacespin', 'bowling_style']]


    #print('got squads right')
    return squads_df, players_df


def clean_name(name):
    if pd.isnull(name):
        return None
    # Remove content within parentheses
    name = re.sub(r'\(.*?\)', '', name)
    # Remove non-letter characters and normalize spaces
    name = re.sub(r'[^a-zA-Z\s]', '', name)
    return re.sub(r'\s+', ' ', name).strip()  # Replace multiple spaces with a single space


def get_batting_innings_df(match) : 
    m = match
    table_MN = pd.read_html(m.match_url)
    #print(table_MN.columns)

    f_inn_bat_1 = table_MN[0]
    f_inn_bat_1 = f_inn_bat_1.dropna(how='all')
    #print(f_inn_bat_1)
    #print(f_inn_bat_1.head(11))
    f_inn_bat_1['innings_number'] = 1
    # Create the 'in at' column
    f_inn_bat_1['in at'] = 0  # Initialize all values to 0
    f_inn_bat_1
    #print(f_inn_bat_1)
    #f_inn_bat_1.loc[2:, 'in at'] = range(3, len(f_inn_bat_1) + 1)  # Assign increasing values starting from 3
    # f_inn_bat_1['in at'] = f_inn_bat_1.index + 1
    # f_inn_bat_1.loc[:2, 'in at'] = [0,0]
    f_inn_bat_1.loc[:, 'in at'] = range(1, len(f_inn_bat_1) + 1)
    #f_inn_bat_1.loc[:2, 'in at'] = [0,0]

    f_inn_bat_2 = table_MN[2]
    f_inn_bat_2 = f_inn_bat_2.dropna(how='all')
    f_inn_bat_2['innings_number'] = 2
    # Create the 'in at' column
    f_inn_bat_2['in at'] = 0  # Initialize all values to 0
    #print(f_inn_bat_2)
    #f_inn_bat_2.loc[2:, 'in at'] = range(3, len(f_inn_bat_2) + 1)  # Assign increasing values starting from 3
    # f_inn_bat_2['in at'] = f_inn_bat_2.index + 1
    # f_inn_bat_2.loc[:2, 'in at'] = [0,0]
    #f_inn_bat_2.loc[2:, 'in at'] = range(3, 3 + len(f_inn_bat_2) - 2)
    f_inn_bat_2.loc[:, 'in at'] = range(1, len(f_inn_bat_2) + 1)
    #f_inn_bat_2.loc[:2, 'in at'] = [0,0]

    #f_inn_bat.con

    df = pd.concat([f_inn_bat_1, f_inn_bat_2], ignore_index=True)
    df['match_id'] = m.match_id

    # Convert 'R' column to numeric, forcing errors to NaN
    df['B'] = pd.to_numeric(df['B'], errors='coerce')


    # Remove rows where 'R' is NaN
    df = df.dropna(subset=['B'])
    df['B'] = df['B'].fillna(0).astype(int)

    # Create is_out column based on whether the player is out
    df['is_out'] = df['Unnamed: 1'].apply(lambda x: 'not out' not in x)


    
    df['Batting'] = df['Batting'].apply(clean_name)


    # Create bowler column by extracting the bowler's name

    def extract_bowler(dismissal):
        if dismissal.startswith('b ') :
            #dismissal = dismissal
            #print(dismissal, "->", dismissal[2:])
            return dismissal[2:]
        if ' b ' in dismissal :
            return dismissal.split(' b ')[-1]  # Extract the bowler's name after 'b'
        return np.nan  # Return NaN if no bowler involved



    # def extract_bowler(dismissal):
    #     if 'b ' in dismissal:
    #         return dismissal.split('b ')[-1]  # Extract the bowler's name after 'b'
    #     return np.nan  # Return NaN if no bowler involved

    df['bowler'] = df['Unnamed: 1'].apply(extract_bowler)
    df['bowler'] = df['bowler'].apply(clean_name)

    df = df.drop(['Unnamed: 1', 'Unnamed: 8', 'Unnamed: 9', 'M'], axis=1)

    fall_of_wickets_1, fall_of_wickets_2 = table_MN[0].iloc[-1]['Batting'], table_MN[2].iloc[-1]['Batting']
    # Combine the two strings
    combined_wickets = f"{fall_of_wickets_1}, {fall_of_wickets_2}"
    #print(combined_wickets)

    # Use regex to find all the wicket information
    wicket_info = re.findall(r'(\d+)-(\d+) \((.*?)\)', combined_wickets)

    #print(wicket_info)
    ## Create a dictionary to map players to their wicket numbers and runs when they got out
    wicket_map = {clean_name(player.split(',')[0])  : {'wicket_number': int(wicket), 'wicket_fell_at': int(runs)}
                    for wicket, runs, player in wicket_info}
    #print(wicket_map)

    # Initialize new columns in the DataFrame
    df['wicket_number'] = None
    df['wicket_fell_at'] = None

    # Map the values from the wicket_map to the DataFrame
    for player, info in wicket_map.items():
        df.loc[df['Batting'] == player, 'wicket_number'] = info['wicket_number']
        df.loc[df['Batting'] == player, 'wicket_fell_at'] = info['wicket_fell_at']

    cols = ['match_id', 'innings_number'] + [col for col in df.columns if col not in ['innings_number','match_id']]
    
    df = df[cols]

## Index(['Batting', 'R', 'B', '4s', '6s', 'SR', 'innings_number', 'in at',
    #    'match_id', 'is_out', 'bowler', 'wicket_number', 'wicket_fell_at'],
    #   dtype='object')
    #df[]
    # Convert columns to appropriate data types, handling errors
    df['R'] = pd.to_numeric(df['R'], errors='coerce').fillna(0).astype(int)
    df['4s'] = pd.to_numeric(df['4s'], errors='coerce').fillna(0).astype(int)
    df['6s'] = pd.to_numeric(df['6s'], errors='coerce').fillna(0).astype(int)
    df['SR'] = pd.to_numeric(df['SR'], errors='coerce').fillna(0)
    df['wicket_number'] = pd.to_numeric(df['wicket_number'], errors='coerce').fillna(0).astype(int)
    df['wicket_fell_at'] = pd.to_numeric(df['wicket_fell_at'], errors='coerce').fillna(0).astype(int)
    #print('got batting right')

    return df
    



import numpy as np
def get_bowling_innings_df(match) :
    df = pd.DataFrame(columns = ['Bowling', 'O', 'M', 'R', 'W', 'ECON', '0s', '4s', '6s', 'WD', 'NB',
       'innings_number', 'match_id']) 
    m = match
    table_MN = pd.read_html(m.match_url)
    #print(table_MN)
    #table_MN = table_MN.dropna(how='all')
    f_inn_bwl = table_MN[1].dropna(how='all')
    f_inn_bwl['innings_number'] = 1
    s_inn_bwl = table_MN[3].dropna(how='all')
    s_inn_bwl['innings_number'] = 2

    #f_inn_bat.con
    
    temp_df = pd.concat([df, f_inn_bwl, s_inn_bwl], ignore_index=True)
    df = temp_df[df.columns]
    df['match_id'] = m.match_id
    
    # Convert 'R' column to numeric, forcing errors to NaN
    df['R'] = pd.to_numeric(df['R'], errors='coerce')
    
    # Remove rows where 'R' is NaN
    df = df.dropna(subset=['R'])
    # Convert 'R' column to numeric, forcing errors to NaN
    df['R'] = pd.to_numeric(df['R'], errors='coerce')

    # Remove rows where 'R' is NaN
    df = df.dropna(subset=['R'])

    # Convert columns to appropriate data types, handling errors
    df['R'] = df['R'].fillna(0).astype(int)
    df['O'] = pd.to_numeric(df['O'], errors='coerce')
    df['M'] = pd.to_numeric(df['M'], errors='coerce').fillna(0).astype(int)
    df['W'] = pd.to_numeric(df['W'], errors='coerce').fillna(0).astype(int)
    df['ECON'] = pd.to_numeric(df['ECON'], errors='coerce')
    df['0s'] = pd.to_numeric(df['0s'], errors='coerce').fillna(0).astype(int)
    df['4s'] = pd.to_numeric(df['4s'], errors='coerce').fillna(0).astype(int)
    df['6s'] = pd.to_numeric(df['6s'], errors='coerce').fillna(0).astype(int)
    df['WD'] = pd.to_numeric(df['WD'], errors='coerce').fillna(0).astype(int)
    df['NB'] = pd.to_numeric(df['NB'], errors='coerce').fillna(0).astype(int)


    df['Bowling'] = df['Bowling'].apply(clean_name)
    
    # # Create is_out column based on whether the player is out
    # df['is_out'] = df['Unnamed: 1'].apply(lambda x: 'not out' not in x)
    
    # # Create bowler column by extracting the bowler's name
    # def extract_bowler(dismissal):
    #     if 'b ' in dismissal:
    #         return dismissal.split('b ')[-1]  # Extract the bowler's name after 'b'
    #     return np.nan  # Return NaN if no bowler involved
    
    # df['bowler'] = df['Unnamed: 1'].apply(extract_bowler)

    #print('got bowling right')

#Index(['Bowling', 'O', 'M', 'R', 'W', 'ECON', '0s', '4s', '6s', 'WD', 'NB',
    #    'innings_number', 'match_id'],
    #   dtype='object'

    
    # df['O'] = pd.to_numeric(df['O'])
    # df['M'] = df['M'].astype('int')
    # df['R'] = df['R'].astype('int')
    # df['W'] = df['W'].astype('int')
    # df['ECON'] = pd.to_numeric(df['ECON'])
    # df['0s'] = df['0s'].astype('int')
    # df['4s'] = df['4s'].astype('int')
    # df['6s'] = df['6s'].astype('int')
    # df['WD'] = df['WD'].astype('int')
    # df['NB'] = df['NB'].astype('int')

    return df



def extract_runs_overs_wickets_from_string(innings_string):
    target = None
    max_overs = None 
    match_team = re.match(r"(.+?)\s+\d+[/]?\d*\s*\(.*?\)", innings_string)
    if match_team:
        team_name = match_team.group(1).strip()
        
        # Extract score data
        score_data = innings_string.split(team_name, 1)[1].strip()
        #print(score_data)

        # Regex pattern to capture runs, wickets, overs, max overs, and optional target
        pattern = re.compile(
            r"(\d+)(?:/(\d+))?\s*\(\s*([\d.]+)\s*/?(\d*)\s*ov(?:,\s*target:\s*(\d+))?(?:/\s*(\d+))?\)"
        )
        
        match = pattern.search(score_data)
        #print(match.lastindex, " ldxsdfsdf")
        if match:
            runs = int(match.group(1))  # Extracted runs
            wickets = int(match.group(2)) if match.group(2) else 10  # Extracted wickets
            overs = float(match.group(3))  # Extracted overs
       
            
            #print(f"Runs: {runs}, Wickets: {wickets}, Overs: {overs}, Target: {target}, Max Overs: {max_overs}")
            return runs, wickets, overs#, target, max_overs
        
        else:
            #print("No match found for score data.")
            return None, None, None 
    
    #print("Team name not found.")
    return None, None, None 



def process_match(match, match_id):
    try:
        #print('in try')
        match_no = match.find('span', class_='match-no').text.strip()
        venue = match_no.split('at ')[1] if 'at' in match_no else 'Not Known'
        match_no = match_no.split('at ')[0]
        date = match.find('span', class_='bold').text.strip()
        team1_innings = match.find('div', class_='innings-info-1').text.strip()
        team2_innings = match.find('div', class_='innings-info-2').text.strip()
        result = match.find('div', class_='match-status').text.strip()
        match_url = match.find('a')['href']
        m = Match(match_id)
        #print('called match')

        squads_df_current_match, players_df_current_match = squads_df_and_players_df_from_a_match_id(m)
        batting_innings_df_current_match = get_batting_innings_df(m)
        bowling_innings_df_current_match = get_bowling_innings_df(m)
        ##1st matching
        rows_before = bowling_innings_df_current_match.shape[0]
        left_columns = bowling_innings_df_current_match.columns.tolist()
        bowling_innings_df_current_match = pd.merge(bowling_innings_df_current_match, players_df_current_match[['player_id', 'known_as']], left_on='Bowling', right_on='known_as', how='left').drop(columns=['known_as'])
        #print(bowling_innings_df_current_match)
        
        
        
        # print(bowling_innings_df_current_match.columns)
        # print(bowling_innings_df_current_match.head(5))
        bowling_innings_df_current_match['row_count'] = bowling_innings_df_current_match.groupby(left_columns)['player_id'].transform('count')
        # Set player_id to None for groups with duplicates and keep only the first row
        bowling_innings_df_current_match.loc[bowling_innings_df_current_match['row_count'] > 1, 'player_id'] = None
        bowling_innings_df_current_match = bowling_innings_df_current_match.drop_duplicates(subset=left_columns).drop(columns=['row_count'])

        
        
        #bowling_innings_df_current_match = bowling_innings_df_current_match.drop_duplicates(subset=['Bowling', 'player_id'], keep='last')
        rows_after = bowling_innings_df_current_match.shape[0]
        if rows_before > rows_after:
            print(f"Duplicates were dropped during bowler_id in bowling scorecard matching. Rows before: {rows_before}, Rows after: {rows_after}")

        ##2nd matching
        rows_before = batting_innings_df_current_match.shape[0]
        left_columns = batting_innings_df_current_match.columns.tolist()
        batting_innings_df_current_match = pd.merge(batting_innings_df_current_match, players_df_current_match[['player_id', 'known_as']], left_on='Batting', right_on='known_as', how='left').drop(columns=['known_as'])
        #print(batting_innings_df_current_match)
        #unmatched_records = batting_innings_df_current_match[batting_innings_df_current_match['player_id'].isnull()]

        # Log unmatched records to a file
        # if not unmatched_records.empty:
        #     file_path = "unmatched_records_log.txt"
        #     with open(file_path, "a") as file:
        #         for row in unmatched_records.iterrows():
        #             file.write(f"Couldn't match ID for Batter: {row}\n")
            
        #     print(f"Unmatched records logged to {file_path}")
        
        # Group by all columns from the left table and filter duplicates
        left_columns = batting_innings_df_current_match.columns.tolist()
        batting_innings_df_current_match['row_count'] = batting_innings_df_current_match.groupby(left_columns)['player_id'].transform('count')
        # Set player_id to None for groups with duplicates and keep only the first row
        batting_innings_df_current_match.loc[batting_innings_df_current_match['row_count'] > 1, 'player_id'] = None
        batting_innings_df_current_match = batting_innings_df_current_match.drop_duplicates(subset=left_columns).drop(columns=['row_count'])
        rows_after = batting_innings_df_current_match.shape[0]
        if rows_before > rows_after:
            print(f"Duplicates were dropped during batting player_id in batting scorecard matching. Rows before: {rows_before}, Rows after: {rows_after}")

        ##3rd matching
        rows_before = batting_innings_df_current_match.shape[0]
        left_columns = batting_innings_df_current_match.columns.tolist()
        batting_innings_df_current_match = pd.merge(batting_innings_df_current_match, players_df_current_match[['player_id', 'card_short']].rename(columns={'player_id': 'bowler_id'}), left_on='bowler', right_on='card_short', how='left').drop(columns=['card_short'])
        #print(batting_innings_df_current_match)
        unmatched_records = batting_innings_df_current_match[batting_innings_df_current_match['bowler_id'].isnull()]

        # Log unmatched records to a file
        # if not unmatched_records.empty:
        #     file_path = "unmatched_records_log.txt"
        #     with open(file_path, "a") as file:
        #         for row in unmatched_records.iterrows():
        #             file.write(f"Couldn't match ID for Bowler in Batting Scorecard: {row}\n")
            
        #     print(f"Unmatched records logged to {file_path}")
        
        
        
        # Group by all columns from the left table and filter duplicates
        batting_innings_df_current_match['row_count'] = batting_innings_df_current_match.groupby(left_columns)['bowler_id'].transform('count')
        # Set player_id to None for groups with duplicates and keep only the first row
        batting_innings_df_current_match.loc[batting_innings_df_current_match['row_count'] > 1, 'bowler_id'] = None
        batting_innings_df_current_match = batting_innings_df_current_match.drop_duplicates(subset=left_columns).drop(columns=['row_count'])
        #batting_innings_df_current_match = batting_innings_df_current_match.drop_duplicates(subset=['bowler_id', 'bowler_id', 'bowler'], keep='last')
        rows_after = batting_innings_df_current_match.shape[0]
        if rows_before > rows_after:
            print(f"Duplicates were dropped during bowler_id in batting scorecard matching. Rows before: {rows_before}, Rows after: {rows_after}")

        batting_innings_df_current_match = batting_innings_df_current_match.drop_duplicates(subset=['Batting', 'bowler']).drop_duplicates(subset=['Batting'])
        bowling_innings_df_current_match = bowling_innings_df_current_match.drop_duplicates(subset=['Bowling'])
        
        team1 = re.search(r"(.+?)\s+\d+[/]?\d*\s*\(.*?\)", team1_innings)
        team2 = re.search(r"(.+?)\s+\d+[/]?\d*\s*\(.*?\)", team2_innings)
        ## new pattern r"(.+?)\s+\d+"
        ## working old pattern r"(.+?)\s+\d+[/]?\d*\s*\(.*?\)"
        d = {
            re.match(r"(.+?)\s+\d+", team1_innings).group(1).strip(): m.innings[0]['batting_team_id'],
            re.match(r"(.+?)\s+\d+", team2_innings).group(1).strip(): m.innings[1]['batting_team_id']
        }
        team_ids_list = list(d.values())
        teams_names_list = list(d.keys())

        winner_match = re.match(r"(.+?) won by (\d+) (wicket|wickets|run|runs)", result)
        if winner_match:
            winner = m.team_1_id if m.team_1_abbreviation == m.match_winner else m.team_2_id
            win_margin = winner_match.group(2)
            win_by = winner_match.group(3).strip()
        else:
            winner = ""
            win_margin = ""
            win_by = ""

        if team1:
            team1 = team1.group(1).strip()
        if team2:
            team2 = team2.group(1).strip()

        individual_match_record = {
            'match_id': match_id,
            'match_no': match_no,
            'date': date,
            'venue': venue,
            'team1': team_ids_list[0],
            'team2': team_ids_list[-1],
            'result': result,
            'match_url': match_url,
            'team1_innings': team1_innings,
            'team2_innings': team2_innings,
            'winner': winner,
            'win_by': win_by,
            'win_margin': win_margin
        }
        individual_match_record['date'] = pd.to_datetime(individual_match_record['date'], errors='coerce')

        teams = [
            {'team_id': team_ids_list[0], 'team_name': teams_names_list[0]},
            {'team_id': team_ids_list[-1], 'team_name': teams_names_list[-1]}
        ]

        team1_runs, team1_wickets, team1_overs = extract_runs_overs_wickets_from_string(team1_innings)
        team2_runs, team2_wickets, team2_overs = extract_runs_overs_wickets_from_string(team2_innings)

        innings_data = [
            {
                'match_id': match_id,
                'team_id': team_ids_list[0],
                'runs': team1_runs,
                'wickets': team1_wickets,
                'overs_played': team1_overs,
                'inning_number': 1
            },
            {
                'match_id': match_id,
                'team_id': team_ids_list[-1],
                'runs': team2_runs,
                'wickets': team2_wickets,
                'overs_played': team2_overs,
                'inning_number': 2
            }
        ]

        result_data = {
            'match_id': match_id,
            'match_no': match_no,
            'team_1': team_ids_list[0],
            'team_2': team_ids_list[-1],
            'result': result,
            'win_by': win_by,
            'win_margin': win_margin
        }

        #squads_df_current_match, players_df_current_match = players_df_current_match, squads_df_current_match
        return individual_match_record, teams, innings_data, result_data, squads_df_current_match, players_df_current_match, batting_innings_df_current_match, bowling_innings_df_current_match

    except Exception as e:
        #print("can't write this game: match_id ", match_id, " " , e)
        with open("error_log.txt", "a") as file:
            file.write(f"Can't write this game: match_id {match_id} {e}\n")

        return None, None, None, None, None, None, None, None

def scrape_series_matches(url):
    # Send a request to the URL
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to retrieve data from {url}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    match_blocks = soup.find_all('section', class_='default-match-block')
    # print("pirinting")
    # print(match_blocks)

    #match_data = []
    match_df_columns = ['match_id', 'match_no', 'date', 'venue', 'team1', 'team2', 'result',
           'match_url', 'team1_innings', 'team2_innings', 'winner', 'win_by', 'win_margin']
    match_df = pd.DataFrame(columns=match_df_columns)
    #teams = []
    teams_df_columns = ['team_id', 'team_name']
    teams_df = pd.DataFrame(columns = ['team_id', 'team_name'])
    #innings_data = []
    innings_df_columns = ['match_id', 'team_id', 'runs', 'wickets', 'overs_played', 'inning_number']
    innings_df = pd.DataFrame(columns = innings_df_columns)
    #result_data = []
    result_df_columns = ['match_id', 'match_no', 'team_1', 'team_2', 'result', 'win_by',
       'win_margin']
    result_df = pd.DataFrame(columns = result_df_columns)
    squad_df_columns = ['player_id', 'match_id', 'team_id']
    squads_df = pd.DataFrame(columns=squad_df_columns) 
    
    players_df_columns = ['player_id', 'card_short', 'known_as', 'dob', 'country_id', 'batting_hand', 'batting_style', 'bowling_hand', 'bowling_pacespin', 'bowling_style']
    players_df = pd.DataFrame(columns = players_df_columns)
    
    batting_innings_df_columns = ['match_id', 'innings_number', 'Batting', 'R', 'B', '4s', '6s', 'SR',
       'in at', 'is_out', 'bowler', 'wicket_number', 'wicket_fell_at',
       'player_id', 'bowler_id']
    batting_innings_df = pd.DataFrame(columns = batting_innings_df_columns) 
    
    bowling_innings_df_columns = ['Bowling', 'O', 'M', 'R', 'W', 'ECON', '0s', '4s', '6s', 'WD', 'NB',
       'innings_number', 'match_id', 'player_id']
    bowling_innings_df = pd.DataFrame(columns = bowling_innings_df_columns)

    for match in match_blocks:
        #print('in match')
        if "match cancelled" not in match.text.lower() and "no result" not in match.text.lower():
            match_url = match.find('a')['href']
            match_id = match_url.split('/')[-2]
            individual_match_record, teams_current, innings_data_current, result_data_current, squads_df_current_match, players_df_current_match, batting_innings_df_current_match, bowling_innings_df_current_match = process_match(match, match_id)
            # if individual_match_record:
            #     match_data.append(individual_match_record)
            #     teams.extend(teams_current)
            #     innings_data.extend(innings_data_current)
            #     result_data.append(result_data_current)
            #     squads_df = pd.concat([squads_df, squads_df_current_match], ignore_index=True)
            #     players_df = pd.concat([players_df, players_df_current_match], ignore_index=True)
            #     batting_innings_df = pd.concat([batting_innings_df, batting_innings_df_current_match], ignore_index=True)
            #     bowling_innings_df = pd.concat([bowling_innings_df, bowling_innings_df_current_match], ignore_index=True)

            if individual_match_record:
                match_df = pd.concat([match_df, pd.DataFrame([individual_match_record])], ignore_index=True)
            if teams_current:
                teams_df = pd.concat([teams_df, pd.DataFrame(teams_current)], ignore_index=True)
            if innings_data_current:
                innings_df = pd.concat([innings_df, pd.DataFrame(innings_data_current)], ignore_index=True)
            if result_data_current:
                result_df = pd.concat([result_df, pd.DataFrame([result_data_current])], ignore_index=True)
                # match_data.append(individual_match_record)
                # teams.extend(teams_current)
                # innings_data.extend(innings_data_current)
                # result_data.append(result_data_current)
            if squads_df_current_match is not None:
                squads_df = pd.concat([squads_df, squads_df_current_match], ignore_index=True)
            if players_df_current_match is not None:
                players_df = pd.concat([players_df, players_df_current_match], ignore_index=True)
            if batting_innings_df_current_match is not None:
                batting_innings_df = pd.concat([batting_innings_df, batting_innings_df_current_match], ignore_index=True)
            if bowling_innings_df_current_match is not None:    
                bowling_innings_df = pd.concat([bowling_innings_df, bowling_innings_df_current_match], ignore_index=True)


    #match_df = pd.DataFrame(match_data)
    #teams_df = pd.DataFrame(teams).drop_duplicates()
    teams_df = teams_df.drop_duplicates(subset=['team_id'], keep='first')
    #innings_df = pd.DataFrame(innings_data)
    
    
    # match_df = pd.DataFrame(match_data)
    # #teams_df = pd.DataFrame(teams).drop_duplicates()
    # teams_df = pd.DataFrame(teams).drop_duplicates(subset=['team_id'], keep='first')
    # innings_df = pd.DataFrame(innings_data)

    # innings_df['runs'] = innings_df['runs'].astype(int)
    # innings_df['wickets'] = innings_df['match_id'].astype(int)
    # innings_df['overs_played'] = pd.to_numeric(innings_df['overs_played'])

    innings_df['runs'] = pd.to_numeric(innings_df['runs'], errors='coerce').fillna(0).astype(int)
    innings_df['wickets'] = pd.to_numeric(innings_df['wickets'], errors='coerce').fillna(0).astype(int)
    innings_df['overs_played'] = pd.to_numeric(innings_df['overs_played'], errors='coerce')
    #result_df = pd.DataFrame(result_data)

    match_df = match_df[match_df_columns]
    teams_df = teams_df[teams_df_columns]
    innings_df = innings_df[innings_df_columns]
    result_df = result_df[result_df_columns]
    squads_df = squads_df[squad_df_columns]
    players_df = players_df[players_df_columns]
    batting_innings_df = batting_innings_df[batting_innings_df_columns]
    bowling_innings_df = bowling_innings_df[bowling_innings_df_columns]

    return match_df, teams_df, innings_df, result_df, squads_df, players_df, batting_innings_df, bowling_innings_df

