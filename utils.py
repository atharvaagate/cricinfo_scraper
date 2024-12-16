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
        print('got data')
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
    
    
    players_df_cols = ['player_id', 'match_id', 'team_id'] + [col for col in players_df.columns if col not in ['player_id', 'match_id', 'team_id']]
    players_df = players_df[players_df_cols]
    #print(df.columns)


    squads_df_cols = ['player_id', 'match_id', 'team_id']
    squads_df = players_df[squads_df_cols]

    players_df = players_df[['player_id', 'card_short', 'known_as', 'dob', 'country_id', 'batting_hand', 'batting_style', 'bowling_hand', 'bowling_pacespin', 'bowling_style']]


    
    #print(df.columns)
    #squads_df = squads_df[['player_id', 'card_short', 'known_as', 'dob', 'batting_hand', 'batting_style', 'bowling_hand', 'bowling_pacespin', 'bowling_style']]


    return players_df, squads_df



def get_batting_innings_df(match) : 
    m = match
    table_MN = pd.read_html(m.match_url)
    
    f_inn_bat_1 = table_MN[0]
    f_inn_bat_1 = f_inn_bat_1.dropna(how='all')
    print(f_inn_bat_1)
    #print(f_inn_bat_1.head(11))
    f_inn_bat_1['innings_number'] = 1
    # Create the 'in at' column
    f_inn_bat_1['in at'] = 0  # Initialize all values to 0
    #print(f_inn_bat_1)
    f_inn_bat_1.loc[2:, 'in at'] = range(3, len(f_inn_bat_1) + 1)  # Assign increasing values starting from 3

    f_inn_bat_2 = table_MN[2]
    f_inn_bat_2 = f_inn_bat_2.dropna(how='all')
    f_inn_bat_2['innings_number'] = 2
    # Create the 'in at' column
    f_inn_bat_2['in at'] = 0  # Initialize all values to 0
    #print(f_inn_bat_2)
    f_inn_bat_2.loc[2:, 'in at'] = range(3, len(f_inn_bat_2) + 1)  # Assign increasing values starting from 3


    #f_inn_bat.con
    
    df = pd.concat([f_inn_bat_1, f_inn_bat_2], ignore_index=True)
    df['match_id'] = m.match_id
    
    # Convert 'R' column to numeric, forcing errors to NaN
    df['B'] = pd.to_numeric(df['B'], errors='coerce')
    
    
    # Remove rows where 'R' is NaN
    df = df.dropna(subset=['B'])
    df['B'] = df['B'].astype(int)
    
    # Create is_out column based on whether the player is out
    df['is_out'] = df['Unnamed: 1'].apply(lambda x: 'not out' not in x)


    def clean_name(name):
    # Remove brackets and their contents, then remove any characters that are not letters or spaces
        name = re.sub(r'\(.*?\)', '', name)  # Remove content within parentheses
        return re.sub(r'[^a-zA-Z\s]', '', name).strip()  # Remove any characters that are not letters or spaces
    df['Batting'] = df['Batting'].apply(clean_name)

    
    # Create bowler column by extracting the bowler's name
    def extract_bowler(dismissal):
        if 'b ' in dismissal:
            return dismissal.split('b ')[-1]  # Extract the bowler's name after 'b'
        return np.nan  # Return NaN if no bowler involved
    
    df['bowler'] = df['Unnamed: 1'].apply(extract_bowler)

    df = df.drop(['Unnamed: 1', 'Unnamed: 8', 'Unnamed: 9', 'M'], axis=1)

    fall_of_wickets_1, fall_of_wickets_2 = table_MN[0].iloc[-1]['Batting'], table_MN[2].iloc[-1]['Batting']
    # Combine the two strings
    combined_wickets = f"{fall_of_wickets_1}, {fall_of_wickets_2}"
    #print(combined_wickets)
    
    # Use regex to find all the wicket information
    wicket_info = re.findall(r'(\d+)-(\d+) \((.*?)\)', combined_wickets)
    
    #print(wicket_info)
    ## Create a dictionary to map players to their wicket numbers and runs when they got out
    wicket_map = {player.split(',')[0]: {'wicket_number': int(wicket), 'wicket_fell_at': int(runs)}
                   for wicket, runs, player in wicket_info}
    #print(wicket_map)
    
    # Initialize new columns in the DataFrame
    df['wicket_number'] = None
    df['wicket_fell_at'] = None
    
    # Map the values from the wicket_map to the DataFrame
    for player, info in wicket_map.items():
        df.loc[df['Batting'] == player, 'wicket_number'] = info['wicket_number']
        df.loc[df['Batting'] == player, 'wicket_fell_at'] = info['wicket_fell_at']

    cols = ['match_id', 'innings_number'] + [col for col in df.columns if col not in ['a', 'b']]
    df = df[cols]

    return df
    



import numpy as np
def get_bowling_innings_df(match) : 
    m = match
    table_MN = pd.read_html(m.match_url)
    #print(table_MN)
    #table_MN = table_MN.dropna(how='all')
    f_inn_bwl = table_MN[1].dropna(how='all')
    f_inn_bwl['innings_number'] = 1
    s_inn_bwl = table_MN[3].dropna(how='all')
    s_inn_bwl['innings_number'] = 2

    #f_inn_bat.con
    
    df = pd.concat([f_inn_bwl, s_inn_bwl], ignore_index=True)
    df['match_id'] = m.match_id
    
    # Convert 'R' column to numeric, forcing errors to NaN
    df['R'] = pd.to_numeric(df['R'], errors='coerce')
    
    # Remove rows where 'R' is NaN
    df = df.dropna(subset=['R'])
    df['R'] = df['R'].astype(int)
    
    # # Create is_out column based on whether the player is out
    # df['is_out'] = df['Unnamed: 1'].apply(lambda x: 'not out' not in x)
    
    # # Create bowler column by extracting the bowler's name
    # def extract_bowler(dismissal):
    #     if 'b ' in dismissal:
    #         return dismissal.split('b ')[-1]  # Extract the bowler's name after 'b'
    #     return np.nan  # Return NaN if no bowler involved
    
    # df['bowler'] = df['Unnamed: 1'].apply(extract_bowler)

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
            return None
    
    #print("Team name not found.")
    return None



def scrape_series_matches(url):
    #print('k')
    # Send a request to the URL
    response = requests.get(url)
    # Check if the request was successful
    if response.status_code != 200:
        print(f"Failed to retrieve data from {url}")
        return []

    # Parse the HTML content
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the match schedule section
        # Extract match info
    match_blocks = soup.find_all('section', class_='default-match-block')
    #print(match_blocks)
    
    # Initialize lists to store data
    match_data = []
    teams = []
    innings_data = []
    result_data = []
    
    for match in match_blocks:
        
        ## creating a dict once and for all
 
        if "match cancelled" not in match.text.lower() and "no result" not in match.text.lower():
            try:

                match_no = match.find('span', class_='match-no').text.strip()
                venue = match_no.split('at ')[1] if 'at' in match_no else 'Not Known'
                match_no = match_no.split('at ')[0]
                #match_no.split(' ')[0]

                date = match.find('span', class_='bold').text.strip()

                #print(match)
                team1_innings = match.find('div', class_='innings-info-1').text.strip()
                team2_innings = match.find('div', class_='innings-info-2').text.strip()
                result = match.find('div', class_='match-status').text.strip()
                match_url = match.find('a')['href']
                match_id = match_url.split('/')[-2]
                m = Match(match_id)

                # squads_df, players_df = squads_df_and_players_df_from_a_match_id(m)
                # batting_innings_df = get_batting_innings_df(m)
                # bowling_innings_df = get_bowling_innings_df(m)

                #print("******")
                

                ## Need this dictionary!!!
                #import re
                #print('team innings here: ', team1_innings)
                #print('team innings here: ', team2_innings)
                
                team1 = re.search(r"(.+?)\s+\d+[/]?\d*\s*\(.*?\)", team1_innings)
                team2 = re.search(r"(.+?)\s+\d+[/]?\d*\s*\(.*?\)", team2_innings)

             


                d = {
                    re.match(r"(.+?)\s+\d+[/]?\d*\s*\(.*?\)", team1_innings).group(1).strip(): m.innings[0]['batting_team_id'],
                    re.match(r"(.+?)\s+\d+[/]?\d*\s*\(.*?\)", team2_innings).group(1).strip(): m.innings[1]['batting_team_id']#, 
                    #"": ""
                }
                team_ids_list = list(d.values())
                teams_names_list = list(d.keys())
                #print(d)

                # Use regex to extract winner and win details
                winner_match = re.match(r"(.+?) won by (\d+) (wicket|wickets|run|runs)", result)

                if winner_match:
                    winner = m.team_1_id if m.team_1_abbreviation == m.match_winner else m.team_2_id
                    #print('found winner', winner_match)
                    #winner = winner_match.group(1).strip()  # Extracts the winning team name
                    win_margin = winner_match.group(2)  # Extracts the win margin (number of runs or wickets)
                    win_by = winner_match.group(3).strip()  # Extracts whether the win was by 'wickets' or 'runs'
                else:
                    #print('not found winner')
                    winner = ""
                    win_margin = ""
                    win_by = ""

                # Extract team names from innings data
                
                if team1:
                    team1 = team1.group(1).strip()  # Return team name without leading/trailing spaces
                
                if team2:
                    team2 = team2.group(1).strip()  # Return team name without leading/trailing spaces
                
                #print('team names here ', team1.strip(), ",", team2.strip())    
                individual_match_record = {
                    'match_id': match_id,
                    'match_no': match_no,  # Extract "1st T20I"
                    'date': date,
                    'venue': venue,
                    'team1': team_ids_list[0],
                    'team2': team_ids_list[-1],
                    'result': result,
                    'match_url': match_url,
                    'team1_innings': team1_innings,
                    'team2_innings': team2_innings,
                    #'winner': d[winner],         # Adding the winner
                    'win_by': win_by,         # Adding if won by 'wickets' or 'runs'
                    'win_margin': win_margin  # Adding how many wickets or runs
                }
                #individual_match_record['winner'] = d[winner] if winner != "" else ""
                individual_match_record['winner'] = winner
                
                # Append all data into match_data list
                match_data.append(individual_match_record
#                     
                )

                ## Logic for getting teams df

                # Append the first and last key-value pairs correctly
                teams.append({'team_id': team_ids_list[0], 'team_name': teams_names_list[0]})
                teams.append({'team_id': team_ids_list[-1], 'team_name': teams_names_list[-1]})
                #print(teams)




                ## Logic for getting inninga_df


                '''
                Testing start
                '''

                    

                team1 = team_ids_list[0]
                team2 = team_ids_list[-1]
                team1_runs, team1_wickets, team1_overs = extract_runs_overs_wickets_from_string(team1_innings)
                team2_runs, team2_wickets, team2_overs = extract_runs_overs_wickets_from_string(team2_innings)
                #print('no problem getting those 3')
                '''
                Testing End
                '''
                

                # Append both innings
                innings_data.append({
                    'match_id': match_id,
                    #'match_no': match_no,
                    'team_id': team1,
                    'runs': team1_runs,
                    'wickets': team1_wickets,
                    'overs_played': team1_overs,
                    'inning_number': 1
                })
                innings_data.append({
                    'match_id': match_id,
                    #'match_no': match_no,
                    'team_id': team2,
                    'runs': team2_runs,
                    'wickets': team2_wickets,
                    'overs_played': team2_overs,
                    'inning_number': 2
                })



                ## Logic for getting results_df


                result_parts = result.split(' ')
                won_by = win_by  # "7 wickets" or "x runs"
                win_margin = win_margin

                result_data.append({
                    'match_id': match_id,
                    'match_no': match_no,
                    'team_1': team1,
                    'team_2': team2,
                    'result': result,
                    'win_by': won_by,
                    'win_margin': win_margin
                })
            except Exception as e :
                print("can't write this game: ", e)
                continue
        #break
                


        
    
    # Create a DataFrame
    match_df = pd.DataFrame(match_data)
    #print(match_df.columns)
    teams_df = pd.DataFrame(teams).drop_duplicates()
    innings_df = pd.DataFrame(innings_data)
    result_df = pd.DataFrame(result_data)
    #print(match_df.head(5))


    return match_df, teams_df, innings_df, result_df #, grounds_df

