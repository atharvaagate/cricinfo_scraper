import requests
from bs4 import BeautifulSoup
import pandas as pd

# Replace this with your actual parent URL

def seasons_and_urls(parent_url):
    #parent_url = "https://www.espncricinfo.com/ci/engine/series/index.html?view=season"  # Change to the correct URL
    
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
            season_links[span.text] = "https://www.espncricinfo.com" + link['href']

    data = []
    
    # Print the results
    for year, link in season_links.items():
        #print(f"{year}: {link}")
        data.append((year, link))
    df = pd.DataFrame(data, columns=['Year', 'Link'])
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

            link = block.find('a', href=True)
            if link:
                series_data.append({
                    'url': link['href'],
                    'match_type': match_type,
                    'series_name': link.text.strip(),  # Extract the series name
                    'year' : year
                })

            df = pd.DataFrame(series_data, columns=['year', 'series_name', 'match_type', 'url'])

        return df
    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")
        return []
