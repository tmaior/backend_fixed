from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
import time
import unicodedata


def clean_name_symbols(name):
    if not isinstance(name, str):
        return name
    return name.replace('*', '').replace('#', '')

def normalize_name(name):
    if not isinstance(name, str):
        return name
    return unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('utf-8')

def scrape_batter_team_2025(output_path: str = "data/raw/batter_team_2025.csv") -> str:
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("user-agent=Mozilla/5.0")

    driver = webdriver.Chrome(options=options)

    url = "https://www.baseball-reference.com/leagues/majors/2025-standard-batting.shtml"
    driver.get(url)

    time.sleep(5)
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")

    table = soup.find('table', {'id': 'players_standard_batting'})

    if table is None:
        driver.quit()
        raise ValueError("Table not found after JS load.")

    df = pd.read_html(str(table))[0]
    df = df[df['Rk'] != 'Rk'].reset_index(drop=True)

    df2 = df[['Player', 'Team']]
    multi_team_flags = ['2TM', '3TM', '4TM', '5TM']
    df2 = df2[~df2['Team'].isin(multi_team_flags)].copy()
    df2 = df2.drop_duplicates(subset='Player', keep='last').reset_index(drop=True)

    df2['Player'] = df2['Player'].apply(clean_name_symbols).apply(normalize_name)

    df2.to_csv(output_path, index=False)
    driver.quit()
    return output_path

if __name__ == "__main__":
    path = scrape_batter_team_2025()
    print(f"✅ Scraped and saved to {path}")
