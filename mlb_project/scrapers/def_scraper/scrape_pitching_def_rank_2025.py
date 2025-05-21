from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
import time
import os

def scrape_pitching_def_rank_2025(output_dir: str = "data/raw/") -> None:
    os.makedirs(output_dir, exist_ok=True)

    # Setup headless browser
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)

    # Access target page
    url = "https://www.baseball-reference.com/leagues/majors/2025.shtml"
    driver.get(url)
    time.sleep(5)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    table = soup.find("table", {"id": "teams_standard_pitching"})
    if not table:
        raise ValueError("⚠️ Pitching table not found.")

    df_pitching = pd.read_html(str(table))[0]

    # Clean function for all metrics
    def process_metric(df: pd.DataFrame, col: str, ascending: bool) -> pd.DataFrame:
        df = df[['Tm', col]].copy()
        df = df[~df['Tm'].str.contains("Average|Total", na=False)]
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=[col, 'Tm'])
        df = df.sort_values(by=col, ascending=ascending).reset_index(drop=True)
        df['Rank'] = range(1, len(df) + 1)
        df = df[['Tm', 'Rank']].rename(columns={"Tm": "Team"})
        return df

    # Process and save all four rankings
    process_metric(df_pitching, "ERA", ascending=True).to_csv(
        os.path.join(output_dir, "batter_rbi_def_rank.csv"), index=False)
    
    process_metric(df_pitching, "SO9", ascending=False).to_csv(
        os.path.join(output_dir, "batter_strikeouts_def_rank.csv"), index=False)
    
    process_metric(df_pitching, "H9", ascending=True).to_csv(
        os.path.join(output_dir, "batter_hits_def_rank.csv"), index=False)
    
    process_metric(df_pitching, "ERA", ascending=True).to_csv(
        os.path.join(output_dir, "batter_runs_def_rank.csv"), index=False)

    print("✅ Defensive pitching rankings saved.")

