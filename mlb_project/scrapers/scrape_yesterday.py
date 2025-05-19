from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import logging
import datetime, time, os
from utils.parser import parse_table

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

def run_yesterday_scraper():
    log.info("Initializing headless Chrome...")
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=options)

    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    date_str = yesterday.strftime("%Y%m%d")
    scoreboard_url = f"https://www.espn.com/mlb/scoreboard/_/date/{date_str}"
    log.info(f"Processing scoreboard for: {date_str}")

    driver.get(scoreboard_url)
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/mlb/boxscore/_/gameId/']"))
        )
    except:
        log.warning("Scoreboard did not load in time.")
        driver.quit()
        return None, None

    anchors = driver.find_elements(By.CSS_SELECTOR, "a[href*='/mlb/boxscore/_/gameId/']")
    hrefs = list({a.get_attribute("href") for a in anchors})
    log.info(f"Found {len(hrefs)} unique game links")

    all_games = []

    for idx, href in enumerate(hrefs, start=1):
        log.info(f"Scraping game {idx}/{len(hrefs)}: {href}")
        try:
            driver.get(href)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.Table"))
            )
            time.sleep(1)

            soup = BeautifulSoup(driver.page_source, "html.parser")
            tables = soup.find_all("table", class_="Table")
            log.info(f"   Found {len(tables)} tables")

            if len(tables) < 6:
                log.warning("   Skipping game — not enough tables")
                continue

            away_names = parse_table(tables[2])
            away_stats = parse_table(tables[3])
            home_names = parse_table(tables[4])
            home_stats = parse_table(tables[5])

            away_df = pd.concat([away_names, away_stats], axis=1)
            away_df.columns = [f"away_{c}" for c in away_df.columns]
            home_df = pd.concat([home_names, home_stats], axis=1)
            home_df.columns = [f"home_{c}" for c in home_df.columns]
            merged = pd.concat([away_df, home_df], axis=1)

            all_games.append(merged)
            log.info("   Scraped and added to list")
            time.sleep(2)
        except Exception as e:
            log.error(f"   Error scraping {href}: {e}")
            time.sleep(2)
            continue

    driver.quit()

    if not all_games:
        log.warning("No data scraped from any games.")
        return None, None

    final_df = pd.concat(all_games, ignore_index=True)
    os.makedirs("data/raw", exist_ok=True)

    yesterday_path = "data/raw/2025_batting_stats_yesterday.csv"
    full_path = "data/raw/2025_batting_stats.csv"

    final_df.to_csv(yesterday_path, index=False)
    log.info(f"Saved yesterday's data to: {yesterday_path}")

    if os.path.exists(full_path) and os.path.getsize(full_path) > 0:
        existing_df = pd.read_csv(full_path)
        combined_df = pd.concat([existing_df, final_df], ignore_index=True)
        combined_df.to_csv(full_path, index=False)
        log.info(f"Appended to existing full file: {full_path}")
    else:
        final_df.to_csv(full_path, index=False)
        log.info(f"Created new full file: {full_path}")

    return yesterday_path, full_path
