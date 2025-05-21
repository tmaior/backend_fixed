from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import time
import os

def parse_table(table):
    thead = table.find('thead')
    headers = [th.get_text(strip=True) for th in thead.find_all('th')] if thead else \
              [f"Col{i+1}" for i,_ in enumerate(table.find('tbody').find('tr').find_all('td'))]
    rows = [[td.get_text(strip=True) for td in tr.find_all('td')] for tr in table.find('tbody').find_all('tr')]
    return pd.DataFrame(rows, columns=headers)

def scrape_mlb_boxscores(start_date: str, end_date: str, output_path: str) -> str:
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=options)

    all_games = []
    current = datetime.datetime.strptime(start_date, "%Y%m%d")
    end = datetime.datetime.strptime(end_date, "%Y%m%d")

    while current <= end:
        date_str = current.strftime("%Y%m%d")
        scoreboard_url = f"https://www.espn.com/mlb/scoreboard/_/date/{date_str}"
        print(f"\n🔄 Processing {date_str}")

        driver.get(scoreboard_url)
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/mlb/boxscore/_/gameId/']"))
            )
        except:
            print("   ⚠️ Scoreboard did not load in time, skipping date")
            current += datetime.timedelta(days=1)
            continue

        anchors = driver.find_elements(By.CSS_SELECTOR, "a[href*='/mlb/boxscore/_/gameId/']")
        hrefs = []
        for a in anchors:
            href = a.get_attribute("href")
            if href and href not in hrefs:
                hrefs.append(href)

        print(f"  🎯 Found {len(hrefs)} unique games")

        for idx, href in enumerate(hrefs, start=1):
            try:
                game_id = href.split("/gameId/")[1].split('?')[0]
                print(f"    🔍 Game {idx}/{len(hrefs)} → ID {game_id}")

                driver.get(href)
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "table.Table"))
                )
                time.sleep(1)

                soup = BeautifulSoup(driver.page_source, 'html.parser')
                tables = soup.find_all('table', class_='Table')
                if len(tables) < 6:
                    print("       ⚠️ Skipping — insufficient tables")
                    continue

                away_df = pd.concat([parse_table(tables[2]), parse_table(tables[3])], axis=1)
                away_df.columns = [f"away_{c}" for c in away_df.columns]

                home_df = pd.concat([parse_table(tables[4]), parse_table(tables[5])], axis=1)
                home_df.columns = [f"home_{c}" for c in home_df.columns]

                merged = pd.concat([away_df, home_df], axis=1)
                merged.insert(0, "gameId", game_id)
                all_games.append(merged)
                print("       ✅ Scraped")
                time.sleep(2)
            except Exception as e:
                print(f"       ❌ Error scraping {href}: {e}")
                continue

        current += datetime.timedelta(days=1)
        time.sleep(5)

    driver.quit()

    if all_games:
        final_df = pd.concat(all_games, ignore_index=True)
        final_df.to_csv(output_path, index=False)
        print(f"\n✅ DONE — total rows: {len(final_df)}")
        return output_path
    else:
        print("⚠️ No data was scraped.")
        return ""
