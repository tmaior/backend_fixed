from scrapers.stats_scraper.scrape_daily_boxscores import scrape_mlb_boxscores

def main():
    start_date = "20240928"
    end_date = "20240929"
    output_path = "data/raw/2024_batting_stats.csv"

    result = scrape_mlb_boxscores(start_date, end_date, output_path)
    if result:
        print(f"✔ Appended to:: {result}")

if __name__ == "__main__":
    main()
