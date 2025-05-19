from scrapers.stats_scraper.scrape_batter_team_2025 import scrape_batter_team_2025

def main():
    output_path = scrape_batter_team_2025()
    print(f"✅ batter_team_2025.csv saved to: {output_path}")

if __name__ == "__main__":
    main()