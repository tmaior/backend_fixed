from scrapers.scrape_yesterday import run_yesterday_scraper

def main():
    daily_path, full_path = run_yesterday_scraper()
    print("✅ Scraped:", daily_path)
    print("📁 Appended to:", full_path)

if __name__ == "__main__":
    main()
