# main_edit_stats.py
from processors.edit_batting_stats import edit_batting_stats

def main():
    input_path = "data/raw/2025_batting_stats.csv"
    team_path = "data/raw/batter_team_2025.csv"
    output_path = "data/raw/2025_batting_stats_edited.csv"
    result = edit_batting_stats(input_path, team_path, output_path)
    print(f"✅ Edited stats saved to {result}")

if __name__ == "__main__":
    main()
