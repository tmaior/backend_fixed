from processors.edit_batting_stats_2024 import generate_batting_stats_2024

def main():
    input_path = "data/raw/2024_batting_stats.csv"
    team_path = "data/raw/batters_team_2024.csv"
    output_path = "data/raw/2024_batting_stats_edited.csv"

    generate_batting_stats_2024(input_path, team_path, output_path)

if __name__ == "__main__":
    main()
