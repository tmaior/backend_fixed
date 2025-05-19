from processors.runs_frequency import generate_runs_frequency

def main():
    input_path = "data/raw/2025_batting_stats_edited.csv"
    over_path, under_path = generate_runs_frequency(input_path)
    print(f"Saved: {over_path}")
    print(f"Saved: {under_path}")

if __name__ == "__main__":
    main()
