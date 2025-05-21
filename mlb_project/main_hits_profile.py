from utils.profile_utils import load_and_prepare_def_rank, process_side_data, generate_profile
import pandas as pd

def main():
    stats_2024 = pd.read_csv('data/raw/2024_batting_stats_edited.csv')
    stats_2025 = pd.read_csv('data/raw/2025_batting_stats_edited.csv')

    def_2024, def_2025 = load_and_prepare_def_rank(
        'data/raw/def_rank_hits_allowed_pitchers_2024.csv',
        'data/raw/batter_hits_def_rank.csv'
    )

    df_2024 = process_side_data(stats_2024, def_2024, value_column='H', threshold_column_prefix='hits')
    df_2025 = process_side_data(stats_2025, def_2025, value_column='H', threshold_column_prefix='hits')

    generate_profile(df_2024, df_2025, prefix='hits', output_name='hits')

if __name__ == "__main__":
    main()
