import pandas as pd
import os

def generate_runs_frequency(input_path: str):
    df = pd.read_csv(input_path)


    df['games_played'] = 1
    df['home_R'] = pd.to_numeric(df.get('home_R', 0), errors='coerce')
    df['away_R'] = pd.to_numeric(df.get('away_R', 0), errors='coerce')


    home_df = df[['home_player', 'home_R', 'away_team', 'games_played']].copy()
    away_df = df[['away_player', 'away_R', 'home_team', 'games_played']].copy()

    home_df.columns = ['player', 'runs', 'team', 'games_played']
    away_df.columns = ['player', 'runs', 'team', 'games_played']

    merged_df = pd.concat([home_df, away_df], ignore_index=True)


    for i in range(1, 5):
        merged_df[f'{i}_runs_count'] = (merged_df['runs'] > i - 0.5).astype(int)

    grouped = merged_df.groupby('player', as_index=False).agg({
        '1_runs_count': 'sum',
        '2_runs_count': 'sum',
        '3_runs_count': 'sum',
        '4_runs_count': 'sum',
        'games_played': 'sum'
    })

    for i in range(1, 5):
        grouped[f'{i}_runs_rate'] = grouped[f'{i}_runs_count'] / grouped['games_played']


    os.makedirs("data/output", exist_ok=True)
    over_path = "data/output/runs_freq_over.csv"
    under_path = "data/output/runs_freq_under.csv"

    grouped.to_csv(over_path, index=False)
    grouped.to_csv(under_path, index=False)  

    return over_path, under_path
