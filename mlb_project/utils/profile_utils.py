import pandas as pd
import os

TEAM_MAPPING = {
    'Milwaukee Brewers': 'MIL', 'Minnesota Twins': 'MIN', 'Chicago Cubs': 'CHC',
    'Colorado Rockies': 'COL', 'Washington Nationals': 'WSH', 'Toronto Blue Jays': 'TOR',
    'Atlanta Braves': 'ATL', 'Baltimore Orioles': 'BAL', 'New York Yankees': 'NYY',
    'Philadelphia Phillies': 'PHI', 'Pittsburgh Pirates': 'PIT', 'Kansas City Royals': 'KCR',
    'Arizona Diamondbacks': 'ARI', 'Boston Red Sox': 'BOS', 'Detroit Tigers': 'DET',
    'Cleveland Guardians': 'CLE', 'Tampa Bay Rays': 'TBR', 'San Francisco Giants': 'SFG',
    'Los Angeles Dodgers': 'LAD', 'Seattle Mariners': 'SEA', 'Cincinnati Reds': 'CIN',
    'Texas Rangers': 'TEX', 'Chicago White Sox': 'CHW', 'Los Angeles Angels': 'LAA',
    'Miami Marlins': 'MIA', 'New York Mets': 'NYM', 'Athletics': 'ATH',
    'St. Louis Cardinals': 'STL', 'San Diego Padres': 'SDP', 'Houston Astros': 'HOU'
}

def load_and_prepare_def_rank(def_rank_2024_path, def_rank_2025_path, key='Team'):
    def_rank_2024 = pd.read_csv(def_rank_2024_path)
    def_rank_2025 = pd.read_csv(def_rank_2025_path)

    def_rank_2024['team'] = def_rank_2024['team'].map(TEAM_MAPPING)
    def_rank_2025[key] = def_rank_2025[key].map(TEAM_MAPPING)
    def_rank_2025.rename(columns={key: 'team'}, inplace=True)

    return def_rank_2024, def_rank_2025


def process_side_data(stats_df, def_rank_df, value_column: str, threshold_column_prefix: str):
    stats_df['games_played'] = 1
    sides = ['home', 'away']
    result = []

    for side in sides:
        player = f'{side}_player'
        value = f'{side}_{value_column}'
        team = 'away_team' if side == 'home' else 'home_team'
        ab = f'{side}_AB'
        bb = f'{side}_BB'

        df = stats_df[[player, value, team, ab, bb, 'games_played']].copy()
        df.columns = ['player', value_column, 'opponent_team', 'AB', 'BB', 'games_played']
        df[['AB', 'BB']] = df[['AB', 'BB']].apply(pd.to_numeric, errors='coerce')
        df = df[(df['AB'] + df['BB']) >= 2].reset_index(drop=True)
        df = df.merge(def_rank_df, left_on='opponent_team', right_on='team', how='left').drop(columns='team')

        for i in range(1, 5):
            df[f'{i}_{threshold_column_prefix}_count'] = (df[value_column] > (i - 0.5)).astype(int)

        result.append(df)

    return pd.concat(result, ignore_index=True)


def generate_profile(df_2024, df_2025, prefix, output_name):
    overs = pd.concat([df_2024[df_2024['Rank'] > 15], df_2025[df_2025['Rank'] > 15]])
    unders = pd.concat([df_2024[df_2024['Rank'] < 16], df_2025[df_2025['Rank'] < 16]])

    overs_freq = overs.groupby('player', as_index=False).agg({
        f'{i}_{prefix}_count': 'sum' for i in range(1, 5)
    } | {'games_played': 'sum'})

    unders_freq = unders.groupby('player', as_index=False).agg({
        f'{i}_{prefix}_count': 'sum' for i in range(1, 5)
    } | {'games_played': 'sum'})

    for i in range(1, 5):
        overs_freq[f'{i}_{prefix}_rate'] = overs_freq[f'{i}_{prefix}_count'] / overs_freq['games_played']
        unders_freq[f'{i}_{prefix}_rate'] = unders_freq[f'{i}_{prefix}_count'] / unders_freq['games_played']

    final = overs_freq.merge(unders_freq, on='player', suffixes=('_over', '_under'))
    final = final[
        (final[f'1_{prefix}_rate_under'] <= final[f'1_{prefix}_rate_over']) &
        (final['games_played_over'] > 30) &
        (final['games_played_under'] > 30)
    ]

    os.makedirs('data/output', exist_ok=True)
    final_over = final[['player'] + [f'{i}_{prefix}_rate_over' for i in range(1, 5)]]
    final_under = final[['player'] + [f'{i}_{prefix}_rate_under' for i in range(1, 5)]]

    final_over.to_csv(f'data/output/overnew{output_name}.csv', index=False)
    final_under.to_csv(f'data/output/undernew{output_name}.csv', index=False)

    print(f"✅ overnew{output_name}.csv and undernew{output_name}.csv generated.")
