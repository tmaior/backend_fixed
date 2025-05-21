import pandas as pd

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

def prepare_data(year, side, stats_df, def_df):
    stats_df['games_played'] = 1
    df = stats_df[[f'{side}_player', f'{side}_RBI', 'home_team' if side == 'away' else 'away_team', 'games_played']].copy()
    df.columns = ['player', 'RBI', 'opponent_team', 'games_played']
    df = df.merge(def_df, left_on='opponent_team', right_on='team', how='left').drop(columns=['team'])
    df = df.dropna(subset=['player'])
    
    for i in range(1, 5):
        df[f'{i}_RBI_count'] = (df['RBI'] > (i - 0.5)).astype(int)

    return df[['player', 'RBI', 'opponent_team', 'Rank', 'games_played'] + [f'{i}_RBI_count' for i in range(1, 5)]]

def generate_rbi_profile():
    stats_2024 = pd.read_csv('data/raw/2024_batting_stats_edited.csv')
    stats_2025 = pd.read_csv('data/raw/2025_batting_stats_edited.csv')
    def_rank_2024 = pd.read_csv('data/raw/def_rank_era_pitchers_2024.csv')
    def_rank_2025 = pd.read_csv('data/raw/batter_rbi_def_rank.csv')

    def_rank_2024['team'] = def_rank_2024['team'].map(TEAM_MAPPING)
    def_rank_2025['Team'] = def_rank_2025['Team'].map(TEAM_MAPPING)
    def_rank_2025.rename(columns={'Team': 'team'}, inplace=True)

    df_2024 = pd.concat([
        prepare_data(2024, 'home', stats_2024, def_rank_2024),
        prepare_data(2024, 'away', stats_2024, def_rank_2024)
    ], ignore_index=True)

    df_2025 = pd.concat([
        prepare_data(2025, 'home', stats_2025, def_rank_2025),
        prepare_data(2025, 'away', stats_2025, def_rank_2025)
    ], ignore_index=True)

    overs = pd.concat([df_2024[df_2024['Rank'] > 15], df_2025[df_2025['Rank'] > 15]])
    unders = pd.concat([df_2024[df_2024['Rank'] < 16], df_2025[df_2025['Rank'] < 16]])

    overs_freq = overs.groupby('player', as_index=False).agg({
        f'{i}_RBI_count': 'sum' for i in range(1, 5)
    } | {'games_played': 'sum'})

    unders_freq = unders.groupby('player', as_index=False).agg({
        f'{i}_RBI_count': 'sum' for i in range(1, 5)
    } | {'games_played': 'sum'})

    for i in range(1, 5):
        overs_freq[f'{i}_RBI_rate'] = overs_freq[f'{i}_RBI_count'] / overs_freq['games_played']
        unders_freq[f'{i}_RBI_rate'] = unders_freq[f'{i}_RBI_count'] / unders_freq['games_played']

    final = overs_freq.merge(unders_freq, on='player', suffixes=('_over', '_under'))
    final = final[
        (final['1_RBI_rate_under'] <= final['1_RBI_rate_over']) &
        (final['games_played_over'] > 30) &
        (final['games_played_under'] > 30)
    ]

    final_over = final[['player'] + [f'{i}_RBI_rate_over' for i in range(1, 5)]]
    final_under = final[['player'] + [f'{i}_RBI_rate_under' for i in range(1, 5)]]

    final_over.to_csv('data/output/overnewrbi.csv', index=False)
    final_under.to_csv('data/output/undernewrbi.csv', index=False)
    print("✅ overnewrbi.csv and undernewrbi.csv generated.")

if __name__ == "__main__":
    generate_rbi_profile()
