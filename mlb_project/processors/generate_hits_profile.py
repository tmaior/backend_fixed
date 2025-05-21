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

def process_hits_data(stats_df, def_rank_df):
    stats_df['games_played'] = 1
    side_data = []

    for side in ['home', 'away']:
        player = f'{side}_player'
        hits = f'{side}_H'
        team = 'away_team' if side == 'home' else 'home_team'
        ab = f'{side}_AB'
        bb = f'{side}_BB'

        df = stats_df[[player, hits, team, ab, bb, 'games_played']].copy()
        df.columns = ['player', 'hits', 'opponent_team', 'AB', 'BB', 'games_played']
        df[['AB', 'BB']] = df[['AB', 'BB']].apply(pd.to_numeric, errors='coerce')
        df = df[(df['AB'] + df['BB']) >= 2].reset_index(drop=True)
        df = df.merge(def_rank_df, left_on='opponent_team', right_on='team', how='left')
        df.drop(columns=['team'], inplace=True)

        for i in range(1, 5):
            df[f'{i}_hits_count'] = (df['hits'] > (i - 0.5)).astype(int)

        side_data.append(df)

    return pd.concat(side_data, ignore_index=True)

def generate_hits_profile():
    stats_2024 = pd.read_csv('../general/stats/2024_batting_stats_edited.csv')
    stats_2025 = pd.read_csv('../general/stats/2025_batting_stats_edited.csv')
    def_rank_2024 = pd.read_csv('def_rank_hits_allowed_pitchers_2024.csv')
    def_rank_2025 = pd.read_csv('../scrapers/def_scraper/batter_hits_def_rank.csv')

    def_rank_2024['team'] = def_rank_2024['team'].map(TEAM_MAPPING)
    def_rank_2025['Team'] = def_rank_2025['Team'].map(TEAM_MAPPING)
    def_rank_2025.rename(columns={'Team': 'team'}, inplace=True)

    df_2024 = process_hits_data(stats_2024, def_rank_2024)
    df_2025 = process_hits_data(stats_2025, def_rank_2025)

    overs = pd.concat([df_2024[df_2024['Rank'] > 15], df_2025[df_2025['Rank'] > 15]])
    unders = pd.concat([df_2024[df_2024['Rank'] < 16], df_2025[df_2025['Rank'] < 16]])

    overs_freq = overs.groupby('player', as_index=False).agg({
        f'{i}_hits_count': 'sum' for i in range(1, 5)
    } | {'games_played': 'sum'})

    unders_freq = unders.groupby('player', as_index=False).agg({
        f'{i}_hits_count': 'sum' for i in range(1, 5)
    } | {'games_played': 'sum'})

    for i in range(1, 5):
        overs_freq[f'{i}_hits_rate'] = overs_freq[f'{i}_hits_count'] / overs_freq['games_played']
        unders_freq[f'{i}_hits_rate'] = unders_freq[f'{i}_hits_count'] / unders_freq['games_played']

    final = overs_freq.merge(unders_freq, on='player', suffixes=('_over', '_under'))
    final = final[
        (final['1_hits_rate_under'] <= final['1_hits_rate_over']) &
        (final['games_played_over'] > 30) &
        (final['games_played_under'] > 30)
    ]

    os.makedirs('data/output', exist_ok=True)
    final_over = final[['player'] + [f'{i}_hits_rate_over' for i in range(1, 5)]]
    final_under = final[['player'] + [f'{i}_hits_rate_under' for i in range(1, 5)]]

    final_over.to_csv('data/output/overnewhits.csv', index=False)
    final_under.to_csv('data/output/undernewhits.csv', index=False)
    print("✅ overnewhits.csv and undernewhits.csv generated.")

if __name__ == "__main__":
    generate_hits_profile()
