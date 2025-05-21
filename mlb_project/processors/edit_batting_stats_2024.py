import pandas as pd
import unicodedata
import re
from difflib import get_close_matches

def normalize_name(name):
    if not isinstance(name, str):
        return name
    return unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('utf-8')

def clean_name_symbols(name):
    if not isinstance(name, str):
        return name
    return name.replace('*', '').replace('#', '')

def clean_name(raw):
    if not isinstance(raw, str):
        return None
    m = re.search(r"((?:[A-Za-z]\.)+\s[A-Za-z'-]+)", raw)
    if m:
        name = m.group(1)
        strip = re.match(r"^((?:[A-Za-z]\.)+\s[A-Za-z'-]+?)(?:[0-9A-Z-]+)$", name)
        return strip.group(1) if strip else name
    m2 = re.search(r"([A-Za-z]+ [A-Za-z'-]+)(?=[0-9-])", raw)
    if m2:
        return m2.group(1)
    m3 = re.search(r"([A-Za-z]+ [A-Za-z'-]+)", raw)
    return m3.group(1) if m3 else None

def match_full_name(short_name, full_names):
    if not isinstance(short_name, str):
        return None
    last_name = short_name.split()[-1]
    full_names = [name for name in full_names if isinstance(name, str)]
    possible_matches = [name for name in full_names if last_name in name and name[0] == short_name[0]]
    if possible_matches:
        return possible_matches[0]
    close = get_close_matches(short_name, full_names, n=1, cutoff=0.5)
    return close[0] if close else None

def assign_game_ids_by_scan(df, away_col='away_hitters', home_col='home_hitters', summary_val='team', id_col='gameID'):
    df = df.copy()
    game_id = 1
    away_seen = home_seen = False
    ids = []
    for _, row in df.iterrows():
        ids.append(game_id)
        if str(row[away_col]).strip().lower() == summary_val:
            away_seen = True
        if str(row[home_col]).strip().lower() == summary_val:
            home_seen = True
        if away_seen and home_seen:
            game_id += 1
            away_seen = home_seen = False
    df[id_col] = ids
    return df

def generate_batting_stats_2024(input_path, team_path, output_path):
    stats_2024 = pd.read_csv(input_path)
    batter_team_2024 = pd.read_csv(team_path)

    batter_team_2024['player'] = batter_team_2024['player'].apply(normalize_name).apply(clean_name_symbols)
    batter_team_2024 = batter_team_2024[~batter_team_2024['team'].isin(['2TM', '3TM', '4TM', '5TM'])]
    batter_team_2024 = batter_team_2024.drop_duplicates(subset='player', keep='last').reset_index(drop=True)

    stats_2024['short_name'] = stats_2024['away_hitters'].apply(clean_name)
    stats_2024['matched_name'] = stats_2024['short_name'].apply(lambda x: match_full_name(x, batter_team_2024['player'].tolist()))
    merged_df = stats_2024.merge(batter_team_2024, left_on='matched_name', right_on='player', how='left')
    merged_df = merged_df.drop(columns=['short_name', 'matched_name']).rename(columns={'player': 'away_player', 'team': 'away_team'})

    merged_df['short_name'] = merged_df['home_hitters'].apply(clean_name)
    merged_df['matched_name'] = merged_df['short_name'].apply(lambda x: match_full_name(x, batter_team_2024['player'].tolist()))
    merged_df = merged_df.merge(batter_team_2024, left_on='matched_name', right_on='player', how='left')
    merged_df = merged_df.drop(columns=['short_name', 'matched_name']).rename(columns={'player': 'home_player', 'team': 'home_team'})

    merged_df = assign_game_ids_by_scan(merged_df)

    for col in ['home_team', 'away_team']:
        merged_df[col] = merged_df.groupby('gameID')[col].transform(lambda x: x.ffill().bfill())

    merged_df.to_csv(output_path, index=False)
    print(f"✅ Saved edited 2024 batting stats to {output_path}")
