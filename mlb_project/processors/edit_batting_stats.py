# processors/edit_batting_stats.py
import pandas as pd
import unicodedata
import re
from difflib import get_close_matches


def normalize_name(name):
    if not isinstance(name, str): return name
    return unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('utf-8')


def clean_name_symbols(name):
    if not isinstance(name, str): return name
    return name.replace('*', '').replace('#', '')


def clean_name(raw):
    if not isinstance(raw, str): return None
    m = re.search(r"((?:[A-Za-z]\.)+\s[A-Za-z'-]+)", raw)
    if m:
        name = m.group(1)
        strip = re.match(r"^((?:[A-Za-z]\.)+\s[A-Za-z'-]+?)(?:[0-9A-Z-]+)$", name)
        return strip.group(1) if strip else name
    m2 = re.search(r"([A-Za-z]+ [A-Za-z'-]+)(?=[0-9-])", raw)
    if m2: return m2.group(1)
    m3 = re.search(r"([A-Za-z]+ [A-Za-z'-]+)", raw)
    return m3.group(1) if m3 else None


def match_full_name(short_name, full_names):
    if not isinstance(short_name, str): return None
    last_name = short_name.split()[-1]
    full_names = [name for name in full_names if isinstance(name, str)]
    possible_matches = [name for name in full_names if last_name in name and name[0] == short_name[0]]
    if possible_matches: return possible_matches[0]
    close = get_close_matches(short_name, full_names, n=1, cutoff=0.5)
    return close[0] if close else None


def assign_game_ids_by_scan(df: pd.DataFrame,
                            away_col: str = 'away_hitters',
                            home_col: str = 'home_hitters',
                            summary_val: str = 'team',
                            id_col: str = 'gameID') -> pd.DataFrame:
    df = df.copy()
    game_id = 1
    away_seen = home_seen = False
    ids = []
    for _, row in df.iterrows():
        ids.append(game_id)
        if str(row[away_col]).strip().lower() == summary_val: away_seen = True
        if str(row[home_col]).strip().lower() == summary_val: home_seen = True
        if away_seen and home_seen:
            game_id += 1
            away_seen = home_seen = False
    df[id_col] = ids
    return df


def edit_batting_stats(input_path: str, team_path: str, output_path: str):
    stats = pd.read_csv(input_path)
    teams = pd.read_csv(team_path)

    teams['Player'] = teams['Player'].apply(normalize_name).apply(clean_name_symbols)

    full_names = teams['Player'].tolist()
    stats['short_name'] = stats['away_hitters'].apply(clean_name)
    stats['matched_name'] = stats['short_name'].apply(lambda x: match_full_name(x, full_names))

    merged = stats.merge(teams, left_on='matched_name', right_on='Player', how='left')
    merged = merged.drop(columns=['short_name', 'matched_name', 'Unnamed: 0'], errors='ignore')
    merged.rename(columns={'Player': 'away_player', 'Team': 'away_team'}, inplace=True)

    merged['short_name'] = merged['home_hitters'].apply(clean_name)
    merged['matched_name'] = merged['short_name'].apply(lambda x: match_full_name(x, full_names))

    merged2 = merged.merge(teams, left_on='matched_name', right_on='Player', how='left')
    merged2 = merged2.drop(columns=['short_name', 'matched_name', 'Unnamed: 0'], errors='ignore')
    merged2.rename(columns={'Player': 'home_player', 'Team': 'home_team'}, inplace=True)

    merged2 = assign_game_ids_by_scan(merged2)
    for col in ['away_team', 'home_team']:
        merged2[col] = merged2.groupby('gameID')[col].transform(lambda x: x.ffill().bfill())

    merged2.to_csv(output_path, index=False)
    return output_path
