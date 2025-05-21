import pandas as pd
import unicodedata
import json
import requests

def normalize_name(name):
    if not isinstance(name, str):
        return name
    return unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('utf-8')

def clean_name_symbols(name):
    if not isinstance(name, str):
        return name
    return name.replace('*', '').replace('#', '')

def calculate_ev(row):
    if row['overUnder'] == 0.5:
        rate = row['1_RBI_rate_over']
    elif row['overUnder'] == 1.5:
        rate = row['2_RBI_rate_over']
    elif row['overUnder'] == 2.5:
        rate = row['3_RBI_rate_over']
    elif row['overUnder'] == 3.5:
        rate = row['4_RBI_rate_over']
    else:
        return None
    return (rate * row['profit']) - ((1 - rate) * 100)

def calculate_under_ev(row):
    if row['overUnder'] == 0.5:
        rate = row['1_RBI_rate_under']
    elif row['overUnder'] == 1.5:
        rate = row['2_RBI_rate_under']
    elif row['overUnder'] == 2.5:
        rate = row['3_RBI_rate_under']
    elif row['overUnder'] == 3.5:
        rate = row['4_RBI_rate_under']
    else:
        return None
    return ((1 - rate) * row['profit']) - (rate * 100)

def generate_rbi_odds():
    under_rbi_rate = pd.read_csv('undernew.csv')
    over_rbi_rate = pd.read_csv('overnew.csv')
    def_rank = pd.read_csv('../scrapers/def_scraper/batter_rbi_def_rank.csv')
    batter_team_2025 = pd.read_csv('../general/stats/batter_team_2025.csv')

    def_rank.rename(columns={'Rank': 'def_rank', 'Team': 'team'}, inplace=True)
    batter_team_2025.rename(columns={'Team': 'team', 'Player': 'player'}, inplace=True)

    team_mapping_5 = {
        'CHC': 'CHC', 'LAD': 'LAD', 'ARI': 'ARI', 'PIT': 'PIT', 'MIA': 'MIA', 'TOR': 'TOR',
        'BOS': 'BOS', 'ATH': 'ATH', 'BAL': 'BAL', 'SEA': 'SEA', 'SDP': 'SDP', 'ATL': 'ATL',
        'PHI': 'PHI', 'TEX': 'TEX', 'MIL': 'MIL', 'WSN': 'WSH', 'NYY': 'NYY', 'STL': 'STL',
        'TBR': 'TBR', 'DET': 'DET', 'KCR': 'KCR', 'CLE': 'CLE', 'COL': 'COL', 'NYM': 'NYM',
        'CIN': 'CIN', 'MIN': 'MIN', 'SFG': 'SFG', 'HOU': 'HOU', 'CHW': 'CHW', 'LAAs': 'LAA'
    }

    team_mapping = {
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

    batter_team_2025['team'] = batter_team_2025['team'].map(team_mapping_5)
    def_rank['team'] = def_rank['team'].map(team_mapping)
    batter_team_2025['player'] = batter_team_2025['player'].apply(normalize_name).apply(clean_name_symbols)

    headers = { 'X-Api-Key': 'fac7e1a69f2baa47da395bde36dc869c' }
    url = 'https://api.sportsgameodds.com/v2/events/'
    next_cursor = None
    event_data = []

    while True:
        try:
            response = requests.get(url, headers=headers, params={
                "sportID": "BASEBALL",
                "leagueID": "MLB",
                "oddIDs": "batting_RBI-PLAYER_ID-game-ou-over",
                "includeOpposingOddIDs": "true",
                "oddsAvailable": "true",
                "cursor": next_cursor,
                "include": "teams",
                'started': "false"
            })
            response.raise_for_status()
            data = response.json()
            event_data.extend(data.get("data", []))
            next_cursor = data.get("nextCursor")
            if not next_cursor:
                break
        except requests.exceptions.RequestException:
            break

    odds_list = []
    for event in event_data:
        event_id = event.get("eventID")
        teams = sorted(set(p.get("teamID") for p in event.get("players", {}).values() if p.get("teamID")))
        odds = event.get("odds", {})
        for odd in odds.values():
            base = {
                "eventID": event_id,
                "teams": ", ".join(teams),
                "oddID": odd.get("oddID"),
                "opposingOddID": odd.get("opposingOddID"),
                "marketName": odd.get("marketName"),
                "playerID": odd.get("statEntityID"),
                "side": odd.get("sideID"),
                "line": odd.get("bookOverUnder", odd.get("fairOverUnder", "N/A")),
                "score": odd.get("score", "N/A"),
            }
            for book, book_data in odd.get("byBookmaker", {}).items():
                row = base.copy()
                row["bookmaker"] = book
                row["odds"] = book_data.get("odds", "N/A")
                row["overUnder"] = book_data.get("overUnder", "N/A")
                odds_list.append(row)

    df = pd.DataFrame(odds_list)
    df['sport'] = df['teams'].str[-3:]
    df[['team_1', 'team_2']] = df['teams'].str.replace('_MLB', '', regex=False).str.split(', ', expand=True)
    df = df.drop(columns=['teams'])
    df['player'] = df['marketName'].str.extract(r'^(.*?)\s*Runs')
    df['team_1'] = df['team_1'].map(team_mapping)
    df['team_2'] = df['team_2'].map(team_mapping)
    df['overUnder'] = pd.to_numeric(df['overUnder'], errors='coerce')
    df['bet'] = 'RBI'
    df = df[['player','side','overUnder','bookmaker','odds','sport','team_1','team_2','bet']]
    df = df.merge(batter_team_2025[['player', 'team']], on='player', how='left')
    df['def_team'] = df.apply(lambda row: row['team_2'] if row['team'] == row['team_1'] else row['team_1'], axis=1)
    df = df.drop(columns=['team_1','team_2'], errors='ignore')
    def_rank = def_rank.rename(columns={"team": "def_team"})
    df = df.merge(def_rank, on='def_team', how='left')
    df['game'] = df['team'] + ' vs ' + df['def_team']
    df = df.dropna().drop(columns=['team', 'def_team'])
    df['odds'] = pd.to_numeric(df['odds'], errors='coerce')
    df['decimal_odds'] = df['odds'].apply(lambda x: (x / 100) + 1 if x > 0 else (100 / abs(x)) + 1)
    df['profit'] = (df['decimal_odds'] * 100) - 100

    overs = df[(df['side'].str.lower() == 'over') & (df['def_rank'] > 15)]
    unders = df[(df['side'].str.lower() == 'under') & (df['def_rank'] < 16)]

    overs = overs.merge(over_rbi_rate, on='player', how='left')
    unders = unders.merge(under_rbi_rate, on='player', how='left')

    overs['edge'] = overs.apply(calculate_ev, axis=1)
    unders['edge'] = unders.apply(calculate_under_ev, axis=1)

    overs = overs[overs['edge'] > 20]
    unders = unders[unders['edge'] > 20]

    final_df = pd.concat([overs, unders], ignore_index=True)
    final_df['bet'] = final_df['side'].astype(str) + ' ' + final_df['overUnder'].astype(str) + ' ' + final_df['bet'].astype(str)
    final_df = final_df.rename(columns={'bookmaker': 'book'})
    final_df['edge'] = (final_df['edge'] / 2).round(1)
    final_df['odds'] = final_df['odds'].apply(lambda x: f"+{x}" if x > 0 else str(x))
    final_df = final_df[['book','sport','player','game','bet','odds','edge']]
    final_df = final_df.sort_values(by='edge', ascending=False).reset_index(drop=True)

    return final_df
