import pandas as pd
import unicodedata
import requests
import json

TEAM_MAPPING_FULL = {
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

TEAM_MAPPING_SHORT = {
    'CHC': 'CHC', 'LAD': 'LAD', 'ARI': 'ARI', 'PIT': 'PIT', 'MIA': 'MIA',
    'TOR': 'TOR', 'BOS': 'BOS', 'ATH': 'ATH', 'BAL': 'BAL', 'SEA': 'SEA',
    'SDP': 'SDP', 'ATL': 'ATL', 'PHI': 'PHI', 'TEX': 'TEX', 'MIL': 'MIL',
    'WSN': 'WSH', 'NYY': 'NYY', 'STL': 'STL', 'TBR': 'TBR', 'DET': 'DET',
    'KCR': 'KCR', 'CLE': 'CLE', 'COL': 'COL', 'NYM': 'NYM', 'CIN': 'CIN',
    'MIN': 'MIN', 'SFG': 'SFG', 'HOU': 'HOU', 'CHW': 'CHW', 'LAAs': 'LAA'
}


def normalize_name(name):
    if not isinstance(name, str):
        return name
    return unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('utf-8')


def clean_name_symbols(name):
    if not isinstance(name, str):
        return name
    return name.replace('*', '').replace('#', '')


def fetch_runs_events():
    url = 'https://api.sportsgameodds.com/v2/events/'
    headers = {'X-Api-Key': 'fac7e1a69f2baa47da395bde36dc869c'}

    next_cursor = None
    event_data = []

    while True:
        try:
            response = requests.get(
                url,
                headers=headers,
                params={
                    "sportID": "BASEBALL",
                    "leagueID": "MLB",
                    "oddIDs": "points-PLAYER_ID-game-ou-over",
                    "includeOpposingOddIDs": "true",
                    "oddsAvailable": "true",
                    "cursor": next_cursor,
                    "include": "teams",
                    'started': "false"
                },
            )
            response.raise_for_status()
            data = response.json()
            event_data.extend(data.get("data", []))
            next_cursor = data.get("nextCursor")
            if not next_cursor:
                break
        except requests.exceptions.RequestException as e:
            print("Error fetching events:", e)
            break

    return event_data


def calculate_edge(row, prefix):
    line = row['overUnder']
    key = f"{int(line * 2)}_{prefix}_rate_{row['side'].lower()}"
    rate = row.get(key)
    if rate is None:
        return None
    win_prob = rate
    loss_prob = 1 - rate
    profit = (row['decimal_odds'] * 100) - 100
    return (win_prob * profit) - (loss_prob * 100)


def run_batter_runs_posting():
    under_df = pd.read_csv('undernewruns.csv')
    over_df = pd.read_csv('overnewruns.csv')
    def_rank = pd.read_csv('scrapers/def_scraper/batter_runs_def_rank.csv')
    teams = pd.read_csv('data/raw/batter_team_2025.csv')

    teams.rename(columns={'Team': 'team', 'Player': 'player'}, inplace=True)
    teams['team'] = teams['team'].map(TEAM_MAPPING_SHORT)
    def_rank.rename(columns={'Team': 'team', 'Rank': 'def_rank'}, inplace=True)
    def_rank['team'] = def_rank['team'].map(TEAM_MAPPING_FULL)

    teams['player'] = teams['player'].apply(normalize_name).apply(clean_name_symbols)

    events = fetch_runs_events()
    odds_list = []

    for event in events:
        event_id = event.get("eventID")
        teams_str = ", ".join(
            sorted(set(p.get("teamID") for p in event.get("players", {}).values() if p.get("teamID")))
        )
        for odd in event.get("odds", {}).values():
            for book, book_data in odd.get("byBookmaker", {}).items():
                odds_list.append({
                    "eventID": event_id,
                    "teams": teams_str,
                    "oddID": odd.get("oddID"),
                    "marketName": odd.get("marketName"),
                    "player": odd.get("marketName").split(" Runs")[0],
                    "side": odd.get("sideID"),
                    "line": odd.get("bookOverUnder"),
                    "bookmaker": book,
                    "odds": book_data.get("odds"),
                    "overUnder": book_data.get("overUnder")
                })

    df = pd.DataFrame(odds_list)
    df[['team_1', 'team_2']] = df['teams'].str.replace('_MLB', '', regex=False).str.split(', ', expand=True)
    df.drop(columns='teams', inplace=True)
    df['team_1'] = df['team_1'].map(TEAM_MAPPING_FULL)
    df['team_2'] = df['team_2'].map(TEAM_MAPPING_FULL)
    df['player'] = df['player'].apply(normalize_name).apply(clean_name_symbols)

    df = df.merge(teams[['player', 'team']], on='player', how='left')
    df['def_team'] = df.apply(lambda row: row['team_2'] if row['team'] == row['team_1'] else row['team_1'], axis=1)
    df = df.merge(def_rank.rename(columns={'team': 'def_team'}), on='def_team', how='left')
    df['game'] = df['team'] + ' vs ' + df['def_team']
    df['decimal_odds'] = pd.to_numeric(df['odds'], errors='coerce').apply(
        lambda x: (x / 100) + 1 if x > 0 else (100 / abs(x)) + 1
    )
    df['profit'] = (df['decimal_odds'] * 100) - 100

    df = df.merge(over_df, on='player', how='left', suffixes=('', '_over'))
    df = df.merge(under_df, on='player', how='left', suffixes=('', '_under'))

    df['edge'] = df.apply(lambda row: calculate_edge(row, 'runs'), axis=1)
    df = df[df['edge'] > 20]

    df['bet'] = df['side'].astype(str) + ' ' + df['overUnder'].astype(str) + ' runs'
    df['odds'] = df['odds'].apply(lambda x: f"+{x}" if x > 0 else str(x))
    df['edge'] = df['edge'] / 2
    df['edge'] = df['edge'].round(1)

    final_df = df[['bookmaker', 'sport', 'player', 'game', 'bet', 'odds', 'edge']].rename(columns={'bookmaker': 'book'})
    final_df = final_df.sort_values(by='edge', ascending=False).reset_index(drop=True)

    final_df.to_csv('data/output/median_runs_bets.csv', index=False)
    print("✅ median_runs_bets.csv saved.")

    # Optional API POST
    data_json = json.loads(final_df.to_json(orient="records"))
    response = requests.post(
        "https://pitstoppicks.com/wp-json/custom-api/v1/mlb_batter_runs",
        headers={"Content-Type": "application/json"},
        json=data_json
    )
    print("Status Code:", response.status_code)
    print("Response Text:", response.text)
