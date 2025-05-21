import pandas as pd
import unicodedata
import json
import requests

from utils.profile_utils import normalize_name, clean_name_symbols, team_mapping_full, team_mapping_abbr, american_to_decimal


def calculate_ev(row):
    if row['overUnder'] == 0.5:
        rate = row['1_hits_rate_over']
    elif row['overUnder'] == 1.5:
        rate = row['2_hits_rate_over']
    else:
        return None
    return (rate * row['profit']) - ((1 - rate) * 100)


def calculate_under_ev(row):
    if row['overUnder'] == 0.5:
        rate = row['1_hits_rate_under']
    elif row['overUnder'] == 1.5:
        rate = row['2_hits_rate_under']
    else:
        return None
    return ((1 - rate) * row['profit']) - (rate * 100)


def generate_batter_hits_bets():
    batter_team_2025 = pd.read_csv('data/raw/batter_team_2025.csv')
    def_rank = pd.read_csv('data/raw/batter_hits_def_rank.csv')
    under_hit_rate = pd.read_csv('data/output/undernewhits.csv')
    over_hit_rate = pd.read_csv('data/output/overnewhits.csv')

    batter_team_2025.rename(columns={'Team': 'team', 'Player': 'player'}, inplace=True)
    batter_team_2025['team'] = batter_team_2025['team'].map(team_mapping_abbr)
    batter_team_2025['player'] = batter_team_2025['player'].apply(normalize_name).apply(clean_name_symbols)

    def_rank.rename(columns={'Team': 'team', 'Rank': 'def_rank'}, inplace=True)
    def_rank['team'] = def_rank['team'].map(team_mapping_full)

    # API call
    headers = {'X-Api-Key': 'fac7e1a69f2baa47da395bde36dc869c'}
    url = 'https://api.sportsgameodds.com/v2/events/'
    event_data, next_cursor = [], None

    while True:
        params = {
            "sportID": "BASEBALL", "leagueID": "MLB", "oddIDs": "batting_hits-PLAYER_ID-game-ou-over",
            "includeOpposingOddIDs": "true", "oddsAvailable": "true", "cursor": next_cursor,
            "include": "teams", 'started': "false"
        }
        try:
            r = requests.get(url, headers=headers, params=params)
            r.raise_for_status()
            data = r.json()
            event_data.extend(data.get("data", []))
            next_cursor = data.get("nextCursor")
            if not next_cursor:
                break
        except requests.RequestException as e:
            print("API error:", e)
            break

    # Parse odds
    odds_list = []
    for event in event_data:
        event_id = event.get("eventID", "Unknown Event")
        teams = sorted({p.get("teamID") for p in event.get("players", {}).values() if p.get("teamID")})
        odds = event.get("odds", {})
        for odd_obj in odds.values():
            base = {
                "eventID": event_id,
                "teams": ", ".join(teams),
                "oddID": odd_obj.get("oddID"),
                "opposingOddID": odd_obj.get("opposingOddID"),
                "marketName": odd_obj.get("marketName"),
                "playerID": odd_obj.get("statEntityID"),
                "side": odd_obj.get("sideID"),
                "line": odd_obj.get("bookOverUnder", odd_obj.get("fairOverUnder")),
                "score": odd_obj.get("score", "N/A"),
            }
            for bookmaker, book_data in odd_obj.get("byBookmaker", {}).items():
                row = base.copy()
                row.update({
                    "bookmaker": bookmaker,
                    "odds": book_data.get("odds"),
                    "overUnder": book_data.get("overUnder")
                })
                odds_list.append(row)

    df = pd.DataFrame(odds_list)
    df['sport'] = df['teams'].str[-3:]
    df[['team_1', 'team_2']] = df['teams'].str.replace('_MLB', '', regex=False).str.split(', ', expand=True)
    df = df.drop(columns='teams')

    df['player'] = df['marketName'].str.extract(r'^(.*?)\s*Hits')
    df['team_1'] = df['team_1'].map(team_mapping_full)
    df['team_2'] = df['team_2'].map(team_mapping_full)
    df['overUnder'] = pd.to_numeric(df['overUnder'], errors='coerce')
    df['bet'] = 'hits'

    df = df.merge(batter_team_2025[['player', 'team']], on='player', how='left')
    df['def_team'] = df.apply(lambda row: row['team_2'] if row['team'] == row['team_1'] else row['team_1'], axis=1)
    df = df.drop(columns=['team_1', 'team_2'], errors='ignore')
    def_rank.rename(columns={"team": "def_team"}, inplace=True)
    df = df.merge(def_rank, on='def_team', how='left')
    df['game'] = df['team'] + ' vs ' + df['def_team']
    df = df.drop(columns=['team', 'def_team'])
    df = df.dropna()

    overs = df[df['side'].str.lower() == 'over']
    unders = df[df['side'].str.lower() == 'under']
    overs = overs.merge(over_hit_rate, on='player', how='left').dropna()
    unders = unders.merge(under_hit_rate, on='player', how='left').dropna()

    for df_odds in [overs, unders]:
        df_odds['odds'] = pd.to_numeric(df_odds['odds'], errors='coerce')
        df_odds['decimal_odds'] = df_odds['odds'].apply(american_to_decimal)
        df_odds['profit'] = (df_odds['decimal_odds'] * 100) - 100

    overs['edge'] = overs.apply(calculate_ev, axis=1)
    unders['edge'] = unders.apply(calculate_under_ev, axis=1)
    overs = overs[overs['edge'] > 20]
    unders = unders[unders['edge'] > 20]

    final = pd.concat([overs, unders])
    final['bet'] = final['side'] + ' ' + final['overUnder'].astype(str) + ' ' + final['bet']
    final['edge'] = (final['edge'] / 2).round(1)
    final['odds'] = final['odds'].apply(lambda x: f'+{x}' if pd.notnull(x) and x > 0 else str(x))
    final = final[['bookmaker', 'sport', 'player', 'game', 'bet', 'odds', 'edge']].rename(columns={'bookmaker': 'book'})
    final = final.sort_values(by='edge', ascending=False).reset_index(drop=True)

    # POST to API
    json_data = json.loads(final.to_json(orient='records'))
    post_url = "https://pitstoppicks.com/wp-json/custom-api/v1/mlb_batter_hits"
    headers = {"Content-Type": "application/json"}
    response = requests.post(post_url, headers=headers, json=json_data)
    print("✅ Status:", response.status_code)
    print("✅ Response:", response.text)

    final.to_csv("data/output/median_hits_bets.csv", index=False)
