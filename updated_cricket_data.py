import pandas as pd
import json
import zipfile
import requests
import io
from sqlalchemy import create_engine
import os

# Use environment variables for security (Set DB_URL in GitHub Secrets)
DB_URL = os.getenv('DB_URL')
CRICSHEET_URL = "https://cricsheet.org/downloads/all_json.zip"

def run_pipeline():
    if not DB_URL:
        print("Error: DB_URL secret not found.")
        return

    # Connection pooling fix for cloud runners
    engine = create_engine(DB_URL, connect_args={'connect_timeout': 10})
    
    # 1. Check database for existing matches to avoid duplicates
    try:
        existing_ids = pd.read_sql("SELECT match_id FROM matches", engine)['match_id'].tolist()
        print(f"Database contains {len(existing_ids)} matches.")
    except Exception:
        existing_ids = []
        print("Starting fresh upload.")

    # 2. Download zip from Cricsheet
    print("Fetching data from Cricsheet...")
    r = requests.get(CRICSHEET_URL, timeout=60)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    
    # 3. Identify new matches
    all_json_files = [f for f in z.namelist() if f.endswith('.json') and f != 'README.txt']
    new_files = [f for f in all_json_files if f.replace('.json', '') not in existing_ids]
    
    if not new_files:
        print("No new data to upload.")
        return

    print(f"Parsing {len(new_files)} matches...")
    # Limit to 500 per run to stay within GitHub free limits
    new_files = new_files[:500] 

    m_info, m_players, m_deliveries = [], [], []

    for filename in new_files:
        match_id = filename.replace('.json', '')
        with z.open(filename) as f:
            data = json.load(f)
            info = data.get('info', {})
            if 'dates' not in info: continue
            
            # --- RESTORED: Full Match Info Table ---
            start_date = info['dates'][0]
            end_date = start_date if len(info['dates']) == 1 else info['dates'][-1]
            t1, t2 = info['teams'][0], info['teams'][1]
            match_key = f"{start_date}_{t1}_{t2}"

            m_info.append({
                'match_id': match_id,
                'match_key': match_key,
                'season': str(info.get('season')),
                'start_date': start_date,
                'end_date': end_date,
                'match_type': info.get('match_type'),
                'match_number': info.get('event', {}).get('match_number', 'None'),
                'event': info.get('event', {}).get('name', 'None'),
                'city': info.get('city', 'None'),
                'venue': info.get('venue'),
                'home_team': t1,
                'away_team': t2,
                'toss_won': info.get('toss', {}).get('winner'),
                'toss_decision': info.get('toss', {}).get('decision'),
                'winner': info.get('outcome', {}).get('winner', info.get('outcome', {}).get('result', 'Draw/No Result')),
                'won_by': " ".join([f"{v} {k}" for k, v in info.get('outcome', {}).get('by', {}).items()]),
                'method': info.get('outcome', {}).get('method', 'None'),
                'standing_ump1': info.get('officials', {}).get('umpires', ['None'])[0],
                'standing_ump2': info.get('officials', {}).get('umpires', ['None', 'None'])[1] if len(info.get('officials', {}).get('umpires', [])) > 1 else 'None',
                'tv_ump': info.get('officials', {}).get('tv_umpires', ['None'])[0] if 'tv_umpires' in info.get('officials', {}) else 'None',
                'match_ref': info.get('officials', {}).get('match_referees', ['None'])[0] if 'match_referees' in info.get('officials', {}) else 'None',
                'reserve_ump': info.get('officials', {}).get('reserve_umpires', ['None'])[0] if 'reserve_umpires' in info.get('officials', {}) else 'None'
            })

            # --- RESTORED: Full Player Mapping ---
            for team_name, player_list in info.get('players', {}).items():
                for player_name in player_list:
                    reg_id = info.get('registry', {}).get('people', {}).get(player_name, 'None')
                    m_players.append({
                        'match_id': match_id,
                        'team_name': team_name,
                        'player_name': player_name,
                        'registry_id': reg_id
                    })

            # --- RESTORED: Full Ball-by-Ball Data ---
            for inn_idx, inning in enumerate(data.get('innings', [])):
                t_name = inning['team']
                for over_data in inning.get('overs', []):
                    o_no = over_data['over']
                    for b_idx, delivery in enumerate(over_data.get('deliveries', [])):
                        runs = delivery.get('runs', {})
                        wickets = delivery.get('wickets', [])
                        
                        f_name = None
                        if wickets and 'fielders' in wickets[0]:
                            f_name = ", ".join([fi.get('name', '') for fi in wickets[0]['fielders']])

                        m_deliveries.append({
                            'match_id': match_id,
                            'team': t_name,
                            'over_no': o_no,
                            'delivery_no': b_idx + 1,
                            'batter': delivery.get('batter'),
                            'bowler': delivery.get('bowler'),
                            'non_striker': delivery.get('non_striker'),
                            'runs': runs.get('batter', 0),
                            'extras': runs.get('extras', 0),
                            'total': runs.get('total', 0),
                            'wicket': 1 if wickets else 0,
                            'kind': wickets[0].get('kind') if wickets else None,
                            'player_out': wickets[0].get('player_out') if wickets else None,
                            'fielder_name': f_name
                        })

    # 4. Upload fresh data
    if m_info:
        print(f"Uploading {len(m_info)} new records...")
        pd.DataFrame(m_info).to_sql('matches', engine, if_exists='append', index=False)
        pd.DataFrame(m_players).to_sql('players', engine, if_exists='append', index=False)
        pd.DataFrame(m_deliveries).to_sql('deliveries', engine, if_exists='append', index=False, chunksize=5000)
        print("Cloud Sync Successful.")

if __name__ == "__main__":
    run_pipeline()


