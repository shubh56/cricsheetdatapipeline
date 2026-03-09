import pandas as pd
import json
import zipfile
import requests
import io
from sqlalchemy import create_engine
import os

# Get DB URL from GitHub Secrets
DB_URL = os.getenv('DB_URL')
CRICSHEET_URL = "https://cricsheet.org/downloads/all_json.zip"

def run_pipeline():
    if not DB_URL:
        print("Error: DB_URL secret not found.")
        return

    engine = create_engine(DB_URL, connect_args={'connect_timeout': 10})
    
    # 1. Get existing Match IDs from the DB to avoid duplicates
    try:
        existing_ids = pd.read_sql("SELECT match_id FROM matches", engine)['match_id'].tolist()
        print(f"Database currently has {len(existing_ids)} matches.")
    except Exception:
        existing_ids = []
        print("Matches table not found. Starting fresh.")

    # 2. Download Cricsheet data in memory
    print("Fetching latest zip from Cricsheet...")
    r = requests.get(CRICSHEET_URL)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    
    # 3. Identify new matches
    all_json_files = [f for f in z.namelist() if f.endswith('.json') and f != 'README.txt']
    new_files = [f for f in all_json_files if f.replace('.json', '') not in existing_ids]
    
    if not new_files:
        print("No new matches found. Script ending.")
        return

    print(f"Parsing {len(new_files)} new matches...")
    # Limit to 500 per run to ensure we stay under the 15-min free limit
    new_files = new_files[:500] 

    m_info, m_players, m_deliveries = [], [], []

    for filename in new_files:
        match_id = filename.replace('.json', '')
        with z.open(filename) as f:
            data = json.load(f)
            info = data.get('info', {})
            if 'dates' not in info: continue
            
            # --- Match Info ---
            m_info.append({
                'match_id': match_id,
                'season': str(info.get('season')),
                'date': info['dates'][0],
                'teams': " vs ".join(info['teams']),
                'winner': info.get('outcome', {}).get('winner', 'No Result')
            })

            # --- Players ---
            for team, players in info.get('players', {}).items():
                for p in players:
                    reg_id = info.get('registry', {}).get('people', {}).get(p, 'None')
                    m_players.append({'match_id': match_id, 'team': team, 'player': p, 'reg_id': reg_id})

            # --- Deliveries ---
            for i_idx, inn in enumerate(data.get('innings', [])):
                for over in inn.get('overs', []):
                    o_no = over['over']
                    for b_idx, d in enumerate(over['deliveries']):
                        m_deliveries.append({
                            'match_id': match_id, 'inning': i_idx+1, 'over': o_no, 'ball': b_idx+1,
                            'batter': d['batter'], 'bowler': d['bowler'], 'runs': d['runs']['total']
                        })

    # 4. Upload to Supabase
    print("Uploading new data to Supabase...")
    pd.DataFrame(m_info).to_sql('matches', engine, if_exists='append', index=False)
    pd.DataFrame(m_players).to_sql('players', engine, if_exists='append', index=False)
    pd.DataFrame(m_deliveries).to_sql('deliveries', engine, if_exists='append', index=False, chunksize=5000)
    print("Upload complete!")

if __name__ == "__main__":

    run_pipeline()
