import wrds
from tqdm import tqdm
import wrds
import pandas as pd
import numpy as np
from datetime import timedelta, datetime

def get_excess_returns(cik, release_date, wrds_conn):
    if not release_date:
        return None, None
    
    try:
        cik = int(cik.lstrip('0'))
    except ValueError:
        print(f"CIK not valid: {cik}")
        return None, None

    release_date = datetime.strptime(release_date, "%Y-%m-%d")  # Adjust format if needed
    start_date = release_date - timedelta(days=5)
    start_date = release_date - timedelta(days=5)  
    end_date = release_date + timedelta(days=5)

    query = f"""
        SELECT a.permno, a.date, a.ret, b.ewretd
        FROM crsp.dsf AS a
        JOIN crsp.dsi AS b ON a.date = b.date
        JOIN crsp.ccmxpf_linktable AS c ON a.permno = c.lpermno
        WHERE c.linkprim = 'P'
        AND c.linktype IN ('LU', 'LC')
        AND c.gvkey = (SELECT DISTINCT gvkey FROM crsp.ccm_lookup WHERE cik = '{str(cik)}')
        AND a.date BETWEEN '{start_date}' AND '{end_date}'
    """
    
    try:
        data = wrds_conn.raw_sql(query, date_cols=['date'])
        if data.empty:
            print(f"can't find: CIK {cik}, date {release_date}")
            return None, None
        
        data['date'] = pd.to_datetime(data['date'])

        event_date = pd.to_datetime(release_date)
        data_3day = data[(data['date'] >= event_date - timedelta(days=1)) &
                         (data['date'] <= event_date + timedelta(days=1))]
        data_4day = data[(data['date'] >= event_date - timedelta(days=1)) &
                         (data['date'] <= event_date + timedelta(days=2))]
        
        def calc_excess_return(df):
            if len(df) < 2: 
                return None
            ret = df['ret'].fillna(0)
            ewretd = df['ewretd'].fillna(0)
            excess_ret = (1 + ret).prod() - (1 + ewretd).prod()
            return excess_ret
        
        excess_3day = calc_excess_return(data_3day)
        excess_4day = calc_excess_return(data_4day)
        
        return excess_3day, excess_4day
    
    except Exception as e:
        print(f"fail to query: CIK {cik}, date {release_date}, Error: {e}")
        return None, None
    
    
def extract_cik_from_filename(filename):
    parts = filename.split('_')
    if len(parts) >= 5:
        accession = parts[4]
        cik = accession.split('-')[0]
        return cik
    return None

def extract_date_from_filename(filename):
    number_date = filename.split('_')[0]
    return f"{number_date[:4]}-{number_date[4:6]}-{number_date[6:8]}"
    
db = wrds.Connection()

excess_returns_3day = []
excess_returns_4day = []

file_list = [r"20200331_10-Q_edgar_data_940944_0000940944-20-000014_1.txt"]

for filename in tqdm(file_list):
    cik = extract_cik_from_filename(filename)
    release_date = extract_date_from_filename(filename)
    ret_3day, ret_4day = get_excess_returns(cik, release_date, db)
    excess_returns_3day.append(ret_3day)
    excess_returns_4day.append(ret_4day)

db.close()