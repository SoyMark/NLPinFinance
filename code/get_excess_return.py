import wrds
from tqdm import tqdm
import pandas as pd
import numpy as np
from datetime import timedelta, datetime

def get_excess_returns(cik, release_date, wrds_conn):
    if not release_date:
        return None, None

    release_date = datetime.strptime(release_date, "%Y-%m-%d")  # Adjust format if needed
    start_date = release_date - timedelta(days=10)  # sometimes federal holiday
    end_date = release_date + timedelta(days=10)

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
        find_index = -1
        for index, row in data.iterrows():
            if row['date'] == event_date:
                find_index = index
                break
        if find_index == -1: # the release date is not a trading day
            for index, row in data.iterrows():
                if row['date'] > event_date:
                    find_index = index
                    break

        data_3day = data.iloc[find_index-1: find_index+2]
        data_4day = data.iloc[find_index-1: find_index+3]
        
        
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

db = wrds.Connection()

for SETTING in ['Harvard', 'LM']:
    result_file = f"result/{SETTING}/result.csv"
    df = pd.read_csv(result_file)
    for index, row in tqdm(df.iterrows()):
        cik = str(row['cik'])
        cik = '0' * (10-len(cik)) + cik
        release_date = row['file_date']
        ret_3day, ret_4day = get_excess_returns(cik, release_date, db)
        df.at[index, 'ret_3day'] = ret_3day
        df.at[index, 'ret_4day'] = ret_4day
    df.to_csv(f"result/{SETTING}/result_with_excess.csv", index=False)

db.close()
