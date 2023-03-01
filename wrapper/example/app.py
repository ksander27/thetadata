from wrapper import Option,fetch_all_contracts

import pandas as pd 
import asyncio
from datetime import datetime, timedelta
import os

def get_option(root,exp,strike,right,params, method):
    args = {"root":root,"exp":exp,"right":right ,"_async":True
            ,"strike":int(int(strike.get("strikes"))/1000)}
    opt = Option(**args)
    func = getattr(opt, method)
    func(**params)
    return opt

def get_df_tmp(contract):
    url = contract.get("url")
    if url:
        data = contract.get("data")
        params = contract.get("params")

        df_tmp = pd.DataFrame(data)
        df_tmp["url"] = url
        for k,v in params.items():
            df_tmp[k] = v
        return df_tmp

if __name__=='__main__':
    root = "NVDA"
    call_type = "at_time"
    endpoint = "implied_volatility"
    s_of_day = 12 * 3600

    timeout = 60
    max_retry = 3
    
    daysAgo = 10
    min_exp_date = "2020-12-01"
    max_exp_date = "2024-06-01"
    
    method = f"get_{call_type}_{endpoint}"

    # Init check dir
    DIR = f"/home/jupyter/data/{root}/{endpoint}"
    if os.path.exists(DIR):
        print(f"[+] {DIR} already exists.")
    else:
        os.makedirs(DIR)
        print(f"Created {DIR}")
        
    # Init expiration
    option = Option(root=root)
    expirations = [str(expiration.get("expirations")) for expiration in option.get_list_expirations()]
    
    # Get desired_expirations
    daterange = pd.date_range(min_exp_date,max_exp_date,freq='WOM-3FRI')
    desired_expirations = [exp for exp in daterange.strftime('%Y%m%d').to_list() if exp in expirations]

    # Get options with url and params
    options = []
    df = pd.DataFrame()
    for exp in desired_expirations:
        
        # Create a date range of n days starting from the input exp
        _ = datetime.strptime(exp, '%Y%m%d')
        date_range = pd.date_range(end=_, periods=daysAgo, freq='B')
        start_date = date_range[0].strftime('%Y%m%d')
        _filename = f"./data/{root}/{endpoint}/{root}_{exp}_{start_date}_{exp}"
        
        # Check if file already exists
        if os.path.exists(_filename):
            print(f"[+] {_filename} already exists.")
        else:
            print(f"\n[+] Building contract for {exp}")
            option.exp = exp 
            strikes = option.get_list_strikes()
            
            fetched_contracts = []
            for idx in range(len(date_range)-1):
                _start,_end = date_range[idx].strftime('%Y%m%d'),date_range[idx+1].strftime('%Y%m%d')
                params = {"start_date": _start, "end_date": _end ,"s_of_day": s_of_day}
                contracts_in_exp = [get_option(root,exp,strike,right,params, method) for strike in strikes
                                                                             for right in ["call","put"]]

                # Fetching responses
                print(f"[+] Fetching {len(contracts_in_exp)} option contracts period {_start} - {_end}...")
                results = asyncio.run(fetch_all_contracts(contracts_in_exp,timeout,max_retry))
                fetched_contracts.extend(results)

            # Format data 
            print(f"[+] Formatting {len(fetched_contracts)} responses.")
            df_tmps = [get_df_tmp(contract) for contract in fetched_contracts]

            # Save
            if not all(df_tmp is None for df_tmp in df_tmps):
                df = pd.concat(df_tmps,axis= 0)
                df.to_csv(_filename,index=False)
                print(f"[+] Saved {_filename} with shape {df.shape}")
            else:
                print(f"[+] All None. Nothing to save for {_filename}")    