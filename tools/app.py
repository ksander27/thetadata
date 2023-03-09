from wrapper import Option
from .downloader import AsyncDownloader
from .transformer import ExpiryBatcher

import pandas as pd
import os



    
# ----------------- #    

def get_method(call_type,endpoint):
    return f"get_{call_type}_{endpoint}"
    
def isFile(_filename,exp):
    if os.path.exists(_filename):
        print(f"[+] {_filename} already exists.")
        return True
    else:
        print(f"\n[+] Building contracts for {exp}")
        return False
    
def isStorage(root,endpoint):
    DIR = f"/home/jupyter/data/{root}/{endpoint}"
    if os.path.exists(DIR):
        print(f"[+] {DIR} already exists.")
    else:
        os.makedirs(DIR)
        print(f"Created {DIR}")
    return None

# ---------------------------#
def get_exp_data(root,exp):
    # Get options with url and params
       
        # Create a date range of n days starting from the input exp - check with TD if there are data basically.
        option = Option(root,exp)
        date_range = option.get_iv_dates_from_days_ago(days_ago)
        if date_range:

            # Check if file already exists
            start_date = date_range[0]
            _filename = f"./data/{root}/{endpoint}/{root}_{exp}_{start_date}_{exp}"
            if not isFile(_filename,exp):
                # Getting all dates with implied volatility for each contract in exp
                option = Option(root,exp)
                desired_strikes = option.get_desired_strikes(strike_multiple)

                batches = pd.DataFrame([{"root":root,"exp":exp,"right":right,"strike":strike}
                                                                    for strike in desired_strikes 
                                                                    for right in ["call","put"]])

                # Fetching response to get all the dates per contract      
                method,key_params = "get_list_dates_implied_volatility",{}
                downloader = AsyncDownloader(batches=batches,method=method,key_params=key_params
                                ,batch_size=BATCH_SIZE,timeout=TIMEOUT,max_retry=MAX_RETRY,sleep=SLEEP)    
                df_dates = downloader.async_download_contracts()               
                print(f"[+] Total {df_dates.shape[0]} contracts with dates in {exp}")                    


                if df_dates is not None:
                    # Build list of args and params
                    batcher = ExpiryBatcher(exp=exp,days_ago=days_ago
                                            ,freq_batch=freq_batch,endpoint_params=endpoint_params)
                    df_batches = batcher.get_batches(df_dates)
                    method = get_method(call_type,endpoint)
                    key_params = ["start_date","end_date"] + list(endpoint_params.keys())
                    
                    # Fetching data for method
                    print(f"[+] Fetching asynchronously data for {exp}")  
                    downloader = AsyncDownloader(batches=df_batches,method=method,key_params=key_params
                                ,batch_size=BATCH_SIZE,timeout=TIMEOUT,max_retry=MAX_RETRY,sleep=SLEEP)

                    df_data = downloader.async_download_contracts()   
                    if df_data is not None:
                        print(f"[+] Fetched {df_data.shape[0]} contracts with dates in {exp}")
                        df_data.to_csv(_filename,index=False)
                        print(f"[+] Saved {_filename}")
                    else:
                        print(f"[+] NO DATA - Nothing to save for {_filename}") 



# ------------------------------ #


BATCH_SIZE,TIMEOUT,MAX_RETRY,SLEEP = 128,60,5,20


roots = ["MSFT"]

call_type = "at_time"
endpoint = "greeks"

endpoint_params = {
    "s_of_day": 12 * 3600
}

days_ago = 100
strike_multiple = 5

freq_batch = 'W-MON'
freq_exp = "monthly"

min_exp_date = "2020-01-01"
max_exp_date = "2023-07-01"


if __name__=='__main__':
    for root in roots:

        _ = isStorage(root,endpoint)
        
        option = Option(root)      
        desired_expirations = option.get_desired_expirations(min_exp_date,max_exp_date,freq_exp)
        for exp in desired_expirations:
            _ = get_exp_data(root,exp)
            

