from tdwrapper.wrapper import Option,NoDataForContract,fetch_all_contracts
import asyncio 
import pandas as pd
from datetime import datetime, timedelta
import os


def prepare_contracts_in_exp_from_list_args_params(list_async_params):
    contracts_in_exp = []
    for args_params in list_async_params:
        args,_params,method = args_params.get("contract"),args_params.get("params"),args_params.get("method")
        params = {"method":method,"params":_params}
        args["_async"] = True 
        option = Option(**args)
        contracts_in_exp.append(option._get_method(**params))
    return contracts_in_exp

def get_contracts_in_exp_async(list_args_params,BATCH_SIZE,TIMEOUT,MAX_RETRY,SLEEP):
    contracts_in_exp = prepare_contracts_in_exp_from_list_args_params(list_args_params) 
    results = asyncio.run(fetch_all_contracts(contracts_in_exp,BATCH_SIZE,TIMEOUT,MAX_RETRY,SLEEP))
    if results:
        df_tmps = [get_df_tmp(contract) for contract in results]
        if not all(df_tmp is None for df_tmp in df_tmps):
            df = pd.concat(df_tmps,axis=0)
    return df

def get_df_tmp(contract):
    url = contract.get("url")
    data = contract.get("data")
    params = contract.get("params")
    if url and data:

        try:
            df_tmp = pd.DataFrame(data)
        except ValueError:
            print(data)
            raise ValueError
        df_tmp["url"] = url
        for k,v in params.items():
            df_tmp[k] = v
        return df_tmp
    
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

# ----------------- #

def get_desired_expirations(root,min_exp_date,max_exp_date):
    option = Option(root=root)
    expirations = [str(expiration.get("expirations")) for expiration in option.get_list_expirations()]

    # Get desired_expirations - improve to allow picking 
    exp_range = pd.date_range(min_exp_date,max_exp_date,freq='WOM-3FRI')
    desired_expirations = [exp for exp in exp_range.strftime('%Y%m%d').to_list() if exp in expirations]
    return desired_expirations

def get_implied_vol_date_range(root,exp,strike=None,right=None, daysAgo=100):
    # This check should happen at the module level
    if strike and right:
        args = {
                "root":root
                ,"exp":exp
                ,"strike":strike
                ,"right":right
        }
    elif not strike and not right:
        args = {
            "root":root
            ,"exp":exp
        }
    else:
        raise ValueError("[+] We need a strike and a right")

    _exp = datetime.strptime(exp, '%Y%m%d')
    if YESTERDAY > _exp:
        date_range = pd.date_range(end=_exp, periods=daysAgo, freq='B')
    else:
        date_range = pd.date_range(end= YESTERDAY, periods=daysAgo, freq='B')

    try:
        option = Option(**args)
        date_implied_vol = option.get_list_dates_implied_volatility()
        date_implied_vol = [str(_dict.get("implied_volatility")) for _dict in date_implied_vol]
                            
        date_range = [date for date in date_range.strftime('%Y%m%d').to_list() if date in date_implied_vol]
        print(f"[+] {len(date_range)} business days for {exp}")
        return date_range
    except NoDataForContract:
        print(f"[+] NO IMPLIED VOL DATA FOR {exp} - check with thetadata")
        return None

def get_strikes(root,exp):
    args = {"root":root,"exp":exp}
    option = Option(**args)
    strikes = option.get_list_strikes()
    return strikes  

# ------------------------#

def get_list_args_params_from_df(df,main_params,method):
    list_args = df[["root","exp","strike","right"]].to_dict(orient="records")
    cols_params = ["start_date","end_date"] + list(main_params.keys())
    list_params = df[cols_params].to_dict(orient="records")
    list_args_params = [{"contract":args
                         ,"params":list_params[idx]
                         ,"method":method} 
                        for idx,args in enumerate(list_args) ]
    return list_args_params

def data_preparation_on_exp_contract(exp,df,main_params,strikeMultiple,daysAgo,freq='MS'):
    # Keep only good strikes                            
    strikeMultiple = 5
    df["strike"] /= 1000
    df = df[df['strike'] % strikeMultiple == 0]

    # Calculate the cut-off date n business days ago from the expiration date
    df["implied_volatility"] = df.implied_volatility.astype(str)
    df['exp_dt'] = pd.to_datetime(df['exp'], format='%Y%m%d')
    df["implied_volatility_dt"] = pd.to_datetime(df['implied_volatility'], format='%Y%m%d')
    df['cut_off'] = df['exp_dt'] - pd.tseries.offsets.BDay(daysAgo)
    df['is_within_n_business_days_ago'] = df['implied_volatility_dt'] > df['cut_off']
    df = df[df['is_within_n_business_days_ago'] == True]
    print(f"[+] Filtered {df.shape[0]} contracts with dates in {exp}")

    # Making batches
    batch = df.groupby(['root','exp', 'strike','right'
                               ,pd.Grouper(key='implied_volatility_dt', freq=freq) # weekly 'W-MON' daily : D
                            ]).agg({'implied_volatility': ['first', 'last']})

    batch.columns = [f"{x}_{y}" for x, y in batch.columns]
    batch.reset_index(inplace=True)
    batch = batch[['root', 'exp', 'strike', 'right'
                                  ,'implied_volatility_first', 'implied_volatility_last']]

    df = batch.rename(columns={'implied_volatility_first':'start_date'
                                 ,'implied_volatility_last':'end_date'})

    # Adding main parameters
    for k,v in main_params.items():
        df[k] = v                                                     
    # Save for check
    print(f"[+] Total dates/contracts batches {df.shape[0]} for {method} in {exp}")
    #df.to_csv('./weekly_batch.csv', index=False)
    return df

# ---------------------------#
def get_exp_data(root,exp):
    # Get options with url and params
       
        # Create a date range of n days starting from the input exp - check with TD if there are data basically.
        date_range = get_implied_vol_date_range(root,exp)
        if date_range:

            # Check if file already exists
            start_date = date_range[0]
            _filename = f"./data/{root}/{endpoint}/{root}_{exp}_{start_date}_{exp}"
            if not isFile(_filename,exp):
                # Getting all dates with implied volatility for each contract in exp
                strikes = [int(int(dict_strike.get("strikes"))/1000) for dict_strike in get_strikes(root,exp)]
                list_args_params = [{"contract":{"root":root,"exp":exp,"right":right,"strike":strike}
                                     ,"params":{}
                                     ,"method":"get_list_dates_implied_volatility"}
                                     
                                    for strike in strikes 
                                    for right in ["call","put"]]

                # Fetching response to get all the dates per contract                
                df = get_contracts_in_exp_async(list_args_params,BATCH_SIZE,TIMEOUT,MAX_RETRY,SLEEP)                 
                print(f"[+] Total {df.shape[0]} contracts with dates in {exp}")                    


                if df is not None:
                    # Build list of args and params
                    df = data_preparation_on_exp_contract(exp,df,main_params,strikeMultiple,daysAgo,freq) 
                    list_args_params = get_list_args_params_from_df(df,main_params,method)

                    # Fetching data for method
                    print(f"[+] Fetching asynchronously data for {exp}")  
                    list_args_params = list_args_params[:50]
                    df = get_contracts_in_exp_async(list_args_params,BATCH_SIZE,TIMEOUT,MAX_RETRY,SLEEP)
                    if df is not None:
                        print(f"[+] Fetched {df.shape[0]} contracts with dates in {exp}")
                        df.to_csv(_filename,index=False)
                        print(f"[+] Saved {_filename}")
                    else:
                        print(f"[+] NO DATA - Nothing to save for {_filename}") 



# ------------------------------ #


BATCH_SIZE,TIMEOUT,MAX_RETRY,SLEEP,PROC = 128,5,5,5,4
YESTERDAY = datetime.now() - timedelta(days=1)

roots = ["MSFT"]

call_type = "at_time"
endpoint = "greeks"
method = f"get_{call_type}_{endpoint}"
main_params = {
    "s_of_day": 12 * 3600
}

daysAgo = 100
freq = 'W-MON' #'MS'
strikeMultiple = 5
min_exp_date = "2020-01-01"
max_exp_date = "2023-07-01"


if __name__=='__main__':
    for root in roots:

        _ = isStorage(root,endpoint)
        
        desired_expirations = get_desired_expirations(root,min_exp_date,max_exp_date)
        tup_args = [(root,exp) for exp in desired_expirations]
        for root,exp in tup_args:
            _ = get_exp_data(root,exp)
            

