# How to retrieve open interest

from wrapper import Option,AsyncFetcher

import pandas as pd 
import asyncio

def get_option(args,strike,right,params_method,method):
    args["strike"] = strike
    args["right"] = right
    args["_async"] = True
    params = {"method":method,"params":params_method}
    opt = Option(**args)
    opt._get_method(**params)
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
    args = {
        "root":"AMD"
        ,"exp":"20230317"
    }

    params_method = {
        "start_date":"20230101"
        ,"end_date":"20230201"
    }

    fetcher_params = {
        "batch_size":42
        ,"timeout":42
        ,"sleep":4
        ,"max_retry":2
    }

    method = "get_hist_open_interest"

    # Init get strikes
    option = Option(**args)
    strikes = [int(strike.get("strikes"))/1000 for strike in option.get_list_strikes()]

    # Get options with url and params
    options = [get_option(args,strike,right,params_method) for strike in strikes for right in ["call","put"]]
    print(f"[+] About to fetch {len(options)} options...")

    # Fetching responses
    fetcher = AsyncFetcher(**fetcher_params)
    fetched_contracts = asyncio.run(fetcher.fetch_all_contracts(options))
    print(f"[+] Fetched {len(fetched_contracts)} responses.")

    # Format data 
    df = pd.DataFrame()
    df_tmps = [get_df_tmp(contract) for contract in fetched_contracts]
    df = pd.concat(df_tmps,axis= 0)

    # Save 
    root,exp = args.get("root"),args.get("exp")
    _start,_end = params_method.get("start_date"),params_method.get("end_date")
    _filename = f"./{root}_{exp}_{_start}_{_end}"
    df.to_csv(_filename,index=False)
    print(f"[+] Save {_filename} with shape {df.shape}")



    