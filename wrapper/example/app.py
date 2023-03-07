#from tdwrapper.wrapper import Option,fetch_all_contracts

from option import Option
from option_utils import get_strikes
from async_fetch import fetch_all_contracts
from date_range_utils import get_implied_vol_date_range,get_desired_expirations

import asyncio
import pandas as pd
import os

# ---------------------------#

class DownloadManager():
    def __init__(self
                 ,root
                 ,call_type="hist",endpoint="eod",endpoint_params={}
                 ,exp=None
                 ,strike_multiple=5
                 ,min_exp_date='2018-01-01',max_exp_date='2024-01-01',freq_exp='WOM-3FRI'
                 ,daysAgo=100,freq_batch="W-MON"
                 ,BATCH_SIZE=128,TIMEOUT=5,MAX_RETRY=3,SLEEP=20):
        self.root = root
        self.exp = exp
        self.call_type = call_type
        self.endpoint = endpoint
        self.endpoint_params = endpoint_params 
        

        self.strike_multiple = strike_multiple *1000

        self.DIR = f"/home/jupyter/data/{self.root}/{self.call_type}/{endpoint}"
        self.min_exp_date = min_exp_date
        self.max_exp_date = max_exp_date
        self.freq_exp = freq_exp

        self.daysAgo = daysAgo
        self.freq_batch = freq_batch

        self.BATCH_SIZE = BATCH_SIZE
        self.TIMEOUT = TIMEOUT
        self.MAX_RETRY = MAX_RETRY
        self.SLEEP = SLEEP

        self.desired_expirations = self.get_desired_expirations()

        self._filename = None
        self._df = None
        self._key_params = None
        self._method = self.get_method()

    def get_method(self):
        self.method = f"get_{self.call_type}_{self.endpoint}"
        return self.method

    def isStorage(self):
            
        if os.path.exists(self.DIR):
            print(f"[+] {self.DIR} already exists.")
        else:
            os.makedirs(self.DIR)
            print(f"Created {self.DIR}")
        return None
    
    def isFile(self,exp,start_date,end_date):
        _filename = f"{self.DIR}/{root}_{exp}_{start_date}_{end_date}"
        if os.path.exists(_filename):
            print(f"[+] {_filename} already exists.")
            return None
        else:
            self._filename = _filename
            print(f"\n[+] Building contracts for {exp}")
            return self._filename
        
    def store_file(self):
        if self._df is not None:
            print(f"[+] Fetched {self._df.shape[0]} contracts with dates in {self.exp}")
            self._df.to_csv(self._filename,index=False)
            print(f"[+] Saved {self._filename}")
        else:
            print(f"[+] NO DATA - Nothing to save for {self._filename}") 

        
    def get_desired_strikes(self):
        strikes = get_strikes(root=self.root,exp=self.exp)
        df = pd.DataFrame(strikes)
        df = df[df["strikes"] % self.strike_multiple == 0]
        return df.strikes.values

    def get_desired_expirations(self):
        return get_desired_expirations(root=root,min_exp_date=self.min_exp_date,max_exp_date=self.min_exp_date
                                    ,freq_exp=self.freq_exp)
    
    def get_contracts_in_exp_async(self,list_args_params):
        def get_df_tmp(contract):
            url = contract.get("url")
            data = contract.get("data")
            params = contract.get("params")
            if url and data:
                df_tmp = pd.DataFrame(data)
                df_tmp["url"] = url
                for k,v in params.items():
                    df_tmp[k] = v
                return df_tmp
        
        def prepare_contracts_in_exp_from_list_args_params(list_async_params):
            contracts_in_exp = []
            for args_params in list_async_params:
                args,_params,method = args_params.get("contract"),args_params.get("params"),args_params.get("method")
                params = {"method":method,"params":_params}
                args["_async"] = True 
                option = Option(**args)
                contracts_in_exp.append(option._get_method(**params))
            return contracts_in_exp
        
        contracts_in_exp = prepare_contracts_in_exp_from_list_args_params(list_args_params) 
        results = asyncio.run(fetch_all_contracts(contracts_in_exp,self.BATCH_SIZE,self.TIMEOUT
                                                                        ,self.MAX_RETRY,self.SLEEP))
        if results:
            df_tmps = [get_df_tmp(contract) for contract in results]
            if not all(df_tmp is None for df_tmp in df_tmps):
                df = pd.concat(df_tmps,axis=0)
        return df

    def get_list_args_params_from_df(self):
        list_args = self._df[["root","exp","strike","right"]].to_dict(orient="records")
        list_params = df[self._key_params].to_dict(orient="records")
        list_args_params = [{"contract":args
                            ,"params":list_params[idx]
                            ,"method":self._method} 
                            for idx,args in enumerate(list_args) ]
        return list_args_params

    def get_list_dates_volatility(self):
        desired_strikes = self.get_desired_strikes()
        contracts = [{"root":self.root,"exp":self.exp,"right":right,"strike":strike}
                            for strike in desired_strikes 
                            for right in ["call","put"]]
        self._df = pd.DataFrame(contracts)
        self._key_params = []
        self._method = "get_list_dates_implied_volatility"
        self.list_args_params = self.get_list_args_params_from_df()
        self._df = self.get_contracts_in_exp_async()
        print(f"[+] Total {self._df.shape[0]} contracts with dates in {self.exp}")
        return self._df
    
    def batching_exp_contract(self,df):

        # Calculate the cut-off date n business days ago from the expiration date
        df["implied_volatility"] = df["implied_volatility"].astype(str)
        df['exp_dt'] = pd.to_datetime(df['exp'], format='%Y%m%d')
        df["implied_volatility_dt"] = pd.to_datetime(df['implied_volatility'], format='%Y%m%d')
        df['cut_off'] = df['exp_dt'] - pd.tseries.offsets.BDay(self.daysAgo)
        df['is_within_n_business_days_ago'] = df['implied_volatility_dt'] > df['cut_off']
        df = df[df['is_within_n_business_days_ago'] == True]
        print(f"[+] Filtered {df.shape[0]} contracts with dates in {self.exp}")

        # Making batches
        batch = df.groupby(['root','exp', 'strike','right'
                                ,pd.Grouper(key='implied_volatility_dt', freq=self.freq_batch) # weekly 'W-MON' daily : D
                                ]).agg({'implied_volatility': ['first', 'last']})

        batch.columns = [f"{x}_{y}" for x, y in batch.columns]
        batch.reset_index(inplace=True)
        batch = batch[['root', 'exp', 'strike', 'right'
                                    ,'implied_volatility_first', 'implied_volatility_last']]

        df = batch.rename(columns={'implied_volatility_first':'start_date'
                                    ,'implied_volatility_last':'end_date'})

        # Adding main parameters
        for k,v in self.endpoint_params.items():
            df[k] = v                                                     
        # Save for check
        print(f"[+] Total dates/contracts batches {df.shape[0]} for {self.get_method()} in {self.exp}")
        #df.to_csv('./weekly_batch.csv', index=False)
        df
        return df
    
    def main_download(self,df):
        self._df = self.batching_exp_contract(df) 
        self.keys_params = self.endpoint_params.keys()
        _ = self.get_method()
        list_args_params = self.get_list_args_params_from_df()

        # Fetching data for method
        print(f"[+] Fetching asynchronously data for {self.exp}")  
        #list_args_params = list_args_params[:50]
        self._df = self.get_contracts_in_exp_async(list_args_params)
        return self._df 

roots = ["AMD","MSFT","AAPL","NVDA","TSLA","GOOGL","META","AMZN","GS","JPM","TSM","INTC","C","MS","BAC"]


manager_args = {
    "call_type":"at_time"
    ,"endpoint":"greeks"
    ,"endpoint_params": {"s_of_day": 12 * 3600}
    ,"daysAgo":100
    ,"freq_batch":"W-MON"
    ,"strikeMultiple":5
    ,"min_exp_date":"2020-01-01"
    ,"max_exp_date":"2023-07-01"
    ,"BATCH_SIZE":128
    ,"TIMEOUT":5
    ,"MAX_RETRY":3
    ,"SLEEP":20
}


if __name__=='__main__':
    for root in roots:
        manager_args["root"] = root
        manager = DownloadManager(**manager_args)
        _ = manager.isStorage()
        
        for exp in manager.desired_expirations:
            manager.exp = exp
       
        # Create a date range of n days starting from the input exp - check with TD if there are data basically.
            date_range = get_implied_vol_date_range(root,exp)
            if date_range:

                # Check if file already exists
                start_date,end_date = date_range[0],date_range[-1]
                _filename = manager.isFile(exp,start_date,end_date)
                if _filename :
                    # Getting all dates with implied volatility for each contract in exp             
                    df = manager.get_list_dates_volatility()                 
                    
                    # Fetching all the required data
                    if df is not None:
                        df = manager.main_download(df)
                        _ = manager.store_file()

                    




            
