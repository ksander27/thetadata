from ..wrapper import Option
from . import AsyncDownloader,ExpiryBatcher,AppManager

import pandas as pd


class ExpiryManager(AppManager):
    def __init__(self,strike_multiple=5,days_ago='70',freq_batch='D'
                 ,exp=None,min_exp_date='2018-01-01',max_exp_date='2024-01-01',freq_exp='monthly'
                 ,BATCH_SIZE=128,TIMEOUT=60,MAX_RETRY=3,SLEEP=30,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.exp = exp
        self.strike_multiple = strike_multiple
        self.days_ago = days_ago
        self.freq_batch = freq_batch
        
        self.min_exp_date = min_exp_date
        self.max_exp_date = max_exp_date
        self.freq_exp = freq_exp
        
        self.BATCH_SIZE = BATCH_SIZE
        self.TIMEOUT = TIMEOUT
        self.MAX_RETRY = MAX_RETRY
        self.SLEEP = SLEEP
        
        
    def get_exp_data(self):
        df_data = None
        # Create a date range of n days starting from the input exp - check with TD if there are data basically.
        option = Option(self.root,self.exp)
        date_range = option.get_iv_dates_from_days_ago(self.days_ago)
        if date_range:
            # Check if file already exists
            start_date,end_date = date_range[0],date_range[-1]
            if not self.isFile():
                # Getting all dates with implied volatility for each contract in exp
                option = Option(self.root,self.exp)
                desired_strikes = option.get_desired_strikes(self.strike_multiple)

                df_batches = pd.DataFrame([{"root":self.root,"exp":self.exp,"right":right,"strike":strike}
                                                                    for strike in desired_strikes 
                                                                    for right in ["call","put"]])

                # Fetching response to get all the dates per contract      
                method,key_params = "get_list_dates_implied_volatility",{}
                downloader = AsyncDownloader(batches=df_batches,method=method,key_params=key_params
                                ,batch_size=self.BATCH_SIZE,timeout=self.TIMEOUT,max_retry=self.MAX_RETRY,sleep=self.SLEEP)    
                df_dates = downloader.async_download_contracts()               
                print(f"[+] Total {df_dates.shape[0]} contracts with dates in {exp}")                    

                if df_dates is not None:
                    # Build list of args and params
                    batcher = ExpiryBatcher(exp=self.exp,days_ago=self.days_ago
                                            ,freq_batch=self.freq_batch,endpoint_params=self.endpoint_params)

                    df_batches = batcher.get_batches(df_dates)
                    method = self.get_method()
                    key_params = ["start_date","end_date"] + list(self.endpoint_params.keys())

                    # Fetching data for method
                    print(f"[+] Fetching asynchronously data for {exp}")  
                    downloader = AsyncDownloader(batches=df_batches,method=method,key_params=key_params
                                                ,batch_size=self.BATCH_SIZE,timeout=self.TIMEOUT
                                                 ,max_retry=self.MAX_RETRY,sleep=self.SLEEP)

                    df_data = downloader.async_download_contracts()
        return df_data

# ------------------------------ #

roots = ["AAPL","AMZN","NVDA"]

args_app_manager = {
    "call_type":"hist"
    ,"endpoint":"eod_quote_greeks"
    ,"endpoint_params": {}
    ,"days_ago": 70
    ,"strike_multiple":5
    ,"freq_exp":"monthly"
    ,"freq_batch": 'D'
    ,"min_exp_date": "2020-01-01"
    ,"min_exp_date": "2023-08-01"
    ,"BATCH_SIZE":512
    ,"TIMEOUT":120
    ,"MAX_RETRY":3
    ,"SLEEP":30
}


if __name__=='__main__':
    for root in roots:
        mn = AppManager(**args_app_manager)
        _ = mn.isStorage()
        
        option = Option(root)      
        desired_expirations = option.get_desired_expirations(mn.min_exp_date,mn.max_exp_date,mn.freq_exp)
        
        for exp in desired_expirations:
            mn.exp = exp
            df_data = mn.get_exp_data()
            _ = mn.store_file(df_data)