import os
import pandas as pd 
import time
from . import AsyncDownloader,ExpiryBatcher
from ..wrapper import Option

class AppManager():
    def __init__(self,call_type,endpoint,endpoint_params,root,exp=None,start_date=None,end_date=None):
        self.call_type = call_type
        self.endpoint = endpoint
        self.endpoint_params = endpoint_params
        self.method = self.get_method()
        self.root = root
        self.exp = exp

        self.start_date = start_date
        self.end_date = end_date
        
        self._DIR = self._get_DIR()
        self._file = self._get_file()
        self.filename = self.get_filename()
    
    def get_method(self):
        self.method = f"get_{self.call_type}_{self.endpoint}"
        return self.method

    def _get_DIR(self):
        self._DIR = f"/home/jupyter/data/{self.call_type}/{self.endpoint}/{self.root}"
        return self._DIR
    
    def _get_file(self):
        self._file = f"{self.root}_{self.exp}_{self.start_date}_{self.end_date}"
        return self._file
    
    def get_filename(self):    

        self.filename = f"{self._get_DIR()}/{self._get_file()}.csv" 
        return self.filename
    
    def isFile(self):
        _ = self.get_filename()
        if os.path.exists(self.filename):
            print(f"[+] {self.filename} already exists.")
            return True
        else:
            print(f"\n[+] Building contracts for {self.exp}")
            return False
    
    def isStorage(self):
        if os.path.exists(self._DIR):
            print(f"[+] {self._DIR} already exists.")
        else:
            os.makedirs(self._DIR)
            print(f"Created {self._DIR}")
        return None
    
    def store_file(self,df_data):
        if df_data is not None:
            print(f"[+] Fetched {df_data.shape[0]} contracts with dates in {self.exp}")
            df_data.to_csv(self.filename,index=False)
            print(f"[+] Saved {self.filename}")
        else:
            print(f"[+] NO DATA - Nothing to save for {self.filename}") 




class ExpiryManager(AppManager):
    def __init__(self,strike_multiple=5,days_ago='70',freq_batch='D'
                 ,min_exp_date='2018-01-01',max_exp_date='2024-01-01',freq_exp='monthly'
                 ,BATCH_SIZE=128,TIMEOUT=60,MAX_RETRY=3,SLEEP=30,*args,**kwargs):
        super().__init__(*args,**kwargs)
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

    def sleep(self):
        print(f"[+] Manager going to sleep for {self.SLEEP}")
        time.sleep(self.SLEEP)
        return None
        
        
    def get_exp_data(self):
        df_data = None
        # Create a date range of n days starting from the input exp - check with TD if there are data basically.
        option = Option(self.root,self.exp)
        date_range = option.get_iv_dates_from_days_ago(self.days_ago)
        if date_range:
            # Check if file already exists
            self.start_date,self.end_date = date_range[0],date_range[-1]
            if not self.isFile():
                # Getting all dates with implied volatility for each contract in exp
                option = Option(self.root,self.exp)
                desired_strikes = option.get_desired_strikes(self.strike_multiple)

                df_batches = pd.DataFrame([{"root":self.root,"exp":self.exp,"right":right,"strike":strike}
                                                                    for strike in desired_strikes 
                                                                    for right in ["call","put"]])

                # Fetching response to get all the dates per contract      
                method,key_params = "get_list_dates_implied_volatility",[]
                downloader = AsyncDownloader(batches=df_batches,method=method,key_params=key_params
                                ,batch_size=self.BATCH_SIZE,timeout=self.TIMEOUT,max_retry=self.MAX_RETRY,sleep=self.SLEEP)    
                df_dates = downloader.async_download_contracts()               
                rows = df_dates.shape[0]

                print(f"[+] Total {rows} contracts with dates in {self.exp} - Split in {int(rows/self.BATCH_SIZE)}")                    

                if df_dates is not None:
                    # Build list of args and params
                    batcher = ExpiryBatcher(exp=self.exp,days_ago=self.days_ago
                                            ,freq_batch=self.freq_batch,endpoint_params=self.endpoint_params)

                    df_batches = batcher.get_batches(df_dates)
                    method = self.get_method()
                    key_params = ["start_date","end_date"] + list(self.endpoint_params.keys())

                    # Fetching data for method
                    print(f"[+] Fetching asynchronously data for {self.filename}")  
                    downloader = AsyncDownloader(batches=df_batches,method=method,key_params=key_params
                                                ,batch_size=self.BATCH_SIZE,timeout=self.TIMEOUT
                                                 ,max_retry=self.MAX_RETRY,sleep=self.SLEEP)

                    df_data = downloader.async_download_contracts()
        return df_data