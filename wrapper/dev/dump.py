


             
        
#from tdwrapper.wrapper import Option,fetch_all_contracts

from tdwrapper.wrapper import Option,_format_date,AsyncFetcher


import asyncio
import pandas as pd
import os

class DownloadError(Exception):
    pass






    
# ---------------------------#

class DownloadManager():
    def __init__(self
                 ,root
                 ,call_type="hist",endpoint="eod",endpoint_params={}
                 ,exp=None
                 ,strike_multiple=5
                 ,min_exp_date='2018-01-01',max_exp_date='2024-01-01',freq_exp='Monthly'
                 ,days_ago=100,freq_batch="W-MON"
                 ,BATCH_SIZE=128,TIMEOUT=5,MAX_RETRY=3,SLEEP=20):
        self.root = root
        self.exp = exp
        self.call_type = call_type
        self.endpoint = endpoint
        self.endpoint_params = endpoint_params 
        

        self.strike_multiple = strike_multiple *1000

        self.DIR = f"/home/jupyter/data/{self.root}/{self.call_type}/{endpoint}"
        self.min_exp_date = _format_date(min_exp_date)
        self.max_exp_date = _format_date(max_exp_date)
        self.freq_exp = freq_exp

        self.days_ago = days_ago
        self.freq_batch = freq_batch

        self.BATCH_SIZE = BATCH_SIZE
        self.TIMEOUT = TIMEOUT
        self.MAX_RETRY = MAX_RETRY
        self.SLEEP = SLEEP



        self._filename = None

        self._dates = None
        self._batches = None
        self._data = None

        self._key_params = None
        self._method = self.get_method()

    def get_method(self):
        self.method = f"get_{self.call_type}_{self.endpoint}"
        return self.method
    
    def store_file(self):
        """ External facing function - check if the storing file exist"""
        if self._data is not None:
            print(f"[+] Fetched {self._data.shape[0]} contracts with dates")
            self._data.to_csv(self._filename,index=False)
            print(f"[+] Saved {self._filename}")
        else:
            print(f"[+] NO DATA - Nothing to save for {self._filename}") 



    def _isStorage(self):
            
        if os.path.exists(self.DIR):
            print(f"[+] {self.DIR} already exists.")
        else:
            os.makedirs(self.DIR)
            print(f"Created {self.DIR}")
        return None
    
    def _isFile(self,exp,start_date,end_date):

        _filename = f"{self.DIR}/{self.root}_{exp}_{start_date}_{end_date}"
        if os.path.exists(_filename):
            print(f"[+] {_filename} already exists.")
            return None
        else:
            self._filename = _filename
            print(f"\n[+] Building contracts for {exp}")
            return self._filename


    # ------------------- #

    def _get_list_dates_volatility(self):
        """
        Internal facing method. 
        Returns the list of 

        Returns:
            _type_: _description_
        """
        option = Option(root=self.root,exp=self.exp)
        desired_strikes = option.get_desired_strikes(self.strike_multiple)
        contracts = [{"root":self.root,"exp":self.exp,"right":right,"strike":strike}
                            for strike in desired_strikes 
                            for right in ["call","put"]]

        #--------------#
        batches = pd.DataFrame(contracts)
        key_params = None
        method = "get_list_dates_implied_volatility"
        asyncdl = AsyncDownloader(batches,method,key_params)
        self._dates = asyncdl.async_fetch_all_contracts()
        
        #--------------#

        if self._dates is not None:
            print(f"[+] Total {self._dates.shape[0]} contracts with dates in {self.exp}")
        else:
            print(f"[+] No implied volatility date for {self.root} - {self.exp}. Check with Thetadata")
        return self._dates
    



    
    def _batching_contracts(self):

        df_dates = self._prepare_dates()
        bm = BatchManager(self.freq_batch)
        df_batch = bm.make_batches(df_dates)
                                                  
        
        print(f"[+] Total dates/contracts batches {df_batch.shape[0]} for {self._get_method()} in {self.exp}")
        return df_batch
    
    def download_batches(self):  
        #--------------#      
        batches = self._batching_contracts() 
        method = self.get_method()
        key_params = self.endpoint_params.keys()

        print(f"[+] Fetching asynchronously data for {self.exp}")  
        asyncdl = AsyncDownloader(batches,method,key_params)
        self._data = asyncdl.async_fetch_all_contracts()
        #--------------#

        if self._data is not None:
            print(f"[+] Total {self._data.shape[0]} contracts with dates")
        else:
            exp = self.exp if self.exp is not None else None
            raise DownloadError(f"[+] No data for {self.root} - {exp}. Check with Thetadata")
        return self._data

roots = ["AAPL"]


manager_args = {
    "call_type":"at_time"
    ,"endpoint":"greeks"
    ,"endpoint_params": {"s_of_day": 12 * 3600}
    ,"days_ago":100
    ,"freq_batch":"W-MON"
    ,"strike_multiple":5
    ,"min_exp_date":"2020-01-01"
    ,"max_exp_date":"2023-07-01"
    ,"freq_exp":"monthly"
    ,"BATCH_SIZE":128
    ,"TIMEOUT":60
    ,"MAX_RETRY":3
    ,"SLEEP":30
}

roots = ["AMZN"] #,"NVDA","TSLA","GOOGL","META","AMZN","GS","JPM","TSM","INTC","C","MS","BAC"] #"AMD","MSFT",

if __name__=='__main__':

    for root in roots:
        manager_args["root"] = root
        manager = DownloadManager(**manager_args)
        _ = manager._isStorage()

        option = Option(root)
        desired_expirations = option.get_desired_expirations(min_exp_date=manager.min_exp_date
                                       ,max_exp_date=manager.max_exp_date,freq_exp=manager.freq_exp)
        
        for exp in desired_expirations:
            manager.exp = exp 
            option = Option(root=root,exp=exp)
            date_range = option.get_iv_dates_from_days_ago(manager.days_ago)
            if date_range:

                # Check if file already exists
                start_date,end_date = date_range[0],date_range[-1]
                _filename = manager._isFile(exp,start_date,end_date)
                if _filename :
                    # Getting all dates with implied volatility for each contract in exp             
                    dates = manager._get_list_dates_volatility()
                    
                    # Fetching all the required data
                    if dates is not None:
                        data = manager.download_batches()
                        _ = manager.store_file()
            break
        break


######

class ExpiryBatcher(BatchManager):
    def __init__(self,exp,df_dates,days_ago,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.exp = exp
        self.df_dates = df_dates
        self.days_ago
        
    def prepare_dates(self):
        # Calculate the cut-off date n business days ago from the expiration date
        self.df_dates["implied_volatility"] = self.df_dates["implied_volatility"].astype(str)
        self.df_dates['exp_dt'] = pd.to_datetime(self.df_dates['exp'], format='%Y%m%d')
        self.df_dates["implied_volatility_dt"] = pd.to_datetime(self.df_dates['implied_volatility'], format='%Y%m%d')
        self.df_dates['cut_off'] = self.df_dates['exp_dt'] - pd.tseries.offsets.BDay(self.days_ago)
        self.df_dates['is_within_n_business_days_ago'] = self.df_dates['implied_volatility_dt'] > self.df_dates['cut_off']
        self.df_dates = self.df_dates[self.df_dates['is_within_n_business_days_ago'] == True]

        self.df_dates = self.df_dates.rename(columns={"implied_volatility_dt":"key_dt"
                                                  ,"implied_volatility":"dt"})
        
        print(f"[+] Filtered {self.df_dates.shape[0]} contracts with dates in {self.exp}")
        return self.df_dates
    
    
    def get_batches(self):
        return self.make_batches(self.df_dates)
                    


##############
                if df is not None:
                    # Build list of args and params
                    batcher = ExpiryBatcher(df_dates=df_dates,freq_batch=freq_batch,days_ago=days_ago
                                            ,endpoint_params=endpoint_params)
                    df_batches = batcher.get_batches()
                    method = f"get_{call_type}_{endpoint}"
                    key_params = ["start_date","end_date"] + endpoint_params.keys()
                    print(key_params)

                    # Fetching data for method
                    print(f"[+] Fetching asynchronously data for {exp}")  
                    df = get_contracts_in_exp_async(df_batches,method,key_params,BATCH_SIZE,TIMEOUT,MAX_RETRY,SLEEP)
                    if df is not None:
                        print(f"[+] Fetched {df.shape[0]} contracts with dates in {exp}")
                        df.to_csv(_filename,index=False)
                        print(f"[+] Saved {_filename}")
                    else:
                        print(f"[+] NO DATA - Nothing to save for {_filename}") 


            
