


             
        
#from tdwrapper.wrapper import Option,fetch_all_contracts

from tdwrapper.wrapper import Option,_format_date,AsyncFetcher


import asyncio
import pandas as pd
import os

class DownloadError(Exception):
    pass



class BatchManager():
    def __init__(self,freq_batch):
        self.freq_batch = freq_batch 

    def make_batches(self,data):
        """
        Must receive a pandas dataframe with columns [root,exp,strike,right,dt_key,dt]
        returns a df grouped by the freq_batch

        Args:
            data (_type_): _description_

        Returns:
            _type_: _description_
        """
        df_batch = data.groupby(['root','exp', 'strike','right'
                                ,pd.Grouper(key='dt_key', freq=self.freq_batch) # weekly 'W-MON' daily : D
                                ]).agg({'dt': ['first', 'last']})

        df_batch.columns = [f"{x}_{y}" for x, y in df_batch.columns]
        df_batch.reset_index(inplace=True)
        df_batch = df_batch[['root', 'exp', 'strike', 'right'
                                    ,'dt_first', 'dt_last']]

        df_batch = df_batch.rename(columns={'dt_first':'start_date'
                                    ,'dt_last':'end_date'})
        return df_batch
    

class AsyncDownloader(AsyncFetcher):
    def __init__(self,batches,method,key_params=[],*args,**kwargs):
        super().__init__(*args,**kwargs)
                    
        self.batches = batches # must be df with root,exp,strike,right
        self.method = method
        self.key_params = key_params # List of parameters alongside a method

        self.list_async_params = None

    def get_list_async_params_from_batches(self):
        """ 
            Internal facing function. Takes a the property ._batches and returns 
            a list of dict [{"contract":{option descr},"method":method,"params":{params alongside the method}}]
        """
        list_args = self.batches[["root","exp","strike","right"]].to_dict(orient="records")

        list_params = self.batches[self.key_params].to_dict(orient="records")
        list_async_params = [{"contract":args
                        ,"params":list_params[idx] if len(list_params)>0 else None
                        ,"method":self.method} 
                        for idx,args in enumerate(list_args)]

        return list_async_params
    
    def prepare_contracts_from_list_args_params(self):
        contracts = []
        for args_params in self.list_async_params:
            args,_params,method = args_params.get("contract"),args_params.get("params"),args_params.get("method")
            params = {"method":method,"params":_params}
            args["_async"] = True 
            option = Option(**args)
            opt = option._get_method(**params)
            contracts.append(opt)
        return contracts
    
    def get_df_tmp(self,contract):
        df_tmp = None 

        url = contract.get("url")
        data = contract.get("data")
        params = contract.get("params")
        if url and data:
            df_tmp = pd.DataFrame(data)
            df_tmp["url"] = url
            for k,v in params.items():
                df_tmp[k] = v
        return df_tmp
    
    def async_fetch_all_contracts(self):
        """ 
            External facing function.
            When given a list of dict following the convention {contract,method,params}
            fetch all the contracts asynchronously and returns a df if some data where 
            received.
        """
        df = None 
        self.list_async_params = self.get_list_async_params_from_batches()
        contracts_in_exp = self.prepare_contracts_from_list_args_params() 
        results = asyncio.run(self.fetch_all_contracts(contracts_in_exp))
        if results:
            df_tmps = [self.get_df_tmp(contract) for contract in results]
            if not all(df_tmp is None for df_tmp in df_tmps):
                df = pd.concat(df_tmps,axis=0)

        return df
    
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
    
    def _prepare_dates(self):
        # Calculate the cut-off date n business days ago from the expiration date
        self._dates["implied_volatility"] = self._dates["implied_volatility"].astype(str)
        self._dates['exp_dt'] = pd.to_datetime(self._dates['exp'], format='%Y%m%d')
        self._dates["implied_volatility_dt"] = pd.to_datetime(self._dates['implied_volatility'], format='%Y%m%d')
        self._dates['cut_off'] = self._dates['exp_dt'] - pd.tseries.offsets.BDay(self.days_ago)
        self._dates['is_within_n_business_days_ago'] = self._dates['implied_volatility_dt'] > self._dates['cut_off']
        self._dates = self._dates[self._dates['is_within_n_business_days_ago'] == True]

        self._dates = self._dates.rename(columns={"implied_volatility_dt":"key_dt"
                                                  ,"implied_volatility":"dt"})
        
        print(f"[+] Filtered {self._dates.shape[0]} contracts with dates in {self.exp}")
        return self._dates


    
    def _batching_contracts(self):

        df_dates = self._prepare_dates()
        bm = BatchManager(self.freq_batch)
        df_batch = bm.make_batches(df_dates)
        # Adding main parameters
        for k,v in self.endpoint_params.items():
            df_batch[k] = v                                                     
        
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





                    





            
