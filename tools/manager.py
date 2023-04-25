import os
import pandas as pd 
import json
import time
import subprocess
import psutil

from . import AsyncDownloaderOption,ExpiryBatcher
from ..wrapper import Option,NoDataForContract,_format_date

import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)


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


        self.PID_terminal = None


    def _get_PID_terminal(self):
        for proc in psutil.process_iter(['name', 'cmdline']):
            if proc.info.get('name') == 'java':
                self.PID_terminal = proc.pid
                logging.info(f"[+] Terminal is running with PID - {self.PID_terminal}")
                break 
        else:
            self.PID_terminal = None
            logging.info('[+] Terminal is not running')
        return self.PID_terminal
    
    def _stop_terminal(self):
        _ = self._get_PID_terminal()
        logging.info(f"[+] Stopping the terminal with PID - {self.PID_terminal}")
        subprocess.run(['kill','-15',str(self.PID_terminal)])
        _ = self._get_PID_terminal()
        return self.PID_terminal
    
    def _start_terminal(self):
        command = "java -jar /etc/thetadata/ThetaTerminal.jar $USERNAME $PASSWORD &"
        subprocess.Popen(command.split())
        _ = self._get_PID_terminal()
        return self.PID_terminal
    
    def get_method(self):
        self.method = f"get_{self.call_type}_{self.endpoint}"
        return self.method
    
    def get_key_params(self):
        return ["start_date","end_date"] + list(self.endpoint_params.keys())

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
            logging.info(f"[+] mn - {self.filename} already exists.")
            return True
        else:
            return False
    
    def isStorage(self):
        if os.path.exists(self._DIR):
            logging.info(f"[+] mn - {self._DIR} already exists.")
        else:
            os.makedirs(self._DIR)
            logging.info(f"[+] mn - Created {self._DIR}")
        return None
    
    def store_file(self,df_data):
        if df_data is not None:
            logging.info(f"[+] mn - Fetched {df_data.shape[0]} contracts with dates in {self.exp}")
            df_data.to_csv(self.filename,index=False)
            logging.info(f"[+] mn - Saved {self.filename}")
        else:
            logging.info(f"[+] mn - NO DATA - Nothing to save for {self.filename}") 




class ExpiryManager(AppManager):
    def __init__(self,strike_multiple=5,days_ago='70',dt=None,freq_batch='D'
                 ,min_exp_date='2018-01-01',max_exp_date='2024-01-01',freq_exp='monthly'
                 ,BATCH_SIZE=128,TIMEOUT=60,MAX_RETRY=3,SLEEP=30,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.strike_multiple = strike_multiple
        self.days_ago = days_ago
        self.freq_batch = freq_batch
        self.dt = dt
        self._dt = self._get_dt()
        
        self.min_exp_date = min_exp_date
        self.max_exp_date = max_exp_date
        self.freq_exp = freq_exp
        
        self.BATCH_SIZE = BATCH_SIZE
        self.TIMEOUT = TIMEOUT
        self.MAX_RETRY = MAX_RETRY
        self.SLEEP = SLEEP

    def sleep(self):
        logging.info(f"[+] Manager going to sleep for {self.SLEEP}")
        time.sleep(self.SLEEP)
        return None
        
    def print(self):
        logging.info("""\n#-------------------------------------------#""")
        logging.info(f"""# mn - Download {self.root} - {self.exp} - {self.BATCH_SIZE} {self.freq_batch} """)
        logging.info("""#-------------------------------------------#\n""")
        return None
    
    def _get_dt(self):
        if self.dt:
            return _format_date(self.dt)

        
    def get_hist_exp_data(self):
        df_data = None
        # Create a date range of n days starting from the input exp - check with TD if there are data basically.
        option = Option(self.root,self.exp)
        try:
            date_range = option.get_iv_dates_from_days_ago(self.days_ago)
        except NoDataForContract:
            date_range = None

        if date_range is None :
            logging.info(f"[+] mn - No IV data for {self.exp}")
        else:
            # Check if file already exists
            self.start_date,self.end_date = date_range[0],date_range[-1]
            if not self.isFile():
                # Getting all dates with implied volatility for each contract in exp
                option = Option(self.root,self.exp)
                desired_strikes = option.get_desired_strikes(self.strike_multiple)
                if len(desired_strikes)==0:
                    logging.info(f"[+] mn - No strikes for {self.root} {self.exp} - check multiples")
                else:

                    df_batches = pd.DataFrame([{"root":self.root,"exp":self.exp,"right":right,"strike":strike}
                                                                        for strike in desired_strikes 
                                                                        for right in ["call","put"]])

                    # Fetching response to get all the dates per contract      
                    method,key_params = "get_list_dates_implied_volatility",[]
                    downloader = AsyncDownloaderOption(batches=df_batches,method=method,key_params=key_params
                                    ,batch_size=self.BATCH_SIZE,timeout=self.TIMEOUT,max_retry=self.MAX_RETRY,sleep=self.SLEEP)    
                    df_dates = downloader.async_download_contracts()               

                    rows = df_dates.shape[0]
                    logging.info(f"[+] mn - Total {rows} contracts with dates in {self.exp}.")                    

                    if df_dates is not None:
                        # Build list of args and params
                        batcher = ExpiryBatcher(exp=self.exp,days_ago=self.days_ago,date_key="implied_volatility"
                                                ,freq_batch=self.freq_batch,endpoint_params=self.endpoint_params)

                        df_batches = batcher.get_batches_from_exp(df_dates=df_dates)
                        if df_batches is not None:
                            method = self.get_method()
                            key_params = ["start_date","end_date"] + list(self.endpoint_params.keys())

                            rows = df_batches.shape[0]
                            logging.info(f"[+] mn - Total {int(rows/self.BATCH_SIZE)+1} batches for {self.exp}.")                    

                            # Fetching data for method
                            downloader = AsyncDownloaderOption(batches=df_batches,method=method,key_params=key_params
                                                        ,batch_size=self.BATCH_SIZE,timeout=self.TIMEOUT
                                                        ,max_retry=self.MAX_RETRY,sleep=self.SLEEP)

                            df_data = downloader.async_download_contracts()

        return df_data
    

    def get_hist_data_dt(self):
        df_data = None 
        option = Option(self.root,self.exp)

        # Test if dt is in implied vol date
        if not option.isIVDate(self.dt):
            logging.info(f"[+] mn - NO IV date for {self.dt}")
        else:
            logging.info(f"[+] mn - Fetching for {self.root} {self.exp} - {self._dt} {self._dt}")
            self.start_date,self.end_date = self._dt,self._dt
            if not self.isFile():
                option = Option(self.root,self.exp)
                desired_strikes = option.get_desired_strikes(self.strike_multiple)

                if len(desired_strikes)==0:
                    logging.info(f"[+] mn - No strikes for {self.root} {self.exp} - check multiples")
                else:
                    df_batches = pd.DataFrame([{"root":self.root, "exp":self.exp,"right":right,"strike":strike}
                                                                        for strike in desired_strikes
                                                                        for right in ["call","put"]])
                    
                    method, key_params = "get_list_dates_implied_volatility",[]
                    downloader = AsyncDownloaderOption(batches=df_batches,method=method,key_params=key_params
                                    ,batch_size=self.BATCH_SIZE,timeout=self.TIMEOUT,max_retry=self.MAX_RETRY,sleep=self.SLEEP)
                    df_dates = downloader.async_download_contracts()

                    rows =df_dates.shape[0]
                    logging.info(f"[+] mn - Total {rows} contracts with dates in {self.exp}.")

                    if df_dates is not None:
                        # Build list of args and params
                        batcher = ExpiryBatcher(exp=self.exp,dt=self._dt,date_key="implied_volatility"
                                                ,freq_batch='D',endpoint_params=self.endpoint_params)

                        df_batches = batcher.get_dt_batches(df_dates=df_dates)
                        if df_batches is not None:
                            method = self.get_method()
                            key_params = ["start_date","end_date"] + list(self.endpoint_params.keys())

                            rows = df_batches.shape[0]
                            logging.info(f"[+] mn - Total {int(rows/self.BATCH_SIZE)+1} batches for {self.exp}.")                    

                            # Fetching data for method
                            downloader = AsyncDownloaderOption(batches=df_batches,method=method,key_params=key_params
                                                        ,batch_size=self.BATCH_SIZE,timeout=self.TIMEOUT
                                                        ,max_retry=self.MAX_RETRY,sleep=self.SLEEP)

                            df_data = downloader.async_download_contracts()


        return df_data    
