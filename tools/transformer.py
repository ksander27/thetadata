import pandas as pd


class BatchManager():
    def __init__(self,freq_batch,endpoint_params):
        self.freq_batch = freq_batch 
        self.endpoint_params = endpoint_params

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
        
        # Adding main parameters
        for k,v in self.endpoint_params.items():
            df_batch[k] = v   
                    
        return df_batch
    
class ExpiryBatcher(BatchManager):
    def __init__(self,exp,days_ago,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.exp = exp
        self.days_ago = days_ago
        
    def prepare_dates(self,df_dates):
        # Normalize strike 

        df_dates["strike"] /=1000
        # Calculate the cut-off date n business days ago from the expiration date
        df_dates["implied_volatility"] = df_dates["implied_volatility"].astype(str)
        df_dates['exp_dt'] = pd.to_datetime(df_dates['exp'], format='%Y%m%d')
        df_dates["implied_volatility_dt"] = pd.to_datetime(df_dates['implied_volatility'], format='%Y%m%d')
        df_dates['cut_off'] = df_dates['exp_dt'] - pd.tseries.offsets.BDay(self.days_ago)
        df_dates['is_within_n_business_days_ago'] = df_dates['implied_volatility_dt'] > df_dates['cut_off']
        df_dates = df_dates[df_dates['is_within_n_business_days_ago'] == True]

        df_dates.to_csv('./debug.csv')
        df_dates = df_dates.rename(columns={"implied_volatility_dt":"dt_key"
                                                  ,"implied_volatility":"dt"})
        
        print(f"[+] Filtered {df_dates.shape[0]} contracts with dates in {self.exp}")
        return df_dates
    
    
    def get_batches(self,df_dates):
        df_prep = self.prepare_dates(df_dates)
        return self.make_batches(df_prep)