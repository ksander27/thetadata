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
    def __init__(self,exp,date_key,days_ago,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.exp = exp
        self.days_ago = days_ago
        self.date_key = date_key

    def prepare_dates(self, df_dates: pd.DataFrame) -> pd.DataFrame:
        """
        Filters and prepares the input DataFrame containing date and implied volatility information based on the number of business days ago.

        Parameters:
        -----------
        df_dates : pd.DataFrame
            The input DataFrame containing date and implied volatility information.
            The expected columns are:
                - "strike": option strike prices
                - "implied_volatility": implied volatility dates in '%Y%m%d' format
                - "exp": expiration dates in '%Y%m%d' format

        Raises:
        -------
        ValueError:
            If all dates are filtered after applying the cut-off date.

        Returns:
        --------
        pd.DataFrame
            A filtered and prepared DataFrame containing the relevant date and implied volatility information.
            The returned DataFrame has the following columns:
                - "strike": normalized strike prices (divided by 1000)
                - "dt_key": implied volatility dates in datetime64 format
                - "dt": implied volatility dates in '%Y%m%d' format
                - "exp_dt": expiration dates in datetime64 format

        """
        df_tmp = df_dates.copy()
        # Normalize strike
        df_dates["strike"] /= 1000

        # Calculate the cut-off date n business days ago from the expiration date
        df_dates[self.date_key] = df_dates[self.date_key].astype(str)
        df_dates['exp_dt'] = pd.to_datetime(df_dates['exp'], format='%Y%m%d')
        df_dates[f"{self.date_key}_dt"] = pd.to_datetime(df_dates[self.date_key], format='%Y%m%d')

        if self.days_ago is not None:
            df_dates['cut_off'] = df_dates['exp_dt'] - pd.tseries.offsets.BDay(self.days_ago)
            df_dates['is_within_n_business_days_ago'] = df_dates[f"{self.date_key}_dt"] > df_dates['cut_off']
            df_dates = df_dates[df_dates['is_within_n_business_days_ago'] == True]

        df_dates = df_dates.rename(columns={f"{self.date_key}_dt": "dt_key",
                                            self.date_key: "dt"})

        print(f"[+] bt - Filtered {df_dates.shape[0]} contracts with dates in {self.exp}")

        if not df_dates.empty:
            return df_dates
        else:
            tmp = "/home/jupyter/data/pre_filter.csv"
            df_tmp.to_csv(tmp,index=False)
            raise ValueError(f"[+] bt - Expiry Batcher - all dates are filtered - file saved @ {tmp}.")
            
    def prepare_yesterday(self, df_dates: pd.DataFrame) -> pd.DataFrame:
        df_tmp = df_dates.copy()
        # Normalize strike
        df_dates["strike"] /= 1000

        # Convert date columns to datetime64 format
        df_dates[self.date_key] = df_dates[self.date_key].astype(str)
        df_dates['exp_dt'] = pd.to_datetime(df_dates['exp'], format='%Y%m%d')
        df_dates[f"{self.date_key}_dt"] = pd.to_datetime(df_dates[self.date_key], format='%Y%m%d')

        # Calculate the cut-off date as the previous business day
        today = pd.Timestamp.now().normalize()
        previous_business_day = today - pd.tseries.offsets.BDay(1)

        # Filter the DataFrame for the previous business day
        df_dates = df_dates[df_dates[f"{self.date_key}_dt"] == previous_business_day]

        df_dates = df_dates.rename(columns={f"{self.date_key}_dt": "dt_key",
                                            self.date_key: "dt"})

        print(f"[+] bt - Filtered {df_dates.shape[0]} contracts with dates in {self.exp}")

        if not df_dates.empty:
            return df_dates
        else:
            df_tmp.to_csv("/home/jupyter/data/pre_filter.csv",index=False)
            raise ValueError("[+] bt - Expiry Batcher - all dates are filtered.")

            
    def get_yesterday_batches(self,df_dates):
        df_prep = self.prepare_yesterday(df_dates)
        return self.make_batches(df_prep)

    def get_batches(self,df_dates):
        df_prep = self.prepare_dates(df_dates)
        return self.make_batches(df_prep)