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
    