from tdwrapper.wrapper import Option
from tdwrapper.tools import ExpiryManager

# ------------------------------ #

roots = ["AMZN","NVDA","INTC","AMD","TSM","TSLA","META","GOOGL","JPM","GS","MS","C"]

args_app_manager = {
    "call_type":"hist"
    ,"endpoint":"open_interest"
    ,"endpoint_params": {}
    ,"days_ago": None
    ,"strike_multiple":1
    ,"freq_exp":"all"
    ,"freq_batch": 'W-MON'
    #,"min_exp_date": "2020-01-01"
    #,"max_exp_date": "2023-06-01"
    ,"BATCH_SIZE":100
    ,"TIMEOUT":60
    ,"MAX_RETRY":5
    ,"SLEEP":30
}


if __name__=='__main__':
    for root in roots:
        args_app_manager["root"] = root
        mn = ExpiryManager(**args_app_manager)
        
        _ = mn.isStorage()
        
        option = Option(root)      
        desired_expirations = option.get_open_expirations(freq_exp=mn.freq_exp)
        
        for exp in desired_expirations:
            mn.print()
            mn.exp = exp

            df_data = mn.get_hist_open_interest_data()
            if df_data is not None:
                _ = mn.store_file(df_data)
