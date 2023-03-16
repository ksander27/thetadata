from tdwrapper.wrapper import Option
from tdwrapper.tools import ExpiryManager

# ------------------------------ #

roots = ["AMZN","AAPL","TSLA","GOOGL","JPM","GS","MS","C"] #"INTC","AMD","NVDA", #"TSM",

args_app_manager = {
    "call_type":"at_time"
    ,"endpoint":"implied_volatility_verbose"
    ,"endpoint_params": {"s_of_day":15*3600+55*60}
    ,"days_ago": 70
    ,"strike_multiple":5
    ,"freq_exp":"monthly"
    ,"freq_batch": 'W-MON'
    ,"min_exp_date": "2020-01-01"
    ,"max_exp_date": "2023-06-01"
    ,"BATCH_SIZE":100
    ,"TIMEOUT":120
    ,"MAX_RETRY":5
    ,"SLEEP":30
}


if __name__=='__main__':
    for root in roots:
        args_app_manager["root"] = root
        mn = ExpiryManager(**args_app_manager)
        
        _ = mn.isStorage()
        
        option = Option(root)      
        desired_expirations = option.get_desired_expirations(mn.min_exp_date,mn.max_exp_date,mn.freq_exp)
        
        for exp in desired_expirations:
            mn.exp = exp
            mn.print()
            
            df_data = mn.get_exp_data()
            if df_data is not None:
                _ = mn.store_file(df_data)
