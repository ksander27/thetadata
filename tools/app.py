from ..wrapper import Option
from . import AppManager

# ------------------------------ #

roots = ["AAPL","AMZN","NVDA","TSLA"]

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