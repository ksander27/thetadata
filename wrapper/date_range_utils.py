from .option import Option 
from .wrapper import NoDataForContract
from datetime import datetime,timedelta
import pandas as pd

YESTERDAY = datetime.now() - timedelta(days=1)

def get_desired_expirations(root,min_exp_date,max_exp_date,freq_exp='WOM-3FRI'):
    option = Option(root=root)
    expirations = [str(expiration.get("expirations")) for expiration in option.get_list_expirations()]

    # Get desired_expirations - improve to allow picking 
    exp_range = pd.date_range(min_exp_date,max_exp_date,freq=freq_exp)
    desired_expirations = [exp for exp in exp_range.strftime('%Y%m%d').to_list() if exp in expirations]
    return desired_expirations

def get_implied_vol_date_range(root,exp,strike=None,right=None, daysAgo=100):
    # This check should happen at the module level
    if strike and right:
        args = {
                "root":root
                ,"exp":exp
                ,"strike":strike
                ,"right":right
        }
    elif not strike and not right:
        args = {
            "root":root
            ,"exp":exp
        }
    else:
        raise ValueError("[+] We need a strike and a right")

    _exp = datetime.strptime(exp, '%Y%m%d')
    if YESTERDAY > _exp:
        date_range = pd.date_range(end=_exp, periods=daysAgo, freq='B')
    else:
        date_range = pd.date_range(end= YESTERDAY, periods=daysAgo, freq='B')

    try:
        option = Option(**args)
        date_implied_vol = option.get_list_dates_implied_volatility()
        date_implied_vol = [str(_dict.get("implied_volatility")) for _dict in date_implied_vol]
                            
        date_range = [date for date in date_range.strftime('%Y%m%d').to_list() if date in date_implied_vol]
        print(f"[+] {len(date_range)} business days for {exp}")
        return date_range
    except NoDataForContract:
        print(f"[+] NO IMPLIED VOL DATA FOR {exp} - check with thetadata")
        return None