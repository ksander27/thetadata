from ._option import Option
from ..wrapper import NoDataForContract
from ..utils import _format_date
from datetime import datetime, timedelta
import pandas as pd
from typing import Optional,List

YESTERDAY = datetime.now() - timedelta(days=1)

class Option(Option):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        
    def get_implied_vol_days_ago(self, days_ago: int) -> Optional[List[str]]:
        """
        Get a list of dates of implied volatility for the option expiring `days_ago` days ago.

        Parameters:
        -----------
        days_ago : int
            The number of days ago to get the list of implied volatility dates for.

        Raises:
        -------
            TypeError: If the days_ago parameter is not an integer.
            ValueError: If the days_ago parameter is a negative integer.

        Returns:
        --------
        Optional[List[str]]
            A list of dates of implied volatility for the option expiring `days_ago` days ago.
            If there is no data for the contract or the contract does not have a strike and a right,
            return None.
        """
        
        if not isinstance(days_ago,int) :
            raise TypeError("[+] days_ago must be positive integer")
        if days_ago <0:
            raise ValueError("[+] days_ago must be positive integer")
            

        args = {"root":self.root,"exp":self.exp}

        if self.strike and self.right:
            args["strike"] = self.strike
            args["right"] = self.right
        elif self.strike or self.right:
            raise ValueError("[+] We need a strike and a right")
        else:
            pass

        _exp = datetime.strptime(self._exp, '%Y%m%d')
        if YESTERDAY > _exp:
            date_range = pd.date_range(end=_exp, periods=days_ago, freq='B')
        else:
            date_range = pd.date_range(end= YESTERDAY, periods=days_ago, freq='B')

        try:
            option = Option(**args)
            date_implied_vol = option.get_list_dates_implied_volatility()
            date_implied_vol = [str(_dict.get("implied_volatility")) for _dict in date_implied_vol]

            date_range = [date for date in date_range.strftime('%Y%m%d').to_list() if date in date_implied_vol]
            print(f"[+] {len(date_range)} business days for {self.exp}")
            return date_range
        except NoDataForContract:
            print(f"[+] NO IMPLIED VOL DATA FOR {self.exp} - check with thetadata")
            return None
        
        
    def get_desired_expirations(self, min_exp_date: str, max_exp_date: str, freq_exp: str = 'monthly') -> List[str]:
        """
        Returns a list of desired expiration dates given a minimum expiration date, maximum expiration date, and 
        frequency of expiration dates. 

        Args:
        - min_exp_date (str or datetime.date): The minimum expiration date in '%Y%m%d' format or as a date object.
        - max_exp_date (str or datetime.date): The maximum expiration date in '%Y%m%d' format or as a date object.
        - freq_exp (str, default='monthly'): The frequency of desired expiration dates. Valid options are 'monthly' 
        or 'weekly'.

        Raises:
        - TypeError: If freq_exp is not a string.
        - ValueError: If freq_exp is not 'monthly' or 'weekly'.

        Returns:
        - desired_expirations (list of str): A list of desired expiration dates in '%Y%m%d' format.
        """

        if not isinstance(freq_exp,str):
            raise TypeError("freq_exp must be a str")

        min_exp_date = _format_date(min_exp_date)
        max_exp_date = _format_date(max_exp_date)

        expirations = [str(expiration.get("expirations")) for expiration in self.get_list_expirations()]

        if 'mon' in freq_exp.lower():
            exp_range = pd.date_range(min_exp_date,max_exp_date,freq='WOM-3FRI')

        elif 'wee' in freq_exp.lower():
            exp_range = pd.date_range(min_exp_date, max_exp_date, freq="W-MON")
            d2 = pd.date_range(min_exp_date, max_exp_date, freq="W-WED")
            d3 = pd.date_range(min_exp_date, max_exp_date, freq="W-FRI")
            exp_range = exp_range.union(d2)
            exp_range = exp_range.union(d3)

        else:
            raise ValueError("freq_exp must be Monthly or Weekly")
        desired_expirations = [exp for exp in exp_range.strftime('%Y%m%d').to_list() if exp in expirations]
        return desired_expirations
        
