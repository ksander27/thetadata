from ._option import Option,OptionError
from ..wrapper import NoDataForContract
from ..utils import _format_date
from datetime import datetime, timedelta
import pandas as pd
from typing import Optional,List

YESTERDAY = datetime.now() - timedelta(days=1)

class Option(Option):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        
    def get_iv_dates_from_days_ago(self, days_ago: int) -> Optional[List[str]]:
        """
        Get a list of dates of implied volatility betweem the option exp and `days_ago` days ago.

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
            A list of dates of implied volatility for the option expiring `days_ago` days ago. If there is no data 
            for the contract or the contract does not have a strike and a right, return None.
            
        Notes:
        ------
        This method raises an exception if there is no data for the specified contract or if the contract does not 
        have a strike and a right.

        Example:
        --------
        >>> option = Option('AAPL', '20220318', 150, 'C')
        >>> dates = option.get_implied_vol_days_ago(10)
        [+] 5 business days for 20220318
        >>> print(dates)
        ['20220304', '20220307', '20220308', '20220309', '20220310']
        """
        
        date_range = None
        if not isinstance(days_ago,int) :
            raise TypeError("[+] days_ago must be positive integer")
        if days_ago <0:
            raise ValueError("[+] days_ago must be positive integer")
            

        args = {"root":self.root,"exp":self.exp}

        if self.strike and self.right:
            args["strike"] = self.strike
            args["right"] = self.right
        elif self.strike or self.right:
            raise OptionError("[+] We need a strike and a right")
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

            if len(date_implied_vol)>0:
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

        Parameters:
        -----------
        min_exp_date : str
            The minimum expiration date in '%Y%m%d' format.
        max_exp_date : str
            The maximum expiration date in '%Y%m%d' format.
        freq_exp : str, default 'monthly'
            The frequency of desired expiration dates. Valid options are 'monthly' or 'weekly'.

        Raises:
        -------
        TypeError:
            If min_exp_date or max_exp_date is not a string or if freq_exp is not a string.
        ValueError:
            If freq_exp is not 'monthly' or 'weekly'.

        Returns:
        --------
        List[str]
            A list of desired expiration dates in '%Y%m%d' format that fall within the specified date range and frequency.

        Example :
        ---------


        """


        desired_expirations = None
        if not isinstance(freq_exp,str):
            raise TypeError("freq_exp must be a str")

        min_exp_date = _format_date(min_exp_date)
        max_exp_date = _format_date(max_exp_date)
        
        try:
            expirations = [str(expiration.get("expirations")) for expiration in self.get_list_expirations()]

            if 'mon' in freq_exp.lower():
                exp_range = pd.date_range(min_exp_date,max_exp_date,freq='WOM-3FRI')

            elif 'wee' in freq_exp.lower():
                exp_range = pd.date_range(min_exp_date, max_exp_date, freq="W-MON")
                d2 = pd.date_range(min_exp_date, max_exp_date, freq="W-WED")
                d3 = pd.date_range(min_exp_date, max_exp_date, freq="W-FRI")
                exp_range = exp_range.union(d2)
                exp_range = exp_range.union(d3)
            elif freq_exp.lower() == 'all':
                exp_range = pd.date_range(min_exp_date, max_exp_date, freq="B")

            else:
                raise ValueError("freq_exp must be Monthly or Weekly or all")
            
            if len(expirations)>0:
                desired_expirations = [exp for exp in exp_range.strftime('%Y%m%d').to_list() if exp in expirations]
            return desired_expirations
        
        except NoDataForContract:
            print(f"[+] NO EXPIRATIONS FOR {self.__str__()} - check with thetadata")
            raise NoDataForContract
        

    def get_open_expirations(self, max_exp_date: Optional[str] = None, freq_exp: str = 'monthly') -> List[str]:
        """
        Returns a list of open expiration dates given an optional maximum expiration date and
        frequency of expiration dates.

        Parameters:
        -----------
        max_exp_date : str, optional
            The maximum expiration date in '%Y%m%d' format. If not provided, all open expirations will be returned.
        freq_exp : str, default 'monthly'
            The frequency of desired expiration dates. Valid options are 'monthly' or 'weekly'.

        Raises:
        -------
        TypeError:
            If max_exp_date is not a string or if freq_exp is not a string.
        ValueError:
            If freq_exp is not 'monthly' or 'weekly'.

        Returns:
        --------
        List[str]
            A list of open expiration dates in '%Y%m%d' format that fall within the specified date range and frequency.

        Example :
        ---------


        """

        desired_open_expirations = None
        if not isinstance(freq_exp, str):
            raise TypeError("freq_exp must be a str")

        today = datetime.today().date()
        min_exp_date = today

        if max_exp_date:
            max_exp_date = _format_date(max_exp_date)
        else:
            max_exp_date = '2099-12-31'  # Arbitrary far-off future date

        try:
            expirations = [str(expiration.get("expirations")) for expiration in self.get_list_expirations()]

            if 'mon' in freq_exp.lower():
                exp_range = pd.date_range(min_exp_date, max_exp_date, freq='WOM-3FRI')

            elif 'wee' in freq_exp.lower():
                exp_range = pd.date_range(min_exp_date, max_exp_date, freq="W-MON")
                d2 = pd.date_range(min_exp_date, max_exp_date, freq="W-WED")
                d3 = pd.date_range(min_exp_date, max_exp_date, freq="W-FRI")
                exp_range = exp_range.union(d2)
                exp_range = exp_range.union(d3)
            elif freq_exp.lower() == 'all':
                exp_range = pd.date_range(min_exp_date, max_exp_date, freq="B")

            else:
                raise ValueError("freq_exp must be Monthly or Weekly or all")

            if len(expirations) > 0:
                desired_open_expirations = [exp for exp in exp_range.strftime('%Y%m%d').to_list() if exp in expirations]
            return desired_open_expirations

        except NoDataForContract:
            print(f"[+] NO EXPIRATIONS FOR {self.__str__()} - check with thetadata")
            raise NoDataForContract


    def get_desired_strikes(self, strike_multiple: int = 5, min_strike: Optional[float] = None, max_strike: Optional[float] = None) -> Optional[List[float]]:
        """
        Returns a list of strikes for the contract {root, exp} respecting the modulo `strike_multiple`.

        Parameters:
        -----------
        strike_multiple : int, default 5
            The desired multiple to use for selecting strikes.
        min_strike : float, optional
            The minimum strike to include in the results.
        max_strike : float, optional
            The maximum strike to include in the results.

        Raises:
        -------
        TypeError:
            If strike_multiple is not a positive integer.
        ValueError:
            If strike_multiple is negative.
        OptionError:
            If an expiration is not provided.

        Returns:
        --------
        Optional[List[float]]
            A list of desired strikes for the contract {root, exp} that are divisible by `strike_multiple`.
            If there is no data for the contract, return None.

        Example:
        --------
        >>> option = Option(root="AAPL", exp="20220318", right="C", strike=150)
        >>> option.get_desired_strikes(strike_multiple=5)
        [145.0, 150.0, 155.0, 160.0, 165.0 [...] 260.0] 

        """

        desired_strikes = None
        if not isinstance(strike_multiple, int):
            raise TypeError("[+] strike_multiple must be a positive integer")
        if strike_multiple < 0:
            raise ValueError("[+] strike_multiple must be a positive integer")
        if self.exp is None:
            raise OptionError("[+] An expiration is required to get the desired strikes")

        try:
            strike_multiple *= 1000
            strikes = self.get_list_strikes()

            if len(strikes) > 0:
                df = pd.DataFrame(strikes)
                df = df[df["strikes"] % strike_multiple == 0]

                if min_strike is not None:
                    df = df[df["strikes"] >= min_strike * 1000]

                if max_strike is not None:
                    df = df[df["strikes"] <= max_strike * 1000]

                desired_strikes = df.strikes.values / 1000
            return desired_strikes

        except NoDataForContract:
            print(f"[+] NO STRIKES FOR {self.__str__()} - check with thetadata")
            raise NoDataForContract
