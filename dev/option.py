from typing import List

from .utils import _format_date,_format_ivl,_isDateRangeValid
from .wrapper import MyWrapper

class OptionError(Exception):
    pass

class RightError(Exception):
    pass

class StrikeError(Exception):
    pass

class Option(MyWrapper):
    def __init__(self,root="SPY",exp=None,right=None,strike=None):
        super().__init__()
        self.sec_type = "option"

        self.root = root
        self.exp = exp
        self.right = right
        self.strike = strike

        self._exp = _format_date(exp)
        self._right = self._format_right()
        self._strike = self._format_strike()

    # Helper
    def _format_right(self):
        if self.right is None:
            return None
        if isinstance(self.right,str):
            if "C" in self.right.upper():
                return "C"
            elif "P" in self.right.upper():
                return "P"
            else:
                raise RightError("Right must be str - Call or Put.")
        else:
            raise RightError("Right must be str - Call or Put.")

    def _format_strike(self):
        if self.strike is None:
            return None
        if not isinstance(self.strike, int):
            raise StrikeError("Strike price must be an integer")
        if self.strike < 0:
            raise StrikeError("Strike price must be non-negative")
        return int(self.strike*1000)
    
    def _isOptionValid(self):
        if _format_date(self.exp) and self._format_right() and self._format_strike():
            return True
        else: 
            return False
        
    def _isOptionRangeValid(self,_start_date,_end_date):
        if _isDateRangeValid(_start_date,_end_date) and _isDateRangeValid(_end_date,self._exp):
            return True
        else:    
            return False
        
    def _get_hist(self,start_date,end_date,ivl):
        self.call_type = "hist"
        
        _start_date = _format_date(start_date)
        _end_date = _format_date(end_date)
        _ivl = _format_ivl(ivl)

        if not self._isOptionValid():
            raise OptionError("The parameters for the Option contract are not valid")
        if not  self._isOptionRangeValid():
            raise OptionError("The start_date end_date and expiry are not valid")
        
        self.url = f"{self.base_url}/{self.call_type}/{self.sec_type}/{self.req_type}"
        self.params = {
            "start_date":_start_date
            ,"end_date":_end_date
            ,"root":self.root
            ,"ivl": _ivl
            ,"exp":self._exp
            ,"right":self._right
            ,"strike":self._strike
        }

        return self._get_data()
        
            
    def _get_at_time_option(self,start_date,end_date,ivl):
        self.call_type = "at_time"
        
        _start_date = _format_date(start_date)
        _end_date = _format_date(end_date)
        _ivl = _format_ivl(ivl)
        
        if not self._isOptionValid():
            raise OptionError("The parameters for the Option contract are not valid")
        if not  self._isOptionRangeValid():
            raise OptionError("The start_date end_date and expiry are not valid")
    
        self.url = f"{self.base_url}/{self.call_type}/{self.sec_type}/{self.req_type}"
        self.params = {
            "start_date":_start_date
            ,"end_date":_end_date
            ,"root":self.root
            ,"ivl": _ivl
            ,"exp":self._exp
            ,"right":self._right
            ,"strike":self._strike
        }
        return self._get_data()
    
    # List endpoints
    def get_list_roots(self) -> List[str]:
        """
        Retrieves all roots for the specified security.
        
        Returns:
            A list of strings containing all roots for the specified security.
        
        Raises:
            ValueError: If sec is not 'opt' or 'stk'.
            Exception: If the server returns an error or the response content is empty.
            RootOrExpirationError: If the response data contains an integer representing a root or expiration error.
        """

        self.req_type = "roots"
        self.url = f"{self.base_url}/{self.call_type}/{self.req_type}"
        self.params = {"sec":self.sec_type.upper()}

        return self._get_data()


    def get_list_strikes(self) -> List[int]:
        """
        Retrieves a list of strike prices for a given expiration date and root symbol.

        Returns:
        List[int]: List of strike prices.

        Raises:
        RootOrExpirationError: If the provided root or expiration date is invalid.
        Exception: If the response content is empty or cannot be parsed.

        """
        self.call_type = "list"
        self.req_type = "strikes"
        _exp = _format_date(self.exp)
        
        self.url = f"{self.base_url}/{self.call_type}/{self.req_type}"
        self.params = {"exp": _exp, "root": self.root}
        return self._get_data()

    def get_list_expirations(self) -> List[str]:
        """
        Retrieves a list of all expiration dates for an underlying root.

        Returns:
        List[str]: List of expiration dates in format "YYYYMMDD".

        Raises:
        RootOrExpirationError: If the provided root symbol is invalid.
        Exception: If the response content is empty or cannot be parsed.

        """
        self.call_type = "list"
        self.req_type = "expirations"
        
        self.url = f"{self.base_url}/{self.call_type}/{self.req_type}"
        self.params = {"root": self.root}
        return self._get_data()
     
    # Hist endpoints
    def get_hist_eod(self,start_date,end_date):
        self.call_type = "hist"
        self.req_type = "eod"
        
        _start_date = _format_date(start_date)
        _end_date = _format_date(end_date)
        
        if self._isOptionValid() and self._isOptionRangeValid():
            self.url = f"{self.base_url}/{self.call_type}/{self.sec_type}/{self.req_type}"
            self.params = {
                "start_date":_start_date
                ,"end_date":_end_date
                ,"root":self.root
                ,"exp":self._exp
                ,"right":self._right
                ,"strike":self._strike
            }

            return self._get_data()
        
    def get_hist_quote(self,start_date,end_date,ivl):
        self.req_type = "quote"
        return self._get_hist(start_date,end_date,ivl)
        
    def get_hist_ohlc(self,start_date,end_date,ivl):
        self.req_type = "ohlc"
        return self._get_hist(start_date,end_date,ivl)
        
    def get_hist_trade(self,start_date,end_date,ivl):
        self.req_type = "trade"
        return self._get_hist(start_date,end_date,ivl)
    
    def get_hist_open_interest(self,start_date,end_date,ivl,root):
        self.req_type = "open_interest"
        return self._get_hist(start_date,end_date,ivl,root)
    
    def get_hist_implied_volatility(self,start_date,end_date,ivl):
        self.req_type = "implied_volatility"
        return self._get_hist(start_date,end_date,ivl)
    
    def get_hist_implied_volatility_verbose(self,start_date,end_date,ivl):
        self.req_type = "implied_volatility_verbose"
        return self._get_hist(start_date,end_date,ivl)
    
    def get_hist_greeks(self,start_date,end_date,ivl):
        self.req_type = "greeks"
        return self._get_hist(start_date,end_date,ivl)
    
    def get_hist_trade_greeks(self,start_date,end_date,ivl):
        self.req_type = "trade_greeks"
        return self._get_hist(start_date,end_date,ivl)
    
    def get_hist_greeks_second_order(self,start_date,end_date,ivl):
        self.req_type = "greeks_second_order"
        return self._get_hist(start_date,end_date,ivl)
    
    def get_hist_greeks_third_order(self,start_date,end_date,ivl):
        self.req_type = "greeks_third_order"
        return self._get_hist(start_date,end_date,ivl)
    
    def get_hist_trade_quote(self,start_date,end_date,ivl):
        self.req_type = "trade_quote"
        return self._get_hist(start_date,end_date,ivl)
    
    def get_hist_eod_quote_greeks(self,start_date,end_date,ivl):
        self.req_type = "eod_quote_greeks"
        return self._get_hist(start_date,end_date,ivl)
    
    # At time endpoints
    def get_at_time_quote(self,start_date,end_date,ivl):
        self.req_type = "quote"
        return self._get_at_time(start_date,end_date,ivl)
    



