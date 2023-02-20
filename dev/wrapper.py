from typing import Dict,Any,Union,List
import requests as rq

from .utils import _format_date,_format_right,_format_strike,_isDateRangeValid,_format_ivl
from .utils import RootOrExpirationError,ResponseFormatError

from typing import Dict,Any,Union,List
import requests as rq

class MyWrapper:
    def __init__(self):
        """
        Initializes the MyWrapper class with the base url and call type.
        """
        self.base_url = "http://localhost:25510"
        self.call_type = None
        self.sec_type = None
        self.req_type = None
        
        self.url = None
        self.params = None
        self.request = None
        self.header = None
        self.response = None
        
        self.format = None
        


    def _parse_header(self) -> Union[List, int]:
        """
        This function checks the header of the response to see if there is an error.


        Returns:
        True: if there is no error

        Raises:
        Exception: if there is an HTTP error
        RootOrExpirationError: if there is a non-existent root symbol or expiration
        ResponseFormatError: if there is an error in the response

        """        
        if not self.request.ok :
            raise Exception(f"HTTP error {self.request.status_code}")

        _ = self.request.json()
        
        self.header = _.get('header')
        err_type = self.header.get('error_type')
        err_msg = self.header.get('error_msg')

        if err_type != "null":
            if "Nonexistent root symbol or expiration" in err_msg:
                raise RootOrExpirationError("Nonexistent root symbol or expiration")
            elif "No data for contract" in err_msg:
                raise ResponseFormatError("No data for contract")
            else:
                raise ResponseFormatError(err_msg)
        return True


    def _parse_response(self):
        """
        This function checks the response to ensure that the format is valid and returns a list
        or a list of dictionaries, using the format from the header or in some cases, the req_type
        from the query.

        Returns:
        A list or a list of dictionaries

        Raises:
        ResponseFormatError: if there is an error in the response

        """
        _ = self.request.json()
        self.response = _.get('response')
        self.format = self.header.get('format')

        if not self.response:
            raise ResponseFormatError("Response content is empty")
        elif not isinstance(self.response, list):
            raise ResponseFormatError(f"Response is {type(self.response)} - should be list")
        elif len(self.response) == 0:
            raise ResponseFormatError(f"Response is [] - check query")

        if self.format is None:
            return [{self.req_type: element} for element in self.response]
        else:
            return [{key: element[idx] for idx, key in enumerate(self.format)} for element in self.response]
        

    def _get_data(self) -> List[Dict[str, Any]]:
        """Helper function that sends a GET request to the API endpoint and parses the response data.

        Returns:
            A list of dictionaries, where each dictionary contains information about a specific contract.

        Raises:
            RootOrExpirationError: If the API returns an error message indicating that the provided root or expiration is nonexistent.
            ResponseFormatError: If the response format is invalid or the response content is empty.
            Exception: If the HTTP response status code is not successful.
        """
        
        self.request = rq.get(self.url, params=self.params) 
        if self._parse_header():
            data = self._parse_response()
            return data


    def _get_at_time_option(self,start_date,end_date,ivl,root,exp,right,strike):
        self.call_type = "at_time"
        self.sec_type = "option"

        _start_date = _format_date(start_date)
        _end_date = _format_date(end_date)
        _ivl = _format_ivl(ivl)
        _exp = _format_date(exp)
        _right = _format_right(right)
        _strike = _format_strike(strike)
        
        if _isDateRangeValid(_start_date,_end_date):
            self.url = f"{self.base_url}/{self.call_type}/{self.sec_type}/{self.req_type}"
            self.params = {
                "start_date":_start_date
                ,"end_date":_end_date
                ,"root":root
                ,"ivl": _ivl
                ,"exp":_exp
                ,"right":_right
                ,"strike":_strike
            }

            return self._get_data()
        

    def _get_hist_option(self,start_date,end_date,ivl,root,exp,right,strike):
        self.call_type = "hist"
        self.sec_type = "option"

        _start_date = _format_date(start_date)
        _end_date = _format_date(end_date)
        _ivl = _format_ivl(ivl)
        _exp = _format_date(exp)
        _right = _format_right(right)
        _strike = _format_strike(strike)
        
        if _isDateRangeValid(_start_date,_end_date):
            self.url = f"{self.base_url}/{self.call_type}/{self.sec_type}/{self.req_type}"
            self.params = {
                "start_date":_start_date
                ,"end_date":_end_date
                ,"root":root
                ,"ivl": _ivl
                ,"exp":_exp
                ,"right":_right
                ,"strike":_strike
            }

            return self._get_data()

    # List functions
    def get_list_roots(self, sec: str) -> List[str]:
        """
        Retrieves all roots for the specified security.

        Args:
            sec: A string representing the security to retrieve roots for. Must be either 'opt' or 'stk'.
        
        Returns:
            A list of strings containing all roots for the specified security.
        
        Raises:
            ValueError: If sec is not 'opt' or 'stk'.
            Exception: If the server returns an error or the response content is empty.
            RootOrExpirationError: If the response data contains an integer representing a root or expiration error.
        """
        self.call_type = "list"
        self.req_type = "roots"
        self.url = f"{self.base_url}/{self.call_type}/{self.req_type}"

        if sec == "opt":
            self.params = {"sec":"OPTION"}
        elif sec == "stk":
            self.params = {"sec":"STOCK"}
        else:
            raise ValueError("Security must be OPTION or STOCK.")
        
        return self._get_data()


    def get_list_strikes(self, exp: str, root: str) -> List[int]:
        """
        Retrieves a list of strike prices for a given expiration date and root symbol.

        Parameters:
        exp (str): Expiration date in format "YYYYMMDD".
        root (str): Root symbol for the underlying security.

        Returns:
        List[int]: List of strike prices.

        Raises:
        RootOrExpirationError: If the provided root or expiration date is invalid.
        Exception: If the response content is empty or cannot be parsed.

        """
        self.call_type = "list"
        self.req_type = "strikes"
        _exp = _format_date(exp)
        
        self.url = f"{self.base_url}/{self.call_type}/{self.req_type}"
        self.params = {"exp": _exp, "root": root}
        return self._get_data()

    def get_list_expirations(self, root: str) -> List[str]:
        """
        Retrieves a list of all expiration dates for an underlying root.

        Parameters:
        root (str): Root symbol for the underlying security.

        Returns:
        List[str]: List of expiration dates in format "YYYYMMDD".

        Raises:
        RootOrExpirationError: If the provided root symbol is invalid.
        Exception: If the response content is empty or cannot be parsed.

        """
        self.call_type = "list"
        self.req_type = "expirations"
        
        self.url = f"{self.base_url}/{self.call_type}/{self.req_type}"
        self.params = {"root": root}
        return self._get_data()


    def _get_at_time_option(self,start_date,end_date,ivl,root,exp,right,strike):
        self.call_type = "at_time"
        self.sec_type = "option"

        _start_date = _format_date(start_date)
        _end_date = _format_date(end_date)
        _ivl = _format_ivl(ivl)
        _exp = _format_date(exp)
        _right = _format_right(right)
        _strike = _format_strike(strike)
        
        if _isDateRangeValid(_start_date,_end_date):
            self.url = f"{self.base_url}/{self.call_type}/{self.sec_type}/{self.req_type}"
            self.params = {
                "start_date":_start_date
                ,"end_date":_end_date
                ,"root":root
                ,"ivl": _ivl
                ,"exp":_exp
                ,"right":_right
                ,"strike":_strike
            }

            return self._get_data()
        
    def _get_hist_option(self,start_date,end_date,ivl,root,exp,right,strike):
        self.call_type = "hist"
        self.sec_type = "option"

        _start_date = _format_date(start_date)
        _end_date = _format_date(end_date)
        _ivl = _format_ivl(ivl)
        _exp = _format_date(exp)
        _right = _format_right(right)
        _strike = _format_strike(strike)
        
        if _isDateRangeValid(_start_date,_end_date):
            self.url = f"{self.base_url}/{self.call_type}/{self.sec_type}/{self.req_type}"
            self.params = {
                "start_date":_start_date
                ,"end_date":_end_date
                ,"root":root
                ,"ivl": _ivl
                ,"exp":_exp
                ,"right":_right
                ,"strike":_strike
            }

            return self._get_data()
     
    # Hist endpoints
    def get_hist_option_eod(self,start_date,end_date,root,exp,right,strike):
        self.call_type = "hist"
        self.sec_type = "option"
        self.req_type = "eod"
        
        _start_date = _format_date(start_date)
        _end_date = _format_date(end_date)
        _exp = _format_date(exp)
        _right = _format_right(right)
        _strike = _format_strike(strike)
        
        if _isDateRangeValid(_start_date,_end_date):
            self.url = f"{self.base_url}/{self.call_type}/{self.sec_type}/{self.req_type}"
            self.params = {
                "start_date":_start_date
                ,"end_date":_end_date
                ,"root":root
                ,"exp":_exp
                ,"right":_right
                ,"strike":_strike
            }

            return self._get_data()
        
    def get_hist_option_quote(self,start_date,end_date,ivl,root,exp,right,strike):
        self.req_type = "quote"
        return self._get_hist_option(start_date,end_date,ivl,root,exp,right,strike)
        
    def get_hist_option_ohlc(self,start_date,end_date,ivl,root,exp,right,strike):
        self.req_type = "ohlc"
        return self._get_hist_option(start_date,end_date,ivl,root,exp,right,strike)
        
    def get_hist_option_trade(self,start_date,end_date,ivl,root,exp,right,strike):
        self.req_type = "trade"
        return self._get_hist_option(start_date,end_date,ivl,root,exp,right,strike)
    
    def get_hist_option_open_interest(self,start_date,end_date,ivl,root,exp,right,strike):
        self.req_type = "open_interest"
        return self._get_hist_option(start_date,end_date,ivl,root,exp,right,strike)
    
    def get_hist_option_implied_volatility(self,start_date,end_date,ivl,root,exp,right,strike):
        self.req_type = "implied_volatility"
        return self._get_hist_option(start_date,end_date,ivl,root,exp,right,strike)
    
    def get_hist_option_implied_volatility_verbose(self,start_date,end_date,ivl,root,exp,right,strike):
        self.req_type = "implied_volatility_verbose"
        return self._get_hist_option(start_date,end_date,ivl,root,exp,right,strike)
    
    def get_hist_option_greeks(self,start_date,end_date,ivl,root,exp,right,strike):
        self.req_type = "greeks"
        return self._get_hist_option(start_date,end_date,ivl,root,exp,right,strike)
    
    def get_hist_option_trade_greeks(self,start_date,end_date,ivl,root,exp,right,strike):
        self.req_type = "trade_greeks"
        return self._get_hist_option(start_date,end_date,ivl,root,exp,right,strike)
    
    def get_hist_option_greeks_second_order(self,start_date,end_date,ivl,root,exp,right,strike):
        self.req_type = "greeks_second_order"
        return self._get_hist_option(start_date,end_date,ivl,root,exp,right,strike)
    
    def get_hist_option_greeks_third_order(self,start_date,end_date,ivl,root,exp,right,strike):
        self.req_type = "greeks_third_order"
        return self._get_hist_option(start_date,end_date,ivl,root,exp,right,strike)
    
    def get_hist_option_trade_quote(self,start_date,end_date,ivl,root,exp,right,strike):
        self.req_type = "trade_quote"
        return self._get_hist_option(start_date,end_date,ivl,root,exp,right,strike)
    
    def get_hist_option_eod_quote_greeks(self,start_date,end_date,ivl,root,exp,right,strike):
        self.req_type = "eod_quote_greeks"
        return self._get_hist_option(start_date,end_date,ivl,root,exp,right,strike)
        
    # At time endpoints
    def get_at_time_option_quote(self,start_date,end_date,ivl,root,exp,right,strike):
        self.req_type = "quote"
        return self._get_at_time_option(start_date,end_date,ivl,root,exp,right,strike)