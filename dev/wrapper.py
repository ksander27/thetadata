from typing import Dict,Any,Union,List
import requests as rq

from .utils import _format_date,_format_right,_format_strike,_isDateRangeValid
from .utils import RootOrExpirationError,ResponseFormatError

    


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

    def _parse_header(self, resp: rq.Response) -> Union[List, int]:
        """
        This function checks the header of the response to see if there is an error.

        Args:
        resp: the response object from requests

        Returns:
        True: if there is no error

        Raises:
        Exception: if there is an HTTP error
        RootOrExpirationError: if there is a non-existent root symbol or expiration
        ResponseFormatError: if there is an error in the response

        """
        if not resp.ok:
            raise Exception(f"HTTP error {resp.status_code}")

        _ = resp.json()
        header = _.get('header')
        err_type = header.get('error_type')
        err_msg = header.get('error_msg')

        if err_type != "null":
            if "Nonexistent root symbol or expiration" in err_msg:
                raise RootOrExpirationError("Nonexistent root symbol or expiration")
            elif "No data for contract" in err_msg:
                raise ResponseFormatError("No data for contract")
            else:
                raise ResponseFormatError(err_msg)
        return True


    def _parse_response(self, resp):
        """
        This function checks the response to ensure that the format is valid and returns a list
        or a list of dictionaries, using the format from the header or in some cases, the req_type
        from the query.

        Args:
        resp: the response object from requests

        Returns:
        A list or a list of dictionaries

        Raises:
        ResponseFormatError: if there is an error in the response

        """
        _ = resp.json()
        response = _.get('response')
        _format = _.get('header').get('format')

        if response is None:
            raise ResponseFormatError("Response content is empty")
        elif not isinstance(response, list):
            raise ResponseFormatError(f"Response is {type(response)} - should be list")
        elif len(response) == 0:
            raise ResponseFormatError(f"Response is [] - check query")

        if _format is None:
            return [{self.req_type: element} for element in response]
        else:
            return [{key: element[idx] for idx, key in enumerate(_format)} for element in response]
        

    def _get_data(self) -> List[Dict[str, Any]]:
        """Helper function that sends a GET request to the API endpoint and parses the response data.

        Returns:
            A list of dictionaries, where each dictionary contains information about a specific contract.

        Raises:
            RootOrExpirationError: If the API returns an error message indicating that the provided root or expiration is nonexistent.
            ResponseFormatError: If the response format is invalid or the response content is empty.
            Exception: If the HTTP response status code is not successful.
        """
        resp = rq.get(self.url, params=self.params)
        if self._parse_header(resp):
            data = self._parse_response(resp)
            return data


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
        self.url = f"{self.base_url}/{self.call_type}/roots"

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
