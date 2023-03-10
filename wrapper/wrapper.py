from typing import Dict,Any,Union,List
import requests as rq
from .utils import ResponseFormatError

class RootOrExpirationError(Exception):
    pass

class NoDataForContract(Exception):
    pass

class MyWrapper:
    def __init__(self,_async=False):
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
        self._async = _async

    def _isRequestOkay(self):
        if not self._async:
            if self.request.ok:
                # If the request was successful, return True
                return True
            else:
                # If the request was not successful, raise an exception with the corresponding status code
                raise Exception(f"HTTP error {self.request.status_code}")
        else:
            if self.request.status == 200:
                # If the status code indicates success, return True
                return True
            else:
                # If the status code indicates an error, raise an exception with the corresponding status code
                raise Exception(f"HTTP error {self.request.status_code}")
            

    def _isResponseOkay(self):
        if not self.response:
            raise NoDataForContract("Response content is empty")
        elif not isinstance(self.response, list):
            raise ResponseFormatError(f"Response is {type(self.response)} - should be list")
        elif len(self.response) == 0:
            raise ResponseFormatError(f"Response is [] - check query")
        return True


        
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

        if not self._async:
            _ = self.request.json()
            self.header = _.get('header')

        err_type = self.header.get('error_type')
        err_msg = self.header.get('error_msg')

        if err_type != "null":
            if "Nonexistent root symbol or expiration" in err_msg:
                raise RootOrExpirationError(f"[+] Error code - {err_type} : {err_msg}")
            elif "No data for the specified timeframe & contract" in err_msg:
                raise NoDataForContract(f"[+] Error code - {err_type} : {err_msg}")
        return True
    
    def _parse_data(self):
        if self.format is None:
            return [{self.req_type: element} for element in self.response]
        else:
            return [{key: element[idx] for idx, key in enumerate(self.format)} for element in self.response]

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
        if not self._async:
            _ = self.request.json()
            self.response = _.get('response')

        self.format = self.header.get('format')
        if self._isResponseOkay():
            return self._parse_data()


    def _get_data(self) -> List[Dict[str, Any]]:
        """Helper function that sends a GET request to the API endpoint and parses the response data.

        Returns:
            A list of dictionaries, where each dictionary contains information about a specific contract.

        Raises:
            RootOrExpirationError: If the API returns an error message indicating that the provided root or expiration is nonexistent.
            ResponseFormatError: If the response format is invalid or the response content is empty.
            Exception: If the HTTP response status code is not successful.
        """
        
        if self._async :
            return {
                "url":self.url
                ,"params":self.params
                }

        else:
            self.request = rq.get(self.url, params=self.params) 
            if self._isRequestOkay() and self._parse_header():                
                data = self._parse_response()
                return data