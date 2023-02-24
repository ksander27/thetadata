from typing import List
from .utils import _format_date,_format_ivl,_isDateRangeValid
from .wrapper import MyWrapper


class Stock(MyWrapper):
    def __init__(self,root):
        super().__init__()
        self.sec_type = "stock"
        self.root = root

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
    
    def _get_hist(self,start_date,end_date,ivl):
        self.call_type = "hist"
        
        _start_date = _format_date(start_date)
        _end_date = _format_date(end_date)
        _ivl = _format_ivl(ivl)
        
        self.url = f"{self.base_url}/{self.call_type}/{self.sec_type}/{self.req_type}"
        self.params = {
            "start_date":_start_date
            ,"end_date":_end_date
            ,"root":self.root
            ,"ivl": _ivl
        }
        return self._get_data()
    
    def _get_at_time(self,start_date,end_date,ivl):
        self.call_type = "at_time"
        
        _start_date = _format_date(start_date)
        _end_date = _format_date(end_date)
        _ivl = _format_ivl(ivl)
        
    
        self.url = f"{self.base_url}/{self.call_type}/{self.sec_type}/{self.req_type}"
        self.params = {
            "start_date":_start_date
            ,"end_date":_end_date
            ,"root":self.root
            ,"ivl": _ivl
        }
        return self._get_data()