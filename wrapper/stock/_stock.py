from typing import List
from ..utils import _format_date,_format_ivl,_isDateRangeValid
from ..wrapper import MyWrapper


class _Stock(MyWrapper):
    def __init__(self,root,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.sec_type = "stock"
        self.root = root
    
    # Call endpoint helpers
    def _get_list_dates(self):
        self.call_type = "list"
        
        self.url = f"{self.base_url}/{self.call_type}/dates/{self.sec_type}/{self.req_type}"
        self.params = {"root":self.root}
        return self._get_data()
    
    def _get_hist(self,start_date,end_date,ivl):
        self.call_type = "hist"
        
        _start_date = _format_date(start_date)
        _end_date = _format_date(end_date)
        _ivl = _format_ivl(ivl)
        
        self.url = f"{self.base_url}/{self.call_type}/{self.sec_type}/{self.req_type}"
        if _isDateRangeValid(_start_date,_end_date):
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

        if _isDateRangeValid(_start_date,_end_date):
            self.params = {
                "start_date":_start_date
                ,"end_date":_end_date
                ,"root":self.root
                ,"ivl": _ivl
            }
            return self._get_data()
    
    # List endpoints
    def get_list_expirations_trade(self) -> List[str]:
        self.req_type = "trade"
        return self._get_list_dates()
    
    def get_list_expirations_quote(self) -> List[str]:
        self.req_type = "quote"
        return self._get_list_dates()
    
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
    
    # hist endpoints
    def get_hist_quote(self,start_date,end_date,ivl):
        self.req_type = "quote"
        return self._get_hist(start_date,end_date,ivl)
                
    def get_hist_trade(self,start_date,end_date,ivl):
        self.req_type = "trade"
        return self._get_hist(start_date,end_date,ivl)
    
    def get_hist_ohlc(self,start_date,end_date,ivl):
        self.req_type = "ohlc"
        return self._get_hist(start_date,end_date,ivl)
    
    def get_hist_eod(self,start_date,end_date):
        ivl = 3600
        self.req_type = "eod"
        return self._get_hist(start_date,end_date,ivl)
    
    # at_times endpoint
    def get_at_time_quote(self,start_date,end_date,ivl):
        self.req_type = "quote"
        return self._get_at_time(start_date,end_date,ivl)
                
    def get_at_time_trade(self,start_date,end_date,ivl):
        self.req_type = "trade"
        return self._get_at_time(start_date,end_date,ivl)
    
    def get_at_time_ohlc(self,start_date,end_date,ivl):
        self.req_type = "ohlc"
        return self._get_at_time(start_date,end_date,ivl)
    
    # def get_at_time_eod(self,start_date,end_date,ivl):
    #     self.req_type = "eod"
    #     return self._get_at_time(start_date,end_date,ivl)