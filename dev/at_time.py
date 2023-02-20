from .utils import _format_date,_format_ivl,_format_right,_format_strike,_isDateRangeValid
from .wrapper import MyWrapper


class HistWrapper(MyWrapper):
    def __init__(self):
        super().__init__()

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
 
        
    # At time endpoints
    def get_at_time_option_quote(self,start_date,end_date,ivl,root,exp,right,strike):
        self.req_type = "quote"
        return self._get_at_time_option(start_date,end_date,ivl,root,exp,right,strike)