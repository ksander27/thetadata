from .utils import _format_date,_format_ivl,_format_right,_format_strike,_isDateRangeValid
from .wrapper import MyWrapper


class HistWrapper(MyWrapper):
    def __init__(self):
        super().__init__()
    
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