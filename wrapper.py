from thetadata import ThetaClient, DateRange, StockReqType, OptionReqType, OptionRight
import pandas as pd 
import pandas_market_calendars as mcal
import logging
from typing import Optional

import re
import datetime
from datetime import date
import dateparser

from .option import Option
from .utils import validate_date_format,validate_req,validate_ticker_format,validate_interval

class ThetaWrapper(ThetaClient):
    """
    A wrapper around the Theta API client that provides additional functionality and error handling.

    Args:
    - kwargs: Additional arguments to pass to the Theta API client.

    Attributes:
    - logger: A logger object for logging Theta API requests and responses.
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.handler = logging.StreamHandler()
        self.handler.setLevel(logging.DEBUG)
        self.formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.handler.setFormatter(self.formatter)
        self.logger.addHandler(self.handler)

    def get_historical_stock(self, ticker: str, start: str, end:Optional[str] = None, interval:Optional[str] = None) -> pd.DataFrame:
        """
        Retrieves end-of-day stock prices for the specified ticker between the specified start and end dates.
        
        Args:
        - ticker (str): The ticker symbol of the stock to retrieve prices for.
        - start (str): The start date in ISO format (e.g. '2022-02-01').
        - end (str): The end date in ISO format (e.g. '2022-02-28').
        - interval (int): The interval through which the price are retrieved for.
        
        Returns:
        - A pandas DataFrame with columns 'date' and 'close' representing the date and closing price of the stock on each day.
        """
        try:
            self._validate_and_handle_error(ticker=ticker, start=start, end=end, interval=interval,req="OHLC")
            # Parse input arguments
            _start: date = dateparser.parse(start).date()
            _end: date = self._get_end(end)
            # Retrieve stock data from API

            with self.connect():
                if interval is not None:
                    out = self.get_hist_stock(
                        req=StockReqType.OHLC,
                        root=ticker,
                        date_range=DateRange(_start, _end),
                        interval_size=interval
                    )
                else:
                    out = self.get_hist_stock(
                        req=StockReqType.EOD,
                        root=ticker,
                        date_range=DateRange(_start, _end)
                    )
            # Convert stock data to DataFrame and return
            return out
        except Exception as e:
            self.logger.error(f"Failed to retrieve end-of-day stock prices for {ticker} between {start} and {end}: {e}")
            raise
            

    def get_historical_option_ohlc(self, option: Option, start: str, end:Optional[str] = None, interval: Optional[int] = None ) -> pd.DataFrame:
        if interval:
            return self._get_historical_option("OHLC",option,start,end,interval)
        else:
            return self._get_EOD_option("OHLC",option,start,end)
    
    def get_historical_option_ohlc_quote(self, option: Option, start: str, end:Optional[str] = None, interval: Optional[int] = None ) -> pd.DataFrame:
        if interval:
            return self._get_historical_option("OHLC_QUOTE",option,start,end,interval)
        else:
            return self._get_EOD_option("OHLC_QUOTE",option,start,end)
    
    def get_historical_option_greeks(self, option: Option, start: str, end:Optional[str] = None, interval: Optional[int] = None ) -> pd.DataFrame:
        if interval:
            return self._get_historical_option("GREEKS",option,start,end,interval)
        else:
            return self._get_EOD_option("GREEKS",option,start,end)

    def get_historical_option_greeks_second_order(self, option: Option, start: str, end:Optional[str] = None, interval: Optional[int] = None ) -> pd.DataFrame:
        if interval:
            return self._get_historical_option("GREEKS_SECOND_ORDER",option,start,end,interval)
        else:
            return self._get_EOD_option("GREEKS_SECOND_ORDER",option,start,end)
    
    def get_historical_option_greeks_third_order(self, option: Option, start: str, end:Optional[str] = None, interval: Optional[int] = None ) -> pd.DataFrame:
        if interval:
            return self._get_historical_option("GREEKS_THIRD_ORDER",option,start,end,interval)
        else:
            return self._get_EOD_option("GREEKS_THIRD_ORDER",option,start,end)
    
    def get_historical_option_implied_volatility(self, option: Option, start: str, end:Optional[str] = None, interval: Optional[int] = None ) -> pd.DataFrame:
        if interval:
            return self._get_historical_option("IMPLIED_VOLATILITY",option,start,end,interval)
        else:
            return self._get_EOD_option("IMPLIED_VOLATILITY",option,start,end)
    
    def get_historical_option_liquidity(self, option: Option, start: str, end:Optional[str] = None, interval: Optional[int] = None ) -> pd.DataFrame:
        if interval:
            return self._get_historical_option("LIQUIDITY",option,start,end,interval)
        else:
            return self._get_EOD_option("LIQUIDITY",option,start,end)
    
    def get_historical_option_quote(self, option: Option, start: str, end:Optional[str] = None,interval: Optional[int] = None ) -> pd.DataFrame:
        if interval:
            return self._get_historical_option("QUOTE",option,start,end,interval)
        else:
            return self._get_EOD_option("QUOTE",option,start,end)
    
    def get_historical_option_trade(self, option: Option, start: str, end:Optional[str] = None, interval: Optional[int] = None ) -> pd.DataFrame:
        if interval:
            return self._get_historical_option("TRADE",option,start,end,interval)
        else:
            return self._get_EOD_option("TRADE",option,start,end)
    
    def get_historical_option_trade_greeks(self, option: Option, start: str, end:Optional[str] = None, interval: Optional[int] = None) -> pd.DataFrame:
        if interval:
            return self._get_historical_option("TRADE_GREEKS",option,start,end,interval)
        else:
            return self._get_EOD_option("TRADE_GREEKS",option,start,end)
    
    def get_historical_option_volume(self, option: Option, start: str, end:Optional[str] = None, interval: Optional[int] = None ) -> pd.DataFrame:
        if interval:
            return self._get_historical_option("VOLUME",option,start,end,interval)
        else:
            return self._get_EOD_option("VOLUME",option,start,end)
        
    def get_EOD_option(self, option: Option, start: str, end:Optional[str] = None ):
        return self._get_EOD_option("EOD",option,start,end)
    
    def get_EOD_open_interest(self, option: Option, start: str, end:Optional[str] = None ):
        return self._get_EOD_option("OPEN_INTEREST",option,start,end)


    def _get_historical_option(self, req : str , option: Option, start: str, end:Optional[str] = None
                               , interval:Optional[int] = None,) -> pd.DataFrame:
        """
        Retrieves end-of-day option prices for the specified option contract between the specified start and end dates.

        Args:
        - option (Option): The option contract to retrieve prices for.
        - start (str): The start date in ISO format (e.g. '2022-02-01').
        - end (str): The end date in ISO format (e.g. '2022-02-28'). If None, uses the last trading day.
        - interval (int): The interval through which the price are retrieved for.

        Returns:
        - A pandas DataFrame with columns 'date', 'strike', 'right', and 'close' representing the date, strike price, right type (call or put), and closing price of the option on each day.
        """
        try:
            self._validate_and_handle_error(req=req, ticker= option.ticker, start=start, end=end, interval=interval)
            # Parse input arguments
            _start: date = dateparser.parse(start).date()
            _end: date = self._get_end(end)
            _exp: date = dateparser.parse(option.exp).date()
            _right: str = option.right.lower()[0]
            _req = self._get_req(req)
            
            # Retrieve option data from API
            with self.connect():
                out = self.get_hist_option(
                    req=OptionReqType.OHLC,
                    root=option.ticker,
                    exp=_exp,
                    strike=option.strike,
                    right=OptionRight.CALL if _right == "c" else OptionRight.PUT,
                    date_range=DateRange(_start, _end),
                    interval_size =interval
                )

            return out
        except Exception as e:
            self.logger.error(f"Failed to retrieve end-of-day option prices for {option} "
                              f"between {start} and {end}: {e}")
            raise

    def _get_EOD_option(self, req : str , option: Option, start: str, end:Optional[str] = None) -> pd.DataFrame:
        """
        Retrieves end-of-day option prices for the specified option contract between the specified start and end dates.

        Args:
        - option (Option): The option contract to retrieve prices for.
        - start (str): The start date in ISO format (e.g. '2022-02-01').
        - end (str): The end date in ISO format (e.g. '2022-02-28'). If None, uses the last trading day.
        - interval (int): The interval through which the price are retrieved for.

        Returns:
        - A pandas DataFrame with columns 'date', 'strike', 'right', and 'close' representing the date, strike price, right type (call or put), and closing price of the option on each day.
        """
        try:
            self._validate_and_handle_error(req=req,ticker=None, start=start, end=end)
            # Parse input arguments
            _start: date = dateparser.parse(start).date()
            _end: date = self._get_end(end)
            _exp: date = dateparser.parse(option.exp).date()
            _right: str = option.right.lower()[0]
            _req = self._get_req(req)
            
            # Retrieve option data from API
            with self.connect():
                out = self.get_hist_option(
                    req=OptionReqType.EOD,
                    root=option.ticker,
                    exp=_exp,
                    strike=option.strike,
                    right=OptionRight.CALL if _right == "c" else OptionRight.PUT,
                    date_range=DateRange(_start, _end),
                )

            return out
        except Exception as e:
            self.logger.error(f"Failed to retrieve end-of-day option prices for {option} "
                              f"between {start} and {end}: {e}")
            raise

    def _get_req(self,req):
        if req == "OHLC":
            return OptionReqType.OHLC
        elif req == "OHLC_QUOTE":
            return OptionReqType.OHLC_QUOTE
        elif req == "GREEKS":
            return OptionReqType.GREEKS
        elif req == "GREEKS_SECOND_ORDER":
            return OptionReqType.GREEKS_SECOND_ORDER
        elif req == "GREEKS_THIRD_ORDER":
            return OptionReqType.GREEKS_THIRD_ORDER
        elif req == "IMPLIED_VOLATILITY":
            return OptionReqType.IMPLIED_VOLATILITY
        elif req == "LIQUIDITY":
            return OptionReqType.LIQUIDITY
        elif req == "QUOTE":
            return OptionReqType.QUOTE
        elif req == "TRADE":
            return OptionReqType.TRADE
        elif req == "TRADE_GREEKS":
            return OptionReqType.TRADE_GREEKS
        elif req == "EOD":
            return OptionReqType.EOD
        elif req == "OPEN_INTEREST":
            return OptionReqType.OPEN_INTEREST
        elif req == "VOLUME":
            return OptionReqType.VOLUME


            
    def _get_end(self,end: Optional[str]) -> datetime.date:
        if not end:
            end = datetime.datetime.now().strftime("%Y-%m-%d")
        else:
            end = dateparser.parse(end).date()

        # Check if the end date is a weekend or holiday
        nyse = mcal.get_calendar('NYSE')
        while not nyse.valid_days(start_date=end, end_date=end).size >0:
            end -= datetime.timedelta(days=1)

        return end

    def _validate_and_handle_error(self, ticker: Optional[str], start: str, req: str, end: Optional[str] = None, interval: Optional[int] = None, greek: Optional[int] = None) -> None:
            """
            Validates the input arguments for retrieving historical stock or option data and handles any errors that occur.

            Args:
            - ticker (str): The ticker symbol of the stock to retrieve prices for.
            - start (str): The start date in ISO format (e.g. '2022-02-01').
            - req (str): The request type (e.g. 'OHLC', 'OHLC_QUOTE', 'GREEKS', 'IMPLIED_VOLATILITY', etc.).
            - end (str): The end date in ISO format (e.g. '2022-02-28').
            - interval (int): The interval through which the price are retrieved for.
            - greek (int): The type of greek to retrieve (e.g. OptionGreek.DELTA, OptionGreek.GAMMA, OptionGreek.VEGA, OptionGreek.THETA, OptionGreek.RHO).

            Raises:
            - ValueError: If any of the input arguments are invalid.
            """
            if ticker is not None and not validate_ticker_format(ticker):
                raise ValueError(f"Invalid ticker symbol: {ticker}")
            if not validate_date_format(start):
                raise ValueError(f"Invalid start date format: {start}")
            if end is not None and not validate_date_format(end):
                raise ValueError(f"Invalid end date format: {end}")
            if end is not None and dateparser.parse(start).date() >= dateparser.parse(end).date():
                raise ValueError("Start date must be earlier than end date")
            if interval is not None and not validate_interval(interval):
                raise ValueError(f"Invalid interval format: {interval}. Expected format: '{{seconds}}_{{milliseconds}}' and must be greater than 0.")
            if not validate_req(req):
                raise ValueError(f"Invalid req format: {req}. Expected valid property from OptionReqType ")
