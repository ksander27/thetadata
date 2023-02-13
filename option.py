from .utils import validate_date_format,validate_option_right,validate_option_strike,validate_ticker_format

class Option:
    def __init__(self, ticker: str, right: str, strike: float, expiry: str):
        """
        Represents an option contract with the specified ticker, right (call or put), strike price, and expiry date.

        Args:
        - ticker (str): The ticker symbol of the underlying asset.
        - right (str): The type of option (call or put).
        - strike (float): The strike price of the option.
        - expiry (str): The expiry date of the option in ISO format.

        Raises:
        - ValueError: If any of the input arguments are invalid.
        """
        self._validate_and_handle_error(ticker, right, strike, expiry)
        self.ticker = ticker
        self.right = right.lower()[0]
        self.strike = strike
        self.exp = expiry

    def _validate_and_handle_error(self, ticker: str, right: str, strike: float, expiry: str) -> None:
        """
        Validates the input arguments for the Option class and handles any errors that occur.

        Raises:
        - ValueError: If any of the input arguments are invalid.
        """
        if not validate_ticker_format(ticker):
            raise ValueError(f"Invalid ticker symbol: {ticker}")
        if not validate_date_format(expiry):
            raise ValueError(f"Invalid option expiry date format: {expiry}")
        if not validate_option_strike(strike):
            raise ValueError(f"Invalid option strike price: {strike}. Strike price must be non-negative.")
        if not validate_option_right(right):
            raise ValueError(f"Invalid option right type: {right}. Right type must be 'call', 'put', 'c', or 'p'.")

            
    