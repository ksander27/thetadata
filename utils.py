import dateparser
import re

def validate_date_format(date_str: str) -> bool:
    """
    Validates whether a string represents a valid date.

    Args:
    - date_str (str): The date string to validate.

    Returns:
    - bool: Whether the string represents a valid date.
    """
    try:
        dateparser.parse(date_str)
        return True
    except Exception:
        return False


def validate_ticker_format(ticker: str) -> bool:
    """
    Validates whether a string represents a valid stock ticker symbol.

    Args:
    - ticker (str): The ticker symbol to validate.

    Returns:
    - bool: Whether the string represents a valid ticker symbol.
    """
    return bool(re.match(r'^[A-Z]+$', ticker))


def validate_option_strike(strike: float) -> bool:
    """
    Validates whether a value represents a valid option strike price.

    Args:
    - strike (float): The strike price to validate.

    Returns:
    - bool: Whether the value represents a valid option strike price.
    """
    return strike >= 0


def validate_option_right(right: str) -> bool:
    """
    Validates whether a string represents a valid option right.

    Args:
    - right (str): The option right to validate.

    Returns:
    - bool: Whether the string represents a valid option right.
    """
    return right.lower() in ['call', 'put', 'c', 'p']


def validate_date_format(date_str: str) -> bool:
    """
    Validates whether a string represents a valid date.

    Args:
    - date_str (str): The date string to validate.

    Returns:
    - bool: Whether the string represents a valid date.
    """
    try:
        dateparser.parse(date_str)
        return True
    except Exception:
        return False


def validate_ticker_format(ticker: str) -> bool:
    """
    Validates whether a string represents a valid stock ticker symbol.

    Args:
    - ticker (str): The ticker symbol to validate.

    Returns:
    - bool: Whether the string represents a valid ticker symbol.
    """
    return bool(re.match(r'^[A-Z]+$', ticker))


def validate_option_strike(strike: float) -> bool:
    """
    Validates whether a value represents a valid option strike price.

    Args:
    - strike (float): The strike price to validate.

    Returns:
    - bool: Whether the value represents a valid option strike price.
    """
    return strike >= 0


def validate_option_right(right: str) -> bool:
    """
    Validates whether a string represents a valid option right.

    Args:
    - right (str): The option right to validate.

    Returns:
    - bool: Whether the string represents a valid option right.
    """
    return right.lower() in ['call', 'put', 'c', 'p']


def validate_interval(interval: int) -> bool:
    """
    Validates the format of the interval argument.

    Args:
    - interval (int): The interval to validate, in milliseconds.

    Returns:
    - bool: True if the interval is a positive integer, False otherwise.
    """
    if not isinstance(interval, int) or interval < 1000:
        return False
    return True

def validate_req(req: str) -> bool:
    """
    Validates the format of the req argument.

    Args:
    - req (str): The type of request we want to make to the API.

    Returns:
    - bool: True if the req is a valid type of request, False otherwise.
    """
    if req not in ["OHLC","OHLC_QUOTE","GREEKS","GREEKS_SECOND_ORDER","GREEKS_THIRD_ORDER","LIQUIDITY"
                  ,"QUOTE","TRADE","TRADE_GREEKS","EOD","OPEN_INTEREST","VOLUME"]:
        return False
    return True
