import datetime as dt
import dateparser

class RootOrExpirationError(Exception):
    pass

class RightError(Exception):
    pass

class StrikeError(Exception):
    pass

class ResponseFormatError(Exception):
    pass

def _format_date(date: str) -> str:
    """
    Helper function to format date as "YYYYMMDD" if not already formatted.

    Args:
        date (str): The date string to format.

    Returns:
        str: The formatted date string in the format "YYYYMMDD".
    """
    try:
        # Check if the time is already in the expected format
        dt.datetime.strptime(date, '%Y%m%d')
        return date
    except ValueError:
        # Use dateparser to parse the time
        _date = dateparser.parse(date)
        if not _date :
            raise ValueError("Date format isn't correct and (ideally) should be YYYYMMDD")
        else:
            return dt.datetime.strftime(_date, "%Y%m%d")


def _format_right(right):
    if isinstance(right,str):
        if "C" in right.upper():
            return "C"
        elif "P" in right.upper():
            return "P"
        else:
            raise RightError("Right must be str - Call or Put.")
    else:
        raise RightError("Right must be str - Call or Put.")
        
        
def _format_strike(strike):
    if not isinstance(strike, int):
        raise StrikeError("Strike price must be an integer")
    if strike < 0:
        raise StrikeError("Strike price must be non-negative")
    return int(strike*1000)

def _isDateRangeValid(start_date,end_date):
    if not dt.datetime.strptime(start_date,"%Y%m%d") < dt.datetime.strptime(end_date,"%Y%m%d"):
        raise ValueError("end_date must be greater than start_date")
    else:
        return True
