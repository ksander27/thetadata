import datetime as dt
import dateparser



class ResponseFormatError(Exception):
    pass

class IVLError(Exception):
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
            raise ValueError("Date format isn't correct and (ideally) should be YYYYMMDD - currently {date}")
        else:
            return dt.datetime.strftime(_date, "%Y%m%d")

def _isDateRangeValid(start_date,end_date):
    if not dt.datetime.strptime(start_date,"%Y%m%d") <= dt.datetime.strptime(end_date,"%Y%m%d"):
        raise ValueError("end_date must be greater than start_date")
    else:
        return True
    
def _format_ivl(ivl):
    if not isinstance(ivl,int):
        raise IVLError("Interval must be an integer")
    if ivl < 0:
        raise IVLError("Interval must be non-negative")
    return int(ivl*1000)
