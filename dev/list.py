from typing import List
from .wrapper import MyWrapper
from .utils import _format_date

class HistWrapper(MyWrapper):
    def __init__(self):
        super().__init__()        

    # List functions
    def get_list_roots(self, sec: str) -> List[str]:
        """
        Retrieves all roots for the specified security.

        Args:
            sec: A string representing the security to retrieve roots for. Must be either 'opt' or 'stk'.
        
        Returns:
            A list of strings containing all roots for the specified security.
        
        Raises:
            ValueError: If sec is not 'opt' or 'stk'.
            Exception: If the server returns an error or the response content is empty.
            RootOrExpirationError: If the response data contains an integer representing a root or expiration error.
        """

        self.req_type = "roots"
        self.url = f"{self.base_url}/{self.call_type}/{self.req_type}"

        if sec == "opt":
            self.params = {"sec":"OPTION"}
        elif sec == "stk":
            self.params = {"sec":"STOCK"}
        else:
            raise ValueError("Security must be OPTION or STOCK.")
        
        return self._get_data()


    def get_list_strikes(self, exp: str, root: str) -> List[int]:
        """
        Retrieves a list of strike prices for a given expiration date and root symbol.

        Parameters:
        exp (str): Expiration date in format "YYYYMMDD".
        root (str): Root symbol for the underlying security.

        Returns:
        List[int]: List of strike prices.

        Raises:
        RootOrExpirationError: If the provided root or expiration date is invalid.
        Exception: If the response content is empty or cannot be parsed.

        """
        self.call_type = "list"
        self.req_type = "strikes"
        _exp = _format_date(exp)
        
        self.url = f"{self.base_url}/{self.call_type}/{self.req_type}"
        self.params = {"exp": _exp, "root": root}
        return self._get_data()

    def get_list_expirations(self, root: str) -> List[str]:
        """
        Retrieves a list of all expiration dates for an underlying root.

        Parameters:
        root (str): Root symbol for the underlying security.

        Returns:
        List[str]: List of expiration dates in format "YYYYMMDD".

        Raises:
        RootOrExpirationError: If the provided root symbol is invalid.
        Exception: If the response content is empty or cannot be parsed.

        """
        self.call_type = "list"
        self.req_type = "expirations"
        
        self.url = f"{self.base_url}/{self.call_type}/{self.req_type}"
        self.params = {"root": root}
        return self._get_data()