from ._stock import _Stock
from ..wrapper import NoDataForContract

class Stock(_Stock):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)

    def isUTP(self):
        params = {
            "start_date":"20221201"
            ,"end_date":"20221201"
        }

        try:
            _ = self.get_hist_eod(**params)
            return True
        except NoDataForContract:
            return False
        except Exception as e:
            print(f"[+] Unexpected errors - {e}")
            raise e