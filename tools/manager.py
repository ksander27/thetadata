import os

class AppManager():
    def __init__(self,call_type,endpoint,endpoint_params,root,start_date=None,end_date=None):
        self.call_type = call_type
        self.endpoint = endpoint
        self.endpoint_params = endpoint_params
        self.method = self.get_method()
        self.root = root

        self.start_date = start_date
        self.end_date = end_date
        
        self._DIR = self._get_DIR()
        self._file = self._get_file()
        self.filename = self.get_filename()
    
    def get_method(self):
        self.method = f"get_{self.call_type}_{self.endpoint}"
        return self.method

    def _get_DIR(self):
        self._DIR = f"/home/jupyter/data/{self.call_type}/{self.endpoint}/{self.root}/"
        return self._DIR
    
    def _get_file(self):
        self._file = f"{self.root}_{self.exp}_{self.start_date}_{self.end_date}"
        return self._file
    
    def get_filename(self):    
        self.filename = self._get_DIR() + self._get_file()
        return self.filename
    
    def isFile(self):
        if os.path.exists(self.filename):
            print(f"[+] {self.filename} already exists.")
            return True
        else:
            print(f"\n[+] Building contracts for {self.exp}")
            return False
    
    def isStorage(self):
        if os.path.exists(self._DIR):
            print(f"[+] {self._DIR} already exists.")
        else:
            os.makedirs(self._DIR)
            print(f"Created {self._DIR}")
        return None
    
    def store_file(self,df_data):
        if df_data is not None:
            print(f"[+] Fetched {df_data.shape[0]} contracts with dates in {self.exp}")
            df_data.to_csv(self.filename,index=False)
            print(f"[+] Saved {self.filename}")
        else:
            print(f"[+] NO DATA - Nothing to save for {self.filename}") 