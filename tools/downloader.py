import asyncio 
import pandas as pd

from ..wrapper import Option,AsyncFetcher



class AsyncDownloader(AsyncFetcher):
    def __init__(self,batches,method,key_params=[],*args,**kwargs):
        super().__init__(*args,**kwargs)
                    
        self.batches = batches # must be df with root,exp,strike,right
        self.method = method
        self.key_params = key_params # List of parameters alongside a method

        self.list_async_params = None

    def get_list_async_params_from_batches(self):
        """ 
            Internal facing function. Takes a the property ._batches and returns 
            a list of dict [{"contract":{option descr},"method":method,"params":{params alongside the method}}]
        """
        list_args = self.batches[["root","exp","strike","right"]].to_dict(orient="records")

        list_params = self.batches[self.key_params].to_dict(orient="records")
        self.list_async_params = [{"contract":args
                        ,"params":list_params[idx] if len(list_params)>0 else None
                        ,"method":self.method} 
                        for idx,args in enumerate(list_args)]

        return self.list_async_params
    
    def prepare_contracts_from_list_async_params(self):
        contracts = []
        for args_params in self.list_async_params:
            args,_params,method = args_params.get("contract"),args_params.get("params"),args_params.get("method")
            params = {"method":method,"params":_params}
            args["_async"] = True 
            option = Option(**args)
            opt = option._get_method(**params)
            contracts.append(opt)
        return contracts
    
    def get_df_tmp(self,contract):
        data = contract.get("data")
        params = contract.get("params")

        df_tmp = pd.DataFrame(data)
        df_tmp["url"] = contract.get("url")
        df_tmp["task_id"] = contract.get("task_id")
        df_tmp["req_id"] = contract.get("req_id")
        df_tmp["latency_ms"] = contract.get("latency_ms")
        df_tmp["err_type"] = contract.get("err_type")
        if params:
            for k,v in params.items():
                df_tmp[k] = v
        return df_tmp
    
    def async_download_contracts(self):
        """ 
            External facing function.
            When given a list of dict following the convention {contract,method,params}
            fetch all the contracts asynchronously and returns a df if some data where 
            received.
        """
        df = None 
        self.list_async_params = self.get_list_async_params_from_batches()
        contracts = self.prepare_contracts_from_list_async_params() 
        results = asyncio.run(self.fetch_all_contracts(contracts))
        if results:
            df_tmps = [self.get_df_tmp(contract) for contract in results]
            if not all(df_tmp is None for df_tmp in df_tmps):
                df = pd.concat(df_tmps,axis=0)

        return df