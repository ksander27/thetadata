import asyncio 
import aiohttp
from .wrapper import NoDataForContract
from .option.option import Option
from typing import List,Dict,Union,Any
import pandas as pd


class AsyncFetcher():
    def __init__(self,batch_size,timeout,max_retry,sleep):
        self.batch_size = batch_size
        self.timeout = timeout
        self.max_retry = max_retry
        self.sleep = sleep

    # Last time it run fine - DO NOT TOUCH
    async def _fetch_task(self,contract, session):
        """
        Fetches data for a single contract asynchronously.

        Parameters:
        -----------
        contract : Contract
            The Contract object to fetch data for.
        session : aiohttp.ClientSession
            The aiohttp ClientSession object to use for making the request.

        Returns:
        --------
        dict:
            A dictionary containing the fetched data, the URL used for the request, and the parameters used for the request.

        Raises:
        -------
        NoDataForContract:
            If there is no data available for the specified contract.

        Example:
        --------
        N/A
        """

        r = await session.get(contract.url, params=contract.params, timeout=self.timeout)
        if r.status != 200:
            r.raise_for_status()
        else:
            try:
                _ = await r.json()
                contract.header = _.get("header")
                contract.response = _.get("response")

                if contract._parse_header():
                    data = contract._parse_response()
                    print(f"[+] Fetched data for contract - {contract.__str__()} - {contract.params}")
                    return {"data": data, "url": contract.url, "params": contract.params}

            except NoDataForContract:
                print(f"[+] No data data for contract - {contract.__str__()} - {contract.params}")
                return {"data": None, "url": None, "params": None}
            
            except Exception as e:
                print(f"Failed to fetch data for contract - {contract.__str__()} - {contract.params}")
                raise e

    async def _fetch_batch(self, batch: List[Any]) -> List[Dict[str, Union[Any, str]]]:
        """
        Fetch data for a batch of contracts asynchronously.

        Parameters:
        -----------
        contracts : List[Contract]
            A list of Contract objects to fetch data for.

        Returns:
        --------
        List[Dict[str, Union[Any, str]]]
            A list of dictionaries containing the fetched data, the URL used for the request, and the parameters used for the request.

        Raises:
        -------
        N/A

        """
        tasks = []
        connector = aiohttp.TCPConnector(limit_per_host=self.batch_size)
        async with aiohttp.ClientSession(connector=connector) as session:
            for contract in batch:
                
                task = asyncio.create_task(self._fetch_task(contract, session))
                tasks.append(task)
            results = await asyncio.gather(*tasks)
        return results

    async def fetch_all_contracts(self,contracts: List[Any]) -> List[Dict[str, Union[None, List[Any]]]]:
        """
        Fetches data for all contracts asynchronously.

        Parameters:
        -----------
        contracts_in_exp : List[Contract] (Option or Stock)
            List of Contract objects for which data needs to be fetched.

        Returns:
        --------
        List[Dict[str, Union[None, List[Any]]]]
            A list of dictionaries containing fetched data, URL, and parameters for each contract in `contracts_in_exp`. 
            If no data is available for a contract, then the dictionary will contain None values.

        Raises:
        -------
        asyncio.TimeoutError:
            If the maximum number of retries have been exceeded.
        """
        data = []
        timeout_incr,sleep_incr = self.timeout,self.sleep
        i, attempt,last_success_batch_idx = self.max_retry, 0,0
        while i > 0:
            try:
                
                while last_success_batch_idx < len(contracts):
                    batch = contracts[last_success_batch_idx:last_success_batch_idx+self.batch_size]
                    results = await self._fetch_batch(batch)
                    data.extend(results)
                    last_success_batch_idx += self.batch_size
                break
            except asyncio.TimeoutError:
                i -= 1
                self.timeout += timeout_incr
                self.sleep += sleep_incr
                attempt += 1
                if i>0:
                    print(f"[+] Timed out {attempt}/{self.max_retry} .. going to sleep for {self.sleep}sec")
                    await asyncio.sleep(self.sleep)
        else:
            print(f"[+] Max retries reached. Giving up")
            raise asyncio.TimeoutError
        return data


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
        list_async_params = [{"contract":args
                        ,"params":list_params[idx] if len(list_params)>0 else None
                        ,"method":self.method} 
                        for idx,args in enumerate(list_args)]

        return list_async_params
    
    def prepare_contracts_from_list_args_params(self):
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
        df_tmp = None 

        url = contract.get("url")
        data = contract.get("data")
        params = contract.get("params")
        if url and data:
            df_tmp = pd.DataFrame(data)
            df_tmp["url"] = url
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
        contracts_in_exp = self.prepare_contracts_from_list_args_params() 
        results = asyncio.run(self.fetch_all_contracts(contracts_in_exp))
        if results:
            df_tmps = [self.get_df_tmp(contract) for contract in results]
            if not all(df_tmp is None for df_tmp in df_tmps):
                df = pd.concat(df_tmps,axis=0)

        return df