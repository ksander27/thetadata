import asyncio 
import aiohttp
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from .wrapper import NoDataForContract
from .option import Option


# Last time it run fine - DO NOT TOUCH
async def _fetch_task(contract, session,TIMEOUT):
    try:
        async with session.get(contract.url, params=contract.params, timeout=TIMEOUT) as r:
            if r.status != 200:
                r.raise_for_status()
            else:
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
    except asyncio.TimeoutError:
        print(f"[+] Timed out for contract - {contract.__str__()} - {contract.params}")
        await asyncio.sleep(1)
        raise asyncio.TimeoutError
    except Exception as e:
        print(f"Failed to fetch data for contract - {contract.__str__()} - {contract.params}")
        raise e

async def fetch_batch(contracts,batch_size,TIMEOUT):
    tasks = []
    connector = aiohttp.TCPConnector(limit_per_host=batch_size)
    async with aiohttp.ClientSession(connector=connector) as session:
        for contract in contracts:
            task = asyncio.create_task(_fetch_task(contract, session, TIMEOUT))
            tasks.append(task)
        results = await asyncio.gather(*tasks)
    return results  

async def fetch_all_contracts(contracts_in_exp, batch_size=32, TIMEOUT=5, MAX_RETRY=2, SLEEP=20):
    """
    Fetch data for all contracts asynchronously.
    """
    data = []
    TIMEOUT = batch_size * TIMEOUT
    i = MAX_RETRY
    while i >0:
        try:
            j = 0
            while j < len(contracts_in_exp):
                contracts = contracts_in_exp[j:j+batch_size]
                results = await fetch_batch(contracts, batch_size,TIMEOUT)
                data.extend(results)
                j+=batch_size
            break
        except asyncio.TimeoutError:
            i -= 1
            print(f"[+] Timed out {MAX_RETRY-i}/{MAX_RETRY} .. going to sleep")
            await asyncio.sleep(SLEEP)
    else:
        print(f"[+] Max retries reached. Giving up")
    return data

# BATCH_SIZE,TIMEOUT,MAX_RETRY,SLEEP = 32,5,3,20
def get_df_tmp(contract):
    url = contract.get("url")
    data = contract.get("data")
    params = contract.get("params")
    if url and data:

        try:
            df_tmp = pd.DataFrame(data)
        except ValueError:
            print(data)
            raise ValueError
        df_tmp["url"] = url
        for k,v in params.items():
            df_tmp[k] = v
        return df_tmp
    
def prepare_contracts_in_exp_from_list_args_params(list_args_params):
    contracts_in_exp = []
    for args_params in list_args_params:
        args,_params,method = args_params.get("args"),args_params.get("params"),args_params.get("method")
        params = {"method":method,"params":_params}
        args["_async"] = True 
        option = Option(**args)
        contracts_in_exp.append(option._get_method(**params))
    return contracts_in_exp

class QueueManager():
    def __init__(self,BATCH_SIZE=100,TIMEOUT=4,MAX_RETRY=3,SLEEP=20,PROCESSES=4):
        self.BATCH_SIZE = BATCH_SIZE
        self.TIMEOUT = TIMEOUT
        self.MAX_RETRY = MAX_RETRY
        self.SLEEP = SLEEP
        self.PROCESSES = PROCESSES
        
    def _get_contracts_in_exp_async(self,list_args_params):
        contracts_in_exp = prepare_contracts_in_exp_from_list_args_params(list_args_params)
        results = asyncio.run(fetch_all_contracts(contracts_in_exp,self.BATCH_SIZE,self.TIMEOUT,self.MAX_RETRY,self.SLEEP))
        if results:
            df_tmps = [get_df_tmp(contract) for contract in results]
            if not all(df_tmp is None for df_tmp in df_tmps):
                df = pd.concat(df_tmps,axis=0)
        return df
    
    def get_contracts_in_exp_pooling(self,list_args_params):
        df = pd.DataFrame()
        with ThreadPoolExecutor(max_workers=self.PROCESSES) as executor:
            results = executor.map(self._get_contracts_in_exp_async,list_args_params)
        if results:
            for df_res in results:
                if df_res is not None:
                    df = pd.concat(df_res,axis=0)
        return df

