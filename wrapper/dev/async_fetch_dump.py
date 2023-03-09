import asyncio 
import aiohttp
from ..wrapper import NoDataForContract
from ..option.option import Option
from ..stock.stock import Stock


class AsyncFetcher():
    def __init__(self,BATCH_SIZE=128,TIMEOUT=5,MAX_RETRY=3,SLEEP=20):
        
        self.BATCH_SIZE = BATCH_SIZE
        self.TIMEOUT = TIMEOUT
        self.MAX_RETRY = MAX_RETRY
        self.SLEEP = SLEEP

    async def _fetch_task(self, contract, session):
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

        r = await session.get(contract.url, params=contract.params, timeout=self.TIMEOUT)
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
                print(contract.url,contract.params)
                print(f"[+] No data data for contract - {contract.__str__()} - {contract.params}")
                return {"data": None, "url": None, "params": None}
            
            except Exception as e:
                print(f"Failed to fetch data for contract - {contract.__str__()} - {contract.params}")
                raise e
            
    async def _fetch_batch(self, contracts: List[Option]) -> List[Dict[str, Union[Any, str]]]:
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
        connector = aiohttp.TCPConnector(limit_per_host=self.BATCH_SIZE)
        async with aiohttp.ClientSession(connector=connector) as session:
            for contract in contracts:
                
                task = asyncio.create_task(self._fetch_task(contract, session))
                tasks.append(task)
            results = await asyncio.gather(*tasks)
        return results
            
    async def fetch_all_contracts(self,contracts_in_exp: List[Option]) -> List[Dict[str, Union[None, List[Any]]]]:
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
        i, attempt,last_success_batch_idx = self.MAX_RETRY, 0,0
        while i > 0:
            try:
                
                while last_success_batch_idx < len(contracts_in_exp):
                    contracts = contracts_in_exp[last_success_batch_idx:last_success_batch_idx+self.BATCH_SIZE]
                    results = await self._fetch_batch(contracts)
                    data.extend(results)
                    last_success_batch_idx += self.BATCH_SIZE
                break
            except asyncio.TimeoutError:
                i -= 1
                self.TIMEOUT += self.TIMEOUT
                self.SLEEP += self.SLEEP
                attempt += 1
                if i>0:
                    print(f"[+] Timed out {attempt}/{self.MAX_RETRY} .. going to sleep")
                    await asyncio.sleep(self.SLEEP)
        else:
            print(f"[+] Max retries reached. Giving up")
            raise asyncio.TimeoutError
        return data






