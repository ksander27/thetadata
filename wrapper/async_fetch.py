import asyncio 
import aiohttp
from .wrapper import NoDataForContract


class AsyncFetcher():
    def __init__(self,batch_size,timeout,max_retry,sleep):
        self.batch_size = batch_size
        self.timeout = timeout
        self.max_retry = max_retry
        self.sleep = sleep

    # Last time it run fine - DO NOT TOUCH
    async def _fetch_task(self,contract, session):
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

    async def _fetch_batch(self,batch ):
        tasks = []
        connector = aiohttp.TCPConnector(limit_per_host=self.batch_size)
        async with aiohttp.ClientSession(connector=connector) as session:
            for contract in batch:
                
                task = asyncio.create_task(self._fetch_task(contract, session, self.timeout))
                tasks.append(task)
            results = await asyncio.gather(*tasks)
        return results

    async def fetch_all_contracts(self,contracts):
        """
        Fetch data for all contracts asynchronously.
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
