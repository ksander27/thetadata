import asyncio 
import aiohttp
from .wrapper import NoDataForContract

class TaskTimeOut(Exception):
    pass

# Function to be added in _get_data and will be played if _async = True
async def _fetch_task(contract, session, max_retry):
    retry_count = 0
    while retry_count < max_retry:
        try:
            async with session.get(contract.url, params=contract.params) as r:
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
        except (aiohttp.ClientError,asyncio.TimeoutError):
            retry_count += 1
            print(f"Timeout: retrying ({retry_count}/{max_retry}) in 1 second...")
            await asyncio.sleep(1)

    print(f"[+] Timeout for {contract.url} after {retry_count} retries")
    raise TaskTimeOut


async def fetch_all_contracts(contracts, timeout=20, max_retry=2):
    """
    Fetch data for all contracts asynchronously.
    """
    async with aiohttp.ClientSession() as session:
        tasks = [_fetch_task(contract, session, max_retry) for contract in contracts]
        data = []
        for task in asyncio.as_completed(tasks, timeout=timeout):
                try:
                    result = await task
                    data.append(result)
                except TaskTimeOut:
                    print("[+] TASK TIME OUT MOVING ON ")
                    pass
    return data



