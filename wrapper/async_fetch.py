import asyncio 
import aiohttp
from .wrapper import NoDataForContract


# Function to be added in _get_data and will be played if _async = True
async def _fetch_task(contract,session):
    retry_count = 0
    while retry_count < 2:
        try:
            print(contract.params)
            async with session.get(contract.url,params=contract.params) as r:
                if r.status !=200:
                    r.raise_for_status()
                else:
                    _ = await r.json()
                    contract.header = _.get("header")
                    contract.response = _.get("response")

                    if contract._parse_header():
                        data = contract._parse_response()
                        return {"data":data,"url":contract.url,"params":contract.params}
        except NoDataForContract:
            return {"data":None,"url":None,"params":None}
        except aiohttp.ClientError:
            retry_count += 1
    raise asyncio.TimeoutError(f"Timeout for {contract.url} after {retry_count} retries")

        
async def _gather_tasks(contracts,session):
    tasks = []
    for contract in contracts:
        task = asyncio.create_task(_fetch_task(contract,session))
        tasks.append(task)
    results = await asyncio.gather(*tasks)
    return results

async def fetch_all_contracts(contracts):
    """
    Fetch data for all contracts asynchronously.
    """
    async with aiohttp.ClientSession() as session:
        for retry in range(3):
            try:
                data = await asyncio.wait_for(_gather_tasks(contracts, session), timeout=120)
                return data
            except asyncio.TimeoutError:
                if retry == 2:
                    print("Timeout: maximum number of retries reached.")
                    raise
                print(f"Timeout: retrying ({retry+1}/2)...")


