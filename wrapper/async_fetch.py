import asyncio 
import aiohttp
from .wrapper import NoDataForContract

class TaskTimeOut(Exception):
    pass

# Function to be added in _get_data and will be played if _async = True
async def _fetch_task(contract, session):
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
    

async def _gather_tasks(contracts,session):
    tasks = []
    for contract in contracts:
        task = asyncio.create_task(_fetch_task(contract,session))
        tasks.append(task)
    results = await asyncio.gather(*tasks)
    return results

async def fetch_all_contracts(contracts,limit=32,timeout=20,max_retry=2):
    """
    Fetch data for all contracts asynchronously.
    """
    connector = aiohttp.TCPConnector(limit=limit)
    async with aiohttp.ClientSession(connector=connector) as session:
        data = await _gather_tasks(contracts,session)
        return data
