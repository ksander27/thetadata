import asyncio 
import aiohttp
from .wrapper import NoDataForContract

class TaskTimeOut(Exception):
    pass

# Function to be added in _get_data and will be played if _async = True
# async def _fetch_task(contract, session):
#     try:
#         async with session.get(contract.url, params=contract.params) as r:
#             if r.status != 200:
#                 r.raise_for_status()
#             else:
#                 _ = await r.json()
#                 contract.header = _.get("header")
#                 contract.response = _.get("response")

#                 if contract._parse_header():
#                     data = contract._parse_response()
#                     print(f"[+] Fetched data for contract - {contract.__str__()} - {contract.params}")
#                     return {"data": data, "url": contract.url, "params": contract.params}
#     except NoDataForContract:
#         print(f"[+] No data data for contract - {contract.__str__()} - {contract.params}")
#         return {"data": None, "url": None, "params": None}

async def _fetch_task(contract, session, TIMEOUT,MAX_RETRY):
    for i in range(MAX_RETRY):
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
            asyncio.sleep(1)
    raise TaskTimeOut(f"Failed to fetch data for contract - {contract.__str__()} - {contract.params}")

    

async def _gather_tasks(contracts,session,TIMEOUT,MAX_RETRY):
    tasks = []
    for contract in contracts:
        task = asyncio.create_task(_fetch_task(contract,session,TIMEOUT,MAX_RETRY))
        tasks.append(task)
    results = await asyncio.gather(*tasks)
    return results

async def fetch_all_contracts(contracts,limit=32,TIMEOUT=20,MAX_RETRY=2):
    """
    Fetch data for all contracts asynchronously.
    """
    connector = aiohttp.TCPConnector(limit=limit)
    async with aiohttp.ClientSession(connector=connector) as session:
        data = await _gather_tasks(contracts,session,TIMEOUT,MAX_RETRY)
        return data
