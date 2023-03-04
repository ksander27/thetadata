import asyncio 
import aiohttp
import random
from .wrapper import NoDataForContract


# async def fetch_all_contracts(contracts, batch_size=32, TIMEOUT=20, MAX_RETRY=2, SLEEP=20):
#     """
#     Fetch data for all contracts asynchronously.
#     """
#     connector = aiohttp.TCPConnector(limit=batch_size)
#     async with aiohttp.ClientSession(connector=connector) as session:
#         data = []
#         start = 0
#         while start < len(contracts):
#             end = min(start + batch_size, len(contracts))
#             batch = contracts[start:end]
#             # fetch a batch of contracts
#             result = await asyncio.wait_for(fetch_batch(batch, session, TIMEOUT, MAX_RETRY, SLEEP), timeout=TIMEOUT)
#             data += result
#             start += batch_size
#         return data


# async def fetch_batch(contracts,session,TIMEOUT,MAX_RETRY,SLEEP):
#     tasks = []
#     for contract in contracts:
#         task = asyncio.create_task(_fetch_task(contract,session,TIMEOUT,MAX_RETRY,SLEEP))
#         tasks.append(task)
#     results = await asyncio.gather(*tasks)
#     return results

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
    except asyncio.TimeoutError:
        print(f"[+] Timed out for contract - {contract.__str__()} - {contract.params}")
        await asyncio.sleep(1)

    print(f"Failed to fetch data for contract - {contract.__str__()} - {contract.params}")
    return {"data": None, "url": contract.url, "params": contract.params}

    

async def fetch_all_contracts(contracts,batch_size=32, TIMEOUT=20, MAX_RETRY=2,SLEEP=20):
    """
    Fetch data for all contracts asynchronously.
    """
    connector = aiohttp.TCPConnector(limit_per_host=batch_size)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [_fetch_task(contract, session) for contract in contracts]
        data = []
        for i in range(MAX_RETRY):
            try:
                # Maybe do a batch with size limit here?
                for task in asyncio.as_completed(tasks, timeout=TIMEOUT):
                        result = task
                        data.append(result)
            except asyncio.TimeoutError:
                i+=1
                print(f"[+] Timed out {i}/{MAX_RETRY} .. going to sleep")
                asyncio.sleep(SLEEP)
        print(f"[+] Max retries reached. Giving up")
                
    return data