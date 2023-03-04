import asyncio 
import aiohttp
from .wrapper import NoDataForContract

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

async def fetch_all_contracts(contracts_in_exp, batch_size=32, TIMEOUT=20, MAX_RETRY=2, SLEEP=20):
    """
    Fetch data for all contracts asynchronously.
    """
    data = []
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
            MAX_RETRY -= 1
            print(f"[+] Timed out {MAX_RETRY-i}/{MAX_RETRY} .. going to sleep")
            await asyncio.sleep(SLEEP)
    else:
        print(f"[+] Max retries reached. Giving up")
    return data