import asyncio 
import aiohttp


# Function to be added in _get_data and will be played if _async = True
async def _fetch_task(contract,session):
    async with session.get(contract.url,params=contract.params) as contract.request:
        if contract._isRequestOkay() and contract._parse_header():
            data = contract._parse_response()
            return await {"data":data,"url":contract.url,"params":contract.params}
        
async def _gather_tasks(contracts,session):
    tasks = []
    for contract in contracts:
        task = asyncio.create_task(_fetch_task(contract,session))
        tasks.append(task)
    results = await asyncio.gather(*tasks)
    return results

async def fetch_all_contracts(contracts):
    """

    Args:
        contracts (_type_): _description_

    Returns:
        _type_: List of dictionnaries with keys data url and params
    """
    async with aiohttp.ClientSession() as session:
        data = await _gather_tasks(contracts,session)
        return data
    
