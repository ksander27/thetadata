import requests as rq
import pandas as pd

 
base_url = "http://localhost:25510/hist/option/eod"
params = {
    "root":"AAPL"
    ,"exp":"20220930"
    ,"right":"C"
    ,"strike":140000
    ,"start_date":"20220901"
    ,"end_date":"20220915"
}


request = rq.get(url=base_url,params=params)
request_json = request.json()
header = request_json.get("header")

if request.ok and header.get("error_msg") == "null":
    format = header.get("format")
    response = request_json.get("response")

    data = [{key: element[idx] for idx, key in enumerate(format)} for element in response]
    df = pd.DataFrame(data)

    root,exp = params.get("root"),params.get("exp")
    right,strike = params.get("right"),int(params.get("strike")/1000)

    filename = f"./data/{root}_{exp}_{right}_{strike}.csv"
    df.to_csv(filename,index=False)
    print(f"[+] Saved file at {filename}")
else:
    print("[+] Request not ok.")

