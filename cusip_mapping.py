import requests
import pandas as pd
import json

data = []

def get_all_cusips():
    return pd.read_csv('cusip_scraped.csv', header=None)


def cusip_to_query(cusip):
    return {"idType": "ID_CUSIP_8_CHR", "idValue": str(cusip)}


def format_response(response):
    if "data" in response and len(response["data"]) != 0:
        match = response["data"][0]
        return match["cusip8"], match["ticker"], match["name"], match["securityType"]

    return "", "", "", ""


def cusips_to_tickers(cusips):
    api_endpoint = "https://api.openfigi.com/v3/mapping"
    headers = {"X-OPENFIGI-APIKEY": "2f6a3c3b-43c0-430b-a8f9-cb8e6faa5fca"}
    query = [cusip_to_query(cusip[0]) for cusip in cusips.itertuples(index=False)]
    response = requests.post(api_endpoint, json=query, headers=headers)

    matches = response.json()
    to_delete = []
    for i in range(len(cusips)):
        try:
            matches[i]['data'][0]['cusip8']=cusips.iloc[i][0]
            data.append(matches)
        except KeyError:
            to_delete.append(i)

    for i in sorted(to_delete, reverse=True):
        del matches[i]        

    return [format_response(match) for match in matches]


def insert_into_db(rows):
    rows.to_csv("holdings-scraped.csv", mode='a', header=False, index=False)


def run():
    cusips = get_all_cusips()

    start = 0
    stop = 100

    while start < len(cusips):
        print(start, stop)
        if stop <= len(cusips):
            cusip_batch = cusips[start:stop]
        else:
            cusip_batch = cusips[start:]

        cusips_map = cusips_to_tickers(cusip_batch)

        insert_into_db(pd.DataFrame(cusips_map))
        start = start + 100
        stop = stop + 100

        if start >= len(cusips):
            break

    with open('all_cusip-scraped_mappings.json', 'w') as f:
        json.dump(data, f, indent=2)

    print("Done")


run()
