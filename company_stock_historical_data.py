from list_of_companies import final_list
import pandas as pd
import json

import requests


def time_format(time):
    return time[:10]


for i in final_list:
    url = "https://stock-market-data.p.rapidapi.com/stock/historical-prices"

    querystring = {"ticker_symbol": i, "years": "1", "format": "json"}

    headers = {
        "X-RapidAPI-Key": "1a4c284a07msh7d418c945acef17p16d9e1jsn432f1f729068",
        "X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)

    data = response.text

    json_data = json.loads(data)
    response = json_data['historical prices']

    df = pd.DataFrame(response)
    date = df['Date'].apply(time_format)
    df['Date'] = date
    df['Stock'] = i
    file_name = str(i) + '.csv'
    df.to_csv(file_name)
