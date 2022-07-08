import json

import requests

url = "https://stock-market-data.p.rapidapi.com/market/index/nasdaq-one-hundred"

headers = {
	"X-RapidAPI-Key": "1a4c284a07msh7d418c945acef17p16d9e1jsn432f1f729068",
	"X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
}

response = requests.request("GET", url, headers=headers)

data = response.text
json_data = json.loads(data)
final_list = json_data['stocks']
# print(json_data['stocks'])