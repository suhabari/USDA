import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import json
import requests
import datetime
from datetime import date

today = date.today().strftime("%Y/%m/%d").split('/')
year=str(today[0])
month='01'#str(today[1])
day=str(today[2])

countryCode=''
headers_dict = {"API_KEY": "2fd09d59-9d3a-4c99-80b5-047239d8b22d"}

main_url = 'https://apps.fas.usda.gov/OpenData'
urls = {
        "Countries":"/api/gats/countries",
        "Units_of_measure":"/api/gats/unitsOfMeasure",
        "Regions":"/api/gats/regions",
        "Commodities":"/api/gats/commodities",
        "HS6Commodities":"/api/gats/HS6Commodities"
       }

def get_static(name,secondary_url):
    url = main_url+secondary_url
    req = requests.get(url=url, headers=headers_dict)
    resp= req.json()
    df = pd.json_normalize(resp)
    temp = dataiku.Dataset(name)
    temp.write_with_schema(df)

    
for i in range(len(urls)):    
    get_static(list(urls.keys())[i],list(urls.values())[i])
    
