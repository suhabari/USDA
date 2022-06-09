# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-

import dataiku
import pandas as pd, numpy as np
import json
import requests
import datetime
from datetime import date

client = dataiku.api_client()
project = client.get_project('USDA')

today = date.today().strftime("%Y/%m/%d").split('/')
year=str(today[0])
month=str(today[1])
day=str(today[2])

headers_dict = {"API_KEY": "2fd09d59-9d3a-4c99-80b5-047239d8b22d"}

main_url = 'https://apps.fas.usda.gov/OpenData'

hsCodesBeefVealFrChFz = ["0201100010","0201100090","0201203550","0201206000","0201303550","0201306010","0201306090","0202100010","0202203550","0202206000","0202303550","0202306000"]
hsCodesVarietyMeatsBeef = ["0206100000","0206210000","0206220000","0206290010","0206290020","0206290030","0206290040","0206290050","0206290090","0504000050","0504000070"]
hsCodesPorkFrChFz = ["0203294000","0203210000","0203229000","0203292000","0203129000","0203110000","0203194000","0203221000","0203121000","0203192000"]
hsCodesVarietyMeatsPork = ["0206490030","0206490090","0206490040","0206490010","0206490020","0206490050","0206300000","1602491000","0504000080","0206410000"]

years = ['2018','2019','2020','2021','2022']
months= ['01','02','03','04','05','06','07','08','09','10','11','12']

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
partnerCode={}
countries = dataiku.Dataset('Countries')
df_countries = countries.get_dataframe()
df_countries = df_countries[df_countries.regionCode != 'R99']
df_countries2 = df_countries.drop(columns=['regionCode','countryParentCode','description','isO3Code','discontinuedOn','gencCode'])
dict_countries = df_countries2.set_index('countryCode').T.to_dict('records')
dict_countries = dict_countries[0]

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def censusImports(year,month):
    folder = dataiku.Folder('censusImports')
    for i in dict_countries:
        #print(i,dict_countries[i])

        url=main_url+'/api/gats/censusImports/partnerCode/'+i+'/year/'+str(year)+'/month/'+str(month)
        req = requests.get(url=url, headers=headers_dict)
        resp= req.json()
        folder.write_json(str(dict_countries[i])+'/'+str(year)+'/'+str(month)+'/'+i+'_file',resp)

def censusExports(year,month):
    folder = dataiku.Folder('censusExports')
    for i in dict_countries:
        #print(i,dict_countries[i])

        url=main_url+'/api/gats/censusExports/partnerCode/'+i+'/year/'+str(year)+'/month/'+str(month)
        req = requests.get(url=url, headers=headers_dict)
        resp= req.json()
        folder.write_json(str(dict_countries[i])+'/'+str(year)+'/'+str(month)+'/'+i+'_file',resp)

def UNTradeImports(year):
    folder = dataiku.Folder('UNTradeImports')
    for i in dict_countries:
        #print(i,dict_countries[i])

        url=main_url+'/api/gats/UNTradeImports/reporterCode/'+i+'/year/'+str(year)
        req = requests.get(url=url, headers=headers_dict)
        resp= req.json()
        folder.write_json(str(dict_countries[i])+'/'+str(year)+'/'+str(month)+'/'+i+'_file',resp)

def UNTradeExports(year):
    folder = dataiku.Folder('UNTradeExports')
    for i in dict_countries:
        #print(i,dict_countries[i])

        url=main_url+'/api/gats/UNTradeExports/reporterCode/'+i+'/year/'+str(year)
        req = requests.get(url=url, headers=headers_dict)
        resp= req.json()
        folder.write_json(str(dict_countries[i])+'/'+str(year)+'/'+str(month)+'/'+i+'_file',resp)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
for year in years:
    UNTradeImports(year)
    UNTradeExports(year)
    for month in months:
        censusImports(year,month)
        censusExports(year,month)
