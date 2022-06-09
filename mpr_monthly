import dataiku
import requests
import time
import json
from datetime import date
import pandas as pd

# DataIku variables
client = dataiku.api_client()
project = client.get_project('USDA')
folder = dataiku.Folder('Monthly')
listToStr=''

# Master URL
main_url_request = requests.get('https://mpr.datamart.ams.usda.gov/services/v1.1/reports')
data = main_url_request.json()
dictionary={}

# Errors
er=[]

# Date
today = date.today()

_dict = [x for x in data if 'Monthly' in x['report_title']]

for i in range(len(_dict)):
    slug_id=(_dict[i])['slug_id']
    sectionNames=(_dict[i])['sectionNames']
    dictionary[slug_id] = sectionNames
    
    if '/' in _dict[i]['report_title']:
        _dict[i]['report_title']=_dict[i]['report_title'].replace('/','_')
    else:
        print('')
    
    try:
        datelist= _dict[0]['published_date'].split(' ')[0].split('/')
        datelist2= [datelist[2],datelist[0],datelist[1]]
        listToStr = (' '.join([str(elem) for elem in datelist2])).replace(' ','/')
    except:
        print(str(slug_id)+'--probably published_date not in json')
        er.append(str(slug_id)+' '+str('resp error or no published date--')+' '+str(today.strftime('%m/%d/%Y')))
    
    for j in range(len(sectionNames)):
        url='https://mpr.datamart.ams.usda.gov/services/v1.1/reports/'
        url = url + str(slug_id)+ '/' +(sectionNames[j])

        try:
            req=requests.get(url)
            secondloop_response=req.json()
            jsondump = secondloop_response

            if '/' in sectionNames[j]:
                sectionNames[j]=sectionNames[j].replace('/','_')
                sectionNames[j]=sectionNames[j].replace(' ','_')
            else:
                print('')
        
        except:
            print(str(slug_id)+'--'+str(sectionNames)[j]+'--probably request/response error in second try loop')
            er.append((str(slug_id)+'--'+str(sectionNames)[j]+'--probably request/response error in second try loop--')+str(today.strftime('%m/%d/%Y')))

        folder.write_json(str(_dict[i]['report_title'])+'/'+str(listToStr)+'/'+str((sectionNames)[j]),jsondump)
        
# df = pd.DataFrame(er,columns=['errors'])
# temp = dataiku.Dataset('Error_dataset')
# temp.write_with_schema(df)
