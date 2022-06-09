# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
import dataiku
import requests
import datetime
import json
from datetime import date

#DataIku variables
client = dataiku.api_client()
project = client.get_project('USDA')
errors=[]

#Datasets dicts
section_yes={}
section_no={}

#Master URL
API_KEY = "GIU4fhfMnYanC3haiy6cUgXjPF9RmuKW"
url = "https://marsapi.ams.usda.gov/services/v1.2/reports/"
folder = dataiku.Folder("MARS")

try:
    req = requests.get(url , auth=(API_KEY,"pEhHNZ2t9plEXyLEAsQA+EPtVSpnBvnS"))
    data = req.json()
except:
    print('First Try Error')
    errors.append('First try error'+' Response error')
    
# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE

section_no={}
section_yes={}
def sections_check():
    for i in range(len(data)):
        slug_id=(data[i])['slug_id']
        sectionNames=(data[i])['sectionNames']
        report_title=(data[i])['report_title']

        if len(sectionNames)==0:
            section_no[slug_id]=report_title
        else:
            if len(data[i]['sectionNames'])>1:
                section_yes[slug_id]=sectionNames

sections_check()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE

url='https://marsapi.ams.usda.gov/services/v1.2/reports/'

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE

for i in range(len(section_no)):
    slug_id = list(section_no.keys())[i]
    report_name = list(section_no.values())[i]
    url_no = url + slug_id+ '?lastReports=1'
    try:
        req = requests.get(url_no , auth=(API_KEY,"pEhHNZ2t9plEXyLEAsQA+EPtVSpnBvnS"))
        data_no = req.json()
    except:
        print(i,' Second Try Error')
        errors.append(str(slug_id)+str(report_name)+'Second Try Error')
    try:
        datelist= (list(data_no['results'][0]['published_date'].split(' ')[0].split('/')))
        datelist2= [datelist[2],datelist[0],datelist[1]]
        listToStr = (' '.join([str(elem) for elem in datelist2])).replace(' ','/')
    except:
        print(i,' Third Try Error')
        errors.append(str(slug_id)+str(report_name)+'Third Try Error')
    folder.write_json(str(report_name)+'/'+str(listToStr)+'/'+str(slug_id),data_no)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE

i=0
for i in range(len(section_yes)):
    slug_id = list(section_yes.keys())[i]
    sectionNames=list(section_yes.values())[i]

    for j in range(len(sectionNames)):
        url_yes=url + slug_id + '/' + sectionNames[j]

        try:
            req = requests.get(url_yes , auth=(API_KEY,"pEhHNZ2t9plEXyLEAsQA+EPtVSpnBvnS"))
            data_yes = req.json()
            datelist= (list(data_yes['results'][0]['published_date'].split(' ')[0].split('/')))
            datelist2= [datelist[2],datelist[0],datelist[1]]
            listToStr = (' '.join([str(elem) for elem in datelist2])).replace(' ','/')
        except:
            print(i,' Fourth Try Error')
            errors.append(str(slug_id)+str(report_name)+'Fourth Try Error')
        folder.write_json(str(sectionNames[j])+'/'+str(listToStr)+'/'+str(slug_id),data_yes)

