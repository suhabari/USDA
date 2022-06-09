def weekly_createsplit():
    import dataiku
    import pandas as pd, numpy as np
    from dataiku import pandasutils as pdu
    import json
    import re

    #DataIku variables
    client = dataiku.api_client()
    project = client.get_project('USDA')
    input_folder=dataiku.Folder('Weekly')
    output_folder=dataiku.Folder('Weekly')
    paths = input_folder.list_paths_in_partition()
    flow = project.get_flow()
    
    name=[]
    for i in range(len(paths)):
        li=re.split('/',(paths)[i])
        x=li[1]
        y=li[len(li)-1]
        var=(str(x)+'_'+str(y)).replace('&','').replace(' ','_').replace('–','').replace('/','').replace(')','').replace('.','').replace('(','').replace(',','').replace('%','').replace('-','').replace('\\','')
        name.append(var)
        
        builder = project.new_managed_dataset(var)
        builder.with_store_into(connection="filesystem_managed")
        zone = flow.get_zone("O1dQLZL")
        
        try:
            # If dataset does not exist
            
            dataset = builder.create()
            zone.add_item(dataset)

        except:
            # print
            print(str(var)+'dataset create error')


        try:
            with input_folder.get_download_stream(paths[i]) as f:
                df=pd.read_csv(f)
                temp = dataiku.Dataset(name[i])
                temp.write_with_schema(df)
        except:
            print(str(var)+'write error')
            
weekly_createsplit()
