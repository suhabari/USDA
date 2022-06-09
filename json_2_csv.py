def weekly_runs():

    import dataiku
    import pandas as pd, numpy as np
    from dataiku import pandasutils as pdu
    import json

    #DataIku variables
    client = dataiku.api_client()
    project = client.get_project('USDA')
    input_folder=dataiku.Folder('Weekly')
    output_folder=dataiku.Folder('Weekly')
    paths = input_folder.list_paths_in_partition()

    for i in paths:
        with input_folder.get_download_stream(i) as f:
            try:
                data = f.read()
                new_data2 = data.decode('utf-8')
                jloads = json.loads(new_data2)['results']
                df = pd.json_normalize(jloads)
                with output_folder.get_writer(i) as w:
                    w.write(df.to_csv().encode("utf-8"))
            except:
                print(i+' return a "NO RESULTS FOUND"')
