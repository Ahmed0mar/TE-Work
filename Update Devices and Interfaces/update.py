import pandas as pd
import pymongo

data = pd.read_csv('parent_interface.csv')
connect = pymongo.MongoClient("mongodb://localhost:27017/")
betDB = connect["bet_prod_v1"]
devices=betDB["devices"]
interfaces=betDB["interfaces"]

for index, ip in enumerate(data['TEDataIP']):
    #update devices table
    query={"ipaddress":data['TEDataIP'][index]}
    newvalues = { "$set": { "parentInterface": data['Parent_Interface'][index] } }
    devices.update_many(query, newvalues)
    #update interfaces table
    query={"ipaddress":data['TEDataIP'][index]}
    newvalues = { "$set": { "parentInterface": data['Parent_Interface'][index] } }
    interfaces.update_many(query, newvalues)