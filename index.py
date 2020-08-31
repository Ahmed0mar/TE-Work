import pandas as pd
import pymongo 

msanFile = pd.read_csv('MsanLinks2608.csv')
rowStatus = [False] * len(msanFile['TE_ipaddress'])
connect = pymongo.MongoClient("mongodb://localhost:27017/")
betDB = connect["bet_prod_v1"]
interfaces=betDB["interfaces"]

for index,ip in enumerate(msanFile['TE_ipaddress']):
    if not rowStatus[index] and msanFile['TE_linkconnection'][index]=='UPLink':
        indexes = [i for i,x in enumerate(msanFile['TE_ipaddress'])\
            if msanFile['TE_ipaddress'][index] == msanFile['TE_ipaddress'][i] and msanFile['TE_linkconnection'][i] == 'UPLink']
        if len(indexes) >2: continue
        maxindex=-1
        for eli in indexes:
            if maxindex < 0 :
                    maxindex=eli
                    continue
            else:
                for elj in indexes:
                    if msanFile['Inbound Throughput (bps)-max'][maxindex] <= msanFile['Outbound Throughput (bps)-max'][elj] and msanFile['Outbound Throughput (bps)-max'][maxindex] <= msanFile['Inbound Throughput (bps)-max'][elj]:
                        maxindex = elj
            rowStatus[eli]=True
        for i in indexes:
            if maxindex == i:
                query={"TE_ipaddress":msanFile['TE_ipaddress'][maxindex],"TE_ifName":msanFile['TE_ifName'][maxindex]}
                newvalues = { "$set": { "linkDirection": "UPLink" } }
            else:
                query={"TE_ipaddress":msanFile['TE_ipaddress'][i],"TE_ifName":msanFile['TE_ifName'][i]}
                newvalues = { "$set": { "linkDirection": "DownLink" } }
            interfaces.update_one(query, newvalues)
