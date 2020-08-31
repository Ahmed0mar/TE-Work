import pandas as pd
import pymongo 

msanFile = pd.read_csv('MsanLinks2608.csv')
rowStatus = [False] * len(msanFile['TE_ipaddress'])
connect = pymongo.MongoClient("mongodb://localhost:27017/")
betDB = connect["bet_prod_v1"]
interfaces=betDB["interfaces"]
nokiaUpInterfaces = ['ntio-1:sfp:1','nt-a:sfp:1','nt-b:sfp:1','nt-a:sfp:1','nt-b:sfp:1']
nokiaDownInterfaces = ['ntio-1:sfp:2','ntio-1:sfp:3','ntio-1:sfp:4','ntio-1:sfp:5']

for index,ip in enumerate(msanFile['TE_ipaddress']):
    if  msanFile['TE_devVendor'][index] in ['ZTE','Huawei']:
        if not rowStatus[index] and msanFile['TE_linkconnection'][index]=='UPLink':
            if list(msanFile['TE_ipaddress']).count(list(msanFile['TE_ipaddress'])[index])>2:
                rowStatus[index]=True
                query={"TE_ipaddress":msanFile['TE_ipaddress'][index],"TE_ifName":msanFile['TE_ifName'][index]}
                newvalues = { "$set": { "linkDirection": "UnDectable" } }
                interfaces.update_one(query, newvalues)

            elif list(msanFile['TE_ipaddress']).count(list(msanFile['TE_ipaddress'])[index])==2 and msanFile['TE_linkconnection'][list(msanFile['TE_ipaddress']).index(ip)]=='UPLink' and msanFile['TE_linkconnection'][list(msanFile['TE_ipaddress']).index(ip,list(msanFile['TE_ipaddress']).index(ip)+1)]=='UPLink':
                maxindex=index
                secondIndex=list(msanFile['TE_ipaddress']).index(ip,index+1)
                if msanFile['Inbound Throughput (bps)-max'][index] <= msanFile['Outbound Throughput (bps)-max'][secondIndex] and msanFile['Outbound Throughput (bps)-max'][index] <= msanFile['Inbound Throughput (bps)-max'][secondIndex]:
                    maxindex= secondIndex
                rowStatus[index]=True
                rowStatus[secondIndex]=True
                if maxindex == index:
                    query={"TE_ipaddress":msanFile['TE_ipaddress'][maxindex],"TE_ifName":msanFile['TE_ifName'][maxindex]}
                    newvalues = { "$set": { "linkDirection": "UPLink" } }
                    interfaces.update_one(query, newvalues)
                    query={"TE_ipaddress":msanFile['TE_ipaddress'][secondIndex],"TE_ifName":msanFile['TE_ifName'][secondIndex]}
                    newvalues = { "$set": { "linkDirection": "DownLink" } }
                    interfaces.update_one(query, newvalues)
                else:
                    query={"TE_ipaddress":msanFile['TE_ipaddress'][maxindex],"TE_ifName":msanFile['TE_ifName'][maxindex]}
                    newvalues = { "$set": { "linkDirection": "UPLink" } }
                    interfaces.update_one(query, newvalues)
                    query={"TE_ipaddress":msanFile['TE_ipaddress'][index],"TE_ifName":msanFile['TE_ifName'][index]}
                    newvalues = { "$set": { "linkDirection": "DownLink" } }
                    interfaces.update_one(query, newvalues)

            elif list(msanFile['TE_ipaddress']).count(list(msanFile['TE_ipaddress'])[index])==1 or (list(msanFile['TE_ipaddress']).count(list(msanFile['TE_ipaddress'])[index])==2 and msanFile['TE_linkconnection'][index]=='UPLink'):
                rowStatus[index]=True
                query={"TE_ipaddress":msanFile['TE_ipaddress'][index],"TE_ifName":msanFile['TE_ifName'][index]}
                newvalues = { "$set": { "linkDirection": "UPLink" } }
                interfaces.update_one(query, newvalues)
   
    elif msanFile['TE_devVendor'][index] =='NokiaSiemens':
        if msanFile['TE_ifName'][index] in nokiaUpInterfaces:
            query={"TE_ipaddress":msanFile['TE_ipaddress'][index],"TE_ifName":msanFile['TE_ifName'][index]}
            newvalues = { "$set": { "linkDirection": "UPLink" } }
            interfaces.update_one(query, newvalues)
        elif msanFile['TE_ifName'][index] in nokiaDownInterfaces:
            query={"TE_ipaddress":msanFile['TE_ipaddress'][index],"TE_ifName":msanFile['TE_ifName'][index]}
            newvalues = { "$set": { "linkDirection": "DownLink" } }
            interfaces.update_one(query, newvalues)