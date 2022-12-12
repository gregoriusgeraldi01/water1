import os
import time
from datetime import datetime
from itertools import islice
import csv
import json
import pandas as pd
import twd97
from geojson import Feature, Point, FeatureCollection

import influxdb_client
from datetime import datetime

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# You can generate a Token from the "Tokens Tab" in the UI
token = "PRBR01qVKtLapy8jqD1c6lYRSvyV6ciDzZWgohDIT6WhFTCZL3VA8kesU0k0lYRpuzEYS937qCubdg06c9Ug1g=="
org = "test"
bucket = "water"

client = InfluxDBClient(url="http://192.168.0.139:8086", token=token)
from influxdb_client import InfluxDBClient, Point, WriteOptions
write_api = client.write_api(write_options=SYNCHRONOUS)

data = "mem,host=host1 used_percent=23.43234543"
write_api.write(bucket, org, data)
from influxdb_client.client.write_api import SYNCHRONOUS

file = 'GW_202210.csv'

def insertdatabase(json_body):
    # 這邊的資訊請填入自己的 不改的話一定連不上平台
    org="test"
    bucket = "water"
    token = "PRBR01qVKtLapy8jqD1c6lYRSvyV6ciDzZWgohDIT6WhFTCZL3VA8kesU0k0lYRpuzEYS937qCubdg06c9Ug1g=="
    url = "http://192.168.0.139:8086" # 使用 CT 執行程式用 localhost
    # url = "http://192.168.0.137:8086" 如果從windows寫入就用 CT IP 連線
    
    # 建立連結
    client = InfluxDBClient(
        url=url,
        token=token,
        org=org
    )
    # 使用寫入 API
    
    write_api = client.write_api(write_options=WriteOptions(batch_size=1000,flush_interval=10000,jitter_interval=2000,retry_interval=5000,max_retries=5,max_retry_delay=30000,exponential_base=2))

    data = "mem,host=host1 used_percent=23.43234543"
    write_api.write(bucket, org, data)

def main():
    print("data name:", file)
    with open('/root/{}'.format(file), 'r',encoding="Big5") as in_file:
        rows = csv.reader(in_file)
        json_body = []
        header = next(rows)
        
        start=time.time()
        i = 0
        for row in islice(rows, 1, None):
            rowdata = {}
            rowdata["measurement"]=str(row[2]).replace(' ','')
            temptime=datetime.strptime(str(row[5]), "%Y-%m-%d %H:%M").isoformat()
            rowdata["time"]=temptime
            rowdata["tags"]={}
            rowdata["fields"]={}
            rowdata["tags"]["ST_NO"]=str(row[2]).replace(' ','')
            rowdata["tags"]["NAME_C"]=str(row[3]).replace(' ','')
            rowdata["fields"]["Water_Level"]=float(row[6])
            json_body.append(rowdata)

            i+=1
            if i==100:
                insertdatabase(json_body)
                json_body = []
                i-=100
            
        end = time.time()
        end = end-start
        end = round(end,4)
        print("Upload to Influxdb time: ",end)

if __name__ == '__main__':
    main()