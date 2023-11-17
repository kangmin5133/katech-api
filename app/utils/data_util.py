import csv
from influxdb_client import Point
from collections import defaultdict
import json
from datetime import datetime

def create_point(file_path, timestamp,device_id,vehicle_id= None ):
    if vehicle_id:
        point = Point("SensorData") \
                .tag("vehicle_id", vehicle_id) \
                .tag("device_id", device_id) \
                .time(timestamp)
    else:
        point = Point("SensorData") \
                .tag("device_id", device_id) \
                .time(timestamp)

    
    with open(file_path, mode='r') as file:
        csv_reader = csv.reader(file)
        for i,row in enumerate(csv_reader):
            for i,item in enumerate(row):
                if "=" not in row[i] and len(row[i]) == 14: 
                    point.field("timestamp", row[i])

                elif i == 1: 
                    if row[i] not in ["NA","NA.1"] and "=" not in row[i]:
                        point.field("latitude", float(row[i]))
                elif i == 2: 
                    if row[i] not in ["NA","NA.1"] and"=" not in row[i]:
                        point.field("logitude", float(row[i]))

                elif "=" in item:
                    field, value = item.split("=")
                    try:
                        # Check if the value contains a decimal point
                        if '.' in value:
                            value = float(value)
                        else:
                            value = int(value)
                    except ValueError:
                        # If conversion fails, keep it as string
                        pass
                    point.field(field, value)
    return point

def get_count(data_list):
    datas = json.loads(data_list)
    current_device_id = ""
    result_count = {}
    for data in datas:
        if data.get("device_id") != current_device_id:
            result_count[data.get("device_id")] = data.get("_value")
            current_device_id = data.get("device_id")
        else: continue
    return result_count
