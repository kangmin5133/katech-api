import csv
from influxdb_client import Point
from collections import defaultdict
import json

def create_point(file_path, timestamp, vehicle_id, device_id):
    point = Point("SensorData") \
            .tag("vehicle_id", vehicle_id) \
            .tag("device_id", device_id) \
            .time(timestamp)
    
    with open(file_path, mode='r') as file:
        csv_reader = csv.reader(file)
        for i,row in enumerate(csv_reader):
            for i,item in enumerate(row):
                if i == 0: 
                    if len(row[i]) == 14: point.field("timestamp", row[i])
                elif i == 1: point.field("latitude", float(row[i]))
                elif i == 2: point.field("logitude", float(row[i]))
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

def influx_parser(query_result, total_count, offset, limit):
    parsed_data = defaultdict(lambda: defaultdict(dict))

    device_id = None
    vehicle_id = None
    final_output = []

    for row in query_result:
        timestamp = row['_time']
        field = row['_field']
        value = row['_value']

        device_id = row.get('device_id', device_id)
        vehicle_id = row.get('vehicle_id', vehicle_id)

        parsed_data[device_id][timestamp][field] = value
        parsed_data[device_id]['vehicle_id'] = vehicle_id

    for device, data in parsed_data.items():
        vehicle_id = data.pop('vehicle_id', None)
        timestamps = [{"timestamp": timestamp, **fields} for timestamp, fields in data.items()]
        final_data = {
            "device_id": device,
            "vehicle_id": vehicle_id,
            "data": timestamps
        }
        final_output.append(final_data)

    result_with_meta = {
        "total": total_count,
        "offset": offset,
        "limit": limit,
        "data": final_output
    }

    return result_with_meta