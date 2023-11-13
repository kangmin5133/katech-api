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

# def influx_parser(query_result, total_count, offset, limit):
#     parsed_data = defaultdict(lambda: defaultdict(dict))

#     device_id = None
#     # vehicle_id = None
#     final_output = []

#     for row in query_result:
#         timestamp = row['_time']
#         field = row['_field']
#         value = row['_value']

#         device_id = row.get('device_id', device_id)

#         parsed_data[device_id][timestamp][field] = value

#     for device, data in parsed_data.items():
#         timestamps = [{"timestamp": timestamp, **fields} for timestamp, fields in data.items()]
#         final_data = {
#             "device_id": device,
#             "data": timestamps
#         }
#         final_output.append(final_data)

#     result_with_meta = {
#         "total": total_count,
#         "offset": offset,
#         "limit": limit,
#         "count" : len(final_output[0].get("data")),
#         "data": final_output,
        
#     }

#     return result_with_meta

# def influx_parser(query_result, total_count, offset, limit):
#     timekeys = list({table.get("_time") for table in query_result if table.get("_time")})
#     grouped_data = {key: {} for key in timekeys}

#     # 각 테이블의 데이터를 적절한 그룹에 할당합니다.
#     for table in query_result:
#         time_key = table.get("_time")
#         device_id = table.get('device_id', None)
#         if time_key:
#             field = table.get("_field")
#             value = table.get("_value")
#             grouped_data[time_key][field] = value

#     # 그룹화된 데이터를 파싱하여 원하는 형식의 리스트로 변환합니다.
#     parsed_data = []
#     for time_key, fields in grouped_data.items():
#         # timestamp 필드가 있는지 확인하고, 해당 데이터를 기반으로 새로운 사전을 만듭니다.
#         if "timestamp" in fields:
#             data_dict = {
#                 "timestamp": fields["timestamp"],
#             }
#             # timestamp를 제외한 나머지 필드를 추가합니다.
#             for field, value in fields.items():
#                 if field != "timestamp":
#                     data_dict[field] = value
#             parsed_data.append(data_dict)

#     result_data = {"device_id":device_id,"data":parsed_data}

#     result_with_meta = {
#         "total": total_count,
#         "offset": offset,
#         "limit": limit,
#         "count" : len(parsed_data),
#         "data": result_data,
#     }
#     return result_with_meta

def influx_parser(query_result, total_count, offset, limit):
    # device_id와 _time으로 데이터를 그룹화합니다.
    grouped_data = {}

    # 각 테이블의 데이터를 적절한 그룹에 할당합니다.
    for table in query_result:
        time_key = table.get("_time")
        device_id = table.get('device_id')
        if time_key and device_id:
            if device_id not in grouped_data:
                grouped_data[device_id] = {}
            if time_key not in grouped_data[device_id]:
                grouped_data[device_id][time_key] = {}
                
            field = table.get("_field")
            value = table.get("_value")
            grouped_data[device_id][time_key][field] = value

    # 그룹화된 데이터를 파싱하여 원하는 형식의 리스트로 변환합니다.
    devices_data = []
    for device_id, timestamps in grouped_data.items():
        parsed_data = []
        for time_key, fields in timestamps.items():
            # timestamp 필드가 있는지 확인하고, 해당 데이터를 기반으로 새로운 사전을 만듭니다.
            data_dict = {}
            if "timestamp" in fields:
                data_dict["timestamp"] = fields["timestamp"]
            if "timestamp" not in fields: continue
            # timestamp를 제외한 나머지 필드를 추가합니다.
            for field, value in fields.items():
                if field != "timestamp":
                    data_dict[field] = value

            parsed_data.append(data_dict)
        
        device_result = {"device_id": device_id, "data": parsed_data}
        devices_data.append(device_result)

    #    # offset과 limit에 따라 결과 데이터를 슬라이싱
    #     paginated_data = parsed_data[offset:offset+limit]

    #     # 각 device_id별 결과 데이터를 추가
    #     devices_data.append({"device_id": device_id, "data": paginated_data})

    result_with_meta = {
        "total": total_count,
        "offset": offset,
        "limit": limit,
        "count": sum(len(device_data["data"]) for device_data in devices_data),
        "data": devices_data,
    }
    return result_with_meta