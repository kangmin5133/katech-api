import csv
from fastapi import HTTPException
from influxdb_client import Point
import datetime
from dateutil.parser import parse
import json
from datetime import timezone
import re

def is_valid_timestamp(value):
    # YYYYMMDDHHMMSS 형식 확인
    if not re.match(r'^\d{14}$', value):
        return False
    year, month, day, hour, minute, second = int(value[:4]), int(value[4:6]), int(value[6:8]), int(value[8:10]), int(value[10:12]), int(value[12:14])
    # 각 부분의 범위 확인
    if year >= 2023 and 1 <= month <= 12 and 1 <= day <= 31 and 0 <= hour < 24 and 0 <= minute < 60 and 0 <= second < 60:
        return True
    else:
        return False

def is_valid_latitude(value):
    try:
        lat = float(value)
        return 33.1 <= lat <= 38.3
    except ValueError:
        return False

def is_valid_longitude(value):
    try:
        lon = float(value)
        return 124.6 <= lon <= 131.9
    except ValueError:
        return False
    
def create_point(file_path, device_id):
    point = Point("SensorData").tag("device_id", device_id)

    with open(file_path, mode='r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            timestamp_found = False
            for item in row:
                # Timestamp 처리
                if is_valid_timestamp(item):
                    point.field("timestamp", item)
                    point.time(datetime.datetime.strptime(item, "%Y%m%d%H%M%S"))
                    timestamp_found = True
                # Latitude 처리
                elif is_valid_latitude(item):
                    point.field("latitude", float(item))
                # Longitude 처리
                elif is_valid_longitude(item):
                    point.field("logitude", float(item))
                # 나머지 데이터 처리
                elif "=" in item:
                    field, value = item.split("=")
                    try:
                        value = float(value) if '.' in value else int(value)
                    except ValueError:
                        pass  # 문자열로 유지
                    point.field(field, value)
            if not timestamp_found:
                raise HTTPException(f"Invalid data: Timestamp not found in row: {row}")

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


