import csv
from fastapi import HTTPException
from influxdb_client import Point
import datetime
from dateutil.parser import parse
import json
from datetime import timezone
import re
import logging

logger = logging.getLogger()

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
    field_values = {}
    with open(file_path, mode='r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            timestamp_found = False
            logger.info(f"received data : {row}")
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
                        # 필드 중복 검사 및 값 유효성 검증
                        if field in field_values:
                            prev_value = field_values[field]
                            # value가 소수였고 중복되어서 들어온경우
                            if '.' in str(prev_value) and '.' in value:
                                continue  
                            # value가 정수였고 중복되어서 들어온경우
                            elif '.' not in str(prev_value) and '.' in value:
                                continue
                        value = float(value) if '.' in value else int(value)
                        field_values[field] = value  # 필드 값 업데이트
                    except ValueError:
                        continue  # 유효하지 않은 값이므로 건너뜀
                    point.field(field, value)
            if not timestamp_found:
                logger.info(f"Invalid data: Timestamp not found in row: {row}")
                raise HTTPException(status_code=500, detail=f"Invalid data:{row}")

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


