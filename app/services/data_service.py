from fastapi import  UploadFile, HTTPException
from pathlib import Path
from sqlalchemy.orm import Session
from app.db.mysql import crud, metadatas
import shutil
from tempfile import NamedTemporaryFile
import datetime
from datetime import timezone
import json
from config.config import Config
import zipfile
import os 
from typing import Optional, List
import logging
from app.db.influxdb.database import InfluxDatabase
from influxdb_client import Point
from app.utils.data_util import influx_parser, get_count

import pandas as pd
from app.db.mysql import crud
import glob
import re

async def get_datas(
    device_id: str = None, 
    start_time: str = None, 
    stop_time: str = None, 
    limit: int = 10,
    offset: int = 0,
    extra_fields: List[str] = None
):
    db = InfluxDatabase()
    filters = []
    
    if device_id:
        device_ids = device_id.split(',')
        device_id_filters = [f'r["device_id"] == "{id_}"' for id_ in device_ids]
        filters.append(f"({' or '.join(device_id_filters)})")

    if extra_fields and extra_fields[0] != '':
        extra_field_filters = [f'r["_field"] == "{field}"' for field in extra_fields]
        extra_field_filters.append('r["_field"] == "timestamp"')  # timestamp를 무조건 포함
        filters.append(f"({' or '.join(extra_field_filters)})")
    
    filter_query = " and ".join(filters) if filters else "true"

    count_query = f'''
        from(bucket: "{db.bucket}")
        |> range(start: {start_time if start_time else Config.DEFAULT_TIME_RANGE}, stop: {stop_time if stop_time else (datetime.datetime.now().isoformat()).split(".")[0]+"Z"})
        |> filter(fn: (r) => {filter_query})
        |> count(column: "_value")
    '''
    logging.info(f'Query from get_datas count total : {count_query}')
    table_count = db.query_data(count_query)
    result_count_json = table_count.to_json()
    total_count = get_count(result_count_json)

    query = f'''
        from(bucket: "{db.bucket}")
        |> range(start: {start_time if start_time else Config.DEFAULT_TIME_RANGE}, stop: {stop_time if stop_time else (datetime.datetime.now().isoformat()).split(".")[0]+"Z"})
        |> filter(fn: (r) => {filter_query})
        |> sort(columns: ["_time"], desc: true)
        |> limit(n: {limit}, offset: {offset})
    '''
    # |> limit(n: {limit}, offset: {offset}) 쿼리 제외, influx_parser에서 처리 
    logging.info(f'Query from get_datas : {query}')

    table = db.query_data(query)
    result = db.flux_to_json(table)

    parsed_result = influx_parser(query_result = result, total_count = total_count, offset = offset, limit = limit)

    return parsed_result


async def get_all_device_ids():
    db = InfluxDatabase()
    query = f'''
        from(bucket: "{db.bucket}")
        |> range(start: 0, stop: {(datetime.datetime.now().isoformat()).split(".")[0]+"Z"})
        |> distinct(column: "device_id")
        |> keep(columns: ["_value"])
    '''
    logging.info(f'Query to get all device_ids: {query}')
    try:
        table = db.query_data(query)
        result = table.to_json()
        device_ids = [row['_value'] for row in json.loads(result)]
        response = {"device_ids": list(set(device_ids))}
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    return response

async def get_device_ids_by_vehicle_type(vehicle_type: str, db: Session):
    try:
        vehicle_metadata = crud.get_vehicle_metadata_by_type(db, vehicle_type)
        if not vehicle_metadata:
            raise HTTPException(status_code=404, detail="Vehicle type not found")
        
        vehicle_type_id = vehicle_metadata.id

        device_ids = crud.get_device_ids_by_vehicle_type_id(db, vehicle_type_id)

        device_ids = [row[0] for row in device_ids]
        
        response = {"device_ids": list(set(device_ids))}
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
    return response


async def get_meta_and_predefined():
    response ={
        "vehicle_type_category_metadata":metadatas.vehicle_type_category_metadata,
        "vehicle_type_category_size_metadata":metadatas.vehicle_type_category_size_metadata,
        "vehicle_type_manufacturer_metadata":metadatas.vehicle_type_manufacturer_metadata,
        "vehicle_info_fuel_type_metadata":metadatas.vehicle_info_fuel_type_metadata,
        "initial_vehicle_type_metadata":metadatas.initial_vehicle_type_metadata,
        "censor_index_metadata":metadatas.censor_index_metadata
    }
    return response

async def data_download(vehicle_type: str, db: Session, start_time: Optional[datetime.datetime] = None, stop_time: Optional[datetime.datetime] = None):
    vehicle_metadata = crud.get_vehicle_metadata_by_type(db, vehicle_type)
    if not vehicle_metadata:
        raise HTTPException(status_code=404, detail="Vehicle type not found")

    vehicle_type_id = vehicle_metadata.id
    terminal_infos = crud.get_device_ids_by_vehicle_type_id(db, vehicle_type_id)
    
    created_files = []  # 생성된 파일들의 경로를 저장할 리스트
    current_timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

    for terminal_info_tuple in terminal_infos:
        terminal_info = terminal_info_tuple[0]
        directory_path = os.path.join(Config.DATA_STORAGE, terminal_info)
        if not os.path.exists(directory_path):
            logging.warning(f"Directory does not exist: {directory_path}")
            continue

        # 파일 목록을 검색하고 정렬
        # files_to_merge = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if is_file_in_date_range(f, start_time, stop_time)]
        # sort_files_by_timestamp(files_to_merge)

        files_to_merge = []
        for file_name in os.listdir(directory_path):
            file_path = os.path.join(directory_path, file_name)
            if (start_time is None and stop_time is None) or is_file_in_date_range(file_name, start_time, stop_time):
                files_to_merge.append(file_path)
        sort_files_by_timestamp(files_to_merge)

        # 병합할 파일들의 데이터를 리스트에 추가
        all_data = []
        for file_path in files_to_merge:
            with open(file_path, 'r') as file:
                # all_data.extend(file.readlines())
                all_data.append(file.readlines())
        
        folder_path = Path(f"{Config.DOWNLOAD_STORAGE}")
        folder_path.mkdir(parents=True, exist_ok=True)

        # 병합된 데이터를 DataFrame으로 변환하고 CSV 파일로 저장
        if all_data:
            if (start_time is None and stop_time is None):
                merged_file_name = f"{current_timestamp}_{vehicle_type}_{terminal_info}.csv"
            else:
                merged_file_name = f"{current_timestamp}_{start_time.strftime('%Y%m%dT%H%M%SZ')}_{stop_time.strftime('%Y%m%dT%H%M%SZ')}_{vehicle_type}_{terminal_info}.csv"
            
            merged_file_path = os.path.join(Config.DOWNLOAD_STORAGE, merged_file_name)
            with open(merged_file_path, 'w') as file:
                # file.writelines(all_data)
                for data in all_data:
                    file.write(' '.join(str(item) for item in data) + '\n')

            created_files.append(merged_file_path)

    # 생성된 파일들을 ZIP 파일로 압축
    if created_files:
        if (start_time is None and stop_time is None):
            zip_filename = f"{current_timestamp}_{vehicle_type}.zip"
        else:
            zip_filename = f"{current_timestamp}_{start_time.strftime('%Y%m%dT%H%M%SZ')}_{stop_time.strftime('%Y%m%dT%H%M%SZ')}_{vehicle_type}.zip"
        zip_filepath = os.path.join(Config.DOWNLOAD_STORAGE, zip_filename)

        with zipfile.ZipFile(zip_filepath, 'w') as zipf:
            for file in created_files:
                zipf.write(file, os.path.basename(file))
        
        return zip_filepath  # 생성된 ZIP 파일의 경로를 반환

    return None  # 파일이 없을 경우 None 반환

def sort_files_by_timestamp(files_list):
    def extract_timestamp(file_path):
        match = re.search(r'_(\d{8})(\d{6})?\.csv$', os.path.basename(file_path))
        return match.group(0) if match else "000000000000"
    files_list.sort(key=extract_timestamp, reverse=True)


def is_file_in_date_range(file_name, start_datetime, stop_datetime):
    match = re.search(r'_(\d{8})(\d{6})?\.csv$', file_name)
    if match:
        file_date_str = match.group(1)
        file_time_str = match.group(2) or "000000"  # HHMMSS 부분이 없으면 자정으로 가정
        file_datetime_str = f"{file_date_str}{file_time_str}"
        file_datetime = datetime.datetime.strptime(file_datetime_str, '%Y%m%d%H%M%S').replace(tzinfo=timezone.utc)
        # 파일 타임스탬프가 시작 및 종료 시간 범위 내인지 확인
        return start_datetime <= file_datetime <= stop_datetime
    return False



    