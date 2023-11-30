from fastapi import  HTTPException
from pathlib import Path
from sqlalchemy.orm import Session
from app.db.mysql import crud, metadatas
import datetime
from datetime import timezone, timedelta
from dateutil.parser import parse
from collections import defaultdict
import json
from config.config import Config
import zipfile
import pandas as pd
import os 
from typing import Optional, List
import logging
from app.db.influxdb.database import InfluxDatabase
from app.utils.data_util import get_count
from app.utils.file_util import get_device_ids, rearrange_csv_data, merge_files, delete_old_files
from app.db.mysql import crud
import re

logger = logging.getLogger()

async def get_datas(
    device_id: str = None, 
    start_time: str = None, 
    stop_time: str = None, 
    limit: int = 10,
    offset: int = 0,
    order : str = "DESC",
    # extra_fields: List[str] = None
):
    db = InfluxDatabase()
    filters = []
    desc = "true"

    if order == "ASC":
        desc = "false"
    elif order == "DESC":
        desc = "true"
    
    if device_id:
        device_ids = device_id.split(',')
        device_id_filters = [f'r["device_id"] == "{id_}"' for id_ in device_ids]
        filters.append(f"({' or '.join(device_id_filters)})")

    # if extra_fields and extra_fields[0] != '':
    #     extra_field_filters = [f'r["_field"] == "{field}"' for field in extra_fields]
    #     extra_field_filters.append('r["_field"] == "timestamp"')  # timestamp를 무조건 포함
    #     filters.append(f"({' or '.join(extra_field_filters)})")

    start_dt = datetime.datetime.fromisoformat(start_time) if start_time else datetime.datetime.now() - timedelta(days=30)
    stop_dt = datetime.datetime.fromisoformat(stop_time) if stop_time else datetime.datetime.now()

    if start_time and stop_time and start_time == stop_time:
        adjusted_start_time_str = (start_dt + timedelta(seconds=0)).isoformat()
        adjusted_stop_time_str = (stop_dt + timedelta(seconds=1)).isoformat()
    else:
        adjusted_start_time_str = (start_dt + timedelta(seconds=0)).isoformat()
        adjusted_stop_time_str = (stop_dt + timedelta(seconds=0)).isoformat()

    # "+" 또는 "." 문자를 기준으로 분할하여 Z 추가
    adjusted_start_time = re.split(r'\+|\.', adjusted_start_time_str)[0] + "Z"
    adjusted_stop_time = re.split(r'\+|\.', adjusted_stop_time_str)[0] + "Z"

    filter_query = " and ".join(filters) if filters else "true"

    count_query = f'''
        from(bucket: "{db.bucket}")
        |> range(start: {adjusted_start_time}, stop: {adjusted_stop_time})
        |> filter(fn: (r) => {filter_query})
        |> filter(fn: (r) => r["_field"] == "timestamp")
        |> count()
        |> yield(name: "count")
    '''
    logger.info(f'Query from get_datas count total : {count_query}')
    table_count = db.query_data(count_query)
    
    query = f'''
        from(bucket: "{db.bucket}")
        |> range(start: {adjusted_start_time}, stop: {adjusted_stop_time})
        |> filter(fn: (r) => {filter_query})
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> sort(columns: ["_time"], desc: {desc})
        |> limit(n: {limit}, offset: {offset})
    '''

    logger.info(f'Query from get_datas query : {query}')

    result_count_json = table_count.to_json()
    total_count = get_count(result_count_json)

    table = db.query_data(query)
    result = db.flux_to_json(table)
    
    parsed_result = db.influx_parser(query_result = result, total_count = total_count, offset = offset, limit = limit)

    return parsed_result

async def get_all_device_ids():
    db = InfluxDatabase()
    query = f'''
        from(bucket: "{db.bucket}")
        |> range(start: 0, stop: {(datetime.datetime.now().isoformat()).split(".")[0]+"Z"})
        |> distinct(column: "device_id")
        |> keep(columns: ["_value"])
    '''
    logger.info(f'Query to get all device_ids: {query}')
    try:
        table = db.query_data(query)
        result = table.to_json()
        device_ids = [row['_value'] for row in json.loads(result)]
        response = {"device_ids": list(set(device_ids))}
    except Exception as e:
        logger.info(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    return response

async def get_unassigned_device_ids(db: Session):
    try:
        assigned_terminals = crud.get_all_terminal_info(db=db)
        assigned_device_ids = [item[0] for item in assigned_terminals]
        uploaded_device_path = get_device_ids()
        uploaded_device_ids = [device_id.split("/")[-1] for device_id in uploaded_device_path]

        print(f"assigned_device_ids:{assigned_device_ids} , uploaded_device_ids:{uploaded_device_ids}")
        unassigned_device_ids = [device_id for device_id in uploaded_device_ids if device_id not in assigned_device_ids]
        response = {"unassigned_device_ids": unassigned_device_ids}
    except Exception as e:
        logger.info(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    return response
    
async def get_device_ids_by_vehicle_type(vehicle_type: str, db: Session):
    try:
        vehicle_metadata = crud.get_vehicle_metadata_by_type(db, vehicle_type)
        if not vehicle_metadata:
            raise HTTPException(status_code=404, detail="Vehicle type not found")
        
        vehicle_type_id = vehicle_metadata.id

        query_result = crud.get_device_ids_by_vehicle_type_id(db, vehicle_type_id)

        response = [
            {"vehicle_number": item[1], "device_id": item[0]}
            for item in query_result
            if item[0] and item[1]
        ]

    except Exception as e:
        logger.info(f"An error occurred: {e}")
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

async def data_download(vehicle_type: str, 
                        db: Session, 
                        device_ids : List[str]= None, 
                        start_time: Optional[datetime.datetime] = None, 
                        stop_time: Optional[datetime.datetime] = None,
                        order : str = "DESC"):

    for device_id in device_ids: merge_files(device_id)
    for device_id in device_ids: delete_old_files(device_id)
    for device_id in device_ids: rearrange_csv_data(device_id)

    vehicle_metadata = crud.get_vehicle_metadata_by_type(db, vehicle_type)
    if not vehicle_metadata:
        raise HTTPException(status_code=404, detail="Vehicle type not found")

    vehicle_type_id = vehicle_metadata.id
    terminal_infos = crud.get_device_ids_by_vehicle_type_id(db, vehicle_type_id)
    terminal_list = [info[0] for info in terminal_infos]

    if device_ids is not None:
        for device_id in device_ids:
            if device_id not in terminal_list:
                raise HTTPException(status_code=404, detail=f"device_id not found: {device_id}")
    
    created_files = []  # 생성된 파일들의 경로를 저장할 리스트
    current_timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

    if device_ids is None:
        terminal_infos_to_process = [terminal_info[0] for terminal_info in terminal_infos]
    else:
        terminal_infos_to_process = device_ids

    for terminal_info in terminal_infos_to_process:
        directory_path = os.path.join(Config.DATA_STORAGE, terminal_info)
        if not os.path.exists(directory_path):
            logger.warning(f"Directory does not exist: {directory_path}")
            continue

        files_to_merge = []
        for file_name in os.listdir(directory_path):
            file_path = os.path.join(directory_path, file_name)
            if (start_time is None and stop_time is None) or is_file_in_date_range(file_name, start_time, stop_time):
                files_to_merge.append(file_path)
            sort_files_by_timestamp(files_to_merge,order)

        # 병합할 파일들의 데이터를 리스트에 추가
        all_data = []
        for file_path in files_to_merge:
            file_name = os.path.basename(file_path)
            match = re.search(r'(\d{8})\.csv$', file_name)
            if match:
                with open(file_path, 'r') as file:
                    lines = file.readlines()
                    if match.group(1):  # 파일 이름이 YYYYMMDD 형식인 경우
                        cleaned_data = filter_data_by_timestamp(lines, start_time, stop_time)
                    else:  # 파일 이름이 YYYYMMDD 형식이 아닌 경우
                        cleaned_data = [remove_unnamed_columns(line) for line in lines]
                    all_data.append(cleaned_data)
            else:
                with open(file_path, 'r') as file:
                    cleaned_data = [remove_unnamed_columns(line) for line in file.readlines()]
                    all_data.append(cleaned_data)
        folder_path = Path(f"{Config.DOWNLOAD_STORAGE}")
        folder_path.mkdir(parents=True, exist_ok=True)

        if all_data:
            sort_data_by_timestamp(all_data,order)
            column_names = extract_column_names(all_data)
            
            # 메타데이터 불러오기
            meta_data = metadatas.censor_index_metadata
            # 메타데이터를 딕셔너리로 변환 (인덱스명을 키로, 한글 항목명을 값으로)
            meta_dict = {meta['index']: meta['item_name'] for meta in meta_data}
            # column_names를 한글 항목명(인덱스명) 형식으로 변경
            column_names_kr = [f"{meta_dict.get(name, '알 수 없음')}({name})" if name in meta_dict else name for name in column_names]

            rearranged_data = rearrange_data(all_data, column_names)
            if (start_time is None and stop_time is None):
                merged_file_name = f"{current_timestamp}_{vehicle_type}_{terminal_info}.csv"
            else:
                merged_file_name = f"{current_timestamp}_{start_time.strftime('%Y%m%dT%H%M%SZ')}_{stop_time.strftime('%Y%m%dT%H%M%SZ')}_{vehicle_type}_{terminal_info}.csv"
            
            merged_file_path = os.path.join(Config.DOWNLOAD_STORAGE, merged_file_name)
            rearranged_data_split = [line.split(',') for line in rearranged_data]

            df = pd.DataFrame(rearranged_data_split, columns=['시간(timestamp)', '위도(latitude)', '경도(longitude)'] + column_names_kr)
            df.to_csv(merged_file_path, index=True, index_label="No")
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
        return zip_filepath
    return None

def remove_unnamed_columns(line):
    # "Unnamed:"이 포함된 부분을 찾아서 제거
    return ','.join([item for item in line.split(',') if "Unnamed:" not in item])

def sort_files_by_timestamp(files_list,order):
    def extract_timestamp(file_path):
        match = re.search(r'_(\d{8})(\d{6})?\.csv$', os.path.basename(file_path))
        return match.group(0) if match else "000000000000"
    rvs = True
    if order == "ASC": rvs = False
    files_list.sort(key=extract_timestamp, reverse=rvs)

def sort_data_by_timestamp(data_list,order):
    def extract_timestamp(item):
        # 각 항목에서 타임스탬프 추출
        return item.split(',')[0] if item else ""
    rvs = True
    if order == "ASC": rvs = False
    for inner_list in data_list:
        inner_list.sort(key=extract_timestamp, reverse=rvs)

def filter_data_by_timestamp(lines, start_datetime, stop_datetime):
    filtered_lines = []
    for line in lines:
        # 타임스탬프 추출
        timestamp_str = line.split(',')[0]

        if not re.match(r'\d{14}', timestamp_str):
            continue
        
        try:
            timestamp = datetime.datetime.strptime(timestamp_str, '%Y%m%d%H%M%S')
            timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)
        except ValueError:
            # 타임스탬프 형식이 맞지 않으면 건너뛰기
            continue
        
        # 타임스탬프가 지정된 범위 내인지 확인
        if start_datetime and stop_datetime:
            if start_datetime <= timestamp <= stop_datetime:
                filtered_lines.append(line)
        else:
            filtered_lines.append(line)

    return filtered_lines

def is_file_in_date_range(file_name, start_datetime, stop_datetime):
    match = re.search(r'_(\d{8})(\d{6})?\.csv$', file_name)
    if match:
        file_date_str = match.group(1)
        file_time_str = match.group(2)

        # 날짜와 시간이 모두 있는 경우
        if file_time_str:
            file_datetime_str = f"{file_date_str}{file_time_str}"
            file_datetime = datetime.datetime.strptime(file_datetime_str, '%Y%m%d%H%M%S').replace(tzinfo=timezone.utc)
            return start_datetime <= file_datetime <= stop_datetime
        # 날짜만 있는 경우
        else:
            file_date = datetime.datetime.strptime(file_date_str, '%Y%m%d').date()
            start_date = start_datetime.date()
            stop_date = stop_datetime.date()
            return start_date <= file_date <= stop_date

    return False

def extract_column_names(all_data):
    column_names = set()
    for data in all_data:
        for line in data:
            items = line.split(',')
            for item in items[3:]: 
                if '=' in item:
                    column_name, _ = item.split('=', 1)
                    if column_name and '.' not in column_name:
                        column_names.add(column_name)
    return sorted(list(column_names))

def rearrange_data(all_data, column_names):
    rearranged_data = []
    for data in all_data:
        for line in data:
            items = line.split(',')
            # timestamp, 위도, 경도 데이터 추출
            line_data = items[:3]
            # 나머지 데이터를 딕셔너리로 변환
            data_dict = {item.split('=')[0]: item.split('=')[1] for item in items[3:] if '=' in item}
            # 정렬된 열 이름에 따라 데이터 재배열
            for col_name in column_names:
                line_data.append(data_dict.get(col_name, 'NA').strip("\n"))
            rearranged_data.append(','.join(line_data))
    return rearranged_data