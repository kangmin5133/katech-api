import os
import pandas as pd
import glob
from datetime import datetime, timedelta
from config.config import Config
from pathlib import Path
import logging
from .data_util import is_valid_timestamp, is_valid_latitude, is_valid_longitude
import re

logger = logging.getLogger()

def get_device_ids():
    data_storage_path =  Path(Config.DATA_STORAGE)
    # 디렉토리 목록을 리스트로 반환
    device_ids_dir_path = [str(dir) for dir in data_storage_path.iterdir() if dir.is_dir()]
    return device_ids_dir_path

def merge_files(device_id_dir_path: str):
    date_to_files = {}

    device_id = device_id_dir_path.split("/")[-1]
    folder_path = f"{Config.DATA_STORAGE}/{device_id}"
    all_files = glob.glob(os.path.join(folder_path, f"{device_id}_*.csv"))

    datetime_pattern = re.compile(r"_(\d{8})(\d{6})\.csv$")
    for file_path in all_files:
        match = datetime_pattern.search(file_path)
        if match:
            date = match.group(1)
            if date not in date_to_files:
                date_to_files[date] = []
            date_to_files[date].append(file_path)

    for date, files in date_to_files.items():
        files.sort()  
        all_columns = []
        merged_df = pd.DataFrame()
        for file_path in files:
            try:
                df = pd.read_csv(file_path, index_col=None, header=0)
                df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
                all_columns.append(list(df.columns))
            except pd.errors.EmptyDataError:
                logger.warning(f"Empty file: {file_path}")
                continue

        merged_filename = f"{folder_path}/{device_id}_{date}.csv"
        if all_columns:
            if os.path.exists(merged_filename):
                merged_df = pd.DataFrame(all_columns)
                merged_df.to_csv(merged_filename, mode='a', index=False, header=False)

            else:
                merged_df = pd.DataFrame(all_columns)
                merged_df.to_csv(merged_filename, index=False, header=False)

def delete_old_files(device_id_dir_path: str):

    device_id = device_id_dir_path.split("/")[-1]
    folder_path = f"{Config.DATA_STORAGE}/{device_id}"
    all_files = glob.glob(os.path.join(folder_path, f"{device_id}_*.csv"))

    date_time_pattern = re.compile(r"_(\d{14})\.csv$")

    for file_path in all_files:
        if date_time_pattern.search(file_path):
            try:
                os.remove(file_path)
            except Exception as e:
                logger.error(f"Error occurred while deleting file {file_path}: {e}")

def extract_datetime(file_name):
    parts = file_name.split('_')
    if len(parts) > 1:
        datetime_str = parts[1].split('.')[0]  # 확장자 제거
        # 날짜와 시간 부분이 충분히 길지 않다면 000000을 추가
        datetime_str = datetime_str.ljust(14, '0')
        return datetime.strptime(datetime_str, "%Y%m%d%H%M%S")
    return None

def rearrange_csv_data(device_id_dir_path):
    device_id = device_id_dir_path.split("/")[-1]
    folder_path = f"{Config.DATA_STORAGE}/{device_id}"
    all_files = glob.glob(os.path.join(folder_path, f"{device_id}_*.csv"))
    for file_path in all_files:
        # 파일 읽기
        with open(file_path, 'r') as file:
            lines = file.readlines()

        processed_lines = []

        for line in lines:
            row = line.strip().split(',')
            # 타임스탬프, 위도, 경도 확인 플래그
            timestamp, latitude, longitude = 'NA', 'NA', 'NA'

            # 전체 row에 대한 순회
            for item in row:
                if is_valid_timestamp(str(item)):
                    timestamp = item 
                if is_valid_latitude(item):
                    latitude = item 
                if is_valid_longitude(item):
                    longitude = item 

            # 유효한 타임스탬프가 있는 경우에만 처리
            if timestamp != 'NA':
                other_data = [item for item in row if item not in [timestamp, latitude, longitude] and item not in ['NA', 'NA.1']]
                rearranged_row = [timestamp, latitude, longitude] + other_data
                processed_lines.append(rearranged_row)
            elif any(item.strip() for item in row):
                processed_lines.append(row)
            

        processed_df = pd.DataFrame(processed_lines)
        processed_df.to_csv(file_path, index=False, header=False)

def clean_download_storage():
    all_downlaoded_files = glob.glob(Config.DOWNLOAD_STORAGE+"/*")
    for file_path in all_downlaoded_files:
        try:
            os.remove(file_path)
        except Exception as e:
            logger.error(f"Error occurred while deleting file {file_path}: {e}")

