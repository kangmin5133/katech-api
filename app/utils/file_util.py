import os
import pandas as pd
import glob
from datetime import datetime, timedelta
from config.config import Config
from pathlib import Path
import logging
import re

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
                all_columns.append(list(df.columns))
            except pd.errors.EmptyDataError:
                logging.warning(f"Empty file: {file_path}")
                continue

        merged_filename = f"{folder_path}/{device_id}_{date}.csv"
        if all_columns:
            merged_df = pd.DataFrame(all_columns)
            merged_df.to_csv(merged_filename, index=False, header=False)
            logging.info(f"{len(files)} files merged into {merged_filename}")

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
                logging.error(f"Error occurred while deleting file {file_path}: {e}")