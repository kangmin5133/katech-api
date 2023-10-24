import os
import pandas as pd
import glob
from datetime import datetime, timedelta
from config.config import Config
from pathlib import Path

def get_device_ids():
    data_storage_path =  Path(Config.DATA_STORAGE)
    # 디렉토리 목록을 리스트로 반환
    device_ids = [str(dir) for dir in data_storage_path.iterdir() if dir.is_dir()]
    return device_ids

def merge_files(device_id: str):

    folder_path = f"{Config.DATA_STORAGE}/{device_id}"
    # 현재 날짜 가져오기
    current_date_str = datetime.now().strftime("%Y%m%d")
    yesterday_date = datetime.now() - timedelta(days=1)
    yesterday_date_str = yesterday_date.strftime("%Y%m%d")

    # 디렉토리에서 파일 목록 가져오기
    all_files = glob.glob(os.path.join(folder_path, f"{device_id}_*.csv"))
    all_file_name = []
    for file_path in all_files:
        all_file_name.append(file_path.split("/")[-1])

    # 어제 날짜와 일치하는 파일만 선택
    selected_files = [f for f in all_file_name if yesterday_date_str in f.split('_')[1]]
    # 빈 DataFrame 생성
    merged_df = pd.DataFrame()

    all_columns = []

    # 각 파일을 읽어서 컬럼을 all_columns에 추가
    for filename in selected_files:
        df = pd.read_csv(f"{folder_path}/{filename}", index_col=None, header=0)
        all_columns.append(list(df.columns))

    # 병합된 컬럼을 새 파일에 저장
    if all_columns:
        merged_df = pd.DataFrame(all_columns)
        merged_filename = f"{folder_path}/{device_id}_{yesterday_date_str}.csv"
        merged_df.to_csv(merged_filename, index=False, header=False)