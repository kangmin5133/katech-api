from fastapi import  UploadFile, HTTPException
from pathlib import Path
import shutil
from tempfile import NamedTemporaryFile
import datetime
import json
from config.config import Config
import zipfile
import os 
import subprocess

from app.db.influxdb.database import InfluxDatabase
from influxdb_client import Point

from app.utils.data_util import create_point

async def upload_file(device_id:str,file:UploadFile):
    # Check file format
    # if file.content_type != "text/csv":
    #     raise HTTPException(status_code=400, detail="Invalid file format. Only CSV files are allowed.")
    
    # Create main storage directory if not exists
    storage_path = Path(Config.DATA_STORAGE)
    storage_path.mkdir(parents=True, exist_ok=True)

    # Create folder for each device_id
    folder_path = Path(f"{Config.DATA_STORAGE}/{device_id}")
    folder_path.mkdir(parents=True, exist_ok=True)

    # Generate timestamp and filename
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    file_name = f"{device_id}_{timestamp}.csv"
    file_path = folder_path / file_name

    # Save the file
    with NamedTemporaryFile(delete=False) as buffer:
        shutil.copyfileobj(file.file, buffer)
    shutil.move(buffer.name, file_path)

    cmd = f"chown -R tbelldev:tbelldev {Config.DATA_STORAGE}"
    subprocess.call(cmd, shell=True, stdin=subprocess.PIPE,
            universal_newlines=True)

    #write to influxDB
    # try:
        # for test data generator
        # test_vehicle_id = "EV6"
    db = InfluxDatabase()
    if device_id == "T20231023":
        test_vehicle_id = "EV6"

        point = create_point(file_path = file_path, 
                        timestamp = timestamp, 
                        device_id = device_id, 
                        vehicle_id= test_vehicle_id) # 임의 작성
    elif device_id == "T20231024":
        test_vehicle_id = "IONIQ6"

        point = create_point(file_path = file_path, 
                        timestamp = timestamp, 
                        device_id = device_id, 
                        vehicle_id= test_vehicle_id) # 임의 작성
    elif device_id == "T20231025":
        test_vehicle_id = "IONIQ5"

        point = create_point(file_path = file_path, 
                        timestamp = timestamp, 
                        device_id = device_id, 
                        vehicle_id= test_vehicle_id) # 임의 작성

    # db = InfluxDatabase()
    # point = create_point(file_path = file_path, 
    #                     timestamp = timestamp, 
    #                     device_id = device_id, 
    #                     vehicle_id= test_vehicle_id) # 임의 작성
    
    db.write_point_obj_data(point)
    # except:
    #     raise HTTPException(status_code=401, detail="errors while insert data to DB")

    return {"file_save_path": str(file_path)}

async def get_files(device_id:str):

    # Create folder for each device_id
    folder_path = Path(f"{Config.DATA_STORAGE}/{device_id}")

    if not folder_path.exists():
        raise HTTPException(status_code=404, detail="Device ID not found")
    
    files = [f.name for f in folder_path.iterdir() if f.is_file()]
    return {"files": files}

async def download_zip(device_id: str):
    folder_path = Path(f"{Config.DATA_STORAGE}/{device_id}")
    if not folder_path.exists():
        raise HTTPException(status_code=404, detail="Device ID not found")
    
    zip_file_path = Path(f"{Config.DATA_STORAGE}/{device_id}.zip")
    with zipfile.ZipFile(zip_file_path, 'w') as zipf:
        for root, _, files in os.walk(folder_path):
            for file in files:
                zipf.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), folder_path))
    
    return zip_file_path

async def download_file(device_id: str,file_name:str):
    file_path = Path(f"{Config.DATA_STORAGE}/{device_id}/{file_name}")
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return file_path

