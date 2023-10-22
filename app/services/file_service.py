from fastapi import  UploadFile, HTTPException
from pathlib import Path
import shutil
from tempfile import NamedTemporaryFile
import datetime
import json
from config.config import Config
import zipfile
import os 

from app.db.influxdb.database import InfluxDatabase
from influxdb_client import Point

from app.utils.data_util import create_point

async def upload_file(device_id:str,file:UploadFile):
    # Check file format
    # if file.content_type != "text/csv":
    #     raise HTTPException(status_code=400, detail="Invalid file format. Only CSV files are allowed.")
    
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

    #write to DB
    db = InfluxDatabase()
    point = create_point(file_path = file_path, 
                         timestamp = timestamp, 
                         device_id = device_id, 
                         vehicle_id= "1")
    
    # point = Point("SensorData") \
    #     .tag("vehicle_id", "1") \
    #     .tag("sensor_id", "T20231020") \
    #     .field("latitude", "37.5687") \
    #     .field("longitude", "126.9855") \
    #     .field("MCU_7", "1") \
    #     .field("BMS_1", "123.45") \
    #     .field("BMS_5", "-18") \
    #     .field("BMS_28", "123456789") \
    #     .time(timestamp)
    
    db.write_point_obj_data(point)

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