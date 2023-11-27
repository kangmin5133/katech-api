from fastapi import  UploadFile, HTTPException
from pathlib import Path
import shutil
from tempfile import NamedTemporaryFile
import datetime
from config.config import Config
import zipfile
import os 
import subprocess
import logging
from app.db.influxdb.database import InfluxDatabase

from app.utils.data_util import create_point
from app.utils.file_util import extract_datetime

logger = logging.getLogger()

async def upload_file(device_id:str,file:UploadFile):
    logger.info(f"file received {file.filename}")
    # Create main storage directory if not exists
    storage_path = Path(Config.DATA_STORAGE)
    storage_path.mkdir(parents=True, exist_ok=True)

    # Create folder for each device_id
    folder_path = Path(f"{Config.DATA_STORAGE}/{device_id}")
    folder_path.mkdir(parents=True, exist_ok=True)

    file_name = file.filename
    file_path = folder_path / file_name

    # Save the file
    with NamedTemporaryFile(delete=False) as buffer:
        shutil.copyfileobj(file.file, buffer)
    shutil.move(buffer.name, file_path)

    cmd = f"chown -R tbelldev:tbelldev {Config.DATA_STORAGE}"
    subprocess.call(cmd, shell=True, stdin=subprocess.PIPE,
            universal_newlines=True)

    #write to influxDB
    try:
        db = InfluxDatabase()
        point = create_point(file_path = file_path, 
                            # timestamp = timestamp, 
                            device_id = device_id, 
                            ) 
        logger.info(f"created point for file {file_path}\n Point Object : {point}")
        db.write_point_obj_data(point)
    except Exception as e:
        logger.error(f"file saved But, errors while write point data to Database with {point}")
        print(e)

    return {"file_save_path": str(file_path)}

async def get_files(device_id:str):

    # Create folder for each device_id
    folder_path = Path(f"{Config.DATA_STORAGE}/{device_id}")

    if not folder_path.exists():
        raise HTTPException(status_code=404, detail="Device ID not found")
    
    files = sorted(
        (f.name for f in folder_path.iterdir() if f.is_file()),
        key=extract_datetime,
        reverse=False  # 최신 파일이 먼저 오도록 하려면 reverse=True
    )
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

