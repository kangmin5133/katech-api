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

async def get_datas(device_id: str):
    db = InfluxDatabase()

    query = f'''
        from(bucket: "{db.bucket}")
        |> range(start: -1h)
        |> filter(fn: (r) => r["device_id"] == "{device_id}")
    '''
    table = db.query_data(query)
    result = db.flux_to_json(table)
    print("result : ",result)
    return result
