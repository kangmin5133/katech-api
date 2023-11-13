from fastapi import  UploadFile, HTTPException
from pathlib import Path
from sqlalchemy.orm import Session
from app.db.mysql import crud, metadatas
import shutil
from tempfile import NamedTemporaryFile
import datetime
from datetime import timezone, timedelta
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

async def get_vehicle_count_by_type(db: Session):
    vehicle_counts = crud.count_vehicle_by_type(db)
    
    data_formatted = [
        {"id": vehicle_type, "label": vehicle_type, "value": count}
        for vehicle_type, count in vehicle_counts.items()
    ]
    
    response = {
        "vehicle_type_total": len(vehicle_counts),
        "data": data_formatted
    }
    
    return response

async def get_sensor_data_count_by_window(window: str):
    db = InfluxDatabase()
    
    start_time_str = (datetime.datetime.now() - timedelta(days=30)).isoformat().split(".")[0]+"Z"
    start_time = datetime.datetime.fromisoformat(start_time_str)

    stop_time_str = datetime.datetime.now().isoformat().split(".")[0]+"Z"
    stop_time = datetime.datetime.fromisoformat(stop_time_str)

    total_duration = stop_time - start_time
    window_duration_seconds = parse_window_to_seconds(window)
    total_index = int(total_duration.total_seconds() / window_duration_seconds)

    count_by_window_query = f'''
            from(bucket: "{db.bucket}")
            |> range(start: {Config.DEFAULT_TIME_RANGE}, stop: {stop_time_str})
            |> filter(fn: (r) => r._measurement == "{db.measurement}")
            |> filter(fn: (r) => r._field == "timestamp")
            |> window(every: {window})
            |> count()
            |> yield(name: "count")
        '''
    logging.info(f'Query for getting sensor data count by window: {count_by_window_query}')
    result = db.query_data(count_by_window_query)

    data_for_chart = [{"index": i, "id": "timestamp", "count": 0} for i in range(total_index)]

    for table in result:
        for record in table.records:
            start_of_window = record.get_start()
            index = int((start_of_window - start_time).total_seconds() / window_duration_seconds)
            if 0 <= index < total_index:
                data_for_chart[index]["count"] = record.get_value()

    return data_for_chart

def parse_window_to_seconds(window: str):
    unit_to_seconds = {'h': 3600, 'd': 86400}
    for unit in unit_to_seconds:
        if window.endswith(unit):
            return int(window.replace(unit, '')) * unit_to_seconds[unit]
    raise ValueError(f"Invalid window format: {window}")



