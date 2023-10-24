from fastapi import  UploadFile, HTTPException
from pathlib import Path
import shutil
from tempfile import NamedTemporaryFile
import datetime
import json
from config.config import Config
import zipfile
import os 
from typing import Optional, List
import logging
from app.db.influxdb.database import InfluxDatabase
from influxdb_client import Point
from app.utils.data_util import influx_parser

async def get_datas(
    device_id: str = None, 
    start_time: str = None, 
    stop_time: str = None, 
    vehicle_type: str = None,
    limit: int = 10,
    offset: int = 0,
    extra_fields: List[str] = None
):
    db = InfluxDatabase()
    filters = []
    
    if device_id:
        filters.append(f'r["device_id"] == "{device_id}"')

    if vehicle_type:
        filters.append(f'r["vehicle_id"] == "{vehicle_type}"')

    if extra_fields and extra_fields[0] != '':
        extra_field_filters = [f'r["_field"] == "{field}"' for field in extra_fields]
        extra_field_filters.append('r["_field"] == "timestamp"')  # timestamp를 무조건 포함
        filters.append(f"({' or '.join(extra_field_filters)})")
    
    filter_query = " and ".join(filters) if filters else "true"
    
    query = f'''
        from(bucket: "{db.bucket}")
        |> range(start: {start_time if start_time else Config.DEFAULT_TIME_RANGE}, stop: {stop_time if stop_time else "now()"})
        |> filter(fn: (r) => {filter_query})
        |> limit(n: {limit}, offset: {offset})
    '''
    logging.info(f'query from get_datas : {query}')

    table = db.query_data(query)
    result = db.flux_to_json(table)
    parsed_result = influx_parser(result)

    return parsed_result
