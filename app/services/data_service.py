from fastapi import  UploadFile, HTTPException
from pathlib import Path
from sqlalchemy.orm import Session
from app.db.mysql import crud, metadatas
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
from app.utils.data_util import influx_parser, get_count

async def get_datas(
    device_id: str = None, 
    start_time: str = None, 
    stop_time: str = None, 
    limit: int = 10,
    offset: int = 0,
    extra_fields: List[str] = None
):
    db = InfluxDatabase()
    filters = []
    
    if device_id:
        device_ids = device_id.split(',')
        device_id_filters = [f'r["device_id"] == "{id_}"' for id_ in device_ids]
        filters.append(f"({' or '.join(device_id_filters)})")

    if extra_fields and extra_fields[0] != '':
        extra_field_filters = [f'r["_field"] == "{field}"' for field in extra_fields]
        extra_field_filters.append('r["_field"] == "timestamp"')  # timestamp를 무조건 포함
        filters.append(f"({' or '.join(extra_field_filters)})")
    
    filter_query = " and ".join(filters) if filters else "true"

    count_query = f'''
        from(bucket: "{db.bucket}")
        |> range(start: {start_time if start_time else Config.DEFAULT_TIME_RANGE}, stop: {stop_time if stop_time else (datetime.datetime.now().isoformat()).split(".")[0]+"Z"})
        |> filter(fn: (r) => {filter_query})
        |> count(column: "_value")
    '''
    logging.info(f'Query from get_datas count total : {count_query}')
    table_count = db.query_data(count_query)
    result_count_json = table_count.to_json()
    total_count = get_count(result_count_json)

    query = f'''
        from(bucket: "{db.bucket}")
        |> range(start: {start_time if start_time else Config.DEFAULT_TIME_RANGE}, stop: {stop_time if stop_time else (datetime.datetime.now().isoformat()).split(".")[0]+"Z"})
        |> filter(fn: (r) => {filter_query})
        |> limit(n: {limit}, offset: {offset})
    '''
    logging.info(f'Query from get_datas : {query}')

    table = db.query_data(query)
    result = db.flux_to_json(table)

    parsed_result = influx_parser(query_result = result, total_count = total_count, offset = offset, limit = limit)

    return parsed_result


async def get_all_device_ids():
    db = InfluxDatabase()
    query = f'''
        from(bucket: "{db.bucket}")
        |> range(start: 0, stop: {(datetime.datetime.now().isoformat()).split(".")[0]+"Z"})
        |> distinct(column: "device_id")
        |> keep(columns: ["_value"])
    '''
    logging.info(f'Query to get all device_ids: {query}')
    try:
        table = db.query_data(query)
        result = table.to_json()
        device_ids = [row['_value'] for row in json.loads(result)]
        response = {"device_ids": list(set(device_ids))}
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    return response

async def get_device_ids_by_vehicle_type(vehicle_type: str, db: Session):
    try:
        vehicle_metadata = crud.get_vehicle_metadata_by_type(db, vehicle_type)
        if not vehicle_metadata:
            raise HTTPException(status_code=404, detail="Vehicle type not found")
        
        vehicle_type_id = vehicle_metadata.id

        device_ids = crud.get_device_ids_by_vehicle_type_id(db, vehicle_type_id)

        device_ids = [row[0] for row in device_ids]
        
        response = {"device_ids": list(set(device_ids))}
    except Exception as e:
        logging.error(f"An error occurred: {e}")
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