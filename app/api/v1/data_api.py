from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from app.db.mysql.database import get_db
from fastapi import Query, Depends
from typing import List, Optional
from fastapi.responses import JSONResponse
from app.services import data_service
import logging

router = APIRouter()

class ExtraFields(BaseModel):
    extra_fields: List[str]

@router.post("/search")
async def get_data(
    device_id: str = Query(None, alias="device_id"),
    start_time: str = Query(None, alias="start_time"),
    stop_time: str = Query(None, alias="stop_time"),
    limit: int = Query(10, alias="limit"),
    offset: int = Query(0, alias="offset"),
    extra_fields: ExtraFields = None
):
    
    if device_id is None:
        raise HTTPException(status_code=400, detail="device_id is required in the params")
    
    logging.info(f"Get data requested with device_id: {device_id}, start_time: {start_time}, stop_time: {stop_time}, limit: {limit}, offset: {offset}, extra_fields: {extra_fields}")
    
    if extra_fields:
        extra_fields = extra_fields.extra_fields
    
    response = await data_service.get_datas(
        device_id=device_id, 
        start_time=start_time, 
        stop_time=stop_time, 
        limit=limit,
        offset=offset,
        extra_fields=extra_fields
    )
    return JSONResponse(content=response)

@router.get("/get/deviceId")
async def get_device_ids():
    response = await data_service.get_all_device_ids()
    return JSONResponse(content=response)

@router.get("/get/deviceIdBy/vehicleType")
async def get_device_ids_by_vehicle_type(vehicle_type: str, db : Session = Depends(get_db)):

    if vehicle_type is None:
        raise HTTPException(status_code=400, detail="vehicle_type is missing in the param")

    logging.info(f"requested vehicle_type : {vehicle_type}")
    
    response = await data_service.get_device_ids_by_vehicle_type(vehicle_type=vehicle_type,db=db)
    return JSONResponse(content=response)

@router.get("/get/metadata")
async def get_metadatas():
    logging.info(f"requested metadata")

    response = await data_service.get_meta_and_predefined()
    return JSONResponse(content=response)