from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from fastapi import Query
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
    vehicle_type: str = Query(None, alias="vehicle_type"),
    limit: int = Query(10, alias="limit"),
    offset: int = Query(0, alias="offset"),
    extra_fields: ExtraFields = None
):
    logging.info(f"Get data requested with device_id: {device_id}, start_time: {start_time}, stop_time: {stop_time}, vehicle_type: {vehicle_type}, limit: {limit}, offset: {offset}, extra_fields: {extra_fields}")
    
    if extra_fields:
        extra_fields = extra_fields.extra_fields
    
    response = await data_service.get_datas(
        device_id=device_id, 
        start_time=start_time, 
        stop_time=stop_time, 
        vehicle_type=vehicle_type, 
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
async def get_device_ids_by_vehicle_type(vehicle_type: str):

    logging.info(f"requested vehicle_type : {vehicle_type}")
    if vehicle_type is None:
        raise HTTPException(status_code=400, detail="vehicle_type is missing in the param")
    
    response = await data_service.get_device_ids_by_vehicle_type(vehicle_type)
    return JSONResponse(content=response)


