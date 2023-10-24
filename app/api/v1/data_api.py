from fastapi import APIRouter, File, UploadFile, Header, HTTPException
from pydantic import BaseModel
from fastapi import Query
from typing import List, Optional
from fastapi.responses import JSONResponse, FileResponse
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
    logging.info(f"Get data requested with device_id: {device_id}, start_time: {start_time}, \
                 stop_time: {stop_time}, vehicle_type: {vehicle_type}, limit: {limit}, offset: {offset}, extra_fields: {extra_fields}")
    
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

