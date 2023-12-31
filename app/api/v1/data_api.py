from fastapi import APIRouter, HTTPException
from datetime import datetime
from sqlalchemy.orm import Session
from app.db.mysql.database import get_db
from fastapi import Query, Depends
from typing import Optional
from fastapi.responses import JSONResponse, FileResponse
from app.services import data_service
import logging
import os

router = APIRouter()
logger = logging.getLogger()

@router.get("/search")
async def get_data(
    device_id: str = Query(None, alias="device_id"),
    start_time: str = Query(None, alias="start_time"),
    stop_time: str = Query(None, alias="stop_time"),
    limit: int = Query(10, alias="limit"),
    offset: int = Query(0, alias="offset"),
    order : str = Query("ASC", alias="order"),
):
    
    if device_id is None:
        raise HTTPException(status_code=400, detail="device_id is required in the params")
    if order not in ["ASC","DESC"]:
        raise HTTPException(status_code=400, detail="order param supports ASC or DESC")
    
    logger.info(f"Get data requested with device_id: {device_id}, start_time: {start_time}, stop_time: {stop_time}, limit: {limit}, offset: {offset}, order:{order}")
    
    response = await data_service.get_datas(
        device_id=device_id, 
        start_time=start_time, 
        stop_time=stop_time, 
        limit=limit,
        offset=offset,
        order=order,
    )
    return JSONResponse(content=response)

@router.get("/get/deviceId")
async def get_device_ids():
    response = await data_service.get_all_device_ids()
    return JSONResponse(content=response)

@router.get("/get/unassigned/deviceId")
async def get_unasigned_device_ids(db : Session = Depends(get_db)):
    response = await data_service.get_unassigned_device_ids(db=db)
    return JSONResponse(content=response)

@router.get("/get/deviceIdBy/vehicleType")
async def get_device_ids_by_vehicle_type(vehicle_type: str, db : Session = Depends(get_db)):

    if vehicle_type is None:
        raise HTTPException(status_code=400, detail="vehicle_type is missing in the param")

    logger.info(f"requested vehicle_type : {vehicle_type}")
    response = await data_service.get_device_ids_by_vehicle_type(vehicle_type=vehicle_type,db=db)
    return JSONResponse(content=response)

@router.get("/get/metadata")
async def get_metadatas():
    logger.info("metadata requested")
    response = await data_service.get_meta_and_predefined()
    return JSONResponse(content=response)

@router.get("/download")
async def download_datas(
    vehicle_type: str,  
    device_id : str = None,
    start_time: Optional[datetime] = None,
    stop_time: Optional[datetime] = None,
    order :Optional[str] = "DESC",
    db : Session = Depends(get_db)
):
    device_ids = []
    if device_id is not None:
        if "," in device_id: 
            device_ids = device_id.split(",")
        else:
            device_ids.append(device_id)

    if vehicle_type is None:
        raise HTTPException(status_code=400, detail="vehicle_type is missing in the param")

    response = await data_service.data_download(vehicle_type = vehicle_type, device_ids = device_ids, start_time = start_time, stop_time = stop_time, order = order,db = db)

    if response:
        return FileResponse(response, filename=os.path.basename(response), media_type='application/zip')
    
    raise HTTPException(status_code=404, detail="No files found for the given parameters.")