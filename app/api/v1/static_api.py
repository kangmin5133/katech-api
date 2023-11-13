from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from datetime import datetime
from sqlalchemy.orm import Session
from app.db.mysql.database import get_db
from fastapi import Query, Depends
from typing import List, Optional
from fastapi.responses import JSONResponse, FileResponse
from app.services import static_service
import logging
import os

router = APIRouter()

@router.get("/count/vehicles")
async def count_vehicles_by_vehicle_type(db : Session = Depends(get_db)):
    response = await static_service.get_vehicle_count_by_type(db)
    return JSONResponse(content=response) 

@router.get("/count/data/by/window")
async def get_sensor_data_count_by_window(window:str = Query("1h")):
    if window is None:
        raise HTTPException(status_code=400, detail="window is required in the params")
    
    result = await static_service.get_sensor_data_count_by_window(window = window)
    return result