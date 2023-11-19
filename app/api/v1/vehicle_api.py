from sqlalchemy.orm import Session
from app.db.mysql.database import get_db
from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel
from typing import Optional
from fastapi.responses import JSONResponse
from app.services import vehicle_service
import logging

router = APIRouter()
logger = logging.getLogger()

class UpdatevehicleData(BaseModel):
    vehicle_type_name : str
    year : int
    fuel_type : str
    terminal_info : str

class UpdatevehicleTypeData(BaseModel):
    vehicle_type: str
    manufacturer : str
    category : str

# Create
@router.post("/register/vehicle")
async def register_vehicle_data(vehicle_number: str,
                                vehicle_type_name : str,
                                year : str,
                                fuel_type : str,
                                terminal_info : str,
                                db: Session = Depends(get_db)):
    
    if vehicle_number is None:
        raise HTTPException(status_code=400, detail="vehicle_number is required in the params")
    
    if vehicle_type_name is None:
        raise HTTPException(status_code=400, detail="vehicle_type_name is required in the params")
    
    if year is None:
        raise HTTPException(status_code=400, detail="year is required in the params")
    
    if fuel_type is None:
        raise HTTPException(status_code=400, detail="fuel_type is required in the params")

    request = {
        "vehicle_number":vehicle_number,
        "vehicle_type_name":vehicle_type_name,
        "year":year,
        "fuel_type":fuel_type,
        "terminal_info":terminal_info
    }
    logger.info(f"requested register vehicle data : {request}")
    
    response = await vehicle_service.create_vehicle_data(request=request, db=db)
    return JSONResponse(content=response)

@router.post("/register/vehicleType")
async def register_vehicle_type_data(vehicle_type_name: str,
                                     manufacturer : str,
                                     size : str,
                                     category : str,
                                     db: Session = Depends(get_db)):
    
    
    if vehicle_type_name is None:
        raise HTTPException(status_code=400, detail="vehicle_type_name is required in the params")
    
    if manufacturer is None:
        raise HTTPException(status_code=400, detail="manufacturer is required in the params")
    
    if size is None:
        raise HTTPException(status_code=400, detail="size is required in the params")

    if category is None:
        raise HTTPException(status_code=400, detail="category is required in the params")

    request = {
        "vehicle_type_name":vehicle_type_name,
        "manufacturer":manufacturer,
        "category": size+" "+category
    }

    logger.info(f"requested register vehicle type data : {request}")
    
    response = await vehicle_service.create_vehicle_type_data(request=request, db=db)
    return JSONResponse(content=response)
# Read
@router.get("/get/vehicle")
async def get_vehicle_data(vehicle_number: str, 
                           db: Session = Depends(get_db)):
    
    if vehicle_number is None:
        raise HTTPException(status_code=400, detail="vehicle_number is required in the params")
    
    response = await vehicle_service.get_vehicle_data(vehicle_number = vehicle_number, db = db)
    return JSONResponse(content=response)

@router.get("/get/all/vehicles")
async def read_all_vehicles(offset: int = Query(0), 
                            limit: int = Query(10),
                            vehicle_types: Optional[str] = None,
                            db: Session = Depends(get_db)):
    
    response, total = await vehicle_service.get_all_vehicle_data(offset=offset, limit=limit, vehicle_types=vehicle_types, db=db)
    final_response  = {"total": total, "count": len(response), "data": response}
    return JSONResponse(content=final_response) 

@router.get("/get/all/vehicleTypes")
async def get_vehicle_type_data(offset: int = Query(0), 
                                limit: int = Query(10),
                                db: Session = Depends(get_db)):
    
    response, total = await vehicle_service.get_vehicle_type(offset=offset, limit=limit, db=db)
    final_response  = {"total": total, "count": len(response), "data": response}
    return JSONResponse(content=final_response)

@router.get("/get/vehicle/location")
async def get_vehicle_terminal_location_data(device_id: str,
                                            start_time: str = Query(None, alias="start_time"),
                                            stop_time: str = Query(None, alias="stop_time"),
                                            ):
    
    if device_id is None:
        raise HTTPException(status_code=400, detail="device_id is required in the params")
    
    response = await vehicle_service.get_terminal_gps(device_id = device_id,start_time=start_time,stop_time=stop_time)
    return JSONResponse(content=response)

# Update
@router.put("/update/vehicle")
async def update_vehicle_data(vehicle_number: str, 
                              request: UpdatevehicleData, 
                              db: Session = Depends(get_db)):
    
    if vehicle_number is None:
        raise HTTPException(status_code=400, detail="vehicle_number is required in the params")
    
    if request is None:
        raise HTTPException(status_code=400, detail="No Body data received")

    response = await vehicle_service.update_vehicle_data(vehicle_number = vehicle_number, request = request.model_dump(), db = db)
    return JSONResponse(content=response)

@router.put("/update/vehicleType")
async def update_vehicle_type_data(vehicle_type_id: int, 
                                   request: UpdatevehicleTypeData, 
                                   db: Session = Depends(get_db)):
    
    if vehicle_type_id is None:
        raise HTTPException(status_code=400, detail="vehicle_type_id is required in the params")
    
    if request is None:
        raise HTTPException(status_code=400, detail="No Body data received")

    response = await vehicle_service.update_vehicle_type_data(vehicle_type_id = vehicle_type_id, request = request.model_dump(), db = db)
    return JSONResponse(content=response)

# Delete
@router.delete("/delete/vehicle")
async def delete_vehicle_data(vehicle_number: str, 
                              db: Session = Depends(get_db)):
    
    if vehicle_number is None or vehicle_number == "" :
        raise HTTPException(status_code=400, detail="vehicle_number is missing in the param")
    
    response = await vehicle_service.delete_vehicle_data(vehicle_number = vehicle_number, db = db)
    return JSONResponse(content=response)

@router.delete("/delete/vehicleType")
async def delete_vehicle_type_data(vehicle_type: str, 
                                   db: Session = Depends(get_db)):
    
    if vehicle_type is None or vehicle_type == "" :
        raise HTTPException(status_code=400, detail="vehicle_type is missing in the param")
    
    response = await vehicle_service.delete_vehicle_type_data(vehicle_type = vehicle_type, db = db)
    return JSONResponse(content=response)