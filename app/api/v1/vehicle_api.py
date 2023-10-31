from sqlalchemy.orm import Session
from app.db.mysql.database import get_db
from fastapi import APIRouter
from fastapi import Query, Depends
from pydantic import BaseModel
from typing import List, Optional, Dict
from fastapi.responses import JSONResponse
from app.services import vehicle_service
import logging

router = APIRouter()

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
    request = {
        "vehicle_number":vehicle_number,
        "vehicle_type_name":vehicle_type_name,
        "year":year,
        "fuel_type":fuel_type,
        "terminal_info":terminal_info
    }
    logging.info(f"requested register vehicle data : {request}")
    
    response = await vehicle_service.create_vehicle_data(request=request, db=db)
    return JSONResponse(content=response)

@router.post("/register/vehicleType")
async def register_vehicle_type_data(vehicle_type_name: str,
                                     manufacturer : str,
                                     category : str,
                                     db: Session = Depends(get_db)):
    request = {
        "vehicle_type_name":vehicle_type_name,
        "manufacturer":manufacturer,
        "category":category
    }

    logging.info(f"requested register vehicle type data : {request}")
    
    response = await vehicle_service.create_vehicle_type_data(request=request, db=db)
    return JSONResponse(content=response)
# Read
@router.get("/get/vehicle")
async def get_vehicle_data(vehicle_number: str, 
                           db: Session = Depends(get_db)):
    response = await vehicle_service.get_vehicle_data(vehicle_number = vehicle_number, db = db)
    return JSONResponse(content=response)

@router.get("/get/vehicle")
async def get_vehicle_data(vehicle_number: str, 
                           db: Session = Depends(get_db)):
    response = await vehicle_service.get_vehicle_data(vehicle_number = vehicle_number, db = db)
    return JSONResponse(content=response)

@router.get("/get/all/vehicles")
async def read_all_vehicles(db: Session = Depends(get_db)):
    response = await vehicle_service.get_all_vehicle_data(db = db)
    return JSONResponse(content=response) 

@router.get("/get/all/vehicleTypes")
async def get_vehicle_type_data(db: Session = Depends(get_db)):
    response = await vehicle_service.get_vehicle_type(db = db)
    return JSONResponse(content=response)

# Update
@router.put("/update/vehicle")
async def update_vehicle_data(vehicle_number: str, 
                              request: UpdatevehicleData, 
                              db: Session = Depends(get_db)):
    response = await vehicle_service.update_vehicle_data(vehicle_number = vehicle_number, request = request.model_dump(), db = db)
    return JSONResponse(content=response)

@router.put("/update/vehicleType")
async def update_vehicle_type_data(vehicle_type_id: int, 
                                   request: UpdatevehicleTypeData, 
                                   db: Session = Depends(get_db)):
    response = await vehicle_service.update_vehicle_type_data(vehicle_type_id = vehicle_type_id, request = request.model_dump(), db = db)
    return JSONResponse(content=response)

# Delete
@router.delete("/delete/vehicle")
async def delete_vehicle_data(vehicle_number: str, 
                              db: Session = Depends(get_db)):
    response = await vehicle_service.delete_vehicle_data(vehicle_number = vehicle_number, db = db)
    return JSONResponse(content=response)

@router.delete("/delete/vehicleType")
async def delete_vehicle_type_data(vehicle_type: str, 
                                   db: Session = Depends(get_db)):
    response = await vehicle_service.delete_vehicle_type_data(vehicle_type = vehicle_type, db = db)
    return JSONResponse(content=response)