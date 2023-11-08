from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, NoResultFound
from app.db.mysql.schemas import VehicleInfoCreate, VehicleMetadataCreate
from app.db.mysql import crud
from typing import List, Optional, Dict
import re

async def create_vehicle_data(request: dict, db: Session):

    pattern = r"\d{2,3}[가-힣]\d{4}"
    if not re.fullmatch(pattern, request["vehicle_number"]):
        raise HTTPException(status_code=400, detail="Invalid vehicle number format")
    
    year_pattern = r"\d{4}"
    if not re.fullmatch(year_pattern, str(request["year"])):
        raise HTTPException(status_code=400, detail="Invalid year format")
    
    vehicle_metadata = crud.get_vehicle_metadata_by_type(db, request["vehicle_type_name"])
    
    if not vehicle_metadata:
        raise HTTPException(status_code=404, detail="Vehicle type not found")
    
    # terminal_info 중복 검사
    try:
        existing_terminal = crud.get_vehicle_info_by_terminal_info(db, request["terminal_info"])
        if existing_terminal:
            raise HTTPException(status_code=400, detail=f"There is duplicated terminal_info {request['terminal_info']}")
    except NoResultFound:
        pass  # terminal_info가 없으면, 생성 진행

    vehicle_info = VehicleInfoCreate(
        vehicle_number=request["vehicle_number"],
        vehicle_type_id=vehicle_metadata.id,
        year=request["year"],
        fuel_type=request["fuel_type"],
        terminal_info=request["terminal_info"]
    )
    try:
        result = crud.create_vehicle_info(db, vehicle_info)
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=400, detail="Vehicle number already exists") 
    return result.to_dict()

async def create_vehicle_type_data(request: dict, db: Session):

    vehicle_metadata = VehicleMetadataCreate(
        vehicle_type=request["vehicle_type_name"],
        manufacturer=request["manufacturer"],
        category=request["category"]
    )
    try:
        result = crud.create_vehicle_metadata(db, vehicle_metadata)
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=400, detail="Vehicle type already exists")

    return result.to_dict()

async def get_vehicle_data(vehicle_number: str, db: Session):
    result = crud.get_vehicle_info_with_metadata_by_number(db, vehicle_number)
    
    if not result:
        raise HTTPException(status_code=404, detail="Vehicle not found")

    vehicle_info, vehicle_metadata = result
    vehicle_info_dict = vehicle_info.to_dict()
    vehicle_metadata_dict = vehicle_metadata.to_dict()

    del vehicle_info_dict['vehicle_type_id']
    del vehicle_metadata_dict['id']

    combined_data = {**vehicle_info_dict, **vehicle_metadata_dict}

    return combined_data

async def get_all_vehicle_data(
    offset: int, 
    limit: int, 
    db: Session, 
    vehicle_types: Optional[str] = None
):
    # 전체 차량 수 계산
    if vehicle_types is None:
        total = crud.get_total_vehicle_count(db)
    else:
        vehicle_types = vehicle_types.split(",")
        total = sum(crud.get_vehicle_count_by_type(db, vehicle_type) for vehicle_type in vehicle_types)  # 리스트에 있는 모든 차량 타입의 수를 합산

    # 차량 정보와 메타데이터를 가져옴
    results = crud.get_all_vehicle_info_with_metadata(
        db=db, 
        offset=offset, 
        limit=limit, 
        vehicle_types=vehicle_types  # 리스트를 CRUD 함수에 전달
    )
    
    all_data = []
    for vehicle_info, vehicle_metadata in results:
        vehicle_info_dict = vehicle_info.to_dict()
        vehicle_metadata_dict = vehicle_metadata.to_dict()

        del vehicle_info_dict['vehicle_type_id']
        del vehicle_metadata_dict['id']

        combined_data = {**vehicle_info_dict, **vehicle_metadata_dict}
        all_data.append(combined_data)

    return all_data, total

async def get_vehicle_type(offset: int, limit: int, db: Session):
    total = crud.get_total_vehicle_type_count(db)
    result = crud.get_vehicle_metadatas(db=db, offset=offset, limit=limit)
    return [metadata.to_dict() for metadata in result], total

async def update_vehicle_data(vehicle_number : str, request : dict , db : Session):
    # 기존 차량 데이터 가져오기
    existing_vehicle = crud.get_vehicle_info_by_number(vehicle_number = vehicle_number, db = db)
    if not existing_vehicle:
        raise HTTPException(status_code=404, detail="Vehicle not found")
    
    # 차종 정보 가져오기
    vehicle_metadata = crud.get_vehicle_metadata_by_type(vehicle_type = request.get("vehicle_type_name"), db = db)
    if not vehicle_metadata:
        raise HTTPException(status_code=404, detail="Vehicle type not found")
        
    # 업데이트할 데이터 생성
    update_data = {
        "vehicle_type_id": vehicle_metadata.id,
        "year": request.get("year"),
        "fuel_type": request.get("fuel_type"),
        "terminal_info": request.get("terminal_info")
    }

    # 데이터 업데이트
    updated_vehicle = crud.update_vehicle_info(db, vehicle_number, update_data)
    return updated_vehicle.to_dict()

async def update_vehicle_type_data(vehicle_type_id : int, request : dict , db : Session):
    # 기존 차종 데이터 가져오기
    existing_metadata = crud.get_vehicle_metadata_by_id(metadata_id = vehicle_type_id,  db = db)
    if not existing_metadata:
        raise HTTPException(status_code=404, detail="Vehicle type not found")
    
    # 업데이트할 데이터 생성
    update_data = {
        "vehicle_type": request.get("vehicle_type"),
        "manufacturer": request.get("manufacturer"),
        "category": request.get("category")
    }

    # 데이터 업데이트
    updated_metadata = crud.update_vehicle_metadata(db, vehicle_type_id, update_data)
    return updated_metadata.to_dict()

async def delete_vehicle_data(vehicle_number:str, db:Session):
    
    pattern = r"\d{2,3}[가-힣]\d{4}"
    if not re.fullmatch(pattern, vehicle_number):
        raise HTTPException(status_code=400, detail="Invalid vehicle number format")

    # 데이터 삭제
    existing_vehicle = crud.get_vehicle_info_by_number(vehicle_number=vehicle_number, db=db)
    if not existing_vehicle:
        raise HTTPException(status_code=404, detail="Vehicle not found")

    # 데이터 삭제
    crud.delete_vehicle_info(db, vehicle_number)
    return {"message": "Vehicle data deleted successfully"}

async def delete_vehicle_type_data(vehicle_type:str, db:Session):
    # 차종 정보 가져오기
    vehicle_metadata = crud.get_vehicle_metadata_by_type(db, vehicle_type)
    if not vehicle_metadata:
        raise HTTPException(status_code=404, detail="Vehicle type not found")

    # 데이터 삭제
    crud.delete_vehicle_metadata(db, vehicle_metadata.id)
    return {"message": "Vehicle type data deleted successfully"}