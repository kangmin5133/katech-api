from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from app.db.mysql.schemas import VehicleInfoCreate, VehicleMetadataCreate
from app.db.mysql import crud

async def create_vehicle_data(request: dict, db: Session):
    # 차량 유형을 먼저 조회합니다.
    vehicle_metadata = crud.get_vehicle_metadata_by_type(db, request["vehicle_type_name"])
    
    if not vehicle_metadata:
        raise HTTPException(status_code=404, detail="Vehicle type not found")
    
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

async def get_all_vehicle_data(db: Session):
    results = crud.get_all_vehicle_info_with_metadata(db)
    
    all_data = []
    for vehicle_info, vehicle_metadata in results:
        vehicle_info_dict = vehicle_info.to_dict()
        vehicle_metadata_dict = vehicle_metadata.to_dict()

        del vehicle_info_dict['vehicle_type_id']
        del vehicle_metadata_dict['id']

        combined_data = {**vehicle_info_dict, **vehicle_metadata_dict}
        all_data.append(combined_data)

    return all_data

async def get_vehicle_type(db: Session):
    result = crud.get_vehicle_metadatas(db=db)
    return [metadata.to_dict() for metadata in result]

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
    crud.update_vehicle_info(db, vehicle_number, update_data)
    return {"message": "Vehicle data updated successfully"}

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
    crud.update_vehicle_metadata(db, vehicle_type_id, update_data)
    return {"message": "Vehicle type data updated successfully"}

async def delete_vehicle_data(vehicle_number:str, db:Session):
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