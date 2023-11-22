from sqlalchemy.orm import Session
from . import models, schemas
from sqlalchemy import func
from typing import List, Optional

# VehicleMetadata CRUD
def create_vehicle_metadata(db: Session, vehicle_metadata: schemas.VehicleMetadataCreate):
    db_vehicle_metadata = models.VehicleMetadata(**vehicle_metadata.model_dump())
    db.add(db_vehicle_metadata)
    db.commit()
    db.refresh(db_vehicle_metadata)
    return db_vehicle_metadata

def get_vehicle_metadatas(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.VehicleMetadata).offset(skip).limit(limit).all()

def get_vehicle_metadata_by_id(db: Session, metadata_id: int):
    return db.query(models.VehicleMetadata).filter(models.VehicleMetadata.id == metadata_id).first()

def update_vehicle_metadata(db: Session, metadata_id: int, vehicle_metadata: schemas.VehicleMetadataCreate):
    db.query(models.VehicleMetadata).filter(models.VehicleMetadata.id == metadata_id).update(vehicle_metadata)
    db.commit()
    return get_vehicle_metadata_by_id(db, metadata_id)

def get_vehicle_metadata_by_type(db: Session, vehicle_type: str):
    return db.query(models.VehicleMetadata).filter(models.VehicleMetadata.vehicle_type == vehicle_type).first()

def delete_vehicle_metadata(db: Session, metadata_id: int):
    db.query(models.VehicleMetadata).filter(models.VehicleMetadata.id == metadata_id).delete()
    db.commit()
    return {"message": "Deleted successfully"}

# VehicleInfo CRUD
def create_vehicle_info(db: Session, vehicle_info: schemas.VehicleInfoCreate):
    db_vehicle_info = models.VehicleInfo(**vehicle_info.model_dump())
    db.add(db_vehicle_info)
    db.commit()
    db.refresh(db_vehicle_info)
    return db_vehicle_info

def get_total_vehicle_count(db: Session):
    return db.query(func.count(models.VehicleInfo.vehicle_number)).scalar()

def get_all_vehicle_info_with_metadata(
    db: Session, 
    offset: int, 
    limit: int, 
    vehicle_types: Optional[List[str]] = None
):
    query = db.query(models.VehicleInfo, models.VehicleMetadata)\
        .join(models.VehicleMetadata, models.VehicleInfo.vehicle_type_id == models.VehicleMetadata.id)
    
    if vehicle_types:
        query = query.filter(models.VehicleMetadata.vehicle_type.in_(vehicle_types))
    
    return query.offset(offset).limit(limit).all()

def get_vehicle_count_by_type(db: Session, vehicle_type: str):
    # 해당 vehicle_type에 맞는 VehicleMetadata의 id를 찾음
    vehicle_metadata_id = db.query(models.VehicleMetadata.id)\
        .filter(models.VehicleMetadata.vehicle_type == vehicle_type)\
        .scalar()

    if vehicle_metadata_id:
        # VehicleInfo 테이블에서 해당 vehicle_type_id를 가진 차량 수를 계산
        return db.query(models.VehicleInfo)\
            .filter(models.VehicleInfo.vehicle_type_id == vehicle_metadata_id)\
            .count()
    else:
        # 주어진 vehicle_type에 해당하는 VehicleMetadata가 없으면 0 반환
        return 0

def get_total_vehicle_type_count(db: Session):
    return db.query(func.count(models.VehicleMetadata.id)).scalar()

def get_vehicle_metadatas(db: Session, offset: int, limit: int):
    return db.query(models.VehicleMetadata).offset(offset).limit(limit).all()

def get_vehicle_type_by_name(db: Session, type_name: str):
    return db.query(models.VehicleMetadata).filter(models.VehicleMetadata.vehicle_type == type_name).first()

def get_vehicle_infos(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.VehicleInfo).offset(skip).limit(limit).all()

def get_vehicle_info_by_number(db: Session, vehicle_number: str):
    return db.query(models.VehicleInfo).filter(models.VehicleInfo.vehicle_number == vehicle_number).first()

def update_vehicle_info(db: Session, vehicle_number: str, vehicle_info: schemas.VehicleInfoCreate):
    db.query(models.VehicleInfo).filter(models.VehicleInfo.vehicle_number == vehicle_number).update(vehicle_info)
    db.commit()
    return get_vehicle_info_by_number(db, vehicle_number)

def delete_vehicle_info(db: Session, vehicle_number: str):
    db.query(models.VehicleInfo).filter(models.VehicleInfo.vehicle_number == vehicle_number).delete()
    db.commit()
    return {"message": "Deleted successfully"}


def get_vehicle_info_with_metadata_by_number(db: Session, vehicle_number: str):
    return (
        db.query(models.VehicleInfo, models.VehicleMetadata)
        .join(models.VehicleMetadata, models.VehicleInfo.vehicle_type_id == models.VehicleMetadata.id)
        .filter(models.VehicleInfo.vehicle_number == vehicle_number)
        .first()
    )

def get_device_ids_by_vehicle_type_id(db: Session, vehicle_type_id: int):
    return db.query(models.VehicleInfo.terminal_info)\
        .filter(models.VehicleInfo.vehicle_type_id == vehicle_type_id)\
        .all()

def get_vehicle_info_by_terminal_info(db: Session, terminal_info: str):
    return db.query(models.VehicleInfo).filter(models.VehicleInfo.terminal_info == terminal_info).first()

def get_vehicle_info_by_vehicle_type_id(db: Session, vehicle_type_id: int, offset: int, limit: int):
    return db.query(models.VehicleInfo)\
        .filter(models.VehicleInfo.vehicle_type_id == vehicle_type_id).offset(offset).limit(limit)\
        .all()

def count_vehicle_by_type(db: Session):
    vehicle_counts = db.query(
        models.VehicleMetadata.vehicle_type,
        func.count(models.VehicleInfo.vehicle_number).label('count')
    ).join(
        models.VehicleMetadata,
        models.VehicleInfo.vehicle_type_id == models.VehicleMetadata.id
    ).group_by(
        models.VehicleMetadata.vehicle_type
    ).all()

    return {vehicle_type: count for vehicle_type, count in vehicle_counts}