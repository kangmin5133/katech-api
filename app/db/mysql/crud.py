from sqlalchemy.orm import Session
from . import models, schemas
from sqlalchemy.orm import join

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

def get_all_vehicle_info_with_metadata(db: Session):
    return db.query(models.VehicleInfo, models.VehicleMetadata).\
            join(models.VehicleMetadata, models.VehicleInfo.vehicle_type_id == models.VehicleMetadata.id).all()