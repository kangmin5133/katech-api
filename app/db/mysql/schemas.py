from pydantic import BaseModel
from typing import List, Optional

class VehicleMetadataBase(BaseModel):
    vehicle_type: str
    manufacturer: str
    category: str

class VehicleMetadataCreate(VehicleMetadataBase):
    pass

class VehicleMetadata(VehicleMetadataBase):
    id: int
    
    class Config:
        from_attributes = True

class VehicleInfoBase(BaseModel):
    vehicle_number: str
    vehicle_type_id: int
    year: int
    fuel_type: str
    terminal_info: Optional[str] = None

class VehicleInfoCreate(VehicleInfoBase):
    pass

class VehicleInfo(VehicleInfoBase):
    vehicle_metadata: VehicleMetadata

    class Config:
        from_attributes = True