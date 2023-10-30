from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class VehicleMetadata(Base):
    __tablename__ = 'vehicle_metadata'

    id = Column(Integer, primary_key=True, autoincrement=True)
    vehicle_type = Column(String(50), nullable=False, unique=True)
    manufacturer = Column(String(50), nullable=False)
    category = Column(String(50), nullable=False)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    vehicles = relationship('VehicleInfo', back_populates='vehicle_metadata')


class VehicleInfo(Base):
    __tablename__ = 'vehicle_info'

    vehicle_number = Column(String(50), primary_key=True)
    vehicle_type_id = Column(Integer, ForeignKey('vehicle_metadata.id'), nullable=False)
    year = Column(Integer, nullable=False)
    fuel_type = Column(String(50), nullable=False)
    terminal_info = Column(String(50))

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    vehicle_metadata = relationship('VehicleMetadata', back_populates='vehicles')
