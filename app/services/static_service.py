from fastapi import  UploadFile, HTTPException
from pathlib import Path
from sqlalchemy.orm import Session
from app.db.mysql import crud, metadatas
import shutil
from tempfile import NamedTemporaryFile
import datetime
from datetime import timezone
import json
from config.config import Config
import zipfile
import os 
from typing import Optional, List
import logging
from app.db.influxdb.database import InfluxDatabase
from influxdb_client import Point
from app.utils.data_util import influx_parser, get_count

import pandas as pd
from app.db.mysql import crud
import glob
import re

async def get_vehicle_count_by_type(db: Session):
    vehicle_counts = crud.count_vehicle_by_type(db)
    
    data_formatted = [
        {"id": vehicle_type, "label": vehicle_type, "value": count}
        for vehicle_type, count in vehicle_counts.items()
    ]
    
    response = {
        "vehicle_type_total": len(vehicle_counts),
        "data": data_formatted
    }
    
    return response