from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from app.db.mysql import crud, models, database, schemas
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker,Session
from app.api.v1 import file_api, data_api, vehicle_api
from app.utils.file_util import merge_files, get_device_ids
import logging

# Initialize FastAPI application
app = FastAPI()


origins =[
    "http://127.0.0.1:8824"
    ]

# Middleware settings
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def file_merge():
    device_ids = get_device_ids()
    for device_id in device_ids: merge_files(device_id)

def start_scheduler():
    scheduler = BackgroundScheduler()
    # scheduler.add_job(file_merge, 'interval',minutes=1) # for test
    scheduler.add_job(file_merge, 'cron', hour=0) # cron trigger to active job at specific time
    scheduler.start()

def create_predefined_data():
    engine = create_engine(database.DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()
    initial_vehicle_metadata = [
        {"vehicle_type": "EV6", "manufacturer": "kia", "category": "준중형 SUV"},
        {"vehicle_type": "IONIQ6", "manufacturer": "hyundai", "category": "중형 SEDAN"},
        {"vehicle_type": "IONIQ5", "manufacturer": "hyundai", "category": "준중형 SUV"},
        {"vehicle_type": "GV70", "manufacturer": "hyundai", "category": "중형 SUV"},
        {"vehicle_type": "GV60", "manufacturer": "hyundai", "category": "준중형 SUV"},
        {"vehicle_type": "G80", "manufacturer": "hyundai", "category": "준대형 SEDAN"},
        {"vehicle_type": "NEXO", "manufacturer": "hyundai", "category": "중형 SUV"},
    ]
    for metadata in initial_vehicle_metadata:
        metadata_obj = schemas.VehicleMetadataCreate(**metadata)
        if not crud.get_vehicle_metadata_by_type(db, metadata["vehicle_type"]):
            crud.create_vehicle_metadata(db, metadata_obj)
    db.close()

@app.on_event("startup")
async def on_startup():
    start_scheduler()
    create_predefined_data()

@app.middleware("http")
async def log_requests(request: Request, call_next):
    logging.info(f"Incoming request: {request.method} {request.url}")
    logging.info(f"Headers: {request.headers}")
    response = await call_next(request)
    logging.info(f"Outgoing response: {response.status_code}")
    return response

app.include_router(file_api.router, prefix="/api/v1/file", tags=["file_upload_api"])
app.include_router(data_api.router, prefix="/api/v1/data", tags=["censor_data_api"])
app.include_router(vehicle_api.router, prefix="/api/v1/vehicle", tags=["vehicle_data_api"])
