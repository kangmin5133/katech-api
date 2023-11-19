from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from app.db.mysql import crud, database, schemas, metadatas
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.api.v1 import file_api, data_api, vehicle_api, static_api
from app.utils.file_util import merge_files, get_device_ids, delete_old_files
import logging
from config.logger_config import setup_logger

# Initialize FastAPI application
app = FastAPI()
setup_logger()
logger = logging.getLogger()

origins =[
    "http://127.0.0.1:8824",
    "http://210.113.122.196:8824"
    ]

# Middleware settings
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def job_end_listener(event):
    if event.exception:
        logging.info(f"The job crashed: {event.exception}")
    else:
        device_id_dir_path = get_device_ids()
        for device_id_dir_path in device_id_dir_path: delete_old_files(device_id_dir_path)

def file_merge():
    device_ids_dir_path = get_device_ids()
    for device_id_dir_path in device_ids_dir_path: merge_files(device_id_dir_path)

def file_delete():
    device_id_dir_path = get_device_ids()
    for device_id_dir_path in device_id_dir_path: delete_old_files(device_id_dir_path)
    
def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_listener(job_end_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    scheduler.add_job(file_merge, 'cron', hour=0)
    scheduler.start()

def create_predefined_data():
    engine = create_engine(database.DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()
    
    for metadata in metadatas.initial_vehicle_type_metadata:
        metadata_obj = schemas.VehicleMetadataCreate(**metadata)
        if not crud.get_vehicle_metadata_by_type(db, metadata["vehicle_type"]):
            crud.create_vehicle_metadata(db, metadata_obj)
    db.close()

@app.on_event("startup")
async def on_startup():
    file_merge()
    file_delete()
    start_scheduler()
    create_predefined_data()

@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"Incoming request: {request.method} {request.url}")
    logger.info(f"Headers: {request.headers}")
    response = await call_next(request)
    logger.info(f"Outgoing response: {response.status_code}")

    return response

app.include_router(file_api.router, prefix="/api/v1/file", tags=["file_upload_api"])
app.include_router(data_api.router, prefix="/api/v1/data", tags=["censor_data_api"])
app.include_router(vehicle_api.router, prefix="/api/v1/vehicle", tags=["vehicle_data_api"])
app.include_router(static_api.router, prefix="/api/v1/static", tags=["static_data_api"])
