from alembic.config import Config
from alembic import command
from alembic.script import ScriptDirectory
from alembic.runtime.migration import MigrationContext
from sqlalchemy import create_engine

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from app.db.mysql import crud, database, schemas, metadatas
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.api.v1 import file_api, data_api, vehicle_api, static_api
from app.utils.file_util import merge_files, get_device_ids, delete_old_files, rearrange_csv_data, clean_download_storage
import logging
from config.logger_config import setup_logger

def check_and_run_migrations():
    engine = create_engine(database.DATABASE_URL)
    conn = engine.connect()

    # 현재 데이터베이스의 리비전 확인
    context = MigrationContext.configure(conn)
    current_rev = context.get_current_revision()

    # Alembic 설정 로드
    alembic_cfg = Config("alembic.ini")
    script = ScriptDirectory.from_config(alembic_cfg)

    # 최신 리비전 확인
    head_rev = script.get_current_head()

    if current_rev != head_rev:
        logger.info("Database is not up to date. Running migrations...")
        command.upgrade(alembic_cfg, "head")
        logger.info("Migrations completed successfully.")
    else:
        logger.info("Database is up to date. No migrations needed.")

    conn.close()

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
        logger.info(f"The job crashed: {event.exception}")
    else:
        device_id_dir_path = get_device_ids()
        for device_id_dir_path in device_id_dir_path: delete_old_files(device_id_dir_path)
        clean_download_storage()

def file_merge():
    device_ids_dir_path = get_device_ids()
    for device_id_dir_path in device_ids_dir_path: merge_files(device_id_dir_path)
    for device_id_dir_path in device_ids_dir_path: rearrange_csv_data(device_id_dir_path)

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
    check_and_run_migrations() 
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
