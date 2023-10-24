from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from app.api.v1 import file_api, data_api
from app.utils.file_util import merge_files, get_device_ids
# Initialize FastAPI application
app = FastAPI()

# Middleware settings
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def merge_files():
    device_ids = get_device_ids()
    for device_id in device_ids: merge_files(device_id)

def start_scheduler():
    scheduler = BackgroundScheduler()
    # scheduler.add_job(merge_files, 'interval',minutes=1) # for test
    scheduler.add_job(merge_files, 'cron', hour=0) # cron trigger to active job at specific time
    scheduler.start()

@app.on_event("startup")
async def on_startup():
    start_scheduler()

app.include_router(file_api.router, prefix="/api/v1/file", tags=["file_upload_api"])
app.include_router(data_api.router, prefix="/api/v1/data", tags=["censor_data_api"])
