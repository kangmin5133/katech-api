from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.v1 import file_api, data_api
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
app.include_router(file_api.router, prefix="/api/v1/file", tags=["file_upload_api"])
app.include_router(data_api.router, prefix="/api/v1/data", tags=["censor_data_api"])
