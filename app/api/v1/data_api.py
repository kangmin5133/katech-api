from fastapi import APIRouter, File, UploadFile, Header, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from app.services import data_service
import logging

router = APIRouter()

@router.get("/search")
async def get_data(device_id: str):
    logging.info(f"get data requested device id : {device_id}")
    response = await data_service.get_datas(device_id=device_id)
    logging.info(f"response : {response}")
    return JSONResponse(content=response)