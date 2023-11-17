from fastapi import APIRouter, File, UploadFile, Header, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from app.services import file_service
import logging

router = APIRouter()
logger = logging.getLogger()

@router.post("/upload")
async def upload_file(device_id: str = Header(None), file: UploadFile = File(...)):
    # Check validation
    logger.info(f"requested device id : {device_id}")
    if device_id is None:
        raise HTTPException(status_code=400, detail="Device ID is missing in the header")
    
    if file is None:
        raise HTTPException(status_code=400, detail="File is missing in form")

    # service
    response = await file_service.upload_file(device_id = device_id, file = file)
    logger.info(f"response : {response}")
    return JSONResponse(content=response)

@router.get("/search")
async def search_file(device_id: str):
    # Check validation
    logger.info(f"requested device id : {device_id}")
    if device_id is None:
        raise HTTPException(status_code=400, detail="Device ID is missing in the param")

    # service
    response = await file_service.get_files(device_id = device_id)
    logger.info(f"response : {response}")
    return JSONResponse(content=response)

@router.post("/download")
async def download_file(device_id: str, file_name:str= None):
    # Check validation
    logger.info(f"requested device id : {device_id}")
    if device_id is None:
        raise HTTPException(status_code=400, detail="Device ID is missing in the param")
    
    if file_name is None:
        # service
        response = await file_service.download_zip(device_id = device_id)
        logger.info(f"download files response : {response}")
        return FileResponse(response, headers={"Content-Disposition": f"attachment; filename={device_id}.zip"})
    
    else:
        response = await file_service.download_file(device_id = device_id, file_name = file_name)
        logger.info(f"download file response : {response}")
        return FileResponse(response, headers={"Content-Disposition": f"attachment; filename={file_name}"})


    