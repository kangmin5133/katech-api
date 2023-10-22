import uvicorn
import logging

logging.basicConfig(level=logging.INFO, filename='logs/app.log', format='%(asctime)s %(message)s')

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8820, reload=True)