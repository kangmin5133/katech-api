from datetime import timedelta

class Config(object):
    TESTING = True
    DEBUG=True
    PASS_LOGIN_REQUIRED = False
    
    HOST="0.0.0.0"
    PORT=8820

    # jwt  
    
    # cors
    CORS_RESOURCES={r"*": {"origins": ["*"]}}
    
    # 
    JSON_AS_ASCII=False

    # default image directory in container
    DATA_STORAGE = "/workspace/data"

    #influxDB token
    INFLUX_TOKEN = ""
    INFLUX_ORG = "tbell"
    INFLUX_USER = "katech"
    INFLUX_PASSWD = "tbell0518"
    INFLUX_BUCKET_NAME = "censorData"