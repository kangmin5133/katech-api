from datetime import timedelta

class Config(object):
    TESTING = True
    DEBUG=True
    PASS_LOGIN_REQUIRED = False
    
    HOST="0.0.0.0"
    PORT=8820

    # jwt  
    
    # cors
    # CORS_RESOURCES={r"*": {"origins": ["*"]}}
    
    # 
    JSON_AS_ASCII=False

    # default data directory in container
    DATA_STORAGE = "/workspace/data/devices"
    DOWNLOAD_STORAGE = "/workspace/data/downloads"

    #influxDB token
    INFLUX_TOKEN = "ppV2K10Ljpz095xDXQyvblXvrAVLLIE4K37E49fLkr2kvnt10VKenDy_07yTkKeH4zNicNjaeXfnoVjisSnUNA=="
    INFLUX_ORG = "tbell"
    INFLUX_USER = "katech"
    INFLUX_PASSWD = "tbell0518"
    INFLUX_BUCKET_NAME = "katech-censor"
    INFLUX_MEASURENENT = "SensorData"

    DEFAULT_TIME_RANGE = "-30d"
    MAX_TIME_RANGE = "-90d"