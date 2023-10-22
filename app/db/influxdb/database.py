from influxdb_client import InfluxDBClient
from config.config import Config
from influxdb_client.client.write_api import SYNCHRONOUS
from typing import List, Dict, Any
import datetime

class InfluxDatabase:
    def __init__(self):
        self.token = Config.INFLUX_TOKEN
        self.org = Config.INFLUX_ORG
        self.bucket = Config.INFLUX_BUCKET_NAME
        self.url = "http://katech-influx-db:8086"  # Docker Compose 서비스 이름과 포트를 사용
        self.client = InfluxDBClient(url=self.url, token=self.token)
    
    def write_data(self, data):
        write_api = self.client.write_api()
        write_api.write(self.bucket, self.org, data)

    def write_point_obj_data(self, point):
        write_api = self.client.write_api(write_options=SYNCHRONOUS)
        write_api.write(bucket=self.bucket, org=self.org, record=point)
    
    def query_data(self, query):
        query_api = self.client.query_api()
        result = query_api.query(org=self.org, query=query)
        return result
    
    def flux_to_json(self, tables) -> List[Dict[str, Any]]:
        results = []
        for table in tables:
            for record in table.records:
                entry = {}
                for key, value in record.values.items():
                    if isinstance(value, datetime.datetime):
                        entry[key] = value.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                    else:
                        entry[key] = value
                results.append(entry)
        return results