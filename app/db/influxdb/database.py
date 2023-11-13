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
        self.measurement = Config.INFLUX_MEASURENENT
        self.url = "http://katech-influx-db:8086"  # Docker 컨테이너 이름 & 포트 사용
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
    
    def get_earliest_time(self):
        earliest_time_query = '''
            from(bucket: "{bucket}")
            |> range(start: 0)
            |> filter(fn: (r) => r._measurement == "{measurement}")
            |> first()
        '''.format(bucket=self.bucket, measurement=self.measurement)

        result = self.query_data(earliest_time_query)
        for table in result:
            for record in table.records:
                return record.get_time()  # 가장 초기의 타임스탬프 반환