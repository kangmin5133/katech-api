from influxdb_client import InfluxDBClient
from config.config import Config
from influxdb_client.client.write_api import SYNCHRONOUS
from typing import List, Dict, Any
import datetime
import re

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
            
    def influx_parser(self,query_result, total_count, offset, limit):
        exclude_keys = ["result", "table", "_start", "_stop", "_time", "_measurement", "device_id"]
        grouped_data = {}

        # query_result를 돌면서 각 device_id별로 데이터 그룹화
        for entry in query_result:
            device_id = entry["device_id"]
            if device_id not in grouped_data:
                grouped_data[device_id] = []

            # 필요없는 키 제거
            filtered_entry = {k: v for k, v in entry.items() if k not in exclude_keys}
            # 필드 정렬 (문자열 + 숫자 형식일 경우 숫자 기준으로 정렬)
            sorted_keys = sorted(filtered_entry.keys(), key=lambda x: (re.split('(\d+)', x)[0], int(re.split('(\d+)', x)[1]) if len(re.split('(\d+)', x)) > 1 else 0))
            sorted_entry = {k: filtered_entry[k] for k in sorted_keys}

            if "logitude" in sorted_entry:
                sorted_entry["longitude"] = sorted_entry.pop("logitude")
            
            if "timestamp" in sorted_entry:
                timestamp_value = sorted_entry.pop("timestamp")
                sorted_entry = {"timestamp": timestamp_value, **sorted_entry}

            grouped_data[device_id].append(sorted_entry)

        # 그룹화된 데이터를 최종 결과 형식으로 변환
        parsed_data = [{"device_id": device_id, "data": data} for device_id, data in grouped_data.items()]

        result_with_meta = {
            "total": total_count,
            "offset": offset,
            "limit": limit,
            "count": sum(len(data["data"]) for data in parsed_data),
            "data": parsed_data,
        }

        return result_with_meta
