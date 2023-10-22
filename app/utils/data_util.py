import csv
from influxdb_client import Point

def create_point(file_path, timestamp, vehicle_id, device_id):
    point = Point("SensorData") \
            .tag("vehicle_id", vehicle_id) \
            .tag("device_id", device_id) \
            .time(timestamp)
    
    with open(file_path, mode='r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            for item in row:
                if "=" in item:
                    field, value = item.split("=")
                    try:
                        # Check if the value contains a decimal point
                        if '.' in value:
                            value = float(value)
                        else:
                            value = int(value)
                    except ValueError:
                        # If conversion fails, keep it as string
                        pass
                    point.field(field, value)
    return point