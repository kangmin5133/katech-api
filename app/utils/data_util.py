import csv
from influxdb_client import Point
from collections import defaultdict

def create_point(file_path, timestamp, vehicle_id, device_id):
    point = Point("SensorData") \
            .tag("vehicle_id", vehicle_id) \
            .tag("device_id", device_id) \
            .time(timestamp)
    
    with open(file_path, mode='r') as file:
        csv_reader = csv.reader(file)
        for i,row in enumerate(csv_reader):
            for i,item in enumerate(row):
                if i == 0: point.field("timestamp", row[i])
                elif i == 1: point.field("latitude", float(row[i]))
                elif i == 2: point.field("logitude", float(row[i]))
                elif "=" in item:
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

def influx_parser(query_result):
    parsed_data = defaultdict(dict)

    # For capturing unique 'device_id' and 'vehicle_id'
    device_id = None
    vehicle_id = None

    # Loop through each row in the sample data
    for row in query_result:
        timestamp = row['_time']
        field = row['_field']
        value = row['_value']

        # Capture 'device_id' and 'vehicle_id' if available
        device_id = row.get('device_id', device_id)
        vehicle_id = row.get('vehicle_id', vehicle_id)

        # Add the value to the appropriate timestamp and field in parsed_data
        parsed_data[timestamp][field] = value

    # Convert the defaultdict to a list of dictionaries
    final_data = [{"timestamp": timestamp, **fields} for timestamp, fields in parsed_data.items()]

    # Add 'device_id' and 'vehicle_id' to the final data
    final_data = {
        "device_id": device_id,
        "vehicle_id": vehicle_id,
        "data": final_data
    }

    return final_data