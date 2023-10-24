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

    # Loop through each row in the sample data
    for row in query_result:
        timestamp = row['_time']
        field = row['_field']
        value = row['_value']
        
        # Add the value to the appropriate timestamp and field in parsed_data
        parsed_data[timestamp][field] = value

    # Convert the defaultdict to a list of dictionaries
    try:
        final_data = [{"timestamp": timestamp, **fields} for timestamp, fields in parsed_data.items()]
    except Exception as e:
        final_data = str(e)

    return final_data