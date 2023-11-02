import json

vehicle_type_category_size_metadata = [
    "경차","소형","준중형","중형","준대형","대형"
]

vehicle_type_category_metadata = [
    "SUV","SEDAN","COUPE","WAGON","CONVERTIBLE","HATCHBACK","LIMOUSINE","VAN","PICKUPTRUCK"
]

vehicle_type_manufacturer_metadata = [
    "Hyundai","Kia","Genesis","Renault","KGmobillity","Chevrolet"
]

vehicle_info_fuel_type_metadata = [
    "Hydrogen","Electronic", "Gasoline", "Diesel","LPG"
]

initial_vehicle_type_metadata = [
        {"vehicle_type": "EV6", "manufacturer": "Kia", "category": "준중형 SUV"},
        {"vehicle_type": "IONIQ6", "manufacturer": "Hyundai", "category": "중형 SEDAN"},
        {"vehicle_type": "IONIQ5", "manufacturer": "Hyundai", "category": "준중형 SUV"},
        {"vehicle_type": "GV70", "manufacturer": "Genesis", "category": "중형 SUV"},
        {"vehicle_type": "GV60", "manufacturer": "Genesis", "category": "준중형 SUV"},
        {"vehicle_type": "G80", "manufacturer": "Genesis", "category": "준대형 SEDAN"},
        {"vehicle_type": "NEXO", "manufacturer": "Hyundai", "category": "중형 SUV"},
    ]

def load_json_data(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data

censor_index_json_path = "app/db/mysql/extracted_data_final.json"
censor_index_metadata = load_json_data(censor_index_json_path)