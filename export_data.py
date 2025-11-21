# Créez export_data.py
from pymongo import MongoClient
import json

client = MongoClient('mongodb://localhost:27017/')
db = client['carburant_db']
stations = list(db['stations'].find({}))

# Convertir ObjectId en string pour JSON
for station in stations:
    station['_id'] = str(station['_id'])

with open('data/stations.json', 'w', encoding='utf-8') as f:
    json.dump(stations, f, ensure_ascii=False, indent=2)

print("✅ Données exportées vers data/stations.json")
