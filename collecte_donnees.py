import requests
from pymongo import MongoClient
from datetime import datetime

def collecte_finale():
    """Version finale avec tous les correctifs"""
    
    client = MongoClient('mongodb://localhost:27017/')
    db = client['carburant_db']
    stations = db['stations']
    
    url = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/prix-des-carburants-en-france-flux-instantane-v2/exports/json"
    
    try:
        print("🚀 Lancement de la collecte finale...")
        response = requests.get(url, timeout=30)
        
        if response.status_code != 200:
            print(f"❌ Erreur HTTP: {response.status_code}")
            return
        
        data = response.json()
        print(f"📥 {len(data)} stations téléchargées depuis l'API")
        
        # Nettoyer l'ancienne collection
        stations.delete_many({})
        print("🧹 Anciennes données supprimées")
        
        stations_inserees = 0
        stations_ignorees = 0
        
        for i, station in enumerate(data[:500]):  # Prendre 500 stations max
            try:
                # Vérifier que la station a au moins un carburant et une ville valide
                has_carburant = any([
                    station.get('gazole_prix'),
                    station.get('sp95_prix'), 
                    station.get('sp98_prix'),
                    station.get('e85_prix'),
                    station.get('gplc_prix'),
                    station.get('e10_prix')
                ])
                
                has_ville = station.get('ville') and station.get('ville') != 'N/A'
                
                if not (has_carburant and has_ville):
                    stations_ignorees += 1
                    continue
                
                # 🔥 CORRECTION DES CHAMPS MANQUANTS 🔥
                nouvelle_station = {
                    "id_station": station.get('id', f'STATION_{i}'),
                    "nom": station.get('adresse', 'Station sans nom'),  # On utilise l'adresse comme nom
                    "adresse": station.get('adresse', 'Adresse non renseignée'),
                    "ville": station.get('ville', 'Ville inconnue'),
                    "code_postal": str(station.get('cp', '00000')),
                    "departement": station.get('departement', station.get('code_departement', 'Département inconnu')),  # Correction ici
                    "code_departement": station.get('code_departement', ''),
                    "region": station.get('region', 'Région inconnue'),
                    "latitude": station.get('latitude', 0),
                    "longitude": station.get('longitude', 0),
                    "services": station.get('services_service', []),
                    "horaires": station.get('horaires_automate_24_24', 'Non renseigné'),
                    "carburants": [],
                    "date_collecte": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                
                # 🔥 CORRECTION DES CARBURANTS - UTILISER LES BONNES CLÉS 🔥
                carburants_mapping = {
                    'gazole_prix': {'nom': 'Gazole', 'date': 'gazole_maj'},
                    'sp95_prix': {'nom': 'SP95', 'date': 'sp95_maj'},
                    'sp98_prix': {'nom': 'SP98', 'date': 'sp98_maj'},
                    'e85_prix': {'nom': 'E85', 'date': 'e85_maj'},
                    'gplc_prix': {'nom': 'GPLc', 'date': 'gplc_maj'},
                    'e10_prix': {'nom': 'E10', 'date': 'e10_maj'}
                }
                
                for api_key, infos in carburants_mapping.items():
                    prix = station.get(api_key)
                    if prix and prix > 0.5:  # Prix minimum réaliste (éviter les 0.001)
                        date_maj = station.get(infos['date'], datetime.now().strftime("%Y-%m-%d"))
                        nouvelle_station["carburants"].append({
                            "type": infos['nom'],
                            "prix": round(prix, 3),
                            "date_maj": date_maj
                        })
                
                # Insérer seulement si au moins 1 carburant valide
                if nouvelle_station["carburants"]:
                    stations.insert_one(nouvelle_station)
                    stations_inserees += 1
                    
                    # Afficher les 3 premières stations pour vérification
                    if stations_inserees <= 3:
                        print(f"\n🔍 EXEMPLE Station {stations_inserees}:")
                        print(f"   📍 {nouvelle_station['ville']} - {nouvelle_station['nom']}")
                        print(f"   ⛽ Carburants: {len(nouvelle_station['carburants'])}")
                        for carb in nouvelle_station['carburants']:
                            print(f"      - {carb['type']}: {carb['prix']}€")
                        
            except Exception as e:
                print(f"⚠️ Erreur sur la station {i}: {e}")
                continue
        
        # 📊 STATISTIQUES FINALES
        print(f"\n{'='*50}")
        print("🎉 COLLECTE TERMINÉE AVEC SUCCÈS!")
        print(f"{'='*50}")
        print(f"📥 Stations téléchargées: {len(data)}")
        print(f"✅ Stations insérées: {stations_inserees}")
        print(f"❌ Stations ignorées: {stations_ignorees}")
        
        total = stations.count_documents({})
        print(f"📊 Total en base MongoDB: {total} stations")
        
        # Répartition par carburant
        pipeline = [
            {"$unwind": "$carburants"},
            {"$group": {"_id": "$carburants.type", "count": {"$sum": 1}, "prix_moyen": {"$avg": "$carburants.prix"}}}
        ]
        stats = list(stations.aggregate(pipeline))
        
        print("\n⛽ STATISTIQUES PAR CARBURANT:")
        for stat in stats:
            print(f"   - {stat['_id']}: {stat['count']} stations, prix moyen: {stat['prix_moyen']:.3f}€")
        
        # Top 5 des villes avec le plus de stations
        pipeline_ville = [
            {"$group": {"_id": "$ville", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 5}
        ]
        top_villes = list(stations.aggregate(pipeline_ville))
        
        print(f"\n🏙️ TOP 5 des villes:")
        for ville in top_villes:
            print(f"   - {ville['_id']}: {ville['count']} stations")
            
        print(f"\n💾 Les données sont maintenant prêtes dans MongoDB!")
        print(f"🌐 Vous pouvez relancer Flask: python3 app.py")
        
    except Exception as e:
        print(f"❌ Erreur générale: {e}")

if __name__ == "__main__":
    collecte_finale()
