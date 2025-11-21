from pymongo import MongoClient
import random
from datetime import datetime, timedelta
import time

def generate_big_data(multiplier=10):
    """
    Génère un volume de données multiplié pour les tests de performance
    multiplier = 10 → 10x plus de données
    multiplier = 100 → 100x plus de données
    """
    
    client = MongoClient('mongodb://localhost:27017/')
    db = client['carburant_db']
    stations = db['stations']
    
    print(f"🚀 Génération de {multiplier}x plus de données...")
    
    # Sauvegarder les données originales dans une nouvelle collection
    original_stations = list(stations.find({}))
    
    if not original_stations:
        print("❌ Aucune donnée originale trouvée. Lancez d'abord la collecte.")
        return
    
    print(f"📊 Données originales: {len(original_stations)} stations")
    
    # Mesurer le temps d'insertion
    start_time = time.time()
    
    nouvelles_stations = []
    station_id_counter = 1000000  # Commencer à un ID élevé
    
    for i in range(multiplier - 1):  # -1 car on a déjà les données originales
        for station in original_stations:
            # Créer une copie modifiée de la station
            nouvelle_station = station.copy()
            
            # Important: supprimer l'_id pour éviter les conflits
            nouvelle_station.pop('_id', None)
            
            # Générer un nouvel ID unique
            nouvelle_station['id_station'] = f"BIG_{station_id_counter}"
            station_id_counter += 1
            
            # Modifier légèrement les coordonnées pour varier
            if nouvelle_station.get('latitude'):
                nouvelle_station['latitude'] = float(nouvelle_station['latitude']) + random.uniform(-0.1, 0.1)
            if nouvelle_station.get('longitude'):
                nouvelle_station['longitude'] = float(nouvelle_station['longitude']) + random.uniform(-0.1, 0.1)
            
            # Modifier légèrement les prix (variations réalistes)
            for carburant in nouvelle_station['carburants']:
                variation = random.uniform(-0.1, 0.1)  # ±10 centimes
                nouveau_prix = carburant['prix'] + variation
                carburant['prix'] = round(max(0.5, nouveau_prix), 3)  # Prix minimum 0.5€
            
            nouvelles_stations.append(nouvelle_station)
            
            # Insérer par lots pour optimiser les performances
            if len(nouvelles_stations) >= 1000:
                result = stations.insert_many(nouvelles_stations)
                print(f"   ✅ Lot de {len(nouvelles_stations)} stations inséré")
                nouvelles_stations = []
    
    # Insérer les dernières stations
    if nouvelles_stations:
        result = stations.insert_many(nouvelles_stations)
        print(f"   ✅ Dernier lot de {len(nouvelles_stations)} stations inséré")
    
    insertion_time = time.time() - start_time
    
    # Statistiques finales
    total_stations = stations.count_documents({})
    
    print(f"\n🎉 GÉNÉRATION TERMINÉE!")
    print(f"⏱️ Temps d'insertion: {insertion_time:.2f} secondes")
    print(f"📈 Stations originales: {len(original_stations)}")
    print(f"📈 Nouvelles stations: {len(original_stations) * (multiplier - 1)}")
    print(f"📊 Total en base: {total_stations} stations")
    print(f"📦 Taille approximative: {(total_stations * 0.5):.1f} MB")  # Estimation 0.5KB par station

def performance_test():
    """Test des performances avec les données actuelles"""
    
    client = MongoClient('mongodb://localhost:27017/')
    db = client['carburant_db']
    stations = db['stations']
    
    print("🧪 LANCEMENT DES TESTS DE PERFORMANCE")
    
    total_stations = stations.count_documents({})
    print(f"📊 Total stations en base: {total_stations}")
    
    # Test 1: Recherche simple
    start_time = time.time()
    results = list(stations.find({"ville": "Paris"}))
    temps_recherche_simple = time.time() - start_time
    print(f"1. Recherche 'Paris': {len(results)} résultats - {temps_recherche_simple:.4f}s")
    
    # Test 2: Recherche avec agrégation
    start_time = time.time()
    pipeline = [
        {"$unwind": "$carburants"},
        {"$match": {"carburants.type": "Gazole"}},
        {"$group": {"_id": "$ville", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ]
    results_agg = list(stations.aggregate(pipeline))
    temps_agregation = time.time() - start_time
    print(f"2. Agrégation Gazole par ville: {temps_agregation:.4f}s")
    
    # Test 3: Export CSV simulé
    start_time = time.time()
    all_stations = list(stations.find({}).limit(1000))  # Limiter pour le test
    # Simuler la création du CSV
    csv_data = []
    for station in all_stations:
        for carburant in station['carburants']:
            csv_data.append({
                'station': station['nom'],
                'ville': station['ville'],
                'carburant': carburant['type'],
                'prix': carburant['prix']
            })
    temps_export = time.time() - start_time
    print(f"3. Export CSV simulé (1000 stations): {temps_export:.4f}s")
    
    # Test 4: Compte total
    start_time = time.time()
    count = stations.count_documents({})
    temps_count = time.time() - start_time
    print(f"4. Compte total: {count} stations - {temps_count:.4f}s")
    
    print(f"\n📈 RÉSUMÉ DES PERFORMANCES:")
    print(f"   • Recherche simple: {temps_recherche_simple:.4f}s")
    print(f"   • Agrégation: {temps_agregation:.4f}s")
    print(f"   • Export: {temps_export:.4f}s")
    print(f"   • Compte: {temps_count:.4f}s")

if __name__ == "__main__":
    print("🔧 GÉNÉRATEUR DE DONNÉES BIG DATA")
    print("1. Générer 10x plus de données")
    print("2. Générer 100x plus de données") 
    print("3. Tester les performances actuelles")
    print("4. Réinitialiser aux données originales")
    
    choix = input("Choisissez une option (1-4): ")
    
    if choix == "1":
        generate_big_data(10)
    elif choix == "2":
        generate_big_data(100)
    elif choix == "3":
        performance_test()
    elif choix == "4":
        client = MongoClient('mongodb://localhost:27017/')
        db = client['carburant_db']
        stations = db['stations']
        # Garder seulement les 480 stations originales
        stations.delete_many({"id_station": {"$regex": "^BIG_"}})
        print("✅ Données réinitialisées aux 480 stations originales")
    else:
        print("❌ Option invalide")
