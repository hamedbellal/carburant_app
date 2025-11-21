from flask import Flask, render_template, request, jsonify, send_file
from pymongo import MongoClient
import pandas as pd
import json
import time
import random
from bson import json_util

app = Flask(__name__)

# Connexion MongoDB
def get_db():
    client = MongoClient('mongodb://localhost:27017/')
    return client['carburant_db']

# Route principale
@app.route('/')
def index():
    return render_template('index.html')

# Route pour la recherche avancée
@app.route('/recherche', methods=['POST'])
def recherche():
    try:
        db = get_db()
        stations = db['stations']
        
        # Récupérer tous les paramètres
        ville = request.form.get('ville', '').strip()
        carburant = request.form.get('carburant', '').strip()
        departement = request.form.get('departement', '').strip()
        prix_min = request.form.get('prix_min', '')
        prix_max = request.form.get('prix_max', '')
        
        # Construire la requête MongoDB
        query = {}
        
        if ville:
            query['ville'] = {'$regex': ville, '$options': 'i'}
        
        if departement:
            query['code_departement'] = departement
        
        if carburant:
            query['carburants.type'] = carburant
            
        # Filtre de prix avancé
        if prix_min or prix_max:
            prix_query = {}
            if prix_min:
                prix_query['$gte'] = float(prix_min)
            if prix_max:
                prix_query['$lte'] = float(prix_max)
            
            query['carburants.prix'] = prix_query
        
        # Exécuter la requête
        results = list(stations.find(query))
        
        # Convertir pour JSON
        results_json = json.loads(json_util.dumps(results))
        
        return render_template('results.html', 
                             results=results_json, 
                             ville=ville, 
                             carburant=carburant,
                             departement=departement,
                             prix_min=prix_min,
                             prix_max=prix_max,
                             count=len(results))
    
    except Exception as e:
        return f"Erreur lors de la recherche: {str(e)}", 500

@app.route('/statistiques')
def statistiques():
    try:
        db = get_db()
        stations = db['stations']
        
        # Statistiques générales
        total_stations = stations.count_documents({})
        
        # Prix moyens par carburant
        pipeline_prix = [
            {"$unwind": "$carburants"},
            {"$group": {
                "_id": "$carburants.type", 
                "moyenne": {"$avg": "$carburants.prix"},
                "minimum": {"$min": "$carburants.prix"},
                "maximum": {"$max": "$carburants.prix"},
                "count": {"$sum": 1}
            }}
        ]
        stats_prix = list(stations.aggregate(pipeline_prix))
        
        # Stations par département
        pipeline_dep = [
            {"$group": {"_id": "$code_departement", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        top_departements = list(stations.aggregate(pipeline_dep))
        
        # Convertir les Decimal en float pour la sérialisation JSON
        stats_prix_serializable = []
        for stat in stats_prix:
            stats_prix_serializable.append({
                "_id": stat["_id"],
                "moyenne": float(stat["moyenne"]) if stat["moyenne"] is not None else 0,
                "minimum": float(stat["minimum"]) if stat["minimum"] is not None else 0,
                "maximum": float(stat["maximum"]) if stat["maximum"] is not None else 0,
                "count": stat["count"]
            })
        
        top_departements_serializable = []
        for dep in top_departements:
            top_departements_serializable.append({
                "_id": dep["_id"] if dep["_id"] else "Inconnu",
                "count": dep["count"]
            })
        
        return render_template('statistiques.html',
                             total_stations=total_stations,
                             stats_prix=stats_prix_serializable,
                             top_departements=top_departements_serializable)
    
    except Exception as e:
        return f"Erreur lors du calcul des statistiques: {str(e)}", 500
#***********************************************************************************


# Route pour la page performance
@app.route('/performance')
def performance():
    db = get_db()
    stations = db['stations']
    total_stations = stations.count_documents({})
    return render_template('performance.html', total_stations=total_stations)

# Route pour générer des données Big Data
@app.route('/generate-big-data/<int:multiplier>')
def generate_big_data_route(multiplier):
    # Implémentation simplifiée pour la démo web
    db = get_db()
    stations = db['stations']
    
    original_count = stations.count_documents({"id_station": {"$not": {"$regex": "^BIG_"}}})
    
    if multiplier > 10:
        return "⚠️ Multiplicateur trop élevé pour la version web. Utilisez le script en console.", 400
    
    new_stations = []
    station_id_counter = 1000000
    
    for i in range(multiplier - 1):
        original_stations = list(stations.find({"id_station": {"$not": {"$regex": "^BIG_"}}}).limit(100))
        
        for station in original_stations:
            new_station = station.copy()
            new_station.pop('_id', None)
            new_station['id_station'] = f"BIG_{station_id_counter}"
            station_id_counter += 1
            
            # Modifier légèrement les prix
            for carburant in new_station['carburants']:
                variation = random.uniform(-0.05, 0.05)
                carburant['prix'] = round(carburant['prix'] + variation, 3)
            
            new_stations.append(new_station)
    
    if new_stations:
        stations.insert_many(new_stations)
    
    new_total = stations.count_documents({})
    return f"✅ {len(new_stations)} nouvelles stations ajoutées. Total: {new_total}"

# Route pour réinitialiser les données
@app.route('/reset-data')
def reset_data():
    db = get_db()
    stations = db['stations']
    # Supprimer seulement les données générées (BIG_)
    result = stations.delete_many({"id_station": {"$regex": "^BIG_"}})
    remaining = stations.count_documents({})
    return f"✅ {result.deleted_count} stations BIG DATA supprimées. Reste: {remaining} stations originales"

# Route pour lancer les tests de performance
@app.route('/run-tests')
def run_tests():
    db = get_db()
    stations = db['stations']
    
    tests = []
    
    # Test 1: Recherche simple
    start = time.time()
    results = list(stations.find({"ville": "Paris"}))
    temps1 = time.time() - start
    tests.append({
        "nom": "Recherche Paris",
        "temps": temps1,
        "resultats": f"{len(results)} stations"
    })
    
    # Test 2: Recherche carburant
    start = time.time()
    results = list(stations.find({"carburants.type": "Gazole"}))
    temps2 = time.time() - start
    tests.append({
        "nom": "Recherche Gazole", 
        "temps": temps2,
        "resultats": f"{len(results)} stations"
    })
    
    # Test 3: Agrégation
    start = time.time()
    pipeline = [{"$unwind": "$carburants"}, {"$group": {"_id": "$carburants.type", "moyenne": {"$avg": "$carburants.prix"}}}]
    results = list(stations.aggregate(pipeline))
    temps3 = time.time() - start
    tests.append({
        "nom": "Agrégation prix",
        "temps": temps3, 
        "resultats": f"{len(results)} carburants"
    })
    
    # Test 4: Compte total
    start = time.time()
    count = stations.count_documents({})
    temps4 = time.time() - start
    tests.append({
        "nom": "Compte total",
        "temps": temps4,
        "resultats": f"{count} stations"
    })
    
    total_stations = count
    
    return render_template('performance.html', tests=tests, total_stations=total_stations)


# Route API pour les données JSON
@app.route('/api/stations')
def api_stations():
    try:
        db = get_db()
        stations = db['stations']
        results = list(stations.find({}))
        return json.loads(json_util.dumps(results))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Route pour l'export CSV
@app.route('/export-csv')
def export_csv():
    try:
        db = get_db()
        stations = db['stations']
        results = list(stations.find({}))
        
        # Préparer les données pour CSV
        data = []
        for station in results:
            for carburant in station['carburants']:
                data.append({
                    'nom_station': station['nom'],
                    'ville': station['ville'],
                    'adresse': station['adresse'],
                    'type_carburant': carburant['type'],
                    'prix': carburant['prix'],
                    'date_maj': carburant['date_maj']
                })
        
        # Créer DataFrame et exporter CSV
        df = pd.DataFrame(data)
        csv_filename = 'export_prix_carburant.csv'
        df.to_csv(csv_filename, index=False)
        
        return send_file(csv_filename, as_attachment=True)
    
    except Exception as e:
        return f"Erreur lors de l'export: {str(e)}", 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)



