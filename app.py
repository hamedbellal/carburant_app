from flask import Flask, render_template, request, jsonify, send_file
import pandas as pd
import json
import time
import random
import os

app = Flask(__name__)

# Charger les données depuis le fichier JSON
def load_stations_data():
    try:
        with open('data/stations.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print("❌ Fichier data/stations.json non trouvé")
        return []

# Charger les données au démarrage
stations_data = load_stations_data()
print(f"✅ {len(stations_data)} stations chargées depuis JSON")

# Route principale
@app.route('/')
def index():
    return render_template('index.html')

# Route pour la recherche avancée
@app.route('/recherche', methods=['POST'])
def recherche():
    try:
        # Récupérer tous les paramètres
        ville = request.form.get('ville', '').strip().lower()
        carburant = request.form.get('carburant', '').strip()
        departement = request.form.get('departement', '').strip()
        prix_min = request.form.get('prix_min', '')
        prix_max = request.form.get('prix_max', '')
        
        # Filtrer les données
        results = []
        for station in stations_data:
            match = True
            
            # Filtre par ville
            if ville and ville not in station.get('ville', '').lower():
                match = False
            
            # Filtre par département
            if departement and departement != station.get('code_departement', ''):
                match = False
            
            # Filtre par carburant
            if carburant:
                has_carburant = any(c['type'] == carburant for c in station.get('carburants', []))
                if not has_carburant:
                    match = False
            
            # Filtre par prix
            if prix_min or prix_max:
                prix_min_val = float(prix_min) if prix_min else 0
                prix_max_val = float(prix_max) if prix_max else float('inf')
                
                has_matching_price = False
                for carb in station.get('carburants', []):
                    if prix_min_val <= carb['prix'] <= prix_max_val:
                        has_matching_price = True
                        break
                
                if not has_matching_price:
                    match = False
            
            if match:
                results.append(station)
        
        return render_template('results.html', 
                             results=results, 
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
        # Statistiques générales
        total_stations = len(stations_data)
        
        # Calculer les statistiques de prix
        stats_prix = {}
        for station in stations_data:
            for carburant in station.get('carburants', []):
                type_carb = carburant['type']
                prix = carburant['prix']
                
                if type_carb not in stats_prix:
                    stats_prix[type_carb] = {'prix': [], 'count': 0}
                
                stats_prix[type_carb]['prix'].append(prix)
                stats_prix[type_carb]['count'] += 1
        
        # Préparer les statistiques finales
        stats_final = []
        for type_carb, data in stats_prix.items():
            prix_list = data['prix']
            stats_final.append({
                '_id': type_carb,
                'moyenne': sum(prix_list) / len(prix_list),
                'minimum': min(prix_list),
                'maximum': max(prix_list),
                'count': data['count']
            })
        
        # Stations par département
        dept_count = {}
        for station in stations_data:
            dept = station.get('code_departement', 'Inconnu')
            dept_count[dept] = dept_count.get(dept, 0) + 1
        
        top_departements = [{'count': v, '_id': k} for k, v in 
                           sorted(dept_count.items(), key=lambda x: x[1], reverse=True)[:10]]
        
        return render_template('statistiques.html',
                             total_stations=total_stations,
                             stats_prix=stats_final,
                             top_departements=top_departements)
    
    except Exception as e:
        return f"Erreur lors du calcul des statistiques: {str(e)}", 500

# Route pour la page performance (version JSON)
@app.route('/performance')
def performance():
    total_stations = len(stations_data)
    return render_template('performance.html', total_stations=total_stations)

# Route pour générer des données Big Data (version JSON simplifiée)
@app.route('/generate-big-data/<int:multiplier>')
def generate_big_data_route(multiplier):
    if multiplier > 10:
        return "⚠️ Multiplicateur trop élevé pour la version web. Utilisez le script en console.", 400
    
    # Cette fonctionnalité n'est pas supportée en mode JSON simple
    return "⚠️ Génération de données Big Data non disponible en mode JSON. Utilisez MongoDB pour cette fonctionnalité."

# Route pour réinitialiser les données
@app.route('/reset-data')
def reset_data():
    # Recharger les données originales
    global stations_data
    stations_data = load_stations_data()
    return f"✅ Données réinitialisées. {len(stations_data)} stations chargées"

# Route pour lancer les tests de performance (version JSON)
@app.route('/run-tests')
def run_tests():
    tests = []
    
    # Test 1: Recherche simple
    start = time.time()
    results = [s for s in stations_data if 'paris' in s.get('ville', '').lower()]
    temps1 = time.time() - start
    tests.append({
        "nom": "Recherche Paris",
        "temps": temps1,
        "resultats": f"{len(results)} stations"
    })
    
    # Test 2: Recherche carburant
    start = time.time()
    results = [s for s in stations_data if any(c['type'] == 'Gazole' for c in s.get('carburants', []))]
    temps2 = time.time() - start
    tests.append({
        "nom": "Recherche Gazole", 
        "temps": temps2,
        "resultats": f"{len(results)} stations"
    })
    
    # Test 3: "Agrégation" (calcul manuel)
    start = time.time()
    prix_par_carburant = {}
    for station in stations_data:
        for carburant in station.get('carburants', []):
            type_carb = carburant['type']
            if type_carb not in prix_par_carburant:
                prix_par_carburant[type_carb] = []
            prix_par_carburant[type_carb].append(carburant['prix'])
    
    results = []
    for type_carb, prix_list in prix_par_carburant.items():
        results.append({
            '_id': type_carb,
            'moyenne': sum(prix_list) / len(prix_list)
        })
    temps3 = time.time() - start
    tests.append({
        "nom": "Agrégation prix",
        "temps": temps3, 
        "resultats": f"{len(results)} carburants"
    })
    
    # Test 4: Compte total
    start = time.time()
    count = len(stations_data)
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
        return jsonify(stations_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Route pour l'export CSV
@app.route('/export-csv')
def export_csv():
    try:
        import csv
        from io import StringIO
        from flask import Response
        
        # Créer le CSV en mémoire
        output = StringIO()
        writer = csv.writer(output)
        
        # En-têtes
        writer.writerow(['nom_station', 'ville', 'adresse', 'departement', 'type_carburant', 'prix', 'date_maj'])
        
        # Données
        for station in stations_data:
            for carburant in station.get('carburants', []):
                writer.writerow([
                    station.get('nom', ''),
                    station.get('ville', ''),
                    station.get('adresse', ''),
                    station.get('code_departement', ''),
                    carburant['type'],
                    carburant['prix'],
                    carburant.get('date_maj', '')
                ])
        
        # Retourner le fichier
        output.seek(0)
        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={"Content-disposition": "attachment; filename=export_prix_carburant.csv"}
        )
    
    except Exception as e:
        return f"Erreur lors de l'export: {str(e)}", 500

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000)