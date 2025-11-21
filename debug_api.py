import requests
import json

def debug_api():
    url = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/prix-des-carburants-en-france-flux-instantane-v2/exports/json"
    
    response = requests.get(url)
    data = response.json()
    
    print("🔍 Analyse de la structure des données...")
    print(f"Nombre total de stations: {len(data)}")
    
    # Trouver une station avec des données complètes
    for i, station in enumerate(data[:10]):  # Regarder les 10 premières
        print(f"\n--- Station {i+1} ---")
        print(f"Clés disponibles: {list(station.keys())}")
        
        # Afficher les valeurs pour les champs importants
        important_fields = ['id', 'name', 'adresse', 'ville', 'cp', 'dep_code', 'gazole', 'sp95', 'sp98']
        for field in important_fields:
            value = station.get(field, 'NON DISPONIBLE')
            print(f"{field}: {value}")
        
        # S'arrêter à la première station avec des données complètes
        if station.get('ville') and station.get('gazole'):
            print("✅ Station complète trouvée, structure analysée")
            break
    
    # Sauvegarder un exemple complet pour inspection
    with open('exemple_station.json', 'w', encoding='utf-8') as f:
        json.dump(data[0], f, ensure_ascii=False, indent=2)
    print("\n💾 Exemple de station sauvegardé dans 'exemple_station.json'")

if __name__ == "__main__":
    debug_api()
