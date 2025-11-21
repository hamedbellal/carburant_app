import requests

def test_api():
    url = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/prix-des-carburants-en-france-flux-instantane-v2/exports/json"
    
    try:
        print("🧪 Test de connexion à l'API...")
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ API accessible - {len(data)} stations disponibles")
            
            # Afficher la première station pour vérifier la structure
            if len(data) > 0:
                first_station = data[0]
                print(f"🔍 Exemple de station : {first_station.get('name', 'N/A')}")
                print(f"📍 Ville : {first_station.get('ville', 'N/A')}")
                print(f"⛽ Carburants disponibles :")
                
                # Lister les carburants disponibles
                carburants = ['gazole', 'sp95', 'sp98', 'e85', 'gplc']
                for carb in carburants:
                    prix = first_station.get(carb)
                    if prix:
                        print(f"   - {carb.upper()}: {prix}€")
                
                return True
        else:
            print(f"❌ Erreur API: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Erreur: {e}")
        return False

if __name__ == "__main__":
    test_api()
