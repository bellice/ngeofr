import os
import requests
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

proxy = os.getenv("PROXY_URL")

if proxy:
    print(f"Utilisation du proxy : {proxy}")
    proxies = {
        "http": proxy,
        "https": proxy
    }
    try:
        # Essayez de faire une simple requête GET via le proxy
        response = requests.get("http://httpbin.org/ip", proxies=proxies, timeout=10)
        response.raise_for_status()
        print(f"Réponse : {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Erreur : {e}")
else:
    print("Aucun proxy défini.")
