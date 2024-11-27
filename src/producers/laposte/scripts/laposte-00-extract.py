import os
import requests
from requests.exceptions import ProxyError, RequestException, HTTPError, Timeout
from dotenv import load_dotenv
from datetime import datetime

# Charger les variables d'environnement à partir du fichier .env
load_dotenv()

# Obtenir les informations de proxy
proxy = os.getenv("PROXY_URL")
proxies = {
    "http": proxy,
    "https": proxy
} if proxy else None

# URL du fichier CSV à télécharger
url = "https://datanova.laposte.fr/data-fair/api/v1/datasets/laposte-hexasmal/data-files/019HexaSmal.csv"

def add_date_to_filename(filename):
    """Ajoute la date actuelle au nom du fichier"""
    base, ext = os.path.splitext(filename)
    date_str = datetime.now().strftime("%Y%m%d")
    return f"{base}_{date_str}{ext}"

# Nom du fichier de sortie
output_file_base = "./src/assets/laposte/019HexaSmal.csv"
output_file = add_date_to_filename(output_file_base)

def download_csv(url, output_file):
    try:
        # Essayer d'envoyer la requête sans proxy
        print("Essai de téléchargement direct sans proxy...")
        response = requests.get(url, timeout=10)
        
        # Vérifier si la requête a été réussie
        response.raise_for_status()

        # Enregistrer le contenu dans un fichier local
        with open(output_file, 'wb') as file:
            file.write(response.content)
        print(f"Fichier téléchargé avec succès et enregistré sous '{output_file}'.")

    except (HTTPError, ProxyError, Timeout, RequestException) as req_err:
        print(f"Erreur lors de la requête directe: {req_err}")
        
        # Si un proxy est défini, tenter de le télécharger via le proxy
        if proxies:
            try:
                print("Essai de téléchargement via proxy...")
                response = requests.get(url, proxies=proxies, timeout=10)
                response.raise_for_status()

                with open(output_file, 'wb') as file:
                    file.write(response.content)
                print(f"Fichier téléchargé avec succès et enregistré sous '{output_file}' via proxy.")
            except (HTTPError, ProxyError, Timeout, RequestException) as proxy_err:
                print(f"Erreur lors de la requête via proxy: {proxy_err}")
    except Exception as e:
        print(f"Une erreur inattendue s'est produite: {e}")

# Test du téléchargement sans proxy pour diagnostic
try:
    response = requests.get(url, timeout=10)
    if response.status_code == 200:
        print("Connexion directe réussie. Le problème peut venir du proxy.")
    else:
        print(f"Erreur de connexion directe: {response.status_code}")
except Exception as e:
    print(f"Erreur de connexion directe: {e}")

# Appeler la fonction pour télécharger le fichier
download_csv(url, output_file)
