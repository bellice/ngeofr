import os
import requests
from requests.exceptions import ProxyError, RequestException, HTTPError, Timeout
from dotenv import load_dotenv

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

# Nom du fichier de sortie
output_file = "./src/assets/laposte/019HexaSmal.csv"

def download_csv(url, output_file, proxies=None):
    try:
        # Envoyer la requête GET pour télécharger le fichier
        response = requests.get(url, proxies=proxies, timeout=10)
        
        # Vérifier si la requête a été réussie
        response.raise_for_status()
        
        # Enregistrer le contenu dans un fichier local
        with open(output_file, 'wb') as file:
            file.write(response.content)
        print(f"Fichier téléchargé avec succès et enregistré sous '{output_file}'.")

    except ProxyError:
        print("Erreur de proxy. Veuillez vérifier vos paramètres de proxy.")
    except HTTPError as http_err:
        print(f"Erreur HTTP: {http_err}")
    except Timeout:
        print("La requête a expiré. Veuillez essayer à nouveau plus tard.")
    except RequestException as req_err:
        print(f"Erreur lors de la requête: {req_err}")
    except Exception as e:
        print(f"Une erreur inattendue s'est produite: {e}")

# Appeler la fonction pour télécharger le fichier
download_csv(url, output_file, proxies=proxy)
