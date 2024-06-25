import os
import requests
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

# Récupérer l'URL du proxy depuis les variables d'environnement
proxy = os.getenv("PROXY_URL")

# Configuration du proxy
proxies = {
    "http": proxy,
    "https": proxy
} if proxy else None

# URL cible pour la requête
url = "https://www.iso.org/obp/ui/?v-1718980266620"

# Effectuer une requête GET via le proxy
try:
    response = requests.get(url, proxies=proxies)
    response.raise_for_status()  # Vérifie que la requête a réussi

    # Traiter la réponse
    data = response.text
    print(data)
except requests.exceptions.RequestException as e:
    print(f"Erreur lors de la requête: {e}")



## Exploration des données 

# Voir les premières lignes
df.head()

# Dimension (ligne x colonne)
df.shape

# Type de données
df.dtypes

df.info()

# Valeur nulle
df.isnull().sum()

# Colonne
df.columns