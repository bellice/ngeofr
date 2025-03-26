import os
import requests
import chardet
from dotenv import load_dotenv
from requests.exceptions import ProxyError, RequestException, HTTPError, Timeout
import re

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

# Pour avoir code siren et code insee
# https://www.banatic.interieur.gouv.fr/export

# Pour avoir liste des périmètre
# https://www.banatic.interieur.gouv.fr/archive

