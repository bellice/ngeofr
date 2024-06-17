# Importation des librairies
from pathlib import Path
import requests

# Fonctions
from src.script.cog.download_files_from_insee import *


# Chemin
path_output = Path("G:/cog/banatic")

# Banatic
path_url = "https://www.banatic.interieur.gouv.fr/V5/fichiers-en-telechargement/telecharger.php?zone=N&date=01/10/2023&format=A"
r = requests.get(path_url, allow_redirects=False)
print(r.headers['Location'])

r.name
r.content

# Téléchargement
with open(file_dest, "wb") as f:
    f.write(r.content)
print(f"--- Téléchargement effectué\n")
  else:
print(f"Réponse du serveur : Échec (Code {r.status_code})")

https://www.banatic.interieur.gouv.fr/V5/fichiers-en-telechargement/telecharger.php?zone=N&date=01/10/2023&format=A