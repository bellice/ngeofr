# Importation des librairies
from pathlib import Path
import requests

# Chemin
path_output = Path("G:/cog/laposte")

# Base des codes postaux - faire page séparé
path_url = "https://koumoul.com/data-fair/api/v1/datasets/laposte-hexasmal/data-files/019HexaSmal.csv"
r = requests.get(path_url)

if r.status_code == 200:
    print(f"Réponse du serveur : Succès (Code {r.status_code})")
    with open(Path(path_output) / path_url.split("/")[-1], "wb") as f:
        f.write(r.content)
        print("Téléchargement réussi.")
else:
    print(f"Réponse du serveur : Échec (Code {r.status_code})")


