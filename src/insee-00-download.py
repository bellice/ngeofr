# Importation des librairies
from pathlib import Path
import re
from bs4 import BeautifulSoup

# Fonctions
from src.script.cog.download_files_from_insee import *


# Chemin
path_output = Path("G:/cog/insee")

# COG 2023
path_url = "https://www.insee.fr/fr/information/6800675"
pattern = re.compile("cog_ensemble_2023_csv.zip")

download_files_from_url(path_url, pattern, path_output)

# Grille de densité 7 niveaux
path_url = "https://www.insee.fr/fr/information/6439600"
pattern = re.compile("grille_densite_7_niveaux.*\.(xlsx|zip)$")

download_files_from_url(path_url, pattern, path_output)

# Zonage étude Insee Aire d'attraction des villes
path_url = "https://www.insee.fr/fr/information/4803954"
pattern = re.compile("AAV2020_au.*\.zip")

download_files_from_url(path_url, pattern, path_output)

# Zonage étude Insee Unité urbaine
path_url = "https://www.insee.fr/fr/information/4802589"
pattern = re.compile("UU2020_au.*\.zip")

download_files_from_url(path_url, pattern, path_output)

# Zonage étude Insee Zone d'emploi
path_url = "https://www.insee.fr/fr/information/4652957"
pattern = re.compile("ZE2020_au.*\.zip")

download_files_from_url(path_url, pattern, path_output)

# Zonage étude Insee Bassin de vie
path_url = "https://www.insee.fr/fr/information/6676988"
pattern = re.compile("BV2022_au.*\.zip")

download_files_from_url(path_url, pattern, path_output)

# Population légale 2020
path_url = "https://www.insee.fr/fr/statistiques/6683035"
pattern = re.compile("ensemble\.xlsx$")

download_files_from_url(path_url, pattern, path_output)

# Population Mayotte
path_url = "https://www.insee.fr/fr/statistiques/5392668"
pattern = re.compile("\.xlsx$")

download_files_from_url(path_url, pattern, path_output)

# Population Saint-Pierre-et-Miquelon, Saint-Barthélemy, Saint-Martin
path_url = "https://www.insee.fr/fr/statistiques/6683025"
pattern = re.compile("\.xlsx$")

download_files_from_url(path_url, pattern, path_output)

# Population Nouvelle-Calédonie
path_url = "https://www.insee.fr/fr/statistiques/5392639"
pattern = re.compile("\.xlsx$")

download_files_from_url(path_url, pattern, path_output)

# Population Polynésie française
path_url = "https://www.insee.fr/fr/statistiques/6690039?sommaire=2122700"
pattern = re.compile("\.xlsx$")

download_files_from_url(path_url, pattern, path_output)

# Population Wallis et Futuna
path_url = "https://www.insee.fr/fr/statistiques/5392492"
pattern = re.compile("\.xlsx$")

download_files_from_url(path_url, pattern, path_output)

# Codification outremer
path_url = "https://www.insee.fr/fr/information/2028040"
r = requests.get(path_url)
soup = BeautifulSoup(r.content, "html.parser")

if r.status_code == 200:
    print(f"Réponse du serveur : Succès (Code {r.status_code})")
    with open(Path(path_output) / "codification-drom-com.html", "w", encoding="UTF-8") as f:
        f.write(str(soup))
        print("Téléchargement réussi.")
else:
        print(f"Réponse du serveur : Échec (Code {r.status_code})")

# Table appartenance géographique
path_url = "https://www.insee.fr/fr/information/2028028"
pattern = re.compile("\.zip$")

download_files_from_url(path_url, pattern, path_output)

# Communes nouvelles
path_url = "https://www.insee.fr/fr/information/2549968"
pattern = re.compile("\.xls|xlsx$")

download_files_from_url(path_url, pattern, path_output)
