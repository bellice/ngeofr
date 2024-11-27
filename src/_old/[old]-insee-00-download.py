# Importation des librairies
import requests
from urllib.parse import urljoin, urlparse
from pathlib import Path
import re
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os

# Charger les variables d'environnement
load_dotenv()

# Récupérer les informations du proxy depuis le fichier .env
proxy = os.getenv("PROXY_URL")

# Configuration du proxy
proxies = {
        "http": proxy,
        "https": proxy
} if proxy else None
    

def get_links_from_url(path_url, pattern):
    # Scrapping de la page
    r = requests.get(path_url, proxies=proxies)
    soup = BeautifulSoup(r.content, "html.parser")

    # Récupération de tous les liens à télécharger
    links = []
    for link in soup.find_all("a", attrs={"href": pattern}):
        links.append(link["href"])

    # Conversion des chemins relatifs en absolu
    parsed_url = urlparse(path_url)
    base_url = parsed_url.scheme + "://" + parsed_url.netloc
    links = [urljoin(base_url, link) for link in links]

    return links

def is_file_downloaded(link, path_output):
    # Vérifie si le fichier est déjà téléchargé dans le dossier path_output
    file_name = Path(link).name
    file_dest = path_output / file_name
    return file_dest.is_file()

def download_file(link, path_output):
    # Envoie requête http
    r = requests.get(link, headers={"User-Agent": "Custom"}, proxies=proxies)

    if r.status_code == 200:
        print(f"Réponse du serveur : Succès (Code {r.status_code})")
        
        # Nom du fichier
        file_name = Path(link).name
        print(f"Téléchargement de {file_name}")

        # Chemin de destination
        file_dest = path_output / file_name

        # Téléchargement
        with open(file_dest, "wb") as f:
            f.write(r.content)
        print(f"--- Téléchargement effectué\n")
    else:
        print(f"Réponse du serveur : Échec (Code {r.status_code})")

def download_files_from_url(path_url, pattern, path_output):
    links = get_links_from_url(path_url, pattern)

    # Liste pour suivre les noms de fichiers téléchargés
    downloaded_files = [f.name for f in path_output.glob("*")]

    links_to_download = []
    for link in links:
        file_name = Path(link).name
        if file_name not in downloaded_files:
            links_to_download.append(link)

    for link in links_to_download:
        download_file(link, path_output)

# Chemin
path_output = Path("./src/params/insee/")

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
