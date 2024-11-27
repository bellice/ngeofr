# Importation des librairies
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from urllib.parse import urljoin, urlparse

def get_links_from_url(path_url, pattern):
    # Scrapping de la page
    r = requests.get(path_url)
    soup = BeautifulSoup(r.content, "html.parser")

    # Récupération de tous les liens à télécharger
    links = []
    for link in soup.find_all("a", attrs={"href": pattern}):
        links.append(link["href"])

    # Conversion des chemins relatifs en absolu

    # Analyse de l'url pour obtenir le nom de domaine
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
    r = requests.get(link, headers={"User-Agent": "Custom"})

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


