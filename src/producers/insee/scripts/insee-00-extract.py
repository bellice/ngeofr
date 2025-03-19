# Importation des librairies
import requests
from requests.exceptions import HTTPError, ProxyError, Timeout, RequestException
from urllib.parse import urljoin
from pathlib import Path
import re
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os
import concurrent.futures

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
    """
    Récupère les liens des fichiers correspondant au pattern donné à partir de l'URL spécifiée.
    
    Parameters:
    path_url (str): URL de la page à scrapper.
    pattern (str): Expression régulière pour matcher les liens à récupérer.
    
    Returns:
    list: Liste des liens absolus des fichiers à télécharger.
    """
    try:
        # Scraping de la page
        response = requests.get(path_url, proxies=proxies)
        response.raise_for_status()  # Assure la gestion des erreurs HTTP
        soup = BeautifulSoup(response.content, "html.parser")
        
        # Extraction des liens correspondants au pattern
        links = [
            urljoin(response.url, link["href"]) for link in soup.find_all("a", href=re.compile(pattern))
        ]
        
        return links
    
    except (HTTPError, ProxyError, Timeout, RequestException) as e:
        print(f"Erreur lors de la récupération des liens depuis {path_url} : {e}")
        return []


def is_file_downloaded(file_name, path_output):
    """
    Vérifie si le fichier existe déjà dans le répertoire de sortie.
    
    Parameters:
    file_name (str): Nom du fichier à vérifier.
    path_output (Path): Chemin du répertoire de sortie.
    
    Returns:
    bool: True si le fichier existe, sinon False.
    """
    file_dest = path_output / file_name
    return file_dest.is_file()


def download_file(link, path_output, suffix=""):
    """
    Télécharge le fichier à partir du lien donné vers le répertoire de sortie.
    
    Parameters:
    link (str): URL du fichier à télécharger.
    path_output (Path): Chemin du répertoire de sortie.
    suffix (str): Suffixe à ajouter au nom du fichier téléchargé.
    """
    try:
        response = requests.get(link, headers={"User-Agent": "Custom"}, proxies=proxies)
        response.raise_for_status()
        
        # Détermination du nom de fichier avec suffixe
        file_name = Path(link).stem + suffix + Path(link).suffix
        file_dest = path_output / file_name
        
        if not file_dest.exists():
            with open(file_dest, "wb") as file:
                file.write(response.content)
            print(f"Téléchargé: {file_name}")
    
    except (HTTPError, ProxyError, Timeout, RequestException) as e:
        print(f"Erreur lors du téléchargement de {link} : {e}")


def download_files_from_url(path_url, pattern, path_output, suffix=""):
    """
    Télécharge tous les fichiers correspondant au pattern donné à partir de l'URL spécifiée.

    Parameters:
    path_url (str): URL de la page à scrapper.
    pattern (str): Expression régulière pour matcher les liens à télécharger.
    path_output (Path): Chemin du répertoire de sortie.
    suffix (str): Suffixe à ajouter au nom des fichiers téléchargés.
    """
    links = get_links_from_url(path_url, pattern)
    
    if not links:
        print(f"Aucun lien à télécharger trouvé pour l'URL : {path_url}")
        return
    
    # Liste des fichiers déjà téléchargés
    downloaded_files = {f.name for f in path_output.glob("*")}
    
    # Filtrage des liens pour les fichiers non encore téléchargés
    links_to_download = [
        link for link in links if Path(link).stem + suffix + Path(link).suffix not in downloaded_files
    ]
    
    if not links_to_download:
        print("Tous les fichiers sont déjà téléchargés.")
        return
    
    # Téléchargement concurrent des fichiers
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(download_file, link, path_output, suffix) for link in links_to_download
        ]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(f"Erreur inconnue : {exc}")


# Chemin de sortie
path_output = Path("./src/producers/insee/assets/")
path_output.mkdir(parents=True, exist_ok=True)  # Création du répertoire si nécessaire

# Définition des URLs, patterns et suffixes pour les téléchargements
urls_patterns_suffixes = [
    ("https://www.insee.fr/fr/information/8377162", "cog_ensemble_2025_csv.zip", ""),
    ("https://www.insee.fr/fr/information/7766585", "cog_ensemble_2024_csv.zip", ""),
    ("https://www.insee.fr/fr/information/6800675", "cog_ensemble_2023_csv.zip", ""),
    ("https://www.insee.fr/fr/information/6051727", "cog_ensemble_2022_csv.zip", ""),
    ("https://www.insee.fr/fr/information/5057840", "cog_ensemble_2021_csv.zip", ""),
    ("https://www.insee.fr/fr/information/4316069", "cog_ensemble_2020_csv.zip", ""),
    ("https://www.insee.fr/fr/information/3720946", "cog_ensemble_2019_csv.zip", ""),
    ("https://www.insee.fr/fr/information/6439600", r"grille_densite_7_niveaux.*\.(xlsx|zip)$", ""),
    ("https://www.insee.fr/fr/information/4803954", r"AAV2020_au.*\.zip", ""),
    ("https://www.insee.fr/fr/information/4802589", r"UU2020_au.*\.zip", ""),
    ("https://www.insee.fr/fr/information/4652957", r"ZE2020_au.*\.zip", ""),
    ("https://www.insee.fr/fr/information/6676988", r"BV2022_au.*\.zip", ""),
    ("https://www.insee.fr/fr/statistiques/8290591?sommaire=8290669&q=population+2022", r"ensemble\.xlsx$", "_pop2022"),
    ("https://www.insee.fr/fr/statistiques/7739582?sommaire=7728826", r"ensemble\.xlsx$", "_pop2021"),
    ("https://www.insee.fr/fr/statistiques/6683035", r"ensemble\.xlsx$", "_pop2020"),
    ("https://www.insee.fr/fr/statistiques/6011070?sommaire=6011075", r"ensemble\.xlsx$", "_pop2019"),
    ("https://www.insee.fr/fr/statistiques/4989724?sommaire=4989761", r"ensemble\.xlsx$", "_pop2018"),
    ("https://www.insee.fr/fr/statistiques/4265429?sommaire=4265511", r"ensemble\.xls$", "_pop2017"),
    ("https://www.insee.fr/fr/statistiques/3677785?sommaire=3677855", r"ensemble\.xls$", "_pop2016"),
    ("https://www.insee.fr/fr/statistiques/5392668?sommaire=2120838", r"\.xlsx$", ""),
    ("https://www.insee.fr/fr/statistiques/8290593?sommaire=8290669", r"\.xlsx$", "_pop2022"),
    ("https://www.insee.fr/fr/statistiques/7728812?sommaire=7728826", r"\.xlsx$", "_pop2021"),
    ("https://www.insee.fr/fr/statistiques/6683025", r"\.xlsx$", "_pop2020"),
    ("https://www.insee.fr/fr/statistiques/6011054?sommaire=6011075", r"\.xlsx$", "_pop2019"),
    ("https://www.insee.fr/fr/statistiques/4989710?sommaire=4989761", r"\.xlsx$", "_pop2018"),
    ("https://www.insee.fr/fr/statistiques/4265451?sommaire=4265511", r"\.xls$", "_pop2017"),
    ("https://www.insee.fr/fr/statistiques/3677785?sommaire=3677855", r"\.xls$", "_pop2016"),
    ("https://www.insee.fr/fr/statistiques/5392639?sommaire=2122859", r"\.xlsx$", "_fr-nc"),
    ("https://www.insee.fr/fr/statistiques/6690039?sommaire=2122700", r"\.xlsx$", "_fr-pf"),
    ("https://www.insee.fr/fr/statistiques/5392492", r"\.xlsx$", "_fr-wf"),
    ("https://www.insee.fr/fr/information/7671867", r"\.zip$", ""),
    ("https://www.insee.fr/fr/information/7671844", r"\.zip$", ""),
    ("https://www.insee.fr/fr/information/2549968", r"\.(xls|xlsx)$", "")
]


# Téléchargement des fichiers pour chaque URL, pattern et suffixe
for url, pattern, suffix in urls_patterns_suffixes:
    download_files_from_url(url, pattern, path_output, suffix)
