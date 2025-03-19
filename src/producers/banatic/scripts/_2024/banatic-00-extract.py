import os
import requests
import chardet
from dotenv import load_dotenv
from requests.exceptions import ProxyError, RequestException, HTTPError, Timeout
import re

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

def extract_department_and_date(url):
    """
    Extrait le département et la date de l'URL.
    """
    dept_match = re.search(r"zone=(D\d+)", url)
    date_match = re.search(r"date=(\d{2}/\d{2}/\d{4})", url)
    
    department = dept_match.group(1) if dept_match else "unknown-department"
    date = date_match.group(1).replace("/", "-") if date_match else "unknown-date"
    
    formatted_date = "-".join(date.split('-')[::-1])
    return department, formatted_date

def file_exists_in_directory(file_path):
    """
    Vérifie si le fichier existe déjà dans le répertoire spécifié.
    """
    return os.path.exists(file_path)

def download_banatic(url_base, zones, path_output="./src/producers/banatic/assets/"):
    """
    Télécharge les fichiers CSV depuis Banatic pour les zones spécifiées, les convertit en UTF-8 et les sauvegarde.
    """
    format_to_filename = {
        "A": "liste-groupement",
        "B": "coordonnees-groupement",
        "C": "competences-groupement",
        "D": "perimetre-groupement",
        "E": "perimetre-epci-fp"
    }
    
    FORMATS = ["A", "B", "C", "D", "E"]
    
    for zone in zones:
        for format in FORMATS:
            url = f"{url_base}&zone={zone}&format={format}"
            print(f"Tentative de téléchargement pour la zone {zone} et le format {format} : {url}")
            
            department, date = extract_department_and_date(url)
            filename_base = format_to_filename.get(format, "fichier-inconnu")
            
            if department == "D2":
                department = zone  # Utilise directement la zone D2A ou D2B comme nom de département
            
            filename = f"{department}-{date}-{filename_base}.csv"
            chemin_complet = os.path.join(path_output, filename)
            
            if file_exists_in_directory(chemin_complet):
                print(f"Le fichier {filename} existe déjà. Téléchargement ignoré.")
                continue
            
            proxy = os.getenv("PROXY_URL")
            proxies = {
                "http": proxy,
                "https": proxy
            } if proxy else None
    
            try:
                print("Tentative de téléchargement...")
                response = requests.get(url, proxies=proxies, timeout=10)
                response.raise_for_status()
    
                # Détection de l'encodage avec chardet
                encoding_result = chardet.detect(response.content)
                encoding = encoding_result['encoding']
                print(f"Encodage détecté par chardet : {encoding}")
    
                # Décodage avec l'encodage détecté
                try:
                    content = response.content.decode(encoding)
                except UnicodeDecodeError:
                    print(f"Erreur de décodage avec l'encodage détecté ({encoding}). Utilisation de 'utf-8' avec remplacement.")
                    content = response.content.decode("utf-8", errors="replace")
    
                # Ré-encodage en UTF-8
                utf8_content = content.encode("utf-8")
    
                # Écriture du fichier en UTF-8
                with open(chemin_complet, "wb") as fichier:
                    fichier.write(utf8_content)
    
                print(f"Fichier téléchargé et encodé en UTF-8 avec succès : {chemin_complet}")
    
            except HTTPError as e:
                if e.response.status_code == 404:
                    print(f"Ressource pour le format {format} et la zone {zone} non trouvée (404).")
                    continue
                else:
                    print(f"Erreur HTTP pour le format {format} et la zone {zone} : {e.response.status_code} - {e.response.reason}")
                    break
            except ProxyError:
                print("Erreur de proxy : Impossible de se connecter via le proxy.")
                break
            except ConnectionError:
                print("Erreur de connexion : Impossible de se connecter au serveur.")
                break
            except Timeout:
                print("Timeout : La requête a mis trop de temps à répondre.")
                break
            except RequestException as e:
                print(f"Erreur de téléchargement pour le format {format} et la zone {zone} : {str(e)}")
                break
    
    print("Processus de téléchargement terminé.")

# URL de base pour le téléchargement
url_base = "https://www.banatic.interieur.gouv.fr/V5/fichiers-en-telechargement/telecharger.php?date=01/01/2024"
# Liste des zones à télécharger
zones = [
    "D01", "D02", "D03", "D04", "D05", "D06", "D07", "D08", "D09", "D10",
    "D11", "D12", "D13", "D14", "D15", "D16", "D17", "D18", "D19", "D21",
    "D22", "D23", "D24", "D25", "D26", "D27", "D28", "D29", "D2A", "D2B",
    "D30", "D31", "D32", "D33", "D34", "D35", "D36", "D37", "D38", "D39",
    "D40", "D41", "D42", "D43", "D44", "D45", "D46", "D47", "D48", "D49",
    "D50", "D51", "D52", "D53", "D54", "D55", "D56", "D57", "D58", "D59",
    "D60", "D61", "D62", "D63", "D64", "D65", "D66", "D67", "D68", "D69",
    "D70", "D71", "D72", "D73", "D74", "D75", "D76", "D77", "D78", "D79",
    "D80", "D81", "D82", "D83", "D84", "D85", "D86", "D87", "D88", "D89",
    "D90", "D91", "D92", "D93", "D94", "D95", "D971", "D972", "D973", "D974",
    "D976", "D2A", "D2B"
]

download_banatic(url_base, zones)
