import os
import requests
import chardet
import csv
import io
from dotenv import load_dotenv
from requests.exceptions import ProxyError, RequestException, HTTPError, ConnectionError, Timeout
from collections import Counter
import re

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

#def detect_separator_csv(el):
#    # Utilisation de la méthode Counter pour détecter les séparateurs les plus fréquents
#    lines = el.splitlines()
#    separators = [';', '\t', '|', ':', ' ']
#    count = Counter()
#    
#    for line in lines:
#        for sep in separators:
#            count[sep] += line.count(sep)
#    
#    # Trouver le séparateur le plus fréquent
#    separator = count.most_common(1)[0][0]
#    return separator

def detect_separator_csv(content):
    # Séparateurs potentiels à tester
    potential_delimiters = [';', ',', '\t', '|', ':']
    
    # Utilisation de csv.Sniffer pour détecter le séparateur
    sniff = csv.Sniffer()

    # Utiliser un StringIO pour traiter le contenu comme un fichier
    csv_file = io.StringIO(content)

    try:
        # Découvrir le dialecte CSV en testant les séparateurs potentiels
        dialect = sniff.sniff(csv_file.read(4096), delimiters=potential_delimiters)
        delimiter = dialect.delimiter
    except csv.Error:
        # Si csv.Sniffer ne peut pas déterminer le séparateur, on utilise par défaut la virgule
        delimiter = ','

    return delimiter



def extract_department_and_date(url):
    """Extrait le département et la date de l'URL."""
    dept_match = re.search(r'zone=(D\d+)', url)
    date_match = re.search(r'date=(\d{2}/\d{2}/\d{4})', url)
    
    department = dept_match.group(1) if dept_match else "unknown-department"
    date = date_match.group(1).replace('/', '-') if date_match else "unknown-date"
    
    # Formater la date au format ISO (YYYY-MM-DD)
    formatted_date = "-".join(date.split('-')[::-1])  # Inverser et reformater la date

    return department, formatted_date


def download_banatic(url_base, dossier_destination="./telechargements"):
    # Dictionnaire pour mapper les formats aux noms de fichiers
    format_to_filename = {
        'A': "liste-groupement",
        'B': "coordonnees-groupement",
        'C': "competences-groupement",
        'D': "perimetre-groupement",
        'E': "perimetre-epci-fp"
    }

    # Extraire le département et la date de l'URL
    department, date = extract_department_and_date(url_base)
    
    formats = ['A', 'B', 'C', 'D', 'E']
    
    for format in formats:
        url = f"{url_base}&format={format}"
        print(f"Tentative de téléchargement pour le format {format} : {url}")
        
        # Déterminer le nom du fichier à partir du format
        nom_fichier_base = format_to_filename.get(format, "fichier-inconnu")
        nom_fichier = f"{department}-{date}-{nom_fichier_base}.csv"
        
        # Créer le dossier de destination s'il n'existe pas
        if not os.path.exists(dossier_destination):
            os.makedirs(dossier_destination)
        
        # Chemin complet où le fichier sera enregistré
        chemin_complet = os.path.join(dossier_destination, nom_fichier)
        
        # Récupérer le proxy depuis les variables d'environnement
        proxy = os.getenv("PROXY_URL")
        
        # Vérifier si le proxy est défini
        if proxy:
            print(f"Proxy trouvé : {proxy}")
        else:
            print("Aucun proxy trouvé. Téléchargement sans proxy.")
        
        # Définir les paramètres du proxy s'il est disponible
        proxies = {
            "http": proxy,
            "https": proxy
        } if proxy else None

        try:
            # Télécharger le fichier via le proxy si disponible, sinon sans proxy
            if proxy:
                print("Tentative de téléchargement avec proxy...")
            else:
                print("Tentative de téléchargement sans proxy...")

            reponse = requests.get(url, proxies=proxies, timeout=10)

            # Vérifier le code de statut de la réponse
            reponse.raise_for_status()

            # Détection de l'encodage avec chardet
            resultat_encodage = chardet.detect(reponse.content)
            encodage = resultat_encodage['encoding']
            print(f"Encodage détecté par chardet : {encodage}")

            # Décodage en utilisant l'encodage détecté
            try:
                el = reponse.content.decode(encodage)
            except UnicodeDecodeError:
                print(f"Erreur de décodage avec l'encodage détecté ({encodage}). Utilisation de UTF-8 avec remplacement.")
                el = reponse.content.decode(encodage, errors='replace')

            # Détecter le séparateur
            separator = detect_separator_csv(content)
            print(f"Le séparateur détecté est : '{separator}'")

            # Remplacer le séparateur par une virgule
            content_csv = content.replace(separator, ',')

            # Enregistrer le contenu en CSV avec encodage UTF-8
            with open(chemin_complet, "w", encoding="utf-8") as fichier:
                fichier.write(econtent_csv)
            
            print(f"Fichier téléchargé avec succès : {chemin_complet}")

        except HTTPError as e:
            if e.response.status_code == 404:
                print(f"Ressource pour le format {format} non trouvée (404).")
            else:
                print(f"Erreur HTTP pour le format {format} : {e.response.status_code} - {e.response.reason}")
        except ProxyError:
            print("Erreur de proxy : Impossible de se connecter via le proxy.")
        except ConnectionError:
            print("Erreur de connexion : Impossible de se connecter au serveur.")
        except Timeout:
            print("Timeout : La requête a mis trop de temps à répondre.")
        except RequestException as e:
            print(f"Erreur de téléchargement pour le format {format} : {str(e)}")

    print("Processus de téléchargement terminé.")

# Exemple d'utilisation de la fonction
url_base = "https://www.banatic.interieur.gouv.fr/V5/fichiers-en-telechargement/telecharger.php?zone=D01&date=01/01/2024"
download_banatic(url_base)
