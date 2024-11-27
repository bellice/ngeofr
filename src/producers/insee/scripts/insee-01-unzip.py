# Importation des librairies
from pathlib import Path
import zipfile
import logging

# Configuration du journal
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Chemin
path = Path("./src/producers/insee/assets/")

# Récupération des fichiers .zip
files = list(path.glob('*.zip'))

# Filtre des fichiers .zip qui ne sont pas déjà extraits
def is_extracted(zip_file, destination):
    with zipfile.ZipFile(zip_file, 'r') as archive:
        for member in archive.namelist():
            member_path = destination / member
            if not member_path.exists():
                return False
    return True

files_filtered = [file for file in files if not is_extracted(file, path)]

# Décompression des fichiers filtrés
for file in files_filtered:
    try:
        with zipfile.ZipFile(file, 'r') as archive:
            logging.info(f"Décompression de {file.name}...")
            archive.extractall(path)
            logging.info(f"{file.name} a été extrait dans {path}.")
    except Exception as e:
        logging.error(f"Erreur lors de la décompression de {file.name}: {e}")

