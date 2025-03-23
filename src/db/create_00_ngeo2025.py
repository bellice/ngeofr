import duckdb
import pandas as pd
import shutil
import os

# Connexion à une base de données DuckDB (crée la base si elle n'existe pas)
conn = duckdb.connect("src/db/ngeo2025.duckdb")

# Fonction pour lire un fichier SQL
def read_sql_file(file_path):
    with open(file_path, "r") as file:
        return file.read()

# Charger les fichiers Parquet dans des vues temporaires
parquet_files = {
    "table_com": {
        "path": "src/data/insee/table_com.parquet",
        "columns": ["com_type", "com_insee", "com_nom", "arr_insee", "dep_insee", "reg_insee"]
    },
    "table_arr": {
        "path": "src/data/insee/table_arr.parquet",
        "columns": ["arr_insee", "arr_nom", "arr_cheflieu"]
    },
    "table_dep": {
        "path": "src/data/insee/table_dep.parquet",
        "columns": ["dep_insee", "dep_nom", "dep_cheflieu", "reg_insee"]
    },
    "table_reg": {
        "path": "src/data/insee/table_reg.parquet",
        "columns": ["reg_insee", "reg_nom", "reg_cheflieu"]
    },
    "table_com_siren": {
        "path": "src/data/banatic/table_com_siren.parquet",
        "columns": ["com_insee", "com_siren", "com_nom"]
    },
    "table_epci_perimetre": {
        "path": "src/data/banatic/table_epci_perimetre.parquet",
        "columns": ["epci_membre_siren", "epci_siren", "epci_nom", "epci_cheflieu", "epci_interdep", "epci_naturejuridique"]
    },
    "table_ept_perimetre": {
        "path": "src/data/banatic/table_ept_perimetre.parquet",
        "columns": ["ept_membre_siren", "ept_siren", "ept_nom", "ept_cheflieu", "ept_naturejuridique"]
    },
    "table_population": {
        "path": "src/data/insee/table_population.parquet",
        "columns": ["com_type", "com_insee", "com_nom", "pop_recens"]
    }
}

# Créer des vues temporaires pour chaque fichier Parquet
for table_name, config in parquet_files.items():
    file_path = config["path"]
    columns = config["columns"]
    
    if os.path.exists(file_path):
        # Créer une vue temporaire pour le fichier Parquet
        conn.execute(f"CREATE OR REPLACE TEMPORARY VIEW {table_name} AS SELECT * FROM read_parquet('{file_path}');")
    else:
        # Créer une vue temporaire vide avec les colonnes attendues
        print(f"Le fichier {file_path} n'est pas disponible. Création d'une vue vide pour {table_name}.")
        columns_with_types = ", ".join([f"NULL AS {col}" for col in columns])
        conn.execute(f"CREATE OR REPLACE TEMPORARY VIEW {table_name} AS SELECT {columns_with_types} WHERE FALSE;")

# Lire la requête SQL depuis le fichier externe
sql_query = read_sql_file("./src/shared/sql/init.sql")

# Exécuter la requête SQL pour créer la table finale ngeofr
conn.execute(sql_query)

# Vérification du résultat
result = conn.execute("SELECT * FROM ngeofr LIMIT 10;").fetchall()
print("Résultat final (10 premières lignes) :")
for row in result:
    print(row)

# Sauvegarder le DataFrame dans un fichier Parquet
df_vf = conn.execute("SELECT * FROM ngeofr").fetchdf()
df_vf.to_parquet(
    path="src/db/ngeo2025.parquet",
    compression="gzip"
)

# Copie des fichiers vers le répertoire public
files_to_copy = ["src/db/ngeo2025.duckdb", "src/db/ngeo2025.parquet"]
destination_directory = "public/"


# Fermer la connexion
conn.close()

for file in files_to_copy:
    destination_file = os.path.join(destination_directory, os.path.basename(file))
    shutil.copy(file, destination_file)
    print(f"Fichier copié : {file} → {destination_file}")

