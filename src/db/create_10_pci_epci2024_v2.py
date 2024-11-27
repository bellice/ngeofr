# Importation des librairies
from pathlib import Path
import duckdb
import pandas as pd
import geopandas as gpd
import os

# Chemin dossier
path_input = Path("D:/ign/parcellaire-express/parquet/dep")
path_output = Path("D:/ign/parcellaire-express/parquet/epci")
version = "2024-07"

path_db = 'O:/Document/carto-engine/ngeofr/src/_db/ngeo2024.duckdb'
print(os.path.exists(path_db))

# Créer la connexion
db_ngeofr = duckdb.connect(path_db)

# Vérifiez les bases de données attachées
db_ngeofr.execute("PRAGMA show_databases;").fetchall() # Cela devrait inclure 'ngeofr' si l'attachement a réussi

db_ngeofr.execute("SHOW TABLES;").fetchall()
db_ngeofr.execute("SHOW ALL TABLES;").fetchall()

# Récupérer la liste des EPCI et des départements et communes associés (dep_insee) dans ngeofr
epci = db_ngeofr.execute("""
SELECT DISTINCT epci_siren, epci_nom, com_nom, com_insee, dep_insee
FROM ngeofr
""").fetchdf()

# Utiliser un générateur pour lister les fichiers existants et extraire les epci_siren
epci_done = {
    file.stem.split('-')[-1] for file in path_output.rglob(f"{version}-pci-*.parquet")
}

# Filtrer les EPCI non encore exportés
epci_todo = epci[~epci['epci_siren'].isin(epci_done)]

# Pour chaque EPCI, charger uniquement les fichiers des départements nécessaires
epci_test = epci_todo.head(1)  # Limite à 1 ligne*

print(epci_test)

for index, row in epci_test.iterrows():
    epci_siren = row['epci_siren']
    epci_nom = row['epci_nom']
    dep_insee = row['dep_insee']  # Ceci est le ou les départements associés à l'EPCI

    # Construire une liste de fichiers Parquet à charger (basée sur dep_insee)
    dep_insee_list = dep_insee.split(',')  # Si dep_insee contient plusieurs départements
    dep_file_patterns = [f"{version}*{dep}*.parquet" for dep in dep_insee_list]  # Créer les motifs pour les fichiers parquet

    # Créer la connexion
    # Connexion à la base
    db = duckdb.connect("")
    db.execute("INSTALL spatial;")
    db.execute("LOAD spatial;")

    # Charger uniquement les fichiers Parquet des départements concernés
    for dep_pattern in dep_file_patterns:
        parquet_files = list(path_input.rglob(dep_pattern))  # Rechercher les fichiers correspondant au motif
        if parquet_files:  # S'il y a des fichiers correspondants
            # Nom de la vue pour cet EPCI
            view_name = f"pci_{epci_siren}"
            
            # Supprimer la vue si elle existe déjà
            db.execute(f"DROP VIEW IF EXISTS {view_name}")

            # Créer la vue pour charger les données Parquet
            db.execute(f"""
            CREATE VIEW {view_name} AS
            SELECT * 
            FROM read_parquet('{parquet_files[0]}')  -- Charger le fichier Parquet
            """)

    # Vérifiation de la projection
    # Ouvrir un fichier sans lire les données
    epsg  = gpd.read_parquet(parquet_files[0]).crs.to_string()

    # Vérification de la géométrie
    db.execute(f"""PRAGMA table_info('pci_{epci_siren}')""").fetchall()

    # Vérification des correspondances com_insee
    ngeofr_com_insee = db_ngeofr.execute(f"""
    SELECT DISTINCT com_insee FROM ngeofr WHERE dep_insee IN ('{"','".join(dep_insee_list)}')
    """).fetchdf()


    # Récupérer les com_insee calculés depuis pci_{epci_siren} pour l'EPCI en cours
    pci_com_insee = db.execute(f"""
    SELECT DISTINCT CODE_DEP || CODE_COM AS com_insee FROM pci_{epci_siren}
    """).fetchdf()

    # Trouver les com_insee dans pci qui ne sont pas dans ngeofr
    missing_in_ngeofr = pci_com_insee[
        ~pci_com_insee['com_insee'].isin(ngeofr_com_insee['com_insee'])
    ]

    # Trouver les com_insee dans ngeofr qui ne sont pas dans parcelle
    missing_in_pci = ngeofr_com_insee[
        ~ngeofr_com_insee['com_insee'].isin(pci_com_insee['com_insee'])
    ]

    if missing_in_ngeofr.empty and missing_in_pci.empty:
        print(f"Validation réussie pour EPCI {epci_siren}. Jointure peut être effectuée.")
        
        # Effectuer la jointure ici

        com_insee_list = (
            db_ngeofr.execute(f"""
                SELECT DISTINCT com_insee 
                FROM ngeofr 
                WHERE epci_siren LIKE '{epci_siren}'
            """)
            .fetchdf()  # Récupérer le DataFrame
            .pipe(lambda x: x.sort_values(by="com_insee"))   # Trier par la colonne
            .pipe(lambda df: df['com_insee'].tolist())  # Extraire la colonne 'com_insee' en liste
        )

        # Effectuer la jointure avec ngeofr pour rapatrier epci_siren et epci_nom
        db.execute(f"""
            DROP VIEW IF EXISTS pci_joint_{epci_siren};  -- Supprimer la vue existante avant de la recréer

            CREATE VIEW pci_joint_{epci_siren} AS
            SELECT
                pci_{epci_siren}.IDU AS pci_id,                  -- Renommer IDU en pci_id
                pci_{epci_siren}.CODE_DEP || pci_{epci_siren}.CODE_COM AS com_insee,  -- Concaténation CODE_DEP et CODE_COM en com_insee
                pci_{epci_siren}.NOM_COM AS com_nom,               -- Renommer NOM_COM en com_nom
                pci_{epci_siren}.COM_ABS AS com_abs,               -- Renommer COM_ABS en com_abs
                pci_{epci_siren}.SECTION AS pci_section,           -- Renommer SECTION en pci_section
                pci_{epci_siren}.NUMERO AS pci_num,                -- Renommer NUMERO en pci_nom
                pci_{epci_siren}.CONTENANCE AS pci_surface,        -- Renommer CONTENANCE en pci_surface
                '{epci_siren}' AS epci_siren,                      -- Ajouter epci_siren
                '{epci_nom.replace("'", "''")}' AS epci_nom,       -- Ajouter epci_nom en échappant les apostrophes
            ST_GeomFromWKB(pci_{epci_siren}.geometry) AS geometry  -- Conversion du BLOB en GEOMETRY puis en WKT
            FROM pci_{epci_siren}
            WHERE pci_{epci_siren}.CODE_DEP || pci_{epci_siren}.CODE_COM IN ('{"','".join(com_insee_list)}')  -- Filtrer par les communes spécifiques
            ORDER BY com_insee, pci_section, pci_num;  -- Trier par com_insee, pci_section, puis pci_num
        """)

        # Exporter les données en fichiers Parquet par EPCI avec COPY
        output_path_str = path_output / f"{version}-pci-{epci_siren}.parquet"
        
        # Créer un fichier Parquet à partir de la vue "pci_joint_{epci_siren}"
        db.execute(f"""
        COPY (SELECT * EXCLUDE geometry, ST_AsWKB(geometry) AS geometry FROM pci_joint_{epci_siren})
        TO '{output_path_str}' (FORMAT PARQUET, COMPRESSION GZIP);
        """)
        print(f"Export terminé pour EPCI {epci_siren}")

    else:
        print(f"Validation échouée pour EPCI {epci_siren}. Communes manquantes détectées.")
        print("Communes absentes dans ngeofr :", missing_in_ngeofr)
        print("Communes absentes dans pci :", missing_in_pci)


# Lire le fichier Parquet, convertir la géométrie, et ajouter le CRS en une seule chaîne
df = (
    pd.read_parquet(output_path_str)  # Lire le fichier Parquet
    .pipe(lambda x: x.assign(geometry=gpd.GeoSeries.from_wkb(x['geometry'])))  # Convertir WKB en géométrie
    .pipe(lambda x: gpd.GeoDataFrame(x, geometry='geometry'))  # Convertir en GeoDataFrame
    .pipe(lambda x: x.set_crs(epsg, allow_override=True))  # Ajouter le CRS (par exemple, EPSG:4326)
)

# Exporter le GeoDataFrame en GeoParquet
df.to_parquet(output_path_str, compression='gzip', engine='pyarrow')
print(f"Export terminé pour EPCI {epci_siren} en format GeoParquet")

db.close()