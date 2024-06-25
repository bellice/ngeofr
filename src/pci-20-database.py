# Importation des librairies
from pathlib import Path
import duckdb
import pandas as pd

# Chemin dossier
path_input = Path("G:/ign/parcellaire-express/parquet/")
path_output = Path("O:/Document/carto-engine/ngeo/public/parcelle")


# Récupération des chemins des fichiers
files_input = list(path_input.rglob("2023-07*.parquet"))



# Création de fichiher parquet par département





















# Connexion à la base
db = duckdb.connect(database=path_output.__str__() + "/2023-07-parcelle.duckdb", read_only=False)

db.execute("INSTALL spatial;LOAD spatial;").df()



# Nouvelle table
table_parcelle = """CREATE TABLE IF NOT EXISTS parcelle(
                    idu VARCHAR NOT NULL,
                    insee_com VARCHAR NOT NULL CHECK(length(insee_com) == 5),
                    lib_com VARCHAR NOT NULL,
                    insee_dep VARCHAR NOT NULL CHECK(length(insee_dep) == 2 OR length(insee_dep) == 3),
                    geom WKB_BLOB,
                    )"""

db.execute(table_parcelle)

db.execute("SET memory_limit='8GB'").df()
db.execute("SET threads TO 1").df()


# Nom des colonnes et type des fichiers parquet
db.execute(f"""DESCRIBE SELECT * FROM read_parquet('{path_input.__str__() + "/2023-07*.parquet"}')""").df()

N = 0
while N < 5000000: 
  db.execute(f"""
            INSERT INTO parcelle
            SELECT IDU as idu,
            CODE_DEP||CODE_COM AS 'insee_com',
            NOM_COM as lib_com,
            CASE CODE_DEP WHEN '97' THEN CODE_DEP||CODE_COM[0:1] ELSE CODE_DEP END AS 'insee_dep',
            ST_AsWKB(ST_GeomFromWKB(geometry)) AS 'geom',
            FROM read_parquet('{path_input.__str__() + "/2023-07*.parquet"}')
            LIMIT {N} + 500000
            OFFSET {N}
            """)
  N = N+500000


#db.execute(f"""CREATE VIEW parcelle AS SELECT * FROM read_parquet('{path_input.__str__() + "/2023-07*.parquet"}')""")




db.execute("RESET memory_limit").df()



# Fin de la connexion
db.close()

