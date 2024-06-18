# Importation des librairies
import pandas as pd
import pathlib
import duckdb


# Chemin dossier
path_input = pathlib.Path("U:/DI/DINNOV/7_DATA/04_Portefeuille UC/99 Bertrand/referentiel-frgeo/src/params/parquet/pci")
path_output = pathlib.Path("U:/DI/DINNOV/7_DATA/04_Portefeuille UC/99 Bertrand/referentiel-frgeo/public")

# Récupération des chemins des fichiers
files_input = list(path_input.rglob("*202304.parquet"))

# Connexion à la base
db = duckdb.connect()

# Lecture de tous les fichiers parquet
db.execute("SELECT * FROM parquet_scan(?)", [path_input.__str__() + "/*.parquet"])

# Nom des colonnes et type des fichiers parquet
db.execute(f"""DESCRIBE SELECT * FROM  '{files_input[0].__str__() }'""").df()
## Schéma interne des fichiers parquet
# db.execute("SELECT * FROM parquet_schema(?)", [files_input[0].__str__()]).df()

# Choix 1 : requêter sur les fichiers parquets

# Créer une vue des fichiers parquet
db.execute(f"""CREATE VIEW parcelle AS SELECT * FROM read_parquet('{path_input.__str__() + "/*.parquet"}')""")

# Nombre de lignes
db.execute("SELECT COUNT(*) from parcelle").fetchall()

# Dénombrer le nombre de valeur NULL pour certaines colonnes
db.execute("""
           SELECT COUNT(*) - COUNT(IDU) AS IDU,
           COUNT(*) - COUNT(CODE_COM) AS CODE_COM,
           COUNT(*) - COUNT(CODE_DEP) AS CODE_DEP,
           COUNT(*) - COUNT(NOM_COM) AS NOM_COM
           FROM parcelle
           """).df()

# Nombre de parcelles par département
db.execute("SELECT CODE_DEP, COUNT(*) AS NB_PARCELLE FROM parcelle GROUP BY CODE_DEP").df()

# Doublon sur les identifiants des parcelles
db.execute("SELECT IDU, COUNT(IDU) FROM parcelle GROUP BY IDU HAVING COUNT(IDU) > 1").df()

# Visualisation d'un doublon sur identifiant de parcelle
db.execute("SELECT * FROM parcelle WHERE IDU LIKE '290710000C1286'").df()

# Commpter nombre de caractère sur identifiant de parcelles
db.execute("""SELECT length(IDU), COUNT(IDU)
           FROM parcelle GROUP BY length(IDU) HAVING COUNT(IDU) """).df()

# Visualisation des parcelles avec nombre de caractères erronés
db.execute("SELECT * FROM parcelle WHERE length(IDU) <> 14").df()

# Commpter nombre de caractère sur code commune
db.execute("""SELECT length(CODE_COM), COUNT(CODE_COM)
           FROM parcelle GROUP BY length(CODE_COM) HAVING COUNT(CODE_COM) """).df()

# Commpter nombre de caractère sur code département
db.execute("""SELECT length(CODE_DEP), COUNT(CODE_DEP)
           FROM parcelle GROUP BY length(CODE_DEP) HAVING COUNT(CODE_DEP) """).df()

# Harmonisation des colonnes
db.execute("""
           SELECT IDU as idu,
           CODE_DEP||CODE_COM AS 'insee_com',
           NOM_COM AS 'lib_com',
           CASE CODE_DEP WHEN '97' THEN CODE_DEP||CODE_COM[0:1] ELSE CODE_DEP END AS 'insee_dep'
           FROM parcelle
           WHERE idu LIKE '97801000AS0001'
           """).df()

# Fin de la connexion
db.close()



# Choix 2 : insérer les données dans une bdd

# Connexion à la base
db = duckdb.connect(path_output.__str__() + "/parcelle.duckdb")

# Nouvelle table
table_parcelle = """CREATE TABLE IF NOT EXISTS parcelle(
                    idu VARCHAR NOT NULL,
                    insee_com VARCHAR NOT NULL CHECK(length(insee_com) == 5),
                    lib_com VARCHAR NOT NULL,
                    insee_dep VARCHAR NOT NULL CHECK(length(insee_dep) == 2 OR length(insee_dep) == 3),
)"""

db.execute(table_parcelle)

db.execute(f"""
           INSERT INTO parcelle
           SELECT DISTINCT IDU as idu,
           CODE_DEP||CODE_COM AS 'insee_com',
           NOM_COM AS 'lib_com',
           CASE CODE_DEP WHEN '97' THEN CODE_DEP||CODE_COM[0:1] ELSE CODE_DEP END AS 'insee_dep'
           FROM read_parquet('{path_input.__str__() + "/*.parquet"}')
           """)

# Fin de la connexion
db.close()






