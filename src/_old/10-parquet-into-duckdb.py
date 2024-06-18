# Importation des librairies
import pathlib
import duckdb

# Chemin dossier
path_input = pathlib.Path("U:/DI/DINNOV/7_DATA/04_Portefeuille UC/99 Bertrand/referentiel-frgeo/src/params/parquet/parcelle")
path_output = pathlib.Path("U:/DI/DINNOV/7_DATA/04_Portefeuille UC/99 Bertrand/referentiel-frgeo/public")

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






