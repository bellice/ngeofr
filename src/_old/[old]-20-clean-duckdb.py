# Importation des librairies
import pathlib
import duckdb

# Chemin dossier
path_input = pathlib.Path("U:/DI/DINNOV/7_DATA/04_Portefeuille UC/99 Bertrand/referentiel-frgeo/public")

# Connexion à la base
db = duckdb.connect(path_input.__str__() + "/parcelle.duckdb")

# Requête

# Nom des colonnes
db.execute("DESCRIBE SELECT * FROM parcelle").df()

# Erreur sur le nombre de caractère
idu_error = db.execute("SELECT * FROM parcelle WHERE length(idu) <> 14").df()
print(idu_error)

# Traitement de l'erreur
idu_error_fixed = idu_error.loc[idu_error["insee_com"] == "70257"].replace(to_replace = r"^\d{4}", value = "70257", regex = True)
print(idu_error_fixed)

# Modification de la base
for i in range(len(idu_error)):
    db.execute(f"""
            UPDATE parcelle
            SET idu = '{ idu_error_fixed["idu"].values[i] }'
            WHERE idu LIKE '{ idu_error["idu"].values[i] }'
            """)

# Erreur sur le nombre de caractère
db.execute("SELECT * FROM parcelle WHERE length(idu) <> 14").df()

# Fin de la connexion
db.close()
