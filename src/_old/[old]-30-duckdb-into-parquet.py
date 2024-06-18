# Importation des librairies
import duckdb
import fastparquet as fp
import pathlib

# Chemin dossier
path = pathlib.Path("U:/DI/DINNOV/7_DATA/04_Portefeuille UC/99 Bertrand/referentiel-frgeo/public")

# Connexion à la base
db = duckdb.connect(path.__str__() + "/parcelle.duckdb")

# Liste des départements
liste_dep = db.execute("SELECT DISTINCT insee_dep FROM parcelle ORDER BY insee_dep").df()

for i in range(0, len(liste_dep)):
    # Sélection d'un département
    dep = liste_dep.at[i,"insee_dep"]
    # Sélection des parcelles du département
    query = db.execute(f"""SELECT * FROM parcelle WHERE insee_dep LIKE '{dep}'""")

    # Nom du fichier
    file_output = "parcelle-" + f"{dep:0>3}" + "-202304.parquet"

    # Ecriture du fichier
    fp.write(path / "parcelle" / file_output, query.df(), compression = 'GZIP')
    print("Ecriture du fichier " + file_output + " OK")

# Fin de la connexion
db.close()
