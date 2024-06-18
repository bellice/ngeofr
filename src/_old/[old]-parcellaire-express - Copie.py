# Importation des librairies
import pandas as pd
import dbfread
import fastparquet as fp
import pathlib
import sqlite3


# Connexion base de données
db_file = pathlib.Path("U:/DI/DINNOV/7_DATA/04_Portefeuille UC/90 Referentiel/public/database/ngeo-fr-cog2023.sqlite3")

query = "SELECT insee_dep FROM departement"

conn = sqlite3.connect(db_file)
cursor = conn.cursor()
result = cursor.execute(query)
dep = result.fetchall()
cursor.close()
conn.close()

## Alternative récupération des départements
# departements = pd.read_sql_query(query, conn)

# Liste des départements sur 3char
list_dep = []
for i in range(0, len(dep)):

    if len(dep[i][0]) == 2:
        list_dep.append("0" + dep[i][0])
    else:
        list_dep.append(dep[i][0])

# Automatiser le chemin en entrée
path_input = pathlib.Path("U:/DI/DINNOV/7_DATA/04_Portefeuille UC/90 Referentiel/public/pci/202304/PARCELLAIRE_EXPRESS_1-1__SHP_LAMB93_D001_2023-04-01/PARCELLAIRE_EXPRESS/1_DONNEES_LIVRAISON_2023-04-00181/PEPCI_1-1_SHP_LAMB93_D001/")

# Chemin dossier
path_input = pathlib.Path("U:/DI/DINNOV/7_DATA/04_Portefeuille UC/90 Referentiel/public/pci/202304/")
path_output = pathlib.Path("U:/DI/DINNOV/7_DATA/04_Portefeuille UC/99 Bertrand/referentiel-frgeo/parquet/pci/")

# Récupération des chemins des fichiers
files_input = list(path_input.rglob("*/*/*/*/PARCELLE.dbf"))

for i in range(0, len(files_input)):

    # Importation du fichier
    dbf = dbfread.DBF(filename=files_input[i], encoding="UTF-8")

    # Transformation en DataFrame
    df = pd.DataFrame(iter(dbf))

    # Exploration
    ## df.head()

    ## df.dtypes
    ## df.size
    ## df.shape

    # Conversion des types
    df = df.convert_dtypes()
    ## df.dtypes

    # Ecriture du fichier
    code_dep = "".join(df.CODE_DEP.unique())
    file_output = "pci-" + code_dep + "-202304.parquet"

    fp.write(path_output / file_output, df, compression = 'GZIP')
    print("Ecriture du fichier " + file_output + " OK")