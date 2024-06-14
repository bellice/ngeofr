# Importation des librairies
import pandas as pd
import dbfread
import fastparquet as fp
import pathlib
import re

# Chemin dossier
path_input = pathlib.Path("U:/DI/DINNOV/7_DATA/04_Portefeuille UC/90 Referentiel/public/pci/202304/")
path_output = pathlib.Path("U:/DI/DINNOV/7_DATA/04_Portefeuille UC/99 Bertrand/referentiel-frgeo/src/params/parquet/parcelle/")

# Récupération des chemins des fichiers
files_input = list(path_input.rglob("*/*/*/*/PARCELLE.dbf"))

for i in range(0, len(files_input)):
    print("Traitement du fichier " + files_input[i].__str__())
    # Importation du fichier
    dbf = dbfread.DBF(filename=files_input[i], encoding="UTF-8")

    # Transformation en DataFrame
    df = pd.DataFrame(iter(dbf))

    # Conversion des types
    df = df.convert_dtypes()

    # Ecriture du fichier
    code_dep = re.search(r"_D(\d{3})\\PARCELLE\.dbf$", files_input[i].__str__()).group(1)
    file_output = "pci-parcelle-" + code_dep + "-202304.parquet"

    fp.write(path_output / file_output, df, compression = 'GZIP')
    print("Ecriture du fichier " + file_output + " OK")