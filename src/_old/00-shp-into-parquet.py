# Importation des librairies
import os
os.environ["USE_PYGEOS"] = "0"
import pathlib
import geopandas as gpd
import dask_geopandas
import re
import time

# Chemin dossier
path_input = pathlib.Path("U:/DI/DINNOV/7_DATA/04_Portefeuille UC/90 Referentiel/public/pci/202304/")
path_output = pathlib.Path("U:/DI/DINNOV/7_DATA/04_Portefeuille UC/99 Bertrand/referentiel-frgeo/src/params/parquet/parcelle")

# Récupération des chemins des fichiers
files_input = list(path_input.rglob("*/*/*/*/PARCELLE.shp"))

for i in range(28,len(files_input)):

    code_dep = re.search(r"_D([0-9]{2}[0-9|A-B])\\PARCELLE\.shp$", files_input[i].__str__()).group(1)
    print(f"Département {code_dep} - index {i}")

    # Importation des données
    start_time = time.time()
    df = gpd.read_file(files_input[i])
    import_time = time.time() - start_time

    print(f"--- Importation OK en {import_time:.2f}s")

    # Exportation des données

    ## Traitement parallèle
    d_df = dask_geopandas.from_geopandas(df, chunksize=500000)
 
    ## Ecriture
    start_time = time.time()
    d_df.to_parquet(path = path_output, name_function = lambda i: f"pci-parcelle-{code_dep}-{i}-202304.parquet", compression = "gzip")
    export_time = time.time() - start_time
    
    print(f"--- Exportation OK en {export_time:.2f}s")

    print(f"--- Durée totale : {import_time + export_time:.2f}s\n")
