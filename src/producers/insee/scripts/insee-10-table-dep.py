# ---- 1 Importation des bibliothèques ----
import pandas as pd
import numpy as np
from src._shared.data_validation import test_length_values, test_no_null_values

# ---- 2 Chargement des données ----
df_init = pd.read_csv("src/producers/insee/assets/v_departement_2024.csv", encoding="UTF-8", na_values="")

# ---- 3 Transformation et nettoyage ----
df = (df_init
      .rename(columns = {
         "DEP": "dep_insee",
         "REG": "reg_insee",
         "CHEFLIEU": "dep_cheflieu",
         "LIBELLE": "dep_nom"})
      .assign(
         dep_insee=lambda df: df["dep_insee"].astype(str),
         reg_insee=lambda df: df["reg_insee"].apply(lambda x: str(int(x)).zfill(2)),
         dep_cheflieu=lambda df: df["dep_cheflieu"].astype(str)
         )
      .drop(columns=["TNCC", "NCC", "NCCENR"])
      .sort_values(by=["dep_insee"]))


# ---- 4 Test d'intégrité -----
try:

    test_no_null_values(df, "dep_insee")
    test_no_null_values(df, "reg_insee")
    test_no_null_values(df, "dep_cheflieu")
    test_no_null_values(df, "dep_nom")
    test_length_values(df, "dep_insee", [2, 3])
    test_length_values(df, "reg_insee", [2])
    test_length_values(df, "dep_cheflieu", [5])

    print("Tous les tests d'intégrité ont été réussis.")

    # ---- 5 Écriture au format Parquet ----

    df.to_parquet("src/data/insee/table_dep.parquet", engine = "pyarrow", compression="gzip")
    print("Les fichiers ont été écrits avec succès.")

except ValueError as e:
    print(e)
    print("Les fichiers n'ont pas été écrits en raison d'une erreur d'intégrité des données.")


