# ---- 1 Importation des bibliothèques ----
import pandas as pd
from src.shared.data_validation import test_length_values, test_no_null_values

# ---- 2 Chargement des données ----
df_init = pd.read_excel("src/producers/banatic/assets/Liste des communes_20250325.xlsx")

# ---- 3 Transformation et nettoyage ----
df = (df_init
      .rename(columns={
         "Siren": "com_siren",
         "Code INSEE de la commune": "com_insee",
         "Nom de la commune": "com_nom"})
      .assign(
         com_siren=lambda df: df["com_siren"].astype(str),
         com_insee=lambda df: df["com_insee"].astype(str),
         )
      .sort_values(by=["com_insee"]))

# ---- 4 Test d'intégrité -----
try:

    test_no_null_values(df, "com_siren")
    test_no_null_values(df, "com_insee")
    test_no_null_values(df, "com_nom")
    test_length_values(df, "com_siren", [9])
    test_length_values(df, "com_insee", [5])
    print("Tous les tests d'intégrité ont été réussis.")

    # ---- 5 Écriture au format Parquet ----

    df.to_parquet("src/data/banatic/table_com_siren.parquet", engine="pyarrow", compression="gzip")
    print("Les fichiers ont été écrits avec succès.")

except ValueError as e:
    print(e)
    print("Les fichiers n'ont pas été écrits en raison d'une erreur d'intégrité des données.")
