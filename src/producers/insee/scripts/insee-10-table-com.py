# ---- 1 Importation des bibliothèques ----
import pandas as pd
import numpy as np
from src.shared.data_validation import test_length_values, test_no_null_values

# ---- 2 Chargement des données ----
df_init = pd.read_csv("src/producers/insee/assets/v_commune_2025.csv", encoding="UTF-8", na_values="")

# ---- 3 Transformation et nettoyage ----

# Commune
df = (df_init
      .rename(columns={
         "TYPECOM": "com_type",
         "COM": "com_insee",
         "LIBELLE": "com_nom",
         "ARR": "arr_insee",
         "REG": "reg_insee",
         "DEP": "dep_insee",
         "COMPARENT": "com_parent"})
      .assign(
         com_nom=lambda df: df["com_nom"],
         com_insee=lambda df: df["com_insee"].astype(str),
         arr_insee=lambda df: np.where(pd.isnull(df["arr_insee"]), df["arr_insee"], df["arr_insee"].astype(str)),
         reg_insee=lambda df: df["reg_insee"].apply(lambda x: str(int(x)).zfill(2) if pd.notnull(x) else None),
         dep_insee=lambda df: np.where(pd.isnull(df["dep_insee"]), df["dep_insee"], df["dep_insee"].astype(str)),
         com_parent=lambda df: df["com_parent"].apply(lambda x: str(int(x)).zfill(5) if pd.notnull(x) else None),
         )
      .drop(columns=["CTCD", "TNCC", "NCC", "NCCENR", "CAN"])
      .sort_values(by=["com_insee"]))

df.loc[df["com_nom"].str.contains(r"\(")]

df.head()

# Créer un DataFrame de référence pour les communes de type "COM"
df_reference = df[df["com_type"] == "COM"][["com_insee", "reg_insee", "dep_insee", "arr_insee"]]

# Effectuer une jointure gauche pour remplir les champs manquants sur "COMA" et "COMD"
df = df.merge(df_reference, how="left", left_on="com_parent", right_on="com_insee", suffixes=('', '_ref'))

# Remplir les champs manquants
df["reg_insee"] = df["reg_insee"].fillna(df["reg_insee_ref"])
df["dep_insee"] = df["dep_insee"].fillna(df["dep_insee_ref"])
df["arr_insee"] = df["arr_insee"].fillna(df["arr_insee_ref"])

# Supprimer les colonnes de référence ajoutées lors de la jointure
df = df.drop(columns=["reg_insee_ref", "dep_insee_ref", "arr_insee_ref", "com_insee_ref"])

df.head()
df[df['com_insee'].astype(str).isin(['58062', '58063'])]

# ---- 4 Test d'intégrité -----
try:

    test_no_null_values(df, "com_insee")
    test_no_null_values(df, "com_nom")
    test_length_values(df, "com_insee", [5])
    test_length_values(df, "arr_insee", [3, 4])
    test_length_values(df, "dep_insee", [2, 3])
    test_length_values(df, "reg_insee", [2])
    test_length_values(df, "com_parent", [5])

    print("Tous les tests d'intégrité ont été réussis.")


# ---- 5 Écriture au format Parquet ----

    df.to_parquet("src/data/insee/table_com.parquet", engine = "pyarrow", compression="gzip")
    print("Les fichiers ont été écrits avec succès.")

except ValueError as e:
    print(e)
    print("Les fichiers n'ont pas été écrits en raison d'une erreur d'intégrité des données.")
