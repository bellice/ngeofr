# ---- 1 Importation des bibliothèques ----
import pandas as pd
from src._shared.data_validation import test_length_values, test_no_null_values

# ---- 2 Chargement des données ----
df_init = pd.read_csv("src/producers/insee/assets/v_region_2025.csv", encoding="UTF-8", na_values="")

# ---- 3 Transformation et nettoyage ----
df = (df_init
      .rename(columns={
         "REG": "reg_insee",
         "CHEFLIEU": "reg_cheflieu",
         "LIBELLE": "reg_nom"})
      .assign(
         reg_insee=lambda df: df["reg_insee"].apply(lambda x: str(int(x)).zfill(2)),
         reg_cheflieu=lambda df: df["reg_cheflieu"].astype(str)
         )
      .drop(columns=["TNCC", "NCC", "NCCENR"])
      .sort_values(by=["reg_insee"]))


# ---- 4 Test d'intégrité -----
try:

    test_no_null_values(df, "reg_insee")
    test_no_null_values(df, "reg_cheflieu")
    test_no_null_values(df, "reg_nom")
    test_length_values(df, "reg_insee", [2])
    test_length_values(df, "reg_cheflieu", [5])

    print("Tous les tests d'intégrité ont été réussis.")

    # ---- 5 Écriture au format Parquet ----

    df.to_parquet("src/data/insee/table_reg.parquet", engine="pyarrow", compression="gzip")
    print("Les fichiers ont été écrits avec succès.")

except ValueError as e:
    print(e)
    print("Les fichiers n'ont pas été écrits en raison d'une erreur d'intégrité des données.")
