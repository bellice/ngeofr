# ---- 1 Importation des bibliothèques ----
import pandas as pd
from src._shared.data_validation import test_length_values, test_no_null_values

# ---- 2 Chargement des données ----
df_init = pd.read_excel("src/producers/banatic/assets/TableCorrespondanceSirenInsee/Banatic_SirenInsee2024.xlsx")

# ---- 3 Transformation et nettoyage ----
df = (df_init
      .rename(columns={
         "siren": "com_siren",
         "insee": "com_insee",
         "nom_com": "com_nom"})
      .assign(
         com_siren=lambda df: df["com_siren"].astype(str),
         com_insee=lambda df: df["com_insee"].astype(str),
         )
      .drop(columns=["Reg_com", "dep_com", "ptot_2024", "pmun_2024", "pcap_2024"])
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

    df.to_parquet("src/params/banatic/table_com_siren.parquet", engine="pyarrow", compression="gzip")
    print("Les fichiers ont été écrits avec succès.")

except ValueError as e:
    print(e)
    print("Les fichiers n'ont pas été écrits en raison d'une erreur d'intégrité des données.")
