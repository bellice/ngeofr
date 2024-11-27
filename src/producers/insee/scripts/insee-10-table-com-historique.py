# ---- 1 Importation des bibliothèques ----
import pandas as pd
import numpy as np
from src._shared.data_validation import test_length_values, test_no_null_values, test_date_format

# ---- 2 Chargement des données ----
df_init = pd.read_csv("src/producers/insee/assets/v_commune_depuis_1943.csv", encoding="UTF-8", na_values="")

# Création du DataFrame pour les communes manquantes
datecom_manquant_init = [
    ("98411", "Îles Saint-Paul et Amsterdam", "2008-01-01", None),
    ("98412", "Archipel des Kerguelen", "2008-01-01", None),
    ("98413", "Archipel des Crozet", "2008-01-01", None),
    ("98414", "La Terre-Adélie", "2008-01-01", None),
    ("98415", "Îles Éparses de l'océan Indien", "2008-01-01", None),
    ("98611", "Alo", "2008-01-01", None),
    ("98612", "Sigave", "2008-01-01", None),
    ("98613", "Uvea", "2008-01-01", None),
    ("98901", "Île de Clipperton", "2008-01-01", None)
    
]

datecom_manquant = (
    pd.DataFrame(datecom_manquant_init, columns=['com_insee', 'com_nom', 'date_debut', 'date_fin'])
    .assign(date_debut=lambda df: pd.to_datetime(df['date_debut']),
            date_fin=pd.NaT)
)

# ---- 3 Transformation et nettoyage ----
df = (df_init
                 .rename(columns={
                    "COM": "com_insee",
                    "LIBELLE": "com_nom",
                    "DATE_DEBUT": "date_debut",
                    "DATE_FIN": "date_fin"})
                 .assign(
                    com_nom=lambda df: df["com_nom"].str.replace(r"\s*\([^)]*\)", "", regex=True),
                    com_insee = lambda df: df["com_insee"].astype(str),
                    )
                 .pipe(lambda df: pd.concat([df, datecom_manquant], ignore_index=True))
                 .drop(columns = ["TNCC", "NCC", "NCCENR"])
                 .sort_values(by=['com_insee', 'date_debut'])
                 .assign(date_debut=lambda df: pd.to_datetime(df["date_debut"], format = "%Y-%m-%d"),
                         date_fin=lambda df: pd.to_datetime(df["date_fin"], format = "%Y-%m-%d")))

df.loc[df["com_nom"].str.contains("\(")]

# ---- 4 Test d'intégrité -----
try:

    test_no_null_values(df, "com_insee")
    test_no_null_values(df, "com_nom")
    test_length_values(df, "com_insee", [5])
    test_date_format(df, "date_debut")
    test_date_format(df, "date_fin")

    print("Tous les tests d'intégrité ont été réussis.")


    # ---- 5 Écriture au format Parquet ----

    df.to_parquet("src/params/insee/table_com_historique.parquet", engine="pyarrow", compression="gzip")
    print("Les fichiers ont été écrits avec succès.")

except ValueError as e:
    print(e)
    print("Les fichiers n'ont pas été écrits en raison d'une erreur d'intégrité des données.")
