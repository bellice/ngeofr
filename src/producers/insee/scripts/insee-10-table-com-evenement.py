# ---- 1 Importation des bibliothèques ----
import pandas as pd
import numpy as np
from src._shared.data_validation import test_length_values, test_no_null_values, test_date_format

# ---- 2 Chargement des données ----
df_init = pd.read_csv("src/producers/insee/assets/v_mvt_commune_2024.csv", encoding="UTF-8", na_values="")

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
    .assign(
            com_insee = lambda df: df["com_insee"].astype(str),
            date_debut = lambda df: pd.to_datetime(df['date_debut']),
            date_fin=pd.NaT
            )
)

# ---- 3 Transformation et nettoyage ----

# Code événement
# 10 Changement de nom
# 20 Création
# 21 Rétablissement
# 30 Suppression
# 31 Fusion simple
# 32 Création de commune nouvelle
# 33 Fusion association
# 34 Transformation de fusion association en fusion simple
# 35 Suppression de commune déléguée
# 41 Changement de code dû à un changement de département
# 50 Changement de code dû à un transfert de chef-lieu
# 70 Transformation de commune associée en commune déléguée

# Transformation des événements
df = (df_init
    .rename(columns={
        "MOD": "code_evenement",
        "DATE_EFF": "date",
        "TYPECOM_AV": "com_type_av",
        "COM_AV": "com_insee_av",
        "LIBELLE_AV": "com_nom_av",
        "TYPECOM_AP": "com_type_ap",
        "COM_AP": "com_insee_ap",
        "LIBELLE_AP": "com_nom_ap"
    })
    .drop_duplicates()
    .query('(com_type_av == "COM") & (com_type_ap == "COM")')
    .query("~code_evenement.isin([34, 35, 70])")
    .drop(columns=["com_type_av", "com_type_ap"])
    .drop(columns=["TNCC_AV", "NCC_AV", "NCCENR_AV", "TNCC_AP", "NCC_AP", "NCCENR_AP"])
    .assign(com_nom_av=lambda df: df["com_nom_av"].str.replace(r"\s*\([^)]*\)", "", regex=True),
            com_nom_ap=lambda df: df["com_nom_ap"].str.replace(r"\s*\([^)]*\)", "", regex=True),
            date=lambda df: pd.to_datetime(df["date"], format="%Y-%m-%d")))

df.loc[df["com_nom_av"].str.contains("\(")]

# Mapping des codes d'événements aux types d'événements spatiaux
event_spatial_map = {
    10: np.nan,  # Changement de nom, pas de changement spatial
    41: np.nan,  # Changement de code dû à un changement de département
    50: np.nan,  # Changement de code dû à un transfert de chef-lieu
    31: "expansion",  # Fusion simple
    32: "expansion",  # Création de commune nouvelle
    33: "expansion",  # Fusion association
    21: "contraction"  # Rétablissement de commune
}

# Pour les créations et suppressions, déterminer le type d'événement
df["evenement_spatial"] = df.apply(
    lambda row: (
        "expansion" if row["code_evenement"] == 30 and row["com_insee_av"] == row["com_insee_ap"] else
        "contraction" if row["code_evenement"] == 20 and row["com_insee_av"] == row["com_insee_ap"] else
        "partition" if row["code_evenement"] in [20, 30] and row["com_insee_av"] != row["com_insee_ap"] else
        event_spatial_map.get(row["code_evenement"], "error")
    ), axis=1
)

# Groupement par `code_evenement`
event_count = df.groupby("code_evenement").size().reset_index(name="count")

print(event_count)

# ---- 4 Test d'intégrité -----

try:

    test_no_null_values(df, "code_evenement")
    test_no_null_values(df, "com_insee_av")
    test_no_null_values(df, "com_insee_ap")
    test_no_null_values(df, "com_nom_av")
    test_no_null_values(df, "com_nom_ap") 
    test_length_values(df, "com_insee_av", [5])
    test_length_values(df, "com_insee_ap", [5])
    test_date_format(df, "date")

    print("Tous les tests d'intégrité ont été réussis.")


    # ---- 5 Écriture au format Parquet ----

    df.to_parquet("src/data/insee/table_com_evenement.parquet", engine="pyarrow", compression= "gzip")
    print("Les fichiers ont été écrits avec succès.")


except ValueError as e:
    print(e)
    print("Les fichiers n'ont pas été écrits en raison d'une erreur d'intégrité des données.")

