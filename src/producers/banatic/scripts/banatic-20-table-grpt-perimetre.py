# ---- 1 Importation des bibliothèques ----
import pandas as pd
import os
import glob
import re
from src.shared.data_validation import test_length_values, test_no_null_values

# ---- 2 Chargement des données ----

# Chemin vers le dossier
path = "src/producers/banatic/assets/"

# Nom du fichier à lire
file_name = "Périmètre_des_groupements_en_2025.xlsx"

# Chemin complet du fichier
file_path = os.path.join(path, file_name)

# Convertir en DataFrame
df_init = pd.read_excel(file_path)

# Chargement des tables de référence INSEE
df_com = pd.read_parquet("src/data/insee/table_com.parquet")[['com_insee', 'com_nom', 'dep_insee']]
df_dep = pd.read_parquet("src/data/insee/table_dep.parquet")[['dep_insee', 'dep_nom']]

# Jointure des tables INSEE pour avoir une table de référence complète
df_ref = pd.merge(
    df_com,
    df_dep,
    on='dep_insee',
    how='inner'
)

# ---- 3 Transformation et nettoyage ----

df = (
    df_init
    .rename(columns={
         "N° SIREN": "grpt_siren",
         "Nom du groupement": "grpt_nom",
         "Nature juridique": "grpt_naturejuridique",
         "Groupement interdépartemental": "grpt_interdep",
         "Siren du membre": "grpt_membre_siren",
         "Nom du membre": "grpt_membre_nom",
         "Type du membre": "grpt_membre_type",
         "Commune siège": "grpt_cheflieu_com",
         "Département siège": "grpt_cheflieu_dep"})
    .pipe(lambda x: x.loc[:, x.columns.str.startswith("grpt")])
    .dropna(subset=['grpt_membre_siren'])  # Supprime les lignes où grpt_membre_siren est NULL
    .assign(
        grpt_siren=lambda x: x["grpt_siren"].astype(str),
        grpt_membre_siren=lambda x: x["grpt_membre_siren"].astype('int64').astype(str),
        grpt_interdep=lambda x: x["grpt_interdep"].map({"OUI": True, "NON": False}),
        grpt_nom=lambda x: x["grpt_nom"].str.replace(r"\s{2,}", " ", regex=True),
    )
)

# A METTRE EN PAUSE
# Jointure avec la table de référence pour obtenir le code INSEE de la commune chef-lieu
df_join = (
    df
    .merge(
        df_ref,
        left_on=['grpt_cheflieu_com', 'grpt_cheflieu_dep'],
        right_on=['com_nom', 'dep_nom'],
        how='left'
    )
    .drop(columns=['grpt_cheflieu_com', 'grpt_cheflieu_dep', 'com_nom', 'dep_nom'])
    .rename(columns={'com_insee': 'grpt_cheflieu'})
)
# A FAIRE PLUS TARD, PROBLEME DE NOMBRE DE VALEURS

df_epci = (
    df
    .query("grpt_naturejuridique in ['Communauté d\\'agglomération', 'Communauté urbaine', 'Communauté de communes', 'Métropole', 'Métropole de Lyon']")
    .rename(columns={
            "grpt_siren": "epci_siren",
            "grpt_nom": "epci_nom",
            "grpt_cheflieu_com": "epci_cheflieu_com",
            "grpt_cheflieu_dep": "epci_cheflieu_dep",
            "grpt_naturejuridique": "epci_naturejuridique",
            "grpt_interdep": "epci_interdep",
            "grpt_membre_siren": "epci_membre_siren",
            "grpt_membre_nom": "epci_membre_nom"})
    .pipe(lambda x: x.loc[:, x.columns.str.startswith("epci")])
    )
# 34923 df_epci

df_ept = (
    df
    .query("grpt_naturejuridique in ['Etablissement public territorial']")
    .rename(columns={
            "grpt_siren": "ept_siren",
            "grpt_nom": "ept_nom",
            "grpt_cheflieu_com": "ept_cheflieu",
            "grpt_naturejuridique": "ept_naturejuridique",
            "grpt_membre_siren": "ept_membre_siren",
            "grpt_membre_nom": "ept_membre_nom"})
    .pipe(lambda x: x.loc[:, x.columns.str.startswith("ept")])
    )
# 130 df_epci
# ---- 4 Test d'intégrité -----

try:
    test_no_null_values(df, "grpt_siren")
    test_no_null_values(df, "grpt_nom")
    test_no_null_values(df, "grpt_naturejuridique")
    test_no_null_values(df, "grpt_interdep")
    test_no_null_values(df, "grpt_membre_type")
    test_no_null_values(df, "grpt_membre_siren")
    test_no_null_values(df, "grpt_membre_nom")
    test_no_null_values(df, "grpt_membre_type")
    test_no_null_values(df, "grpt_cheflieu_com")
    test_length_values(df, "grpt_siren", [9])
    test_length_values(df, "grpt_membre_siren", [9])

    print("Tous les tests d'intégrité ont été réussis.")


# ---- 5 Écriture au format Parquet ----

    df.to_parquet("src/data/banatic/table_grpt_perimetre.parquet", engine="pyarrow", compression="gzip")
    df_epci.to_parquet("src/data/banatic/table_epci_perimetre.parquet", engine="pyarrow", compression="gzip")
    df_ept.to_parquet("src/data/banatic/table_ept_perimetre.parquet", engine="pyarrow", compression="gzip")
    print("Les fichiers ont été écrits avec succès.")


except ValueError as e:
    print(e)
    print("Les fichiers n'ont pas été écrits en raison d'une erreur d'intégrité des données.")
