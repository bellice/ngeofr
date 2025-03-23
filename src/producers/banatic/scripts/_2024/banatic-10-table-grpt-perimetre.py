# ---- 1 Importation des bibliothèques ----
import pandas as pd
import os
import glob
import re
from src._shared.data_validation import test_length_values, test_no_null_values

# ---- 2 Chargement des données ----

# Chemin vers le dossier
path = "src/producers/banatic/assets/"

# Lister tous les fichiers CSV et les filtrer selon le pattern
pattern = re.compile(r'^D[0-9A-Z]{2,3}-2024-01-01-perimetre-groupement\.csv$')
files_filtered = [
    f for f in glob.glob(os.path.join(path, "*.csv")) 
    if pattern.search(os.path.basename(f))
]

# Lire et combiner les fichiers CSV en un seul DataFrame
df_init = pd.concat(
    (pd.read_table(f, sep='\t', index_col=0) for f in files_filtered),
    ignore_index=False
).reset_index()

# ---- 3 Transformation et nettoyage ----

df = (
    df_init
    .assign(
        grpt_cheflieu_reg=lambda x: x['Région siège'].str.split(' - ', expand=True)[0],
        grpt_cheflieu_dep=lambda x: x['Département siège'].str.split(' - ', expand=True)[0],
        grpt_cheflieu_com=lambda x: x['Commune siège'].str.split(' - ', expand=True)[0],
    )
    .rename(columns={
         "N° SIREN": "grpt_siren",
         "Nom du groupement": "grpt_nom",
         "Nature juridique": "grpt_naturejuridique",
         "Groupement interdépartemental": "grpt_interdep",
         "Siren membre": "grpt_membre_siren",
         "Nom membre": "grpt_membre_nom",
         "Type": "grpt_membre_type"})
    .pipe(lambda x: x.loc[:, x.columns.str.startswith("grpt")])
    .assign(
        grpt_siren=lambda x: x["grpt_siren"].astype(str),
        grpt_membre_siren=lambda x: x["grpt_membre_siren"].astype(str),
        grpt_interdep=lambda x: x["grpt_interdep"].astype(int),
        grpt_nom=lambda x: x["grpt_nom"].str.replace(r"\s{2,}", " ", regex=True)
        )
)

df_epci = (
    df
    .query("grpt_naturejuridique in ['METRO', 'CC', 'CA', 'CU', 'MET69']")
    .rename(columns={
            "grpt_siren": "epci_siren",
            "grpt_nom": "epci_nom",
            "grpt_cheflieu_com" :"epci_cheflieu",
            "grpt_naturejuridique": "epci_naturejuridique",
            "grpt_interdep": "epci_interdep",
            "grpt_membre_siren": "epci_membre_siren",
            "grpt_membre_nom": "epci_membre_nom"})
    .pipe(lambda x: x.loc[:, x.columns.str.startswith("epci")])
    )

df_ept = (
    df
    .query("grpt_naturejuridique in ['EPT']")
    .rename(columns={
            "grpt_siren": "ept_siren",
            "grpt_nom": "ept_nom",
            "grpt_cheflieu_com": "ept_cheflieu",
            "grpt_naturejuridique": "ept_naturejuridique",
            "grpt_membre_siren": "ept_membre_siren",
            "grpt_membre_nom": "ept_membre_nom"})
    .pipe(lambda x: x.loc[:, x.columns.str.startswith("ept")])
    )

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
    test_no_null_values(df, "grpt_cheflieu_reg")
    test_no_null_values(df, "grpt_cheflieu_dep")
    test_no_null_values(df, "grpt_cheflieu_com")
    test_length_values(df, "grpt_siren", [9])
    test_length_values(df, "grpt_membre_siren", [9])
    test_length_values(df, "grpt_interdep", [1])
    test_length_values(df, "grpt_cheflieu_reg", [2])
    test_length_values(df, "grpt_cheflieu_dep", [2, 3])
    test_length_values(df, "grpt_cheflieu_com", [9])

    print("Tous les tests d'intégrité ont été réussis.")


# ---- 5 Écriture au format Parquet ----

    df.to_parquet("src/params/banatic/table_grpt_perimetre.parquet", engine="pyarrow", compression="gzip")
    df_epci.to_parquet("src/params/banatic/table_epci_perimetre.parquet", engine="pyarrow", compression="gzip")
    df_ept.to_parquet("src/params/banatic/table_ept_perimetre.parquet", engine="pyarrow", compression="gzip")
    print("Les fichiers ont été écrits avec succès.")


except ValueError as e:
    print(e)
    print("Les fichiers n'ont pas été écrits en raison d'une erreur d'intégrité des données.")
