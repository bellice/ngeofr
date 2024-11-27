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
pattern = re.compile(r'^D[0-9A-Z]{2,3}-2024-01-01-liste-groupement\.csv$')
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
    # Décaler les colonnes sur la gauche
    .set_axis(df_init.columns[1:].to_list() + [df_init.columns[0]], axis=1)
    .drop(columns=["index"])
    .assign(
        grpt_cheflieu_reg=lambda x: x['Région siège'].str.split(' - ', expand=True)[0],
        grpt_cheflieu_dep=lambda x: x['Département siège'].str.split(' - ', expand=True)[0],
        grpt_cheflieu_com=lambda x: x['Commune siège'].str.split(' - ', expand=True)[0],
    )
    .rename(columns={
         "N° SIREN": "grpt_siren",
         "Nom du groupement": "grpt_nom",
         "Nature juridique": "grpt_naturejuridique",
         "Groupement interdépartemental": "grpt_interdep"})
    .pipe(lambda x: x.loc[:, x.columns.str.startswith("grpt")])
)


# ---- 4 Test d'intégrité -----

try:
    test_no_null_values(df, "grpt_siren")
    test_no_null_values(df, "grpt_nom")
    test_no_null_values(df, "grpt_naturejuridique")
    test_no_null_values(df, "grpt_interdep")
    test_no_null_values(df, "grpt_cheflieu_reg")
    test_no_null_values(df, "grpt_cheflieu_dep")
    test_no_null_values(df, "grpt_cheflieu_com")
    test_length_values(df, "grpt_siren", [9])
    test_length_values(df, "grpt_interdep", [1])
    test_length_values(df, "grpt_cheflieu_reg", [2])
    test_length_values(df, "grpt_cheflieu_dep", [2, 3])
    test_length_values(df, "grpt_cheflieu_com", [9])

    print("Tous les tests d'intégrité ont été réussis.")


# ---- 5 Écriture au format Parquet ----

    df.to_parquet("src/params/banatic/table_grpt_liste.parquet", engine="pyarrow", compression="gzip")
    print("Les fichiers ont été écrits avec succès.")


except ValueError as e:
    print(e)
    print("Les fichiers n'ont pas été écrits en raison d'une erreur d'intégrité des données.")

