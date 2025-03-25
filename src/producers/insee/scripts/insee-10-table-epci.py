import pandas as pd
import os

# ---- 1 Importation des bibliothèques ----
from src.shared.data_validation import test_length_values, test_no_null_values

# ---- 2 Chargement des données ----

# Chemin vers le dossier
path = "src/producers/insee/assets/"

# Nom du fichier à lire
file_name_epci = "EPCI_au_01-01-2025.xlsx"
# Chemin complet du fichier
file_path_epci = os.path.join(path, file_name_epci)
# Chargement des données depuis la feuille "Composition_communale" avec en-tête à la ligne 6
sheet_name_epci = "Composition_communale"

df_epci_init = pd.read_excel(file_path_epci, sheet_name=sheet_name_epci, skiprows=5, dtype=str)


# Nom du fichier à lire
file_name_ept = "EPT_au_01-01-2025.xlsx"
# Chemin complet du fichier
file_path_ept = os.path.join(path, file_name_ept)
# Chargement des données depuis la feuille "Composition_communale" avec en-tête à la ligne 6
sheet_name_ept = "Composition_communale"

# Chargement du fichier Excel
df_ept_init = pd.read_excel(file_path_ept, sheet_name=sheet_name_ept, skiprows=5, dtype=str, engine="openpyxl")


# ---- 3 Transformation et nettoyage ----

df_epci = (
    df_epci_init[["CODGEO", "LIBGEO", "EPCI", "LIBEPCI"]]
    .rename(columns={
        "CODGEO": "epci_membre_insee",
        "LIBGEO": "epci_membre_nom",
        "EPCI": "epci_siren",
        "LIBEPCI": "epci_nom"
    })
    .assign(
        epci_nom=lambda x: x["epci_nom"].str.replace("Communauté de communes", "CC")
                                        .str.replace("Communauté d'agglomération", "CA")
                                        .str.replace("Communauté urbaine", "CU")
    )
)


# Chargement, sélection, renommage et filtrage en une seule chaîne
df_ept = (
    pd.read_excel(file_path_ept, sheet_name=sheet_name_ept, skiprows=5, dtype=str, engine="openpyxl")
    .loc[:, ["CODGEO", "LIBGEO", "EPT", "LIBEPT"]]
    .rename(columns={"CODGEO": "ept_membre_siren", "LIBGEO": "ept_membre_nom", "EPT": "ept_siren", "LIBEPT": "ept_nom"})
    .query('ept_nom != "Sans objet"')
)




# ---- 4 Test d'intégrité ----
try:
    # Test d'intégrité pour df_epci
    test_no_null_values(df_epci, "epci_siren")
    test_no_null_values(df_epci, "epci_nom")
    test_no_null_values(df_epci, "epci_membre_insee")
    test_no_null_values(df_epci, "epci_membre_nom")
    test_length_values(df_epci, "epci_siren", [9])
    test_length_values(df_epci, "epci_membre_insee", [5])

    # Test d'intégrité pour df_ept
    test_no_null_values(df_ept, "ept_siren")
    test_no_null_values(df_ept, "ept_nom")
    test_no_null_values(df_ept, "ept_membre_siren")
    test_no_null_values(df_ept, "ept_membre_nom")
    test_length_values(df_ept, "ept_siren", [9])
    test_length_values(df_ept, "ept_membre_siren", [5])
    
    print("Tous les tests d'intégrité ont été réussis.")

 
# ---- 5 Écriture au format Parquet ----

    df_epci.to_parquet("src/data/insee/table_epci_perimetre.parquet", engine="pyarrow", compression="gzip")
    df_ept.to_parquet("src/data/insee/table_ept_perimetre.parquet", engine="pyarrow", compression="gzip")
    print("Les fichiers ont été écrits avec succès.")


except ValueError as e:
    print(e)
    print("Les fichiers n'ont pas été écrits en raison d'une erreur d'intégrité des données.")
