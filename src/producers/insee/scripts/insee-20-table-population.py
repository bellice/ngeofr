# ---- 1 Importation des bibliothèques ----
import pandas as pd
import numpy as np
from src.shared.data_validation import test_length_values, test_no_null_values

# ---- 2 Chargement des données ----
df_pop_com_init = pd.read_excel("src/producers/insee/assets/ensemble_pop2022.xlsx", sheet_name="Communes", header=7)
df_pop_coma_init = pd.read_excel("src/producers/insee/assets/ensemble_pop2022.xlsx", sheet_name="Communes associées ou déléguées", header=7)


df_init = pd.read_parquet("src/data/insee/table_com.parquet")
df_com_evenement_init = pd.read_parquet("src/data/insee/table_com_evenement.parquet")


# ---- 3 Transformation et nettoyage ----

df_pop_com = (
    df_pop_com_init
    .rename(columns={
        "Code commune": "com_insee",
        "Nom de la commune": "com_nom",
        "Code département": "dep_insee",
        "Population municipale": "pop_recens"
    })
    .assign(
        dep_insee=lambda df: df["dep_insee"].astype(str).str[:2],  # Prendre les deux premiers caractères du code département
        com_insee=lambda df: df["dep_insee"] + df["com_insee"].astype(str).str.zfill(3)  # Concaténer avec le code commune
    )
    .drop(columns=["Code région", "Nom de la région", "dep_insee", "Code arrondissement", 
                   "Code canton", "Population comptée à part", "Population totale"])
    .sort_values(by=["com_insee"])
)


df_pop_coma = (
    df_pop_coma_init
    .rename(columns={
        "Code commune": "com_insee",
        "Nom de la commune": "com_nom",
        "Code département": "dep_insee",
        "Population municipale": "pop_recens"
    })
    .assign(com_insee=lambda df: df["dep_insee"].astype(str).str.zfill(2) + df["com_insee"].astype(str).str.zfill(3),
            com_nom=lambda df: df["com_nom"].str.replace(r"L' ", "L'", regex=True)) # Supprimer l'espace après "L'"
    .drop(columns=["Code département commune pole", "Code commune pôle", "Nom de la commune pôle",
                   "dep_insee", "Population comptée à part", "Population totale"])
    .sort_values(by=["com_insee"]))

# Filtrer les événements pour la date spécifique
df_com_evenement = (
    df_com_evenement_init
    .assign(date=lambda df: pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce"))  # Convertir en datetime
    .query("'2024-01-02' <= date <= '2025-01-01'")  # Filtrer les dates
    .reset_index(drop=True)  # Réinitialiser l'index (optionnel)
)

## Traitement des fusions
df_fusion = df_com_evenement[df_com_evenement["evenement_spatial"] == "expansion"]

df_pop_fusion = (
    df_fusion
    .merge(df_pop_com, left_on="com_insee_av", right_on="com_insee", how="left")
    .groupby("com_insee_ap", as_index=False)["pop_recens"]
    .sum()
    .rename(columns={"com_insee_ap": "com_insee"})
)

## Traitement des scissions
df_scission = df_com_evenement[df_com_evenement["evenement_spatial"] == "contraction"]

df_pop_scission = (
    df_com_evenement[df_com_evenement["evenement_spatial"] == "contraction"]
    .merge(df_pop_coma, left_on="com_insee_ap", right_on="com_insee", how="left")
    .drop(columns=["code_evenement", "date", "com_insee_av", "com_nom_av", "com_nom_ap", "evenement_spatial", "com_insee", "com_nom"])
    .rename(columns={"com_insee_ap": "com_insee"})
)

## Traitement des évenements (hors spatial)
df_pop_changement = (
    df_com_evenement[df_com_evenement["evenement_spatial"].isna()]
    .merge(df_pop_com, left_on="com_insee_av", right_on="com_insee", how="left")
    .drop(columns=["code_evenement", "date", "com_insee_av", "com_nom_av", "com_nom_ap", "evenement_spatial", "com_insee", "com_nom"])
    .rename(columns={"com_insee_ap": "com_insee"})
)

# Combiner df_fusion, df_scission et df_changement
df_pop_evenement = (pd.concat([df_pop_fusion, df_pop_scission, df_pop_changement])
    .sort_values(by=["com_insee"]))

print(df_pop_fusion[df_pop_fusion["pop_recens"].isna()])
print(df_pop_scission[df_pop_scission["pop_recens"].isna()])
print(df_pop_changement[df_pop_changement["pop_recens"].isna()])

## Traitements des communes
# Filtrer les communes de type "COMD" et "COMA"
df_subset_coma = df_init[df_init["com_type"].isin(["COMD", "COMA"])]

# Effectuer une jointure avec df_pop_coma pour récupérer la population
df_pop_subset_coma = (
    df_subset_coma
    .merge(
        df_pop_coma,
        left_on=["com_insee", "com_nom"],  # Colonnes de df_subset_coma
        right_on=["com_insee", "com_nom"],  # Colonnes de df_pop_coma
        how="left"
    )
    [["com_type", "com_insee", "com_nom", "pop_recens"]]  # Sélectionner les colonnes souhaitées
)

# Avant les opérations de fusion, c'était des communes simples
# dans la table population, c'est des communes simples
# sur le COG2025 c'est devenu des COMA ou COMD

df_subset_pop_coma_fusion = (
    df_pop_subset_coma[df_pop_subset_coma["com_insee"].isin(df_fusion["com_insee_av"].unique())]  # Filtrer les communes ayant subi une expansion
    .merge(df_pop_com, on="com_insee", how="left")  # Jointure avec df_pop_com
    [["com_insee", "com_nom_x", "pop_recens_y"]]  # Sélectionner les colonnes souhaitées
    .rename(columns={"com_nom_x": "com_nom", "pop_recens_y": "pop_recens"})  # Renommer la colonne pop_recens_y en pop_recens
)

# Mettre à jour df_subset_coma avec les populations corrigées
df_subset_pop_coma_vf = (
    df_pop_subset_coma
    .merge(df_subset_pop_coma_fusion, on=["com_insee", "com_nom"], how="left", suffixes=("", "_new"))
    .assign(pop_recens=lambda df: df["pop_recens_new"].combine_first(df["pop_recens"]))
    .drop(columns=["pop_recens_new"])
)

# Valeur manquante sur pop
print(df_subset_pop_coma_vf[df_subset_pop_coma_vf["pop_recens"].isna()])


# Filtrer les communes autre que "COMD" et "COMA"
df_subset_com = df_init[~df_init["com_type"].isin(["COMD", "COMA"])]


# Effectuer une jointure avec df_pop_com pour récupérer la population
# Filtrer avec les communes qui ont eu un événement
df_subset_pop_com = (
    df_subset_com[~df_subset_com["com_insee"].isin(df_pop_evenement["com_insee"])]
    .merge(
        df_pop_com[["com_insee", "pop_recens"]],
        on=["com_insee"],
        how="left"
    )
    [["com_type", "com_insee", "com_nom", "pop_recens"]]  # Sélectionner les colonnes souhaitées
)

# Valeur manquante sur pop
print(df_subset_pop_com[df_subset_pop_com["pop_recens"].isna()])

# Filtrer les ARM dans df_subset_com
df_subset_pop_arm = df_subset_pop_com[df_subset_pop_com["com_type"] == "ARM"]

# Grouper par commune englobante et sommer les populations
df_subset_pop_arm_vf = (
    df_subset_pop_arm
    .assign(
        com_insee=lambda df: df["com_insee"].apply(
            lambda x: "13055" if x.startswith("13")  # Marseille
            else "69123" if x.startswith("69")  # Lyon
            else "75056" if x.startswith("75")  # Paris
            else None  # Ignorer les autres cas
        )
    )
    .groupby("com_insee", as_index=False)["pop_recens"]
    .sum()
)


# Mettre à jour df_subset_com avec les populations des communes englobantes
df_subset_pop_com_vf = (
    df_subset_pop_com
    .merge(df_subset_pop_arm_vf, on="com_insee", how="left")
    .assign(pop_recens_x=lambda df: df["pop_recens_x"].fillna(df["pop_recens_y"]))  # Remplir les valeurs manquantes
    .drop(columns=["pop_recens_y"])  # Supprimer la colonne temporaire
    .rename(columns={"pop_recens_x": "pop_recens"})
)

# Valeur manquante sur pop
print(df_subset_pop_com_vf[df_subset_pop_com_vf["pop_recens"].isna()])


df_pop_evenement_vf = (
    df_pop_evenement
    .merge(df_subset_com[["com_insee", "com_type", "com_nom"]],
           on=["com_insee"],
           how="left")
)


# Concaténer les DataFrames verticalement
df_vf = (
    pd.concat([df_subset_pop_com_vf, df_subset_pop_coma_vf, df_pop_evenement_vf], ignore_index=True)
    .assign(pop_recens=lambda df: pd.to_numeric(df["pop_recens"], errors="coerce").astype("Int64"))
)

len(df_init) == len(df_vf)

# Valeurs NULL ?
df_vf.isnull().sum()



# ---- 4 Test d'intégrité -----
try:

    test_no_null_values(df_vf, "com_insee")
    test_no_null_values(df_vf, "com_nom")
    test_length_values(df_vf, "com_insee", [5])


    print("Tous les tests d'intégrité ont été réussis.")


# ---- 5 Écriture au format Parquet ----

    df_vf.to_parquet("src/data/insee/table_population.parquet", engine = "pyarrow", compression="gzip")
    print("Les fichiers ont été écrits avec succès.")

except ValueError as e:
    print(e)
    print("Les fichiers n'ont pas été écrits en raison d'une erreur d'intégrité des données.")
