import pandas as pd
import numpy as np

# Lecture des fichiers
mvtcom_init = pd.read_csv("src/assets/insee/v_mvt_commune_2024.csv", encoding="UTF-8", na_values="")
datecom_init = pd.read_csv("src/assets/insee/v_commune_depuis_1943.csv", encoding="UTF-8", na_values="")

# Création du DataFrame pour les communes manquantes
datecom_manquant = pd.DataFrame({
    'insee_com': ["98411", "98412", "98413", "98414", "98415", "98611", "98612", "98613", "98901"],
    'lib_com': [
        "Îles Saint-Paul et Amsterdam", "Archipel des Kerguelen", "Archipel des Crozet",
        "La Terre-Adélie", "Îles Éparses de l'océan Indien", "Alo", "Sigave", "Uvea", "Île de Clipperton"
    ],
    'date_debut': pd.to_datetime("2008-01-01"),
    'date_fin': pd.NaT
})

# Transformation et nettoyage
table_commune = (datecom_init
                 .rename(columns={'COM': 'insee_com', 'LIBELLE': 'lib_com', 'DATE_DEBUT': 'date_debut', 'DATE_FIN': 'date_fin'})
                 .assign(lib_com=lambda df: df['lib_com'].str.replace(r'\s*\([^)]*\)', '', regex=True))
                 .append(datecom_manquant, ignore_index=True)
                 .sort_values(by=['insee_com', 'date_debut'])
                 .assign(date_debut=lambda df: pd.to_datetime(df['date_debut']),
                         date_fin=lambda df: pd.to_datetime(df['date_fin']).fillna(pd.Timestamp('today'))))


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
table_evenement = (mvtcom_init
                   .rename(columns={
                       'MOD': 'code_evenement', 'DATE_EFF': 'date', 'TYPECOM_AV': 'type_com_av', 'COM_AV': 'insee_com_av',
                       'LIBELLE_AV': 'lib_com_av', 'TYPECOM_AP': 'type_com_ap', 'COM_AP': 'insee_com_ap', 'LIBELLE_AP': 'lib_com_ap'
                   })
                   .drop_duplicates()
                   .query('(type_com_av == "COM") & (type_com_ap == "COM")')
                   .query('~code_evenement.isin([34, 35, 70])')
                   .drop(columns=['type_com_av', 'type_com_ap'])
                   .assign(lib_com_av=lambda df: df['lib_com_av'].str.replace(r'\s*\([^)]*\)', '', regex=True),
                           lib_com_ap=lambda df: df['lib_com_ap'].str.replace(r'\s*\([^)]*\)', '', regex=True),
                           date=lambda df: pd.to_datetime(df['date'], format='%Y-%m-%d')))

# Mapping des codes d'événements aux types d'événements spatiaux
event_spatial_map = {
    10: np.nan,  # Changement de nom, pas de changement spatial
    41: np.nan,  # Changement de code dû à un changement de département
    50: np.nan,  # Changement de code dû à un transfert de chef-lieu
    31: 'expansion',  # Fusion simple
    32: 'expansion',  # Création de commune nouvelle
    33: 'expansion',  # Fusion association
    21: 'contraction'  # Rétablissement de commune
}

# Pour les créations et suppressions, déterminer le type d'événement
table_evenement['evenement_spatial'] = table_evenement.apply(
    lambda row: (
        'expansion' if row['code_evenement'] == 30 and row['insee_com_av'] == row['insee_com_ap'] else
        'contraction' if row['code_evenement'] == 20 and row['insee_com_av'] == row['insee_com_ap'] else
        'partition' if row['code_evenement'] in [20, 30] and row['insee_com_av'] != row['insee_com_ap'] else
        event_spatial_map.get(row['code_evenement'], 'error')
    ), axis=1
)

# Groupement par `code_evenement`
event_count = table_evenement.groupby('code_evenement').size().reset_index(name='count')

print(event_count)

# Écriture au format Parquet
table_commune.to_parquet("table_commune.parquet", engine='pyarrow', compression='snappy')
table_evenement.to_parquet("table_evenement.parquet", engine='pyarrow', compression='snappy')
