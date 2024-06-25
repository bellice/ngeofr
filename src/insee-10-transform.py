import pandas as pd
from datetime import datetime

# Lecture des fichiers CSV et Excel
mvtcom_init = pd.read_csv("src/assets/insee/v_mvt_commune_2024.csv",encoding="UTF-8", na_values="")
datecom_init = pd.read_csv("src/assets/insee/v_commune_depuis_1943.csv", encoding="UTF-8", na_values="")
# mvtepci_init = pd.read_csv("src/script/ngeo-cog/table-evenement-epci.csv", encoding="UTF-8")

# Liste des codes et libellés manquants
code_manquant = ["98411", "98412", "98413", "98414", "98415", "98611", "98612", "98613", "98901"]
libelle_manquant = [
    "Îles Saint-Paul et Amsterdam", "Archipel des Kerguelen", "Archipel des Crozet",
    "La Terre-Adélie", "Îles Éparses de l'océan Indien", "Alo", "Sigave", "Uvea", "Île de Clipperton"
]

# Création du DataFrame pour les communes manquantes
datecom_manquant = pd.DataFrame({
    'insee_com': code_manquant,
    'lib_com': libelle_manquant,
    'date_debut': pd.to_datetime("2008-01-01"),
    'date_fin': pd.NaT
})

# Transformation de `datecom_init`
datecom = datecom_init[['COM', 'LIBELLE', 'DATE_DEBUT', 'DATE_FIN']].rename(
    columns={
        'COM': 'insee_com',
        'LIBELLE': 'lib_com',
        'DATE_DEBUT': 'date_debut',
        'DATE_FIN': 'date_fin'
    }
)

# Suppression des parenthèses et du texte à l'intérieur des parenthèses
datecom['lib_com'] = datecom['lib_com'].str.replace(r'\s*\([^)]*\)', '', regex=True)

# Vérification des codes manquants
missing_codes = datecom[datecom['insee_com'].isin(code_manquant)]

# Fusion des données et formatage des dates
table_commune = pd.concat([datecom, datecom_manquant]).sort_values(by=['insee_com', 'date_debut'])
table_commune['date_debut'] = pd.to_datetime(table_commune['date_debut'])
table_commune['date_fin'] = pd.to_datetime(table_commune['date_fin']).fillna(pd.Timestamp('today'))

# Filtrage des événements

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

table_evenement = mvtcom_init[[
    'MOD', 'DATE_EFF', 'TYPECOM_AV', 'COM_AV', 'LIBELLE_AV', 'TYPECOM_AP', 'COM_AP', 'LIBELLE_AP'
]].rename(
    columns={
        'MOD': 'code_evenement',
        'DATE_EFF': 'date',
        'TYPECOM_AV': 'type_com_av',
        'COM_AV': 'insee_com_av',
        'LIBELLE_AV': 'lib_com_av',
        'TYPECOM_AP': 'type_com_ap',
        'COM_AP': 'insee_com_ap',
        'LIBELLE_AP': 'lib_com_ap'
    }
).drop_duplicates()

table_evenement = table_evenement[
    (table_evenement['type_com_av'] == "COM") & (table_evenement['type_com_ap'] == "COM")
]
table_evenement = table_evenement[~table_evenement['code_evenement'].isin([34, 35, 70])]
table_evenement = table_evenement.drop(columns=['type_com_av', 'type_com_ap'])

# Suppression des parenthèses et du texte à l'intérieur des parenthèses
table_evenement['lib_com_av'] = table_evenement['lib_com_av'].str.replace(r'\s*\([^)]*\)', '', regex=True)
table_evenement['lib_com_ap'] = table_evenement['lib_com_ap'].str.replace(r'\s*\([^)]*\)', '', regex=True)

# Conversion de la date
table_evenement['date'] = pd.to_datetime(table_evenement['date'], format='%Y-%m-%d')

# Ajout de la colonne `evenement_spatial`
conditions = [
    table_evenement['code_evenement'].isin([10, 41, 50]),
    table_evenement['code_evenement'].isin([31, 32, 33]),
    table_evenement['code_evenement'] == 21,
    (table_evenement['code_evenement'] == 30) & (table_evenement['insee_com_av'] == table_evenement['insee_com_ap']),
    (table_evenement['code_evenement'] == 20) & (table_evenement['insee_com_av'] == table_evenement['insee_com_ap']),
    (table_evenement['code_evenement'].isin([20, 30])) & (table_evenement['insee_com_av'] != table_evenement['insee_com_ap'])
]

choices = [
    pd.NA,
    'expansion',
    'contraction',
    'expansion',
    'contraction',
    'partition'
]

table_evenement['evenement_spatial'] = pd.Series(pd.NA, index=table_evenement.index)
table_evenement['evenement_spatial'] = pd.cut(table_evenement['code_evenement'], bins=[0,10,20,30,31,41,50],labels=['error','expansion','partition','contraction'])

# Groupement par `code_evenement`
event_count = table_evenement.groupby('code_evenement').size().reset_index(name='count')

print(event_count)

# table_commune, table_evenement