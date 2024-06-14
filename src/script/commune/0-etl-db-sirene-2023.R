# Chargement des libraires ------------
library(data.table) #manipulation gros jeu de données
library(tidyverse) #manipulation de données
library(here) #gestion des chemins relatifs
library(RSQLite) #base de données SQLite


# Source des données
# https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/

# Les données étant volumineuses, elles ne sont pas stockés dans le projet

# Importation des données ---------
db <- dbConnect(drv = SQLite(),
                dbname = "C://Users/bmiroux/Desktop/sirene.sqlite3")





# Traitement des données -----------------

# Prise en compte nomenclature d'activité 84.11Z
# Administration publique générale

stock_etablissement <- dbGetQuery(db, "
                                  SELECT *
                                  FROM stock_etablissement
                                  WHERE activitePrincipaleEtablissement == '84.11Z'
                                  ")

stock_unite_legale <- dbGetQuery(db, "
                                 SELECT *
                                 FROM stock_unitelegale
                                 WHERE activitePrincipaleUniteLegale == '84.11Z'
                                 ")


dbListTables(db)
dbListFields(db, "stock_etablissement")
dbListFields(db, "stock_unitelegale")



# Catégorie juridique
# 7210	Commune et commune nouvelle 
# 7220	Département 
# 7225	Collectivité et territoire d'Outre Mer
# 7229	(Autre) Collectivité territoriale 
# 7230	Région 

# Suppression des provinces de Nouvelle-Calédonie 200012490, 200012656, 200012979
# Suppression Métropole de Lyon (déjà présent dans BANATIC) 200046977
# Suppression Conseil Teritorial St Barthelemy 200015816
# Ajout echelon departement pour Collectivité Européenne d'Alsace 200094332
# Ajout multi-echelon region, departement pour Collectivité territoriale de Martinique 200055507
# Ajout multi-echelon region, departement pour Collectivité territoriale de Guyane 200052678
# Ajout multi-echelon departement, commune pour ville de Paris 217500016
# Ajout echelon region pour Collectivite de Corse 200076958
# Ajout echelon tom pour Collectivité de Saint Martin 219711272
# Ajout echelon tom pour Saint Barthélémy 219711231
# Ajout echelon tom pour Saint Pierre et Miquelon 229750013
# Ajout echelon tom pour TAAF 229840004
# Ajout de la nature juridique

data_unite_legale <- stock_unite_legale %>%
  filter(etatAdministratifUniteLegale == "A" &
           (categorieJuridiqueUniteLegale %in% c(7220, 7225, 7229, 7230))) %>%
  mutate(nature_juridique = case_when(
    categorieJuridiqueUniteLegale == 7220 ~ "DEP",
    categorieJuridiqueUniteLegale == 7225 ~ "COLTER",
    categorieJuridiqueUniteLegale == 7229 ~ "COLTER",
    categorieJuridiqueUniteLegale == 7230 ~ "REG",
    T ~ NA_character_
    )) %>%
  select(siren, categorieJuridiqueUniteLegale, denominationUniteLegale, nature_juridique) %>%
  filter(!siren %in% c("200012490", "200012656", "200012979", "200046977", "200015816")) %>%
  mutate(echelon_geo = case_when(
    grepl("^DEPARTEMENT", denominationUniteLegale) ~ "departement",
    grepl("^REGION|REGION", denominationUniteLegale) ~ "region",
    grepl("^TERRITOIRE", denominationUniteLegale) ~ "territoire",
    siren == "200094332" ~ "region", #Alsace
    siren == "200055507" ~ "region ; departement", #Martinique
    siren == "200052678" ~ "region ; departement", #Guyane
    siren == "229850003" ~ "region ; departement", #Mayotte
    siren == "217500016" ~ "departement ; commune", #Paris
    siren == "200076958" ~ "region", #Corse
    siren == "219711272" ~ "territoire ; commune",  #Saint-Martin
    siren == "219711231" ~ "territoire ; commune", #Saint-Barthélemy
    siren == "229750013" ~ "territoire", #Saint Pierre et Miquelon
    siren == "229840004" ~ "territoire", #TAAF 
    T ~ NA_character_
  ))


data_unite_legale_com <- stock_unite_legale %>%
  filter(categorieJuridiqueUniteLegale %in% c(7210)) %>%
  mutate(nature_juridique = "COM") %>%
  select(siren, categorieJuridiqueUniteLegale, denominationUniteLegale, nature_juridique)


data_unite_legale_com %>% group_by(siren) %>%  filter(n()>1)


# Préparation jeu de donnée table stock_etablissement
data_etablissement <- stock_etablissement %>%
  filter(etatAdministratifEtablissement == "A" & etablissementSiege == 1) %>%
  select(siren, codeCommuneEtablissement)

data_etablissement %>% group_by(siren) %>% filter(n()>1)

# Etablissement du fichier final
# Formatage libellé
sirene <- data_unite_legale %>%
  left_join(data_etablissement, by = c("siren")) %>%
  rename(siren_groupement = siren,
         cheflieu_groupement = codeCommuneEtablissement) %>%
  select(siren_groupement, denominationUniteLegale, nature_juridique, cheflieu_groupement, echelon_geo)

sirene_com <- data_unite_legale_com %>%
  rename(siren_groupement = siren) %>%
  select(siren_groupement, denominationUniteLegale, nature_juridique)

save(sirene,
     sirene_com,
     file = here("src/script/ngeo-fr/2023/rdata", "etl-db-sirene.Rdata"))



