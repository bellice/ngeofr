# Chargement des librairies
library(tidyverse) #manipulation des données
library(RSQLite) #base de données SQLite
library(DBI) #interface base de données
library(odbc) #interface compatible DBI
library(RecordLinkage) #enregistrement lien entre les données
library(here) #gestion des chemins relatifs

# 1/ Import des variables
# 2/ Préparation des tables
# 3/ Initialisation de la base de données
# 4/ Intégration dans la base de données

# Import des fonctions
source(here("src/script/function.R"))

# 1/ Import des variables
# ----------------------------------------------
rdata <- list.files(here("src/script/ngeo-fr/2023/rdata/"), pattern = ".Rdata")
lapply(here("src/script/ngeo-fr/2023/rdata/", rdata), load, .GlobalEnv)

# Vérification commune laposte
# verif_laposte <- ngeo_lib_com_m %>%
#     left_join(ngeo_laposte, by = c("insee_com")) %>%
#     mutate(pct_match = levenshteinSim(lib_com, lib_com_m))

# 2/ Préparation des tables
# ----------------------------------------------
table_ngeo <- ngeo_com %>%
  left_join(ngeo_banatic, by = c("insee_com")) %>%
  left_join(ngeo_pop, by = c("insee_com")) %>%
  left_join(select(france, insee_territoire, iso_territoire_2), by = c("insee_territoire")) %>%
  mutate(iso_territoire_2 = ifelse(is.na(insee_territoire), "FR", iso_territoire_2)) %>%
  select(-insee_territoire)

table_arrondissement_municipal <- arrondissement_municipal %>%
  left_join(arrondissement_municipal_pop, by = c("insee_arm"))

table_france <- france

table_region <- region %>%
  arrange(lib_reg) %>%
  bind_cols(region_iso_reg) %>%
  select(-nom_subdivision) %>%
  arrange(insee_reg)

table_departement <- departement %>%
  left_join(departement_iso_dep, by = c("insee_dep"))

table_arrondissement <- arrondissement
table_canton_ville <- canton_ville
table_la_poste <- laposte

table_ngeo_la_poste <- ngeo_laposte %>%
  ungroup()

table_zone_emploi_2020 <- ze
table_unite_urbaine_2020 <- uu
table_aire_attraction_ville_2020 <- aav
table_bassin_vie_2022 <- bv
table_epci <- epci
table_etablissement_public_territorial <- ept
table_petr <- petr
table_epci_petr <- epci_petr
table_ngeo_petr <- ngeo_petr
table_syndicat_intercommunal <- syndicat_intercommunal
table_ngeo_syndicat_intercommunal <- ngeo_syndicat_intercommunal
table_syndicat_mixte <- syndicat_mixte
table_ngeo_syndicat_mixte <- ngeo_syndicat_mixte
table_pole_metropolitain <- pole_metropolitain
table_ngeo_pole_metropolitain <- ngeo_pole_metropolitain
# table_quartier_prioritaire <- qp
# table_ngeo_quartier_prioritaire <- ngeo_qp
# table_iris <- iris
# table_grand_quartier <- grand_quartier

# 3/ Initialisation de la bdd
# ----------------------------------------------
con <- dbConnect(RSQLite::SQLite(),here("src/script/ngeo-fr/2023/params/ngeo-fr-cog2023.sqlite3")) # Ouverture de la base de données
RSQLite::initRegExp(con) #implémentation de l'opérateur regex

# dbRemoveTable(con, "ngeo")
getSQL(here("src/script/ngeo-fr/2023/params/ngeo-fr-cog2023-tables.sql")) # Création des tables


dbListTables(con) # Liste des tables
dbGetQuery(con, "SELECT count(*) AS nb_table FROM sqlite_master WHERE type = 'table'") #Nombre de table


# 4/ Intégration dans la base de données
# ----------------------------------------------
dbReadTable(con,"ngeo")

dbAppendTable(con, "ngeo", table_ngeo)
dbAppendTable(con, "arrondissement_municipal", table_arrondissement_municipal)
dbAppendTable(con, "france", table_france)
dbAppendTable(con, "region", table_region)
dbAppendTable(con, "departement", table_departement)
dbAppendTable(con, "arrondissement", table_arrondissement)
dbAppendTable(con, "canton_ville", table_canton_ville)
dbAppendTable(con, "la_poste", table_la_poste)
dbAppendTable(con, "ngeo_la_poste", table_ngeo_la_poste)
dbAppendTable(con, "zone_emploi_2020", table_zone_emploi_2020)
dbAppendTable(con, "unite_urbaine_2020", table_unite_urbaine_2020)
dbAppendTable(con, "aire_attraction_ville_2020", table_aire_attraction_ville_2020)
dbAppendTable(con, "bassin_vie_2022", table_bassin_vie_2022)
dbAppendTable(con, "epci", table_epci)
dbAppendTable(con, "etablissement_public_territorial", table_etablissement_public_territorial)
dbAppendTable(con, "petr", table_petr)
dbAppendTable(con, "epci_petr", table_epci_petr)
dbAppendTable(con, "ngeo_petr", table_ngeo_petr)
dbAppendTable(con, "syndicat_intercommunal", table_syndicat_intercommunal)
dbAppendTable(con, "ngeo_syndicat_intercommunal", table_ngeo_syndicat_intercommunal)
dbAppendTable(con, "syndicat_mixte", table_syndicat_mixte)
dbAppendTable(con, "ngeo_syndicat_mixte", table_ngeo_syndicat_mixte)
dbAppendTable(con, "pole_metropolitain", table_pole_metropolitain)
dbAppendTable(con, "ngeo_pole_metropolitain", table_ngeo_pole_metropolitain)
# dbAppendTable(con, "quartier_prioritaire", table_quartier_prioritaire)
# dbAppendTable(con, "ngeo_quartier_prioritaire", table_ngeo_quartier_prioritaire)
# dbAppendTable(con, "iris", table_iris)
# dbAppendTable(con, "grand_quartier", table_grand_quartier)

# Copie de la base de donnée vers public
con_copy <- dbConnect(RSQLite::SQLite(),here("public/2023/", "ngeo-fr-cog2023.sqlite3")) # Ouverture de la base de données
sqliteCopyDatabase(con, con_copy)

dbDisconnect(con)
dbDisconnect(con_copy)

# Copie du diagramme UML
file.copy(from =here("src/script/ngeo-fr/2023/params/ngeo-fr-cog2023.png"),
          to = here("public/2023/", "ngeo-fr-cog2023.png"),
          overwrite = T)
