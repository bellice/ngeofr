# Chargement des librairies
library(tidyverse) #manipulation des données
library(RSQLite) #base de données SQLite
library(DBI) #interface base de données
library(odbc) #interface compatible DBI
library(RecordLinkage) #enregistrement lien entre les données
library(here) #gestion des chemins relatifs
library(fuzzyjoin) #jointure inexacte

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

# Chemin du map process
path_mapp <- here("src/script/")





# 2/ Préparation des tables
# ----------------------------------------------


# Intercommunalité - epci
epci_vf <- epci %>%
  select(siren_epci, lib_epci, nature_juridique, cheflieu_epci) %>%
  rename(siren_groupement = siren_epci,
         lib_groupement = lib_epci,
         cheflieu = cheflieu_epci)


ept_vf <- ept %>%
  select(siren_ept, lib_ept, cheflieu_ept) %>%
  mutate(nature_juridique = "EPT") %>%
  rename(siren_groupement = siren_ept,
         lib_groupement = lib_ept,
         cheflieu = cheflieu_ept) %>%
  relocate(cheflieu, .after = everything())

petr_vf <- petr %>%
  select(siren_petr, lib_petr, cheflieu_petr) %>%
  mutate(nature_juridique = "PETR") %>%
  rename(siren_groupement = siren_petr,
         lib_groupement = lib_petr,
         cheflieu = cheflieu_petr) %>%
  relocate(cheflieu, .after = everything())

pole_metropolitain_vf <- pole_metropolitain %>%
  select(siren_polem, lib_polem, cheflieu_polem) %>%
  mutate(nature_juridique = "POLEM") %>%
  rename(siren_groupement = siren_polem,
         lib_groupement = lib_polem,
         cheflieu = cheflieu_polem) %>%
  relocate(cheflieu, .after = everything())

syndicat_intercommunal_vf <- syndicat_intercommunal %>%
  select(siren_siv, lib_siv, cheflieu_siv, nature_juridique) %>%
  rename(siren_groupement = siren_siv,
         lib_groupement = lib_siv,
         cheflieu = cheflieu_siv)

syndicat_mixte_vf <- syndicat_mixte %>%
  select(siren_sm, lib_sm, cheflieu_sm, nature_juridique) %>%
  rename(siren_groupement = siren_sm,
         lib_groupement = lib_sm,
         cheflieu = cheflieu_sm)



table_groupement_epci <- bind_rows(epci_vf,
                              petr_vf,
                              ept_vf,
                              pole_metropolitain_vf,
                              syndicat_intercommunal_vf,
                              syndicat_mixte_vf)


table_groupement_epci_vf <- table_groupement_epci %>%
  mutate(echelon_geo = "epci")



table_groupement_epci_vf %>%  group_by(siren_groupement) %>%  filter(n()>1)

table_groupement_epci_vf %>%  group_by(nature_juridique) %>%  count()

# Commune
# Contient code insee et code siren des communes suivant ngeo
com_process <- ngeo_com %>%
  select(insee_com, lib_com, insee_dep) %>%
  right_join(select(ngeo_banatic, siren_com, insee_com), by = c("insee_com")) %>%
  select(siren_com, insee_com, lib_com, insee_dep) %>%
  mutate(nature_juridique = "COM") %>%
  rename(siren_groupement = siren_com,
         lib_groupement = lib_com) %>%
  mutate(nature_juridique = replace(nature_juridique, siren_groupement == "217500016", "COLTER")) #Paris



# Harmonisation libellé
# L'objectif final est de match avec la base sirene sur les libellés

sirene_com_vf <- sirene_com %>%
  mutate(
    lib_groupement = gsub("(?<=[\\s])\\s*|^\\s+|\\s+$", "", denominationUniteLegale, perl = T),
    lib_groupement = sub("^COMMUNE DES|^COMMUNE DE|^COMMUNE D'|^COMMUNE D|^COMMUNE", "", lib_groupement),
    lib_groupement = sub("CIRCONSCRIPTION DE|CIRCONSCRIPTION TERRITORRIALE DE", "", lib_groupement),
    lib_groupement = str_trim(lib_groupement))
  
# Harmonisation pour éviter les NA
com_no_siren <- anti_join(ngeo_com, com_process, by = c("insee_com")) %>%
  select(insee_com, lib_com) %>%
  filter(!str_detect(insee_com, "^984|^989")) %>% #TAAF + Clipperton
  mutate(lib_com_m  = toupper(lib_com),
         lib_com_m = iconv(lib_com_m, from = "UTF-8", to = "ASCII//TRANSLIT"),
         lib_com_m = gsub("\\-|\\'", " ", lib_com_m),
         lib_com_m = gsub("MOOREA MAIAO", "MOREA MAIAO", lib_com_m),
         lib_com_m = gsub("LE MONT DORE", "MONT DORE", lib_com_m)) %>%
  mutate(lib_com_m = replace(lib_com_m, insee_com == "98809", "ILE DES PINS")) %>%
  left_join(sirene_com_vf, by = c("lib_com_m" = "lib_groupement"), multiple = "all")


# Prendre les bonnes valeurs des doublons
# Saint-Pierre
# Saint-Barthélemy
# Saint-Martin
# Arue
# Le Mont-Dore

com_no_siren_doublon <- com_no_siren %>%
  group_by(insee_com) %>%
  filter(n()>1) %>%
  ungroup()

select_com_no_siren_doublon <- com_no_siren_doublon%>%
  filter(siren_groupement %in% c("219755022", "200013662", "200012532")) #Saint-Pierre, Arue, Mont-Dore


# Ajout commune manquante Saint-Barthélémy (collectivité)
# Ajout commune manquante Saint-Martin (collectivité)
com_no_siren_manquant <- sirene %>%
  filter(siren_groupement %in% c("219711231", "219711272")) %>%
  left_join(select(com_no_siren, insee_com, lib_com), by = c("cheflieu_groupement" = "insee_com"), multiple = "all") %>%
  unique() %>%
  rename(insee_com = cheflieu_groupement)

com_no_siren_doublon_vf <- com_no_siren %>%
  filter(!insee_com %in% com_no_siren_doublon$insee_com) %>%
  bind_rows(com_no_siren_manquant, select_com_no_siren_doublon) %>%
  mutate(siren_groupement = as.character(siren_groupement)) %>%
  select(-c(lib_com_m, denominationUniteLegale))


table_groupement_com_vf <- com_no_siren_doublon_vf %>%
  bind_rows(com_process) %>%
  rename(insee_geo = insee_com) %>%
  mutate(lib_groupement = ifelse(is.na(lib_groupement), lib_com, lib_groupement),
         echelon_geo = ifelse(is.na(echelon_geo), "commune", echelon_geo),
         cheflieu = siren_groupement) %>%
  mutate(echelon_geo = replace(echelon_geo, echelon_geo == "territoire ; commune", "commune")) %>%
  select(siren_groupement, lib_groupement, nature_juridique, cheflieu, insee_geo, echelon_geo, insee_dep)

table_groupement_com_vf %>% group_by(echelon_geo) %>% count()

# Departement - region
dep_reg_sirene <- sirene %>%
  filter(str_detect(echelon_geo, "region|departement")) %>%
  mutate(lib_groupement = gsub("(?<=[\\s])\\s*|^\\s+|\\s+$", "", denominationUniteLegale, perl = T),
         lib_groupement = sub("^REGION DES|^REGION|^COLLECTIVITE TERRITORIALE DE|^COLLECTIVITE DE|^CONSEIL REGIONAL DE LA|^VILLE DE|^DEPARTEMENT DE LA|^DEPARTEMENT DU|^DEPARTEMENT DE L'|^DEPARTEMENT DE L|^DEPARTEMENT DES|^COLLECTIVITE EUROPEENNE D'|^DEPARTEMENT DE|^DEPARTEMENT D", "", lib_groupement),
         lib_groupement = gsub("\\-|\\'", " ", lib_groupement),
         lib_groupement = str_trim(lib_groupement)) %>%
  mutate(lib_groupement = replace(lib_groupement, siren_groupement == "229740014", "LA REUNION"),
         lib_groupement = replace(lib_groupement, siren_groupement == "239740012", "LA REUNION"))


departement_vf <- departement %>%
  select(insee_dep, lib_dep, cheflieu_dep) %>%
  rename(insee = insee_dep,
         lib = lib_dep,
         cheflieu = cheflieu_dep) %>%
  left_join(select(table_groupement_com_vf, insee_geo, siren_groupement), by = c("cheflieu" = "insee_geo")) %>%
  select(-cheflieu) %>%
  rename(cheflieu = siren_groupement)
  
region_vf <- region %>%
  select(insee_reg, lib_reg, cheflieu_reg) %>%
  rename(insee = insee_reg,
         lib = lib_reg,
         cheflieu = cheflieu_reg) %>%
  left_join(select(table_groupement_com_vf, insee_geo, siren_groupement), by = c("cheflieu" = "insee_geo")) %>%
  select(-cheflieu) %>%
  rename(cheflieu = siren_groupement)

dep_insee <- departement_vf %>%
  mutate(lib_m = toupper(lib),
         lib_m = iconv(lib_m, from = "Windows-1252", to = "ASCII//TRANSLIT"),
         lib_m = gsub("\\-|\\'", " ", lib_m))


reg_insee <- region_vf %>%
  mutate(lib_m = toupper(lib),
         lib_m = iconv(lib_m, from = "Windows-1252", to = "ASCII//TRANSLIT"),
         lib_m = gsub("\\-|\\'", " ", lib_m))


dep_reg_sirene_vf <- dep_reg_sirene %>%
  separate_rows(echelon_geo, sep = " ; ")


dep_sirene <- dep_reg_sirene_vf %>%
  filter(echelon_geo == "departement")

reg_sirene <- dep_reg_sirene_vf %>%
  filter(echelon_geo == "region")


dep_sirene_vf <- dep_insee %>%
  left_join(dep_sirene, by = c("lib_m" = "lib_groupement")) %>%
  rename(lib_groupement = lib,
         insee_geo = insee) %>%
  select(siren_groupement, lib_groupement, nature_juridique, cheflieu, insee_geo, echelon_geo) %>%
  mutate(siren_groupement = as.character(siren_groupement)) %>%
  unique() %>%
  mutate(echelon_geo = replace(echelon_geo, is.na(echelon_geo), "departement")) %>%
  mutate(insee_dep = insee_geo)

dep_reg <- ngeo_com %>%
  select(insee_reg, insee_dep) %>%
  unique() %>%
  filter(!is.na(insee_dep)) %>%
  group_by(insee_reg) %>%
  summarise(insee_dep = paste0(insee_dep, collapse = " ; "), .groups = "drop")

reg_sirene_vf <- reg_insee %>%
  left_join(reg_sirene, by = c("lib_m" = "lib_groupement")) %>%
  rename(lib_groupement = lib,
         insee_geo = insee) %>%
  select(siren_groupement, lib_groupement, nature_juridique, cheflieu, insee_geo, echelon_geo) %>%
  mutate(siren_groupement = as.character(siren_groupement)) %>%
  unique() %>%
  left_join(dep_reg, by = c("insee_geo" = "insee_reg"))


# Territoire français

territoire_vf <- france %>%
  select(insee_territoire, lib_territoire) %>%
  filter(!lib_territoire %in% reg_sirene_vf$lib_groupement)
  

territoire_insee <- territoire_vf %>%
  mutate(lib_m = toupper(lib_territoire),
         lib_m = iconv(lib_m, from = "UTF-8", to = "ASCII//TRANSLIT"),
         lib_m = gsub("\\-|\\'", " ", lib_m))

  
territoire_sirene <- sirene %>%
  filter(grepl("^9[78][5-8]\\w{2}", cheflieu_groupement)) %>%
  filter(echelon_geo != "departement") %>%
  mutate(echelon_geo = "territoire",
         lib_groupement = gsub("COLLECTIVITE DE|COLLECTIVITE TERRITORRIALE DE|TERRITOIRE DE", "", denominationUniteLegale)) %>%
  stringdist_left_join(territoire_insee, by = c("lib_groupement" = "lib_m"), max_dist = 5)

territoire_sirene_vf <- territoire_sirene %>%
  select(-c(denominationUniteLegale, lib_groupement, lib_m)) %>%
  rename(insee_geo = insee_territoire,
         lib_groupement = lib_territoire,
         cheflieu = cheflieu_groupement) %>%
  mutate(siren_groupement = as.character(siren_groupement)) %>%
  select(siren_groupement, lib_groupement, nature_juridique, cheflieu, insee_geo, echelon_geo)
  

table_groupement <- bind_rows(table_groupement_com_vf, table_groupement_epci_vf, dep_sirene_vf, reg_sirene_vf, territoire_sirene_vf) %>%
  unique() %>%
  filter(!is.na(siren_groupement)) %>%
  arrange(echelon_geo, nature_juridique, insee_geo, siren_groupement)
    
table_groupement %>% group_by(siren_groupement) %>% filter(n()>1) %>% arrange(lib_groupement)

# 3/ Initialisation de la bdd
# ----------------------------------------------
# Ouverture de la base de données ngeo-fr-grpt
con <- dbConnect(RSQLite::SQLite(), here(paste0(path_mapp, "/ngeo-fr/2023/params/ngeo-fr-grpt-cog2023.sqlite3")))

dbListTables(con)

dbGetQuery(con, "SELECT count(*) AS nb_table FROM sqlite_master WHERE type = 'table'") #Nombre de table

# Création de la table groupement
dbExecute(con,
          "CREATE TABLE IF NOT EXISTS groupement(
          siren_groupement TEXT NOT NULL,
          lib_groupement TEXT NOT NULL,
          nature_juridique TEXT NOT NULL,
          cheflieu TEXT NOT NULL,
          insee_geo TEXT,
          echelon_geo TEXT NOT NULL,
          insee_dep TEXT,
          PRIMARY KEY (siren_groupement, echelon_geo)
          )")

# 4/ Intégration dans la base de données
# ----------------------------------------------
dbReadTable(con,"groupement")

dbAppendTable(con, "groupement", table_groupement)


# Copie de la base de donnée vers public
con_copy <- dbConnect(RSQLite::SQLite(),here("public/2023/", "grpt-fr-cog2023.sqlite3")) # Ouverture de la base de données
sqliteCopyDatabase(con, con_copy)

dbDisconnect(con)

