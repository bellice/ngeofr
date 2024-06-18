# Chargement des librairies
library(tidyverse) #manipulation des données
library(readxl) # manipulation des fichiers excel
library(rvest) #scrapper des données sur le web
library(data.table) #manipulation des données
library(here) #gestion des chemins relatifs

# Source des données
# https://www.insee.fr/

# Communes outre-mer
# https://www.insee.fr/fr/information/2028040


# Import des fichiers

arrondissement_init <- read.csv(file = here("src/assets/insee/2023/cog_ensemble_2023_csv/v_arrondissement_2023.csv"),
                                fileEncoding = "UTF-8",
                                colClasses = "character")
  
canton_init <- read.csv(file = here("src/assets/insee/2023/cog_ensemble_2023_csv/v_canton_2023.csv"),
                        fileEncoding = "UTF-8",
                        colClasses = "character")
  
departement_init <- read.csv(file = here("src/assets/insee/2023/cog_ensemble_2023_csv/v_departement_2023.csv"),
                             fileEncoding = "UTF-8",
                             colClasses = "character")
  
region_init <- read.csv(file = here("src/assets/insee/2023/cog_ensemble_2023_csv/v_region_2023.csv"),
                        fileEncoding = "UTF-8",
                        colClasses = "character")

commune_init <- read.csv(file = here("src/assets/insee/2023/cog_ensemble_2023_csv/v_commune_2023.csv"),
                         fileEncoding = "UTF-8",
                         colClasses = "character")

grille_densite_7niv_init <- read_excel(here("src/assets/insee/2023/grille_densite_7_niveaux_detaille_2023.xlsx"), skip = 1)

outremer_init <- read_html(x = here("src/assets/insee/2023/Codification des collectivités d'outre-mer (COM) Insee.htm"))


# iris_init <- read_excel(here("src/assets/insee/2023/reference_IRIS_geo2023/reference_IRIS_geo2023.xlsx"), skip = 5)
# iris_supra_init <- read_excel(here("src/assets/insee/2023/reference_IRIS_geo2023/reference_IRIS_geo2023.xlsx"), sheet = 2, skip = 5)


#bureau_vote_init <- fread(paste0(wd, "src/assets/insee/2021/Bureaux_de_vote.csv"),
#                          encoding = "UTF-8")

# Zonage d'étude de l'INSEE
aav_init <- read_excel(here("src/assets/insee/2023/AAV2020_au_01-01-2023/AAV2020_au_01-01-2023.xlsx"),
                       skip = 5)

ngeo_aav_init <- read_excel(here(paste0("src/assets/insee/2023/AAV2020_au_01-01-2023/AAV2020_au_01-01-2023.xlsx")),
                       sheet = 2, skip = 5)

uu_init <- read_excel(here("src/assets/insee/2023/UU2020_au_01-01-2023/UU2020_au_01-01-2023.xlsx"),
                      skip = 5)

ngeo_uu_init <- read_excel(here("src/assets/insee/2023/UU2020_au_01-01-2023/UU2020_au_01-01-2023.xlsx"),
                           sheet = 2, skip = 5)

ze_init <- read_excel(here("src/assets/insee/2023/ZE2020_au_01-01-2023/ZE2020_au_01-01-2023.xlsx"),
                      skip = 5)

ngeo_ze_init <- read_excel(here("src/assets/insee/2023/ZE2020_au_01-01-2023/ZE2020_au_01-01-2023.xlsx"),
                           sheet = 2, skip = 5)

bv_init <- read_excel(here("src/assets/insee/2023/BV2022_au_01-01-2023/BV2022_au_01-01-2023.xlsx"),
                      skip = 5)

ngeo_bv_init <- read_excel(here("src/assets/insee/2023/BV2022_au_01-01-2023/BV2022_au_01-01-2023.xlsx"),
                           sheet = 2, skip = 5)


# ***********************************************
# REGION
# ***********************************************

region <- region_init %>%
  rename(insee_reg = REG,
         cheflieu_reg = CHEFLIEU,
         lib_reg = LIBELLE) %>%
  select(insee_reg, lib_reg, cheflieu_reg)


# ***********************************************
# DEPARTEMENT
# ***********************************************

departement <- departement_init %>%
  rename(insee_dep = DEP,
         insee_reg = REG,
         cheflieu_dep = CHEFLIEU,
         lib_dep = LIBELLE) %>%
  select(insee_dep, insee_reg, cheflieu_dep, lib_dep)


# ***********************************************
# ARRONDISSEMENT
# ***********************************************

arrondissement <- arrondissement_init %>%
  rename(insee_arr = ARR,
         insee_dep = DEP,
         insee_reg = REG,
         cheflieu_arr = CHEFLIEU,
         lib_arr = LIBELLE) %>%
  select(insee_arr, insee_dep, insee_reg, cheflieu_arr, lib_arr)

# ***********************************************
# CANTON
# ***********************************************

canton_ville <- canton_init %>%
  rename(insee_cv = CAN,
         lib_cv = LIBELLE,
         insee_dep = DEP,
         insee_reg = REG,
         bureau_central = BURCENTRAL,
         comp_cv = COMPCT,
         type_cv = TYPECT
         ) %>%
  mutate(across(where(is.character), ~na_if(., ""))) %>%
  select(insee_cv, insee_dep, insee_reg, comp_cv, lib_cv, type_cv, bureau_central)

# ***********************************************
# COMMUNE
# ***********************************************

#outremer

ngeo_com_outremer <- outremer_init %>% #lecture du contenu html
  html_table(fill = T) %>%  #scrap table html
  bind_rows() %>% #conversion en dataframe
  filter(is.na(Iris)) %>%
  mutate(Communes = ifelse(is.na(`Districts administratifs`), Communes, `Districts administratifs`),
         Communes = ifelse(is.na(`Circonscription territoriale`), Communes, `Circonscription territoriale`)) %>%
  select(Codes, Communes) %>%
  rename(insee_com = Codes,
         lib_com = Communes) %>%
  mutate(insee_com = str_replace_all(insee_com, "\\s+",""),
         lib_com = replace(lib_com, insee_com == "98809", "L'Île-des-Pins"),
         lib_com = replace(lib_com, insee_com == "98817", "Le Mont-Dore"))


commune <- commune_init %>%
  filter(TYPECOM == "COM") %>%
  rename(insee_com = COM,
         lib_com = LIBELLE,
         insee_reg = REG,
         insee_dep = DEP,
         insee_arr = ARR,
         insee_cv = CAN) %>%
  select(insee_com, lib_com, insee_reg, insee_dep, insee_arr, insee_cv) %>%
  mutate(across(where(is.character), ~na_if(., "")))
  

# ***********************************************
# BUREAU DE VOTE
# ***********************************************

# bureau_vote <- bureau_vote_init %>%
#   rename(insee_com = 1, libelle_com = 2, id_can = 12, id_circonscription = 13) %>%
#   select(insee_com, libelle_com, id_can, id_circonscription) %>%
#   group_by(across()) %>%
#   summarise()



# ***********************************************
# COMMUNE grille de densité
# ***********************************************

ngeo_grille_densite_7niv <- grille_densite_7niv_init %>%
  select(`depcom`, `dens_2023`) %>%
  rename(insee_com = 1,
         grille_densite_7niv = 2) %>%
  mutate(grille_densite_7niv = as.character(grille_densite_7niv))


ngeo_grille_densite_3niv <- ngeo_grille_densite_7niv %>%
  mutate(grille_densite_3niv = case_when(
    grille_densite_7niv %in% c("1") ~ "1",
    grille_densite_7niv %in% c("2", "3", "4") ~ "2",
    grille_densite_7niv %in% c("5", "6", "7") ~ "3",
    is.na(grille_densite_7niv) ~ NA_character_,
    .default = "erreur"
  )) %>%
  select(insee_com, grille_densite_3niv)
  
ngeo_grille_densite_3niv %>% group_by(grille_densite_3niv) %>% count()



# ***********************************************
# ARRONDISSEMENT MUNICIPAL
# ***********************************************

arrondissement_municipal <- commune_init %>%
  filter(TYPECOM == "ARM") %>%
  rename(insee_arm = COM,
         lib_arm = LIBELLE) %>%
  select(insee_arm, lib_arm) %>%
  mutate(lib_arm = sub("^(\\S*\\s+\\S+).*", "\\1", lib_arm)) %>% #suppression après le 2ème espace
  mutate(insee_com = case_when(str_detect(insee_arm, "^751") ~ "75056", #ajout code insee commune
                               str_detect(insee_arm, "^132") ~ "13055",
                               str_detect(insee_arm, "^693") ~ "69123"))



# ***********************************************
# AIRE ATTRACTION VILLE 2020
# ***********************************************

aav <- aav_init %>%
  select(AAV2020, LIBAAV2020, TAAV2017) %>%
  rename(insee_aav = AAV2020,
         lib_aav = LIBAAV2020,
         tr_aav = TAAV2017)

ngeo_aav <- ngeo_aav_init %>%
  select(CODGEO, AAV2020, CATEAAV2020) %>%
  rename(insee_com = CODGEO,
         insee_aav = AAV2020,
         cate_aav = CATEAAV2020)


# ***********************************************
# UNITE URBAINE 2020
# ***********************************************

uu <- uu_init %>%
  select(UU2020, LIBUU2020, TUU2017) %>%
  rename(insee_uu = UU2020,
         lib_uu = LIBUU2020,
         tr_uu = TUU2017)

ngeo_uu <- ngeo_uu_init %>%
  select(CODGEO, UU2020, STATUT_2017) %>%
  rename(insee_com = CODGEO,
         insee_uu = UU2020,
         type_uu = STATUT_2017)


# ***********************************************
# ZONE EMPLOI 2020
# ***********************************************

# Les zones d'emploi contient les arrondissements

ze <- ze_init %>%
  select(ZE2020, LIBZE2020) %>%
  rename(insee_ze = ZE2020,
         lib_ze = LIBZE2020)


ngeo_ze <- ngeo_ze_init %>%
  select(CODGEO, ZE2020) %>%
  rename(insee_com = CODGEO,
         insee_ze = ZE2020) %>%
  filter(!str_detect(insee_com, "^751|^132|^693")) #enlever les arrondissements municipaux
    
 

# ***********************************************
# BASSIN VIE 2022
# ***********************************************

bv <- bv_init %>%
  select(BV2022, LIBBV2022, TYPE) %>%
  rename(insee_bv = BV2022,
         lib_bv = LIBBV2022,
         type_bv = TYPE) %>%
  filter(insee_bv != "ZZZZZ")

ngeo_bv <- ngeo_bv_init %>%
  select(CODGEO, BV2022, TYPE_COM) %>%
  rename(insee_com = CODGEO,
         insee_bv = BV2022,
         cate_bv = TYPE_COM) %>%
  mutate(insee_bv = na_if(insee_bv,"ZZZZZ"))


# ***********************************************
# IRIS
# ***********************************************

# iris <- iris_init %>%
#   select(CODE_IRIS, LIB_IRIS, TYP_IRIS, GRD_QUART, DEPCOM) %>%
#   rename(insee_iris = CODE_IRIS,
#          lib_iris = LIB_IRIS,
#          type_iris = TYP_IRIS,
#          insee_quartier = GRD_QUART,
#          insee_com = DEPCOM) %>%
#   mutate(lib_iris = gsub("\\s*\\([^\\)]+\\)", "", lib_iris),
#          lib_iris = str_trim(lib_iris))


# ***********************************************
# GRAND QUARTIER
# ***********************************************

# grand_quartier <- iris_supra_init %>%
#   select(CODGEO, LIBGEO) %>%
#   rename(insee_quartier = CODGEO,
#          lib_quartier = LIBGEO) %>%
#   mutate(lib_quartier = ifelse(lib_quartier == "Sans objet ou non disponible", NA_character_, lib_quartier))




# Global

ngeo_com <- commune %>%
  bind_rows(ngeo_com_outremer) %>%
  left_join(ngeo_uu, by = c("insee_com")) %>%
  left_join(ngeo_aav, by = c("insee_com")) %>%
  left_join(ngeo_ze, by = c("insee_com")) %>%
  left_join(ngeo_bv, by = c("insee_com")) %>%
  left_join(ngeo_grille_densite_3niv, by = c("insee_com")) %>%
  left_join(ngeo_grille_densite_7niv, by = c("insee_com")) %>%
  mutate(insee_territoire = ifelse(grepl("^97|^98", insee_com), str_sub(insee_com, 1, 3), NA),
         lib_com_carto = gsub("^(.*?) \\(.*", "\\1", lib_com),
         lib_com_carto = ifelse(nchar(lib_com)>12, str_replace_all(lib_com, "Saint-\\b|Saint \\b", "St-"), lib_com_carto),
         lib_com_carto = ifelse(nchar(lib_com)>12, str_replace_all(lib_com_carto, "Sainte-\\b|Sainte \\b", "Ste-"), lib_com_carto),
         lib_com_carto = str_replace_all(lib_com_carto, "-", " "),
         lib_com_carto = str_squish(lib_com_carto),
         lib_com_carto = str_replace(lib_com_carto, " sur ", " s/ "),
         lib_com_carto = str_replace(lib_com_carto, " sous ", " /s "))

# Libellé

ngeo_lib_com_m <- ngeo_com %>%
  select(insee_com, lib_com) %>%
  mutate(lib_com = str_to_upper(lib_com),
         lib_com = iconv(lib_com, from = "UTF-8", to = "ASCII//TRANSLIT"),
         lib_com = str_replace_all(lib_com, "'|-", " "))


save(departement,
     region,
     arrondissement,
     arrondissement_municipal,
     canton_ville,
     ngeo_com,
     ngeo_lib_com_m,
     aav,
     uu,
     bv,
     ze,
     # iris,
     # grand_quartier,
     file = here("src/script/ngeo-fr/2023/rdata", "etl-db-insee.Rdata"))
