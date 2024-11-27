# Chargement des librairies
library(sf) #gestion des objets spatiaux
library(tidyverse) #manipulation des données
library(readxl) # manipulation des fichiers excel
library(here) #gestion des chemins relatifs

# --------------------------------------------------------------------
# POPULATION POUR L'ENSEMBLE DES TERRITOIRES FRANCAIS
# ECHELON COMMUNE
# --------------------------------------------------------------------

# source

# France métropolitaine et DROM
# https://www.legifrance.gouv.fr/jorf/id/JORFTEXT000044806592
#
# Saint-Barthélemy, Saint-Martin, Saint-Pierre-et-Miquelon
# https://www.legifrance.gouv.fr/jorf/id/JORFTEXT000044806592
# 
# Mayotte
# https://www.legifrance.gouv.fr/eli/decret/2017/12/14/ECOO1733137D/jo/texte
# 
# Wallis et Futuna
# https://www.legifrance.gouv.fr/eli/decret/2018/12/13/2018-1152/jo/texte
# 
# Polynésie française
# https://www.legifrance.gouv.fr/jorf/id/JORFTEXT000046768003
# 
# Nouvelle-Calédonie
# https://www.legifrance.gouv.fr/jo_pdf.do?id=JORFTEXT000041636143

# Pas de recensement de population pour les Terres australes et antarctiques françaises et l'Île de Clipperton.

# Compile en un seul fichier les population à l'échelon de la commune
# de l'ensemble du territoire français.
# Le sigle [X] indique qu'il n'y a pas de recensement dans les territoires concernés.

# FRA, GLP, GUF, MTQ, REU, SPM, BLM, MAF    [V]   recensement 2020    décret du 31 décembre 2021
# MYT                                       [V]   recensement 2017    décret du 14 décembre 2017
# WLF                                       [V]   recensement 2018    décret du 13 décembre 2018
# PYF                                       [V]   recensement 2022    décret du 20 décembre 2022
# NCL                                       [V]   recensement 2019    décret du 25 février 2020


# 1/ Import des fichiers

# Fichier regroupant FRA, GLP, GUF, MTQ, REU
# Evolution dans le périmètre des communes. Le RP2016 est sur la géographie 2018.
# https://www.insee.fr/fr/statistiques/6011070?sommaire=6011075
pop_fr_drom_init <- read_excel(here("src/assets/insee/2023/ensemble.xlsx"), sheet = "Communes", skip = 6)

pop_myt_init <- read_excel(here("src/assets/insee/2023/mayotte-RP2017-tableaux_pop_legale.xlsx"), sheet = "Communes", range = "A3:D20")

pop_wlf_init <- read_excel(here("src/assets/insee/2023/wetf-RP2018-tableaux_pop_legale.xlsx"))

pop_pyf_init <- read_excel(here("src/assets/insee/2023/tableau resultats pop legale2022xlsx.xlsx"), sheet = "Communes")

pop_ncl_init <- read_excel(here("src/assets/insee/2023/nc-RP2019-tableaux_pop_legale.xlsx"), sheet = "Communes", skip = 2)

pop_spm_init <- read_excel(here("src/assets/insee/2023/dep975.xlsx"),
                                  skip = 7)
  
pop_blm_init <- read_excel(here("src/assets/insee/2023/dep977.xlsx"),
                           skip = 7)
  
pop_maf_init <- read_excel(here("src/assets/insee/2023/dep978.xlsx"),
                         skip = 7)

pop_fr_drom_by_reg_init <- read_excel(here("src/assets/insee/2023/ensemble.xlsx"), sheet = "Régions", skip = 7)

# Table de passage des commune de la géo 2021 à la géo 2023
# https://www.insee.fr/fr/information/2028028
table_passage <- read_excel(here("src/assets/insee/2023/table_passage_geo2003_geo2023/table_passage_geo2003_geo2023.xlsx"),
                            sheet = "Table de passage",
                            skip = 5)

# Pas d'évolution dans le périmètre des communes SPM, MYT, BLM, MAF, ATF, WLF, PYF, NCL, CP

pop_myt <- pop_myt_init %>%
  select(1,3) %>%
  rename(com = 1, pop = 2) %>%
  separate(col = com, into = c("insee_com", "lib_com"), sep = " - ") %>%
  mutate(insee_com = paste0("976", insee_com)) %>%
  select(-lib_com)

pop_wlf <- pop_wlf_init %>%
  select(1,2) %>%
  rename(lib_com = 1, pop = 2) %>%
  filter(lib_com %in% c("Alo", "Sigave", "Uvea")) %>%
  mutate(insee_com = case_when(
    lib_com == "Alo" ~ "98611",
    lib_com == "Sigave" ~ "98612",
    lib_com == "Uvea" ~ "98613",
    TRUE ~ "")) %>%
  relocate(insee_com, everything()) %>%
  select(-lib_com)

pop_pyf <- pop_pyf_init %>%
  filter(!grepl("^Polyn", COMMUNES)) %>%
  filter(!is.na(COMMUNES)) %>%
  select(1,2) %>%
  rename(lib_com = 1, pop = 2) %>%
  separate(lib_com, into = c("insee_com", "lib_com"), sep = "\\. ") %>%
  mutate(insee_com = paste0("987", insee_com)) %>%
  select(-lib_com)

pop_ncl <- pop_ncl_init %>%
  filter(!grepl("NOUVELLE-CAL", .[[1]])) %>%
  filter(!is.na(.[[1]])) %>%
  select(1,3) %>%
  rename(lib_com = 1, pop = 2) %>%
  separate(lib_com, into = c("insee_com", "lib_com"), sep = "\\. ") %>%
  mutate(insee_com = paste0("988", insee_com)) %>%
  select(-lib_com)

pop_spm <- pop_spm_init %>%
  select(1, 3, 5) %>%
  mutate(insee_com = paste0(str_sub(.[[1]], 1, 2), .[[2]])) %>%
  rename(pop = 3) %>%
  select(insee_com, pop)
  
pop_blm <- pop_blm_init %>%
  select(1, 3, 5) %>%
  mutate(insee_com = paste0(str_sub(.[[1]], 1, 2), .[[2]])) %>%
  rename(pop = 3) %>%
  select(insee_com, pop)

pop_maf <- pop_maf_init %>%
select(1, 3, 5) %>%
  mutate(insee_com = paste0(str_sub(.[[1]], 1, 2), .[[2]])) %>%
  rename(pop = 3) %>%
  select(insee_com, pop)

pop_outremer <- bind_rows(pop_myt, pop_wlf, pop_pyf, pop_ncl, pop_spm, pop_blm, pop_maf) %>%
  arrange(insee_com)



# 2/ Traitement des fichiers
# L'objectif est d'assembler les deux fichiers de données


# ***********************************************
# FRA, GLP, GUF, MTQ, REU
# ***********************************************

# la population est établie sur le cog2022.

pop_fr_drom <- pop_fr_drom_init %>%
  select(`Code département`, `Code commune`, `Population municipale`) %>%
  rename(insee_com = `Code commune`,
         insee_dep = `Code département`,
         pop = `Population municipale`) %>%
  # prendre en compte les deux premier caractères du code département
  # compléter les codes communes avec des 0 devant jusqu'à max 3 caractères
  mutate(insee_com = paste0(str_extract(insee_dep, "^.{2}"), str_pad(insee_com, 3, pad = "0"))) %>% 
  select(-insee_dep) %>%
  left_join(select(table_passage, CODGEO_INI, CODGEO_2023),
            by = c("insee_com" = "CODGEO_INI")) %>%
  mutate(CODGEO_2023 = if_else(is.na(CODGEO_2023), insee_com, CODGEO_2023)) %>%
  group_by(CODGEO_2023) %>%
  # du cog 2022 au cog 2023, en évolution territoriale, seulement des fusions
  summarise(pop = sum(pop)) %>%
  rename(insee_com = CODGEO_2023) %>%
  arrange(insee_com)


# Population des communes (agrégation des arrondissements municipaux)
arrondissement_municipal_pop <- pop_fr_drom %>%
  filter(str_detect(insee_com, "^751|^132|^693")) %>%
  rename(insee_arm = insee_com)


  
# Les 45 NA crées par la jointure avec table_passage proviennent
#         - des 20 arrondissements municipaux de Paris
#         - des 16 arrondissements municipaux de Marseille
#         - des 9 arrondissmeents municipaux de Lyon


pop_fra <- pop_fr_drom %>%
  mutate(insee_com = case_when(str_detect(insee_com, "^751") ~ "75056", #ajout code insee commune
                               str_detect(insee_com, "^132") ~ "13055",
                               str_detect(insee_com, "^693") ~ "69123",
                               TRUE ~ insee_com)) %>%
  group_by(insee_com) %>%
  summarise(pop = sum(pop))
  

  
# Selon les sources officielles, Mayotte présente 256518 habitants (population municipale) en 2017

# La population municipale de la France métropolitaine + DROM hors Mayotte
pop_fr_drom_tot <- pop_fr_drom_by_reg_init %>%
  summarise(pop = sum(`Population municipale`))


sum((pop_fra$pop), na.rm = T) == pop_fr_drom_tot

ngeo_pop <- pop_fra %>%
  rbind(pop_outremer)


save(arrondissement_municipal_pop,
     ngeo_pop,
     file = here("src/script/ngeo-fr/2023/rdata", "etl-population.Rdata"))
