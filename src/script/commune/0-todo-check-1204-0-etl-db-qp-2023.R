# Chargement des librairies
library(tidyverse) #manipulation des données
library(here) #gestion des chemins relatifs


# Source Insee
# https://www.insee.fr/fr/statistiques/6327672?sommaire=2500477

# Chargement des communes
load(here("src/script/ngeo-fr/2023/rdata/etl-db-insee.Rdata"))

# Liste QP
qp_init <- read.csv(here("src/assets/onpv/liste_Quartiers_Prioritaires.csv"),
                    sep = ";", encoding = "Windows-1252")




# Correspondance QP/COM -- pas encore paru --
# qp_corresp_init <- read.csv(here("src/assets/insee/2023/table_appartenance_geo_QPV_2023_csv/data_table_appartenance_geo_QPV_2023.csv"),
#                     sep = ";", encoding = "UTF-8")





# ***********************************************
# QUARTIER PRIORITAIRE
# ***********************************************

# Liste QP ONPV
qp_process <- qp_init %>%
  rename(id_qp = Code.quartier,
         lib_qp = Quartier.prioritaire,
         com_concerne = Noms.des.communes.concernées) %>%
  select(id_qp, lib_qp, com_concerne, DEPARTEMENT) %>%
  mutate(lib_qp = str_trim(lib_qp)) %>%
  unique()


# Correspondance QP/Commune Insee
perimetre_qp <- qp_corresp_init %>%
  select(X.U.FEFF.qp, lib_qp, list_com_2023) %>%
  mutate(list_com_2023 = gsub("(.{5})", "\\1;", list_com_2023)) %>% #ajout ; tous les 5 caractères
  mutate(list_com_2023 = sub(".{1}$", "\\1", list_com_2023)) %>% #suppression du dernier caractère
  separate_rows(list_com_2023, sep = ";") %>%
  mutate(lib_qp = str_trim(lib_qp)) %>%
  rename(id_qp = X.U.FEFF.qp,
         lib_qp = lib_qp,
         insee_com = list_com_2023)


# QP qui ne matche pas
anti_join(perimetre_qp, qp_process, by = c("id_qp"))

anti_join(qp_process, perimetre_qp, by = c("id_qp"))

# Les QP situés dans les COM doivent être ajoutés
com_filter <- ngeo_com %>%
  select(insee_com, lib_com) %>%
  filter(grepl("^978|^987", insee_com))

# Correction des libellés

qp_to_add <- anti_join(qp_process, perimetre_qp, by = c("id_qp")) %>%
  mutate(com_concerne = replace(com_concerne, com_concerne == "Saint-martin", "Saint-Martin"),
         com_concerne = replace(com_concerne, com_concerne == "Moorea", "Moorea-Maiao")) %>%
  left_join(com_filter, by = c("com_concerne" = "lib_com")) %>%
  select(id_qp, lib_qp, insee_com)


ngeo_qp <- perimetre_qp %>%
  bind_rows(qp_to_add) %>%
  select(insee_com, id_qp) %>%
  arrange(insee_com)

qp <- qp_process %>%
  select(id_qp, lib_qp) %>%
  arrange(id_qp)


# save(ngeo_qp,
#      qp,
#      file = here("src/script/ngeo-fr/2023/rdata", "etl-db-qp.Rdata"))
