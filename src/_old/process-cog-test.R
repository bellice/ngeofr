# Chargement des librairies --------
library(tidyverse)
library(here)
library(readxl)
library(lubridate)

# Chargement des données ------------


# # Table passage
# passage_init <- read_excel(here("src/assets/insee/2022/table_passage_annuelle_2022.xlsx"),
#                                  sheet = 1, skip = 5)
# 
# 
# # Liste des fusions
# fusion_init <- read_excel(here("src/assets/insee/2022/table_passage_annuelle_2022.xlsx"),
#                           sheet = 2, skip = 5)
# 
# # Liste des scissions
# scission_init <- read_excel(here("src/assets/insee/2022/table_passage_annuelle_2022.xlsx"),
#                             sheet = 3, skip = 5)

here()

# Type d'événement sur les communes
mvtcom_init <- read.csv(here("src/assets/insee/v_mvt_commune_2024.csv"),
                        encoding = "UTF-8", na.strings = "")

# Intervalle temporelle des communes
datecom_init <- read.csv(here("src/assets/insee/v_commune_depuis_1943.csv"),
                         encoding = "UTF-8", na.strings = "")



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

# Génération des tables -------


## Table commune --------

# Suppression des parenthèses et du texte à l'intérieur des parenthèses
# Ajout des codes communes manquants

code_manquant <- c("98411", "98412", "98413", "98414", "98415", "98611", "98612", "98613", "98901")
libelle_manquant <- c("Îles Saint-Paul et Amsterdam", "Archipel des Kerguelen", "Archipel des Crozet",
                      "La Terre-Adélie", "Îles Éparses de l'océan Indien", "Alo", "Sigave", "Uvea", "Île de Clipperton")

datecom_ajout <- data.frame(insee_com = code_manquant,
                  lib_com = libelle_manquant,
                  date_debut = "2008-01-01",
                  date_fin = NA_character_)

datecom <- datecom_init %>%
  select(COM, LIBELLE, DATE_DEBUT, DATE_FIN) %>%
  rename(insee_com = COM,
         lib_com = LIBELLE,
         date_debut = DATE_DEBUT,
         date_fin = DATE_FIN) %>%
  mutate(lib_com = gsub("\\s*\\([^\\)]+\\)","",lib_com))

# Les codes sont-ils vraiments absents de la base ?
datecom %>% filter(insee_com %in% code_manquant)

table_commune <- bind_rows(datecom, datecom_ajout) %>% arrange(insee_com, date_debut) %>%
  mutate(date_debut = as.Date(date_debut, format = "%Y-%m-%d"),
         date_fin = ifelse(is.na(date_fin), as.character(today()), date_fin),
         date_fin = as.Date(date_fin, format = "%Y-%m-%d"))

# Suppression des communes associées et les communes déléguées
# Suppression des codes événements 34, 35 et 70 qui n'a pas de conséquence sur les limites communales
# Suppression des parenthèses et du texte à l'intérieur des parenthèses

## Table evenement --------
table_evenement <- mvtcom_init %>%
  select(MOD, DATE_EFF,
         TYPECOM_AV, COM_AV, LIBELLE_AV,
         TYPECOM_AP, COM_AP, LIBELLE_AP) %>%
  rename(code_evenement = MOD,
         date = DATE_EFF,
         type_com_av = TYPECOM_AV,
         insee_com_av = COM_AV,
         lib_com_av = LIBELLE_AV,
         type_com_ap = TYPECOM_AP,
         insee_com_ap = COM_AP,
         lib_com_ap = LIBELLE_AP) %>%
  unique() %>%
  filter(type_com_av == "COM" & type_com_ap == "COM") %>%
  filter(!code_evenement %in% c(34, 35, 70)) %>%
  select(-c(type_com_av, type_com_ap)) %>%
  mutate(lib_com_av = gsub("\\s*\\([^\\)]+\\)","",lib_com_av),
         lib_com_ap = gsub("\\s*\\([^\\)]+\\)","",lib_com_ap),
         date = as.Date(date, formatformat = "%Y-%m-%d"),
         evenement_spatial = case_when(
           code_evenement %in% c(10, 41,50) ~ NA_character_,
           code_evenement %in% c(31, 32, 33) ~ "expansion",
           code_evenement == 21 ~ "contraction",
           code_evenement == 30 & insee_com_av == insee_com_ap ~ "expansion",
           code_evenement == 20 & insee_com_av == insee_com_ap ~ "contraction",
           code_evenement %in% c(20, 30) & insee_com_av != insee_com_ap ~ "partition",
           T ~ "error"
         ))

table_evenement %>% group_by(code_evenement) %>% count()


# # Table passage
# passage_init %>% group_by(NIVGEO) %>% count()
# 
# table_passage <- passage_init %>%
#   filter(NIVGEO == "COM") %>%
#   select(-NIVGEO) %>%
#   setNames(gsub("^CODGEO(.)","insee_com\\1", names(.))) %>%
#   setNames(gsub("LIBGEO(.)", "lib_com\\1", names(.)))


## Table evenement epci --------

table_evenement_epci <- mvtepci_init %>%
  mutate(date = as.Date(date, formatformat = "%Y-%m-%d"),
         siren_epci_av = as.character(siren_epci_av),
         siren_epci_ap = as.character(siren_epci_ap))





# # Export des tables --------
# save(table_commune,
#      table_evenement,
#      table_evenement_epci,
#      file = here("src/script/ngeo-cog/table-cog.Rdata"))
