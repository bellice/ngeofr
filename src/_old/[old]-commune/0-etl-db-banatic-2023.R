# Chargement des librairies
library(tidyverse) #manipulation des données
library(readxl) #ouverture fichier excel
library(here) #gestion des chemins relatifs


# Source des données
# https://www.banatic.interieur.gouv.fr/


# Import des fichiers

# Fichier pour tables PETR/EPT
fileNames <- list.files(path = here("src/assets/banatic/2023/"), pattern = "^P.* - R|M.*\\.xls$") # Liste des fichiers du répertoire

perimetre_groupement <- vector("list") # Stockage dans une liste perimetreGroupement

# Parcourir la liste des noms des fichiers
for (i in 1:length(fileNames)){
  name <- sapply(str_split(fileNames[i],paste0(" - ")), `[`, 2) #extrait la 1ère entrée lors du split
  
  # Attribution automatique des noms des variables
  perimetre_groupement[[i]] <- read.csv(here("src/assets/banatic/2023", paste0("/", fileNames[i])),
                                        encoding = "Windows-1252", sep = "\t", colClasses="character")
  names(perimetre_groupement)[i] <- name
}



# Transformation de la liste en un tableau unique
perimetre_groupement <- bind_rows(perimetre_groupement)

# Voir la liste des groupements
perimetre_groupement %>%
  group_by(Nature.juridique) %>%
  count()


# Fichier pour table EPCI
epci_init <- read.csv(here("src/assets/banatic/2023/Périmètre des EPCI à fiscalité propre - France entière.xls"), sep = "\t", colClasses = "character")

epci_init %>%
  group_by(Nature.juridique) %>%
  count()

# Fichier pour table ngeo, variable code INSEE/SIREN
siren_insee_init <- read_excel(here("src/assets/banatic/2023/TableCorrespondanceSirenInsee/Banatic_SirenInsee2023.xlsx"),
                               col_types = c(rep("text",5), rep("numeric", 3)))



# ***********************************************
# NGEO siren_com
# ***********************************************

ngeo_siren_com <- siren_insee_init %>%
  select(siren, insee) %>%
  rename(siren_com = siren,
         insee_com = insee)
  


# ***********************************************
# SYNDICAT INTERCOMMUNAL
# ***********************************************

perimetre_syndicat_intercommunal <- perimetre_groupement %>%
  filter(Nature.juridique == "SIVOM" | Nature.juridique == "SIVU") %>%
  select(Commune.siège, N..SIREN, Nom.du.groupement, Nature.juridique, Siren.membre, Nom.membre, Type) %>%
  separate(col = Commune.siège, into = c("cheflieu_siv", "lib_cheflieu_siv"), sep = " - ") %>%
  rename(siren_siv = N..SIREN,
         lib_siv = Nom.du.groupement,
         siren_com = Siren.membre,
         nature_juridique = Nature.juridique) %>%
  select(-Type, -Nom.membre, -lib_cheflieu_siv) %>%
  mutate(across(.cols = where(is.character), str_trim), # Supprime des espaces en début et fin de caractère
         cheflieu_siv = str_squish(cheflieu_siv))


syndicat_intercommunal <- perimetre_syndicat_intercommunal %>%
  select(siren_siv, lib_siv, nature_juridique, cheflieu_siv) %>%
  group_by(across()) %>%
  summarise(.groups = "drop")

ngeo_syndicat_intercommunal <- perimetre_syndicat_intercommunal %>%
  select(siren_com, siren_siv) %>%
  group_by(across()) %>%
  summarise(.groups = "drop")


# ***********************************************
# SYNDICAT MIXTE
# ***********************************************

perimetre_syndicat_mixte <- perimetre_groupement %>%
  filter(Nature.juridique %in% c("SMF", "SMO")) %>%
  select(Commune.siège, N..SIREN, Nom.du.groupement, Siren.membre, Nom.membre, Type, Nature.juridique, Représentation.substitution) %>%
  separate(col = Commune.siège, into = c("cheflieu_sm", "lib_cheflieu_sm"), sep = " - ", extra = "merge") %>%
  rename(siren_sm = N..SIREN,
         lib_sm = Nom.du.groupement,
         id_groupement = Siren.membre,
         type_groupement = Type,
         nature_juridique = Nature.juridique,
         representation_substitution = Représentation.substitution) %>%
  select(-Nom.membre, -lib_cheflieu_sm) %>%
  mutate(across(.cols = where(is.character), str_trim), # Supprime des espaces en début et fin de caractère
         cheflieu_sm = str_squish(cheflieu_sm))

syndicat_mixte <- perimetre_syndicat_mixte %>%
  select(siren_sm, lib_sm, nature_juridique, cheflieu_sm) %>%
  group_by(across()) %>%
  summarise(.groups = "drop")

ngeo_syndicat_mixte <- perimetre_syndicat_mixte %>%
  select(siren_sm, id_groupement, type_groupement, representation_substitution) %>%
  mutate(type_groupement = case_when(
    type_groupement == "Commune" ~ "commune",
    type_groupement == "Groupement" ~ "groupement",
    type_groupement == "Autre organisme" ~ "autre organisme",
    is.na(type_groupement) ~ "",
    .default = "autre")) %>%
  filter(siren_sm != "256900861" | type_groupement != "autre organisme") # Commune membre 200046977 en doublon du groupement 256900861

ngeo_syndicat_mixte %>%
  group_by(siren_sm, id_groupement) %>%
  filter(n()>1)

ngeo_syndicat_mixte %>% group_by(type_groupement) %>% count()

# ***********************************************
# POLE METROPOLITAIN
# ***********************************************

perimetre_pole_metropolitain <- perimetre_groupement %>%
  filter(Nature.juridique %in% c("POLEM")) %>%
  select(Commune.siège, N..SIREN, Nom.du.groupement, Siren.membre, Nom.membre, Type, Représentation.substitution) %>%
  separate(col = Commune.siège, into = c("cheflieu_polem", "lib_cheflieu_polem"), sep = " - ") %>%
  rename(siren_polem = N..SIREN,
         lib_polem = Nom.du.groupement,
         id_groupement = Siren.membre,
         type_groupement = Type,
         representation_substitution = Représentation.substitution) %>%
  select(-Nom.membre, -lib_cheflieu_polem) %>%
  mutate(across(where(is.character), str_trim), # Supprime des espaces en début et fin de caractère
         cheflieu_polem = str_squish(cheflieu_polem))


pole_metropolitain <- perimetre_pole_metropolitain %>%
  select(siren_polem, lib_polem, cheflieu_polem) %>%
  group_by(across()) %>%
  summarise(.groups = "drop")

ngeo_pole_metropolitain <- perimetre_pole_metropolitain %>%
  select(siren_polem, id_groupement, type_groupement, representation_substitution) %>%
  mutate(type_groupement = case_when(
    type_groupement == "Groupement" ~ "groupement",
    type_groupement == "Autre organisme" ~ "autre organisme",
    is.na(type_groupement) ~ "",
    .default = "autre"))


# ***********************************************
# EPCI
# ***********************************************

epci_process <- epci_init %>%
  select(Commune.siège, N..SIREN, Nom.du.groupement, Nature.juridique, Siren.membre, Nom.membre) %>%
  separate(col = Commune.siège, into = c("cheflieu_epci", "lib_cheflieu_epci"), sep = " - ") %>%
  select(-lib_cheflieu_epci) %>%
  rename(siren_epci = N..SIREN,
         lib_epci = Nom.du.groupement,
         nature_juridique = Nature.juridique,
         siren_com = Siren.membre,
         lib_com = Nom.membre) %>%
  mutate(across(where(is.character), str_trim), # Supprime des espaces en début et fin de caractère
         cheflieu_epci = str_squish(cheflieu_epci)) # Supprime les espaces en doublons


epci <- epci_process %>%
  select(-siren_com, -lib_com) %>%
  group_by(across()) %>% # Groupe sur toutes les colonnes
  summarise(.groups = "drop") %>%
  mutate(lib_epci = str_squish(lib_epci)) # Supprime les espaces en doublons


# ***********************************************
# NGEO siren_epci
# ***********************************************

ngeo_siren_epci <- epci_process %>%
  select(siren_epci, siren_com)

ngeo_siren_epci %>% group_by(siren_com) %>% filter(n()>1)



# ***********************************************
# PETR
# ***********************************************

# Un PETR (Pôle d’équilibre territorial et rural) est un établissement public
# regroupant plusieurs établissements publics de coopération intercommunale à fiscalité propre 
# Un epci ne peut appartenir à plus d’un PETR

perimetre_petr <- perimetre_groupement %>%
  filter(Nature.juridique == "PETR") %>%
  select(N..SIREN, Nom.du.groupement, Type, Siren.membre, Nom.membre, Commune.siège, Représentation.substitution) %>%
  separate(col = Commune.siège, into = c("cheflieu_petr", "lib_cheflieu_petr"), sep = " - ") %>%
  filter(Type == "Groupement") %>%
  rename(siren_petr = N..SIREN,
         lib_petr = Nom.du.groupement,
         siren_epci = Siren.membre,
         representation_substitution = Représentation.substitution) %>%
  select(-Type, -Nom.membre, -lib_cheflieu_petr) %>%
  mutate(lib_petr = str_trim(lib_petr)) %>% # Suppression des espaces en début et fin de caractère
  mutate(lib_petr = gsub("\\s+", " ", lib_petr)) %>% # Suppression des multiples espaces entre les caractères
  mutate(lib_petr = str_replace(lib_petr, "pays", "Pays"),
         lib_petr = gsub("^SM|Pôle Territorial|Pôle territorial", "PETR", lib_petr),
         lib_petr = if_else(grepl("PETR", lib_petr), lib_petr, paste0("PETR ", lib_petr))) # Ajout PETR si absent


sum(duplicated(perimetre_petr)) # test si présence de doublons sur ligne entière
perimetre_petr %>% group_by(siren_epci) %>% filter(n() >1) # test si un epci est présent sur plusieurs EPT

# le CA Tarbes-Lourdes-Pyrénées fait partie de 2 PETR à cause de la fusion d'EPCI

petr <- perimetre_petr %>%
  select(-siren_epci, -representation_substitution) %>%
  group_by(across()) %>%
  summarise(.groups = "drop")

epci_petr <- perimetre_petr %>%
  select(siren_epci, siren_petr, representation_substitution)

ngeo_petr <- perimetre_petr %>%
  select(siren_petr, siren_epci, representation_substitution) %>%
  left_join(ngeo_siren_epci, by = c("siren_epci"), multiple = "all") %>%
  select(siren_com, siren_petr, representation_substitution)
# ne pas prendre en compte tant que la situation ci-dessus n'est pas résolue

ngeo_petr %>% group_by(siren_com, siren_petr) %>% filter(n()>1)

# ***********************************************
# EPT
# ***********************************************

perimetre_ept <- perimetre_groupement %>%
  filter(Nature.juridique == "EPT") %>%
  select(N..SIREN, Nom.du.groupement, Type, Siren.membre, Nom.membre, Commune.siège) %>%
  separate(col = Commune.siège, into = c("cheflieu_ept", "lib_cheflieu_ept"), sep = " - ") %>%
  rename(siren_ept = N..SIREN,
         lib_ept = Nom.du.groupement,
         siren_com = Siren.membre) %>%
  select(-Type, -Nom.membre, -lib_cheflieu_ept) %>%
  mutate(lib_ept = str_trim(lib_ept)) %>% # Suppression des espaces en début et fin de caractère
  mutate(lib_ept = gsub("\\s+", " ", lib_ept)) %>% # Suppression des multiples espaces entre les caractères
  mutate(lib_ept = replace(lib_ept, siren_ept == "200057941", "Paris Est Marne et Bois"),
         lib_ept = replace(lib_ept, siren_ept == "200057966", "Vallée Sud - Grand Paris"))

sum(duplicated(perimetre_ept)) # test si présence de doublons sur ligne entière
perimetre_ept %>% group_by(siren_com) %>% filter(n() >1) # test si un epci est présent sur plusieurs EPT


ngeo_siren_ept <- perimetre_ept %>%
  select(siren_com, siren_ept)

perimetre_ept_epci <- ngeo_siren_ept %>%
  left_join(ngeo_siren_epci, by = c("siren_com")) %>%
  select(-siren_com)

ept <- perimetre_ept %>%
  select(-siren_com) %>%
  left_join(perimetre_ept_epci, by = c("siren_ept"), multiple = "all") %>%
  group_by(across()) %>%
  summarise(.groups = "drop") %>%
  mutate(lib_ept = paste0("EPT ", lib_ept))
  


# ***********************************************
# NGEO banatic
# ***********************************************

ngeo_banatic <- ngeo_siren_com %>%
  left_join(ngeo_siren_epci, by = c("siren_com")) %>%
  left_join(ngeo_siren_ept, by = c("siren_com"))

# Selon les chiffres de BANATIC
# 1254 EPCI, 4872 SIVU, 1233 SIVOM, 1991 SMF, 809 SMO, 25 POLEM, 124 PETR, 11 EPT

nrow(epci) == 1254 +1 #ajout MET69
nrow(syndicat_intercommunal) == 4637+1207
nrow(syndicat_mixte) == 1971+800
nrow(pole_metropolitain) == 26
nrow(petr) == 125
nrow(ept) == 11



save(ept,
     epci,
     petr,
     epci_petr,
     ngeo_petr,
     syndicat_intercommunal,
     ngeo_syndicat_intercommunal,
     syndicat_mixte,
     ngeo_syndicat_mixte,
     pole_metropolitain,
     ngeo_pole_metropolitain,
     ngeo_banatic,
     file = here("src/script/ngeo-fr/2023/rdata", "etl-db-banatic.Rdata"))
