library(tidyverse)  # Traitement des données
library(rmarkdown)  # Génération pdf
library(kableExtra) # Tableaux stylisés
library(readxl)     # Gestion fichier excel
library(here)       # Gestion chemin du projet
library(RSQLite)    # Accès base de données SQLite
# Paramètre utilisateur
# ------------------------------------------------
# ------------------------------------------------

# ------------------------------------------------
## COG2023
# ------------------------------------------------

# Ouverture de la base de données
con_fr <- dbConnect(RSQLite::SQLite(), here("public/2023/ngeo-fr-cog2023.sqlite3"))
dbListTables(con_fr)

# Formatage des métadonnées --------------

# id 1 Identification des données
identifiant               <- "ngeo-fr"
titre                     <- "Référentiel de données"
theme                     <- ""
description               <- "Référentiel de données de zonages administratifs et d'études sur l'ensemble du territoire français"

# id 2 Situation géographique
echelon                   <- "multi-échelon"
millesime                 <- "2023"

# id 3 Référence temporelle
date_de_publication       <- "2022-08-01"
date_de_creation          <- "2022-08-01"
date_de_derniere_revision <- as.character(Sys.Date())
etendue_temporelle        <- "2023"

# id 4 Responsabilité
proprietaire              <- "ANCT"
point_de_contact          <- "cartographie@anct.gouv.fr"

# Chemin de destination des données ------------
path_output <- here("public/2023/")

# Chemin métadonnées ------------
path_metadata <- here("src/script/ngeo-fr/2023/params/metadata-ngeo.Rmd")

# Template des métadonnées
metadata <- read.csv(here("src/dependencies/data-gouv/metadata.csv"), encoding = "UTF-8")

# Dictionnaire de variable
dict_col <- read.csv(here("src/dependencies/data-gouv/dictionnaire-variable.csv"), encoding = "UTF-8")



# Importation des données -------------

# Données
liste_table <- dbListTables(con_fr)

data_init <- data.frame()
for (i in 1:length(liste_table)){
data_init <- bind_rows(data_init, dbGetQuery(con_fr, paste0("SELECT * from pragma_table_info('", liste_table[i],"')")))
}

dbDisconnect(con_fr)


# Traitement des données --------------

data <- data_init %>%
  select(name) %>%
  unique() %>%
  arrange(name) %>%
  mutate(value = NA_character_) %>%
  pivot_wider(values_from = value)


# Métadonnées -----------------

# Attribution des variables

# id 1 Identification des données
metadata[metadata$id == 1 & metadata$key == "identifiant", "value"] <- identifiant
metadata[metadata$id == 1 & metadata$key == "titre", "value"] <- titre
metadata[metadata$id == 1 & metadata$key == "thème", "value"] <- theme
metadata[metadata$id == 1 & metadata$key == "description", "value"] <- description

# id 2 Situation géographique
metadata[metadata$id == 2 & metadata$key == "échelon", "value"] <- echelon
metadata[metadata$id == 2 & metadata$key == "millésime", "value"] <- millesime

# id 3 Référence temporelle
metadata[metadata$id == 3 & metadata$key == "date de publication", "value"] <- date_de_publication
metadata[metadata$id == 3 & metadata$key == "date de création", "value"] <- date_de_creation
metadata[metadata$id == 3 & metadata$key == "millésime", "value"] <- millesime
metadata[metadata$id == 3 & metadata$key == "étendue temporelle", "value"] <- etendue_temporelle
metadata[metadata$id == 3 & metadata$key == "propriétaire", "value"] <- proprietaire
metadata[metadata$id == 3 & metadata$key == "point de contact", "value"] <- point_de_contact

# id 4 Responsabilité
metadata[metadata$id == 4 & metadata$key == "propriétaire", "value"] <- proprietaire
metadata[metadata$id == 4 & metadata$key == "point de contact", "value"] <- point_de_contact

metadata[metadata$key == "date de dernière révision", "value"] <- date_de_derniere_revision

# Ecriture fichier
output_name <- paste0(identifiant, "-metadata")

render(input = path_metadata,
       output_format = "pdf_document",
       output_file = output_name,
       output_dir = path_output)


