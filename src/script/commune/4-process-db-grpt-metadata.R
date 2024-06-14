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
## COG2022
# ------------------------------------------------

# Ouverture de la base de données
con_grpt <- dbConnect(RSQLite::SQLite(), here("public/2023/grpt-fr-cog2023.sqlite3"))
dbListTables(con_grpt)

# Formatage des métadonnées --------------

# id 1 Identification des données
identifiant               <- "grpt-fr"
titre                     <- "Référentiel des groupements"
theme                     <- ""
description               <- "Référentiel des groupements avec pour code NAF 84.11Z"

# id 2 Situation géographique
echelon                   <- "multi-échelon"
millesime                 <- "2022"

# id 3 Référence temporelle
date_de_publication       <- "2022-09-01"
date_de_creation          <- "2022-09-01"
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
data_init <- dbGetQuery(con_grpt, "SELECT * from groupement")

dbDisconnect(con_grpt)


# Traitement des données --------------

data_grpt <- data_init

# Compilation données pour référencement des colonnes
data <- bind_rows(data_grpt) %>% .[NULL,]




# Ecriture des données ------------

# A l'échelle du dispositif
name_echelon <- "cog"
output_name <- paste0(identifiant, "-", name_echelon, millesime)

write.csv(data_grpt, 
          here(paste0(path_output, "/", output_name, ".csv")),
          fileEncoding = "UTF-8", row.names = F, na = "")




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


