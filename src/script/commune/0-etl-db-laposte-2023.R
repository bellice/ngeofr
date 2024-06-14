# Chargement des librairies
library(tidyverse) #manipulation des données
library(here) #gestion des chemins relatifs

# Source des données
# https://datanova.legroupe.laposte.fr


# Import des fichiers

codepostal_init <- read.csv(file = here("src/assets/laposte/2023/laposte_hexasmal.csv"),
                            encoding = "UTF-8", sep = ";", colClasses = c("character"))


# Important de garder le nom des communes pour une jointure future.


perimetre_laposte <- codepostal_init %>%
  select(-Ligne_5, -coordonnees_gps) %>%
  rename(insee_com = "X.U.FEFF.Code_commune_INSEE",
         lib_com_m =  Nom_commune,
         code_postal = Code_postal,
         lib_postal = Libellé_d_acheminement) %>%
  group_by(across()) %>%
  summarise(.groups = "drop")

laposte <- perimetre_laposte %>%
  select(code_postal, lib_postal) %>%
  group_by(code_postal, lib_postal) %>%
  summarise(.groups = "drop")

ngeo_laposte <- perimetre_laposte  %>%
  select(insee_com, code_postal, lib_postal)


# En se basant que le code_postal et le lib_postal
# 1 ligne en doublon
perimetre_laposte %>%
  group_by(code_postal, lib_postal) %>%
  filter(n()>1)

save(ngeo_laposte,
     laposte,
     file = here("src/script/ngeo-fr/2023/rdata", "etl-db-laposte.Rdata"))

