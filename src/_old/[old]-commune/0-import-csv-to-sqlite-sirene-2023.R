library(data.table)
library(tidyverse)
library(RSQLite)


path_stocketablissement <- "C://Users/bmiroux/Desktop/StockEtablissement_utf8/StockEtablissement_utf8.csv"
path_stockunitelegale <- "C://Users/bmiroux/Desktop/StockUniteLegale_utf8/StockUniteLegale_utf8.csv"

# Fichier Stock etablissmeent
stock_etablissement_init <- fread(path_stocketablissement, 
                                  select = c("activitePrincipaleEtablissement",
                                  "siren",
                                  "nomenclatureActivitePrincipaleEtablissement",
                                  "codeCommuneEtablissement",
                                  "numeroVoieEtablissement",
                                  "indiceRepetitionEtablissement",
                                  "typeVoieEtablissement",
                                  "libelleVoieEtablissement",
                                  "complementAdresseEtablissement",
                                  "codePostalEtablissement",
                                  "libelleCommuneEtablissement",
                                  "denominationUsuelleEtablissement",
                                  "etablissementSiege",
                                  "etatAdministratifEtablissement"),
                                  showProgress = F)


# Fichier Stock Unite Legale
stock_unite_legale_init <- fread(path_stockunitelegale,
                                 select = c("activitePrincipaleUniteLegale",
                                            "categorieJuridiqueUniteLegale",
                                            "denominationUniteLegale",
                                            "etatAdministratifUniteLegale",
                                            "nomUniteLegale",
                                            "nomUsageUniteLegale",
                                            "sigleUniteLegale",
                                            "siren"),
                                 showProgress = F)



glimpse(stock_etablissement_init)
glimpse(stock_unite_legale_init)

# stock_etablissement <- stock_etablissement_init %>% mutate_all(as.character(.))


# Création de la base de données
con <- dbConnect(RSQLite::SQLite(),"C://Users/bmiroux/Desktop/sirene.sqlite3") # Ouverture de la base de données



# Création de la table stock_etablissement
dbExecute(con,
          "CREATE TABLE IF NOT EXISTS stock_etablissement(
          activitePrincipaleEtablissement TEXT,
          siren TEXT NOT NULL,
          nomenclatureActivitePrincipaleEtablissement TEXT,
          codeCommuneEtablissement TEXT,
          numeroVoieEtablissement TEXT,
          indiceRepetitionEtablissement TEXT,
          typeVoieEtablissement TEXT,
          libelleVoieEtablissement TEXT,
          complementAdresseEtablissement TEXT,
          codePostalEtablissement TEXT,
          libelleCommuneEtablissement TEXT,
          denominationUsuelleEtablissement TEXT,
          etablissementSiege TEXT,
          etatAdministratifEtablissement TEXT
          )")


# Création de la table stock_etablissement
dbExecute(con,
          "CREATE TABLE IF NOT EXISTS stock_unitelegale(
          activitePrincipaleUniteLegale TEXT,
          categorieJuridiqueUniteLegale TEXT,
          denominationUniteLegale TEXT,
          etatAdministratifUniteLegale TEXT,
          nomUniteLegale TEXT,
          nomUsageUniteLegale TEXT,
          sigleUniteLegale TEXT,
          siren TEXT
          )")





# 4/ Intégration dans la base de données
# ----------------------------------------------
dbReadTable(con,"stock_etablissement")
dbReadTable(con,"stock_unitelegale")



dbAppendTable(con, "stock_etablissement", stock_etablissement_init)
dbAppendTable(con, "stock_unitelegale", stock_unite_legale_init)


dbListTables(con) # Liste des tables
dbDisconnect(con)


# 
# stock_etablissement <- stock_etablissement_init %>%
#   filter(activitePrincipaleEtablissement == "84.11Z") %>%
#   filter(etatAdministratifEtablissement == "A" & etablissementSiege == TRUE)
# 
# 
# stock_unite_legale <- stock_unite_legale_init %>%
#   filter(activitePrincipaleUniteLegale == "84.11Z"& etatAdministratifUniteLegale == "A")
# 
# 
# 
# liste_departement <- stock_unite_legale %>%
#   filter(grepl(pattern = "^DEPARTEMENT", x = denominationUniteLegale))
#   
# 
# liste_region <- stock_unite_legale %>%
#   filter(grepl(pattern = "^REGION", x = denominationUniteLegale))
# 
