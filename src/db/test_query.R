# Charger les bibliothèques
library(DBI)
library(duckdb)
library(arrow)  # Utilisé pour l'exportation en parquet

# Connexion à la base de données DuckDB (modifie le chemin vers ta base si nécessaire)
con <- dbConnect(duckdb::duckdb(), dbdir = "./public/ngeo2024.duckdb")

# Lire le fichier SQL
query <- readLines("./src/_shared/query_epci_ept.sql", warn = FALSE)
query <- paste(query, collapse = "\n")

# Exécuter la requête et récupérer le résultat sous forme de data frame
df <- dbGetQuery(con, query)
dbReadTable(con)
# Afficher les 5 premières lignes pour vérifier le résultat
print(head(df))

# Si tu souhaites sauvegarder les résultats dans un fichier parquet
write_parquet(df, "path/to/save/output_file.parquet")

# Fermer la connexion à la base de données
dbDisconnect(con, shutdown = TRUE)
