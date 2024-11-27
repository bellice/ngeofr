import duckdb

# Connexion à la base de données DuckDB (modifie le chemin vers ta base si nécessaire)
con = duckdb.connect("public/ngeo2024.duckdb")

# Lire le fichier SQL
with open("src/_shared/query_epci_ept.sql", "r") as file:
    query = file.read()

# Exécuter la requête et récupérer le résultat sous forme de DataFrame pandas
df = con.execute(query).fetchdf()

# Afficher les 5 premières lignes pour vérifier le résultat
print(df.head())

# Si tu souhaites sauvegarder les résultats dans un nouveau fichier parquet
# df.to_parquet("path/to/save/output_file.parquet")

# Fermer la connexion à la base de données
con.close()
