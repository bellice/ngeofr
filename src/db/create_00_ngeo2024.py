import duckdb
import pandas as pd
import shutil
import os

# Connexion à une base de données DuckDB (crée la base si elle n"existe pas)
conn = duckdb.connect("src/_db/ngeo2024.duckdb")

# Ordre des colonnes
column_order = [
    "com_type", "com_insee", "com_siren", "com_nom",
    "reg_insee", "reg_nom", "reg_cheflieu",
    "arr_insee", "arr_nom", "arr_cheflieu",
    "dep_insee", "dep_nom", "dep_cheflieu",
    "epci_siren",  "epci_nom", "epci_cheflieu" ,"epci_interdep", "epci_naturejuridique",
    "ept_siren", "ept_nom", "ept_cheflieu", "ept_naturejuridique"
]


com_siren_to_insee = pd.read_parquet("src/params/banatic/table_com_siren.parquet").set_index("com_siren")["com_insee"]

df = (
    pd.read_parquet("src/params/insee/table_com.parquet")  # table com
    .drop(columns=["com_parent"])
    .query("com_type == 'COM'")
    .merge(
        pd.read_parquet("src/params/insee/table_arr.parquet")  # table arr
        .drop(columns=["dep_insee", "reg_insee"]),
        how="left", on="arr_insee"
    )
    .merge(
        pd.read_parquet("src/params/insee/table_dep.parquet")  # table dep
        .drop(columns=["reg_insee"]),
        how="left", on="dep_insee"
    )
    .merge(
        pd.read_parquet("src/params/insee/table_reg.parquet"),  # table reg
        how="left", on="reg_insee"
    )
    .merge(
        pd.read_parquet("src/params/banatic/table_com_siren.parquet")  # table insee/siren
        .drop(columns=["com_nom"]),
        how="left", on="com_insee"
    )
    .merge(
        pd.read_parquet("src/params/banatic/table_epci_perimetre.parquet")  # table epci
        .drop(columns=["epci_membre_nom"]),
        how="left", left_on="com_siren", right_on="epci_membre_siren"
    )
    .merge(
        pd.read_parquet("src/params/banatic/table_ept_perimetre.parquet")  # table ept
        .drop(columns=["ept_membre_nom"]),
        how="left", left_on="com_siren", right_on="ept_membre_siren"
    )
    .drop(columns=["epci_membre_siren", "ept_membre_siren"])
    .assign(epci_interdep=lambda x: x['epci_interdep'].fillna(0).astype(int))
)

df.shape    
df.dtypes


#df.to_parquet("src/params/table_test.parquet", engine="pyarrow", compression="gzip")

# Au 1er janvier il y a 34 935 communes

df.head()
df.shape

cleaned_column_order = [col.strip() for col in column_order]
# Trouver les colonnes présentes dans df mais pas dans column_order
columns_in_df_only = set(df.columns) - set(cleaned_column_order)
# Trouver les colonnes présentes dans column_order mais pas dans df
columns_in_order_only = set(cleaned_column_order) - set(df.columns)

print(f"Colonnes présentes dans df mais pas dans column_order : {columns_in_df_only}")
print(f"Colonnes présentes dans column_order mais pas dans df : {columns_in_order_only}")

df_vf = (
    df.assign(
        epci_cheflieu=lambda x: x['epci_cheflieu'].map(
            x.set_index('com_siren')['com_insee']
        ).fillna(x['epci_cheflieu']),
        ept_cheflieu=lambda x: x['ept_cheflieu'].map(
            x.set_index('com_siren')['com_insee']
        ).fillna(x['ept_cheflieu'])
    )[column_order]
)

# Sauvegarder le DataFrame dans la base de données DuckDB
conn.execute("CREATE OR REPLACE TABLE ngeofr AS SELECT * FROM df_vf")
conn.execute("VACUUM")

# Fermer la connexion
conn.close()

# Copie du fichier
source_file = "src/_db/ngeo2024.duckdb"
destination_directory = "public/"
destination_file = os.path.join(destination_directory, os.path.basename(source_file))

# Copier le fichier
shutil.copy(source_file, destination_file)

print(f"Fichier copié vers {destination_file}")
