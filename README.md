![GitHub last commit](https://img.shields.io/github/last-commit/bellice/ngeofr)
![DuckDB](https://img.shields.io/badge/DuckDB-%E2%9C%94-brightgreen)
![Apache Parquet](https://img.shields.io/badge/Apache%20Parquet-%E2%9C%94-brightgreen)
![MIT License](https://img.shields.io/badge/License-MIT-green)

# ngeofr

## Description
**ngeofr** est un référentiel de données géographiques à l'échelle communale, basé sur le Code officiel géographique en vigueur. Ce projet permet de centraliser et de structurer des données géographiques provenant de sources officielles pour faciliter leur utilisation dans des analyses, des visualisations ou des applications.

### Public cible
- Développeurs et data scientists cherchant des données géographiques fiables, légères et performantes pour des intégrations efficaces dans des applications et analyses.
- Utilisateurs nécessitant des bases de données géographiques performantes avec une faible empreinte mémoire et optimisées pour des traitements rapides.

## Table des matières
- [Installation](#installation)
- [Utilisation](#utilisation)
- [Structure du projet](#structure-du-projet)
- [Base de données](#base-de-données)
- [FAQ](#faq)
- [Méthodologie](#méthodologie)
- [Sources utilisées](#sources-utilisées)
- [Licence](#licence)

## Installation
Pour installer le projet ngeofr, clonez le dépôt :

```bash
git clone https://github.com/bellice/ngeofr.git
```

## Utilisation

### Charger la base de données
Pour utiliser la base de données DuckDB, vous pouvez la charger dans un script Python :

```python
import duckdb

# Connexion à la base de données
conn = duckdb.connect('public/ngeo<millésime du COG>.duckdb')

# Exemple de requête
result = conn.execute("SELECT * FROM ngeofr LIMIT 10").fetchall()
print(result)
```

Les données sont également disponibles au format Parquet pour une intégration facile avec des outils comme Pandas ou Spark.

```python
import pandas as pd

# Charger les données Parquet
df = pd.read_parquet('public/ngeo<millésime du COG>.parquet')
print(df.head())
```

🚧 En cours de rédaction...

## Structure du projet
```
ngeofr/
├── public/                       # Données publiques pour les utilisateurs finaux
│   ├── ngeo20XX.duckdb           # Base de données principale (DuckDB)
│   └── ngeo20XX.parquet          # Export de la base (format Parquet)
├── src/                          # Code source du projet
│   │── data/                     # Données intermédiaires (Parquet)
│   │── db/                       # Scripts et configs de la base de données
│   │── producers/                # Traitement des données par producteur
│   │   ├── producer A            # Producteur A
│   │   │   │── assets/           # Fichiers sources du producteur A
│   │   │   └── scripts/          # Scripts ETL pour le producteur A
│   │   ├── producer B            # Producteur B
│   │   └── ...                   # Autres producteurs
│   └── shared/                   # Code partagé entre les modules
│       │── sql                   # Requêtes SQL du projet
│       └── data_validation.py    # Validation des données
└── .gitignore                    # Fichiers et dossiers ignorés par Git
└── README.md                     # Documentation du projet
```

## Base de données
Le projet produit une base de données DuckDB, structurée autour de la table `ngeofr`. Cette table centralise les données géographiques à différents niveaux (communes, régions, départements, EPCI, etc.) pour faciliter les requêtes et les analyses.

### Structure de la table `ngeofr`
La table `ngeofr` est divisée en sous-sections pour plus de clarté. Voici les champs principaux :

#### Commune
| Colonne              | Type    | Description                                       |
|----------------------|---------|---------------------------------------------------|
| com_type             | VARCHAR | Type de commune                                   |
| com_insee            | VARCHAR | Code insee commune                                |
| com_siren            | VARCHAR | Code siren commune                                |
| com_nom              | VARCHAR | Nom de la commune                                 |

#### Région
| Colonne              | Type    | Description                                       |
|----------------------|---------|---------------------------------------------------|
| reg_insee            | VARCHAR | Code insee région                                 |
| reg_nom              | VARCHAR | Nom de la région                                  |
| reg_cheflieu         | VARCHAR | Code insee de la commune chef-lieu de la région   |

#### Département
| Colonne              | Type    | Description                                       |
|----------------------|---------|---------------------------------------------------|
| dep_insee            | VARCHAR | Code insee département                            |
| dep_nom              | VARCHAR | Nom du département                                |
| dep_cheflieu         | VARCHAR | Code insee de la commune chef-lieu du département |

#### Établissement public de coopération intercommunale à fiscalité propre
| Colonne              | Type    | Description                                       |
|----------------------|---------|---------------------------------------------------|
| epci_siren           | VARCHAR | Code siren de l'epci                              |
| epci_nom             | VARCHAR | Nom de l'epci                                     |
| epci_cheflieu        | VARCHAR | Code insee de la commune chef-lieu de l'epci      |
| epci_interdep        | INTEGER | État inter-départemental de l'epci                |
| epci_naturejuridique | VARCHAR | Nature juridique de l'epci                        |

#### Établissement public territorial
| Colonne              | Type    | Description                                       |
|----------------------|---------|---------------------------------------------------|
| ept_siren            | VARCHAR | Code siren de l'ept                               |
| ept_nom              | VARCHAR | Nom de l'ept                                      |
| ept_cheflieu         | VARCHAR | Code insee de la commune chef-lieu de l'ept       |
| ept_naturejuridique  | VARCHAR | Nature juridique de l'ept                         |



## FAQ

🚧 En cours de rédaction...

## Méthodologie

### Collecte des données
Les données sont collectées à partir de sources officielles et téléchargées automatiquement via des scripts Python.

### Traitement des données
Les données sont nettoyées, transformées et agrégées à l'aide de scripts ETL (Extract, Transform, Load) dans le dossier `src/producers/`.

### Validation des données
Les données sont validées à l'aide de tests automatisés dans `src/shared/data_validation.py` pour garantir leur qualité et leur cohérence.

## Sources utilisées

### Données externes
- **Insee** : Institut national de la statistique et des études économiques. Données démographiques et géographiques. [🔗 Site officiel](https://www.insee.fr/)

- **Banatic** : Base nationale sur l'intercommunalité. Informations sur les EPCI. [🔗 Site officiel](https://www.banatic.interieur.gouv.fr/)

- **dataNOVA** : Portail de données ouvertes de La Poste Groupe. Données sur les codes postaux. [🔗 Site officiel](https://datanova.laposte.fr/)
