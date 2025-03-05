# ngeofr

## Description
**ngeofr** est un référentiel de données géographiques à l'échelle communale, basé sur le Code officiel géographique (COG) en vigueur. Ce projet permet de centraliser et de structurer des données géographiques provenant de sources officielles pour faciliter leur utilisation dans des analyses, des visualisations ou des applications.

## Statut des millésimes du COG


![COG 2025](https://img.shields.io/badge/COG%202025-🔄%20En%20cours-orange)
![COG 2024](https://img.shields.io/badge/COG%202024-✅%20Disponible-brightgreen)

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
conn = duckdb.connect('public/ngeo2024.duckdb')

# Exemple de requête
result = conn.execute("SELECT * FROM ngeofr LIMIT 10").fetchall()
print(result)
```

Les données sont également disponibles au format Parquet pour une intégration facile avec des outils comme Pandas ou Spark.

```python
import pandas as pd

# Charger les données Parquet
df = pd.read_parquet('public/ngeo2024.parquet')
print(df.head())
```

## Structure du projet
```
ngeofr/
├── public/                       # Données publiques pour les utilisateurs finaux
│   ├── ngeo2024.duckdb           # Base de données principale (DuckDB)
│   └── ngeo2024.parquet          # Export de la base (format Parquet)
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

### Formats de données disponibles
Les données du projet **ngeofr** sont disponibles dans les formats suivants :

![DuckDB](https://img.shields.io/badge/DuckDB-%E2%9C%94-blue)  
![Apache Parquet](https://img.shields.io/badge/Apache%20Parquet-%E2%9C%94-blue)

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


[![Source INSEE](https://img.shields.io/badge/Source-INSEE-blue)](https://www.insee.fr/)
[![Source Banatic](https://img.shields.io/badge/Source-Banatic-blue)](https://www.banatic.interieur.gouv.fr/)
[![Source dataNOVA](https://img.shields.io/badge/Source-dataNOVA-blue)](https://datanova.laposte.fr/)

## Licence
Ce projet est sous licence MIT - voir le fichier [LICENSE](./LICENSE) pour plus de détails