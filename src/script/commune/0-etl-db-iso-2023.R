# Chargement des librairies
library(tidyverse) #manipulation des données
library(rvest) #scrapper des données sur le web
library(httr)
library(jsonlite)
library(here) #gestion des chemins relatifs

# Source des données
# https://www.iso.org/obp/ui/

# Import des fichiers

# https://www.iso.org/obp/ui/#iso:code:3166:FR
# https://www.iso.org/obp/ui/?v-1658157989552

iso_3166_2_init <- fromJSON(here("src/assets/iso/2023/iso-org-code-3166-fr-20230315.json"))


iso_3166_2 <- iso_3166_2_init %>%
  .[[2]] %>% #sélection
  read_html() %>% #lecture du contenu html
  html_table(fill = T) %>% #scrap table html
  .[[2]] %>% #sélection de la table
  rename(iso_3166_2 = `3166-2 code`,
         categorie_subdivision = `Subdivision category`,
         nom_subdivision = `Subdivision name`)


iso_3166_2 %>%
  group_by(categorie_subdivision) %>%
  count()


departement_iso_dep <- iso_3166_2 %>%
  filter(categorie_subdivision %in% c("metropolitan department",
                                      "metropolitan collectivity with special status",
                                      "overseas unique territorial collectivity",
                                      "overseas departmental collectivity")) %>%
  filter(!iso_3166_2 %in% c("FR-20R", "FR-69M")) %>% #ne pas prendre en compte Métropole de Lyon et Corse
  separate(iso_3166_2, into = c("iso_pays", "insee_dep"), remove = F) %>%
  select(iso_3166_2, insee_dep) %>%
  rename(iso_dep = iso_3166_2) %>%
  mutate(insee_dep = replace(insee_dep, insee_dep == "75C", "75")) %>%
  arrange(insee_dep)

region_iso_reg <- iso_3166_2 %>%
  filter(categorie_subdivision %in% c("metropolitan region",
                                      "overseas unique territorial collectivity",
                                      "overseas departmental collectivity",
                                      "metropolitan collectivity with special status")) %>%
  filter(!iso_3166_2 %in% c("FR-69M", "FR-75C")) %>% #ne pas prendre en compte Métropole de Lyon et Paris
  select(iso_3166_2, nom_subdivision) %>%
  mutate(nom_subdivision = gsub("\\s*\\([^\\)]+\\)", "", nom_subdivision)) %>% #supprime parenthèse et intérieur parenthèse
  mutate(nom_subdivision = str_squish(nom_subdivision)) %>% #supprime espace start/end et double espace
  rename(iso_reg = iso_3166_2) %>%
  arrange(nom_subdivision)

france <- iso_3166_2 %>%
  filter(categorie_subdivision %in% c("dependency",
                                      "overseas departmental collectivity",
                                      "overseas unique territorial collectivity",
                                      "overseas collectivity with special status",
                                      "overseas collectivity",
                                      "overseas territory")) %>%
  select(iso_3166_2, nom_subdivision) %>%
  mutate(
    iso_territoire_2 = case_when( #ajout code iso_alpha3
      iso_3166_2 == "FR-CP" ~ "CP",
      iso_3166_2 == "FR-971" ~ "GP",
      iso_3166_2 == "FR-973" ~ "GF",
      iso_3166_2 == "FR-974" ~ "RE",
      iso_3166_2 == "FR-972" ~ "MQ",
      iso_3166_2 == "FR-976" ~ "YT",
      iso_3166_2 == "FR-NC" ~ "NC",
      iso_3166_2 == "FR-PF" ~ "PF",
      iso_3166_2 == "FR-BL" ~ "BL",
      iso_3166_2 == "FR-MF" ~ "MF",
      iso_3166_2 == "FR-PM" ~ "PM",
      iso_3166_2 == "FR-TF" ~ "TF",
      iso_3166_2 == "FR-WF" ~ "WF",
      TRUE ~ as.character(iso_3166_2)),
    iso_territoire_3 = case_when( #ajout code iso_alpha3
    iso_3166_2 == "FR-CP" ~ "CPT",
    iso_3166_2 == "FR-971" ~ "GLP",
    iso_3166_2 == "FR-973" ~ "GUF",
    iso_3166_2 == "FR-974" ~ "REU",
    iso_3166_2 == "FR-972" ~ "MTQ",
    iso_3166_2 == "FR-976" ~ "MYT",
    iso_3166_2 == "FR-NC" ~ "NCL",
    iso_3166_2 == "FR-PF" ~ "PYF",
    iso_3166_2 == "FR-BL" ~ "BLM",
    iso_3166_2 == "FR-MF" ~ "MAF",
    iso_3166_2 == "FR-PM" ~ "SPM",
    iso_3166_2 == "FR-TF" ~ "ATF",
    iso_3166_2 == "FR-WF" ~ "WLF",
    TRUE ~ as.character(iso_3166_2)),
    nom_subdivision = gsub(" \\(.*", "", nom_subdivision), #suppression des éléments après la 1ère parenthèse
    insee_territoire = case_when( #ajout code insee_territoire
      iso_3166_2 == "FR-CP" ~ "989",
      iso_3166_2 == "FR-971" ~ "971",
      iso_3166_2 == "FR-973" ~ "973",
      iso_3166_2 == "FR-974" ~ "974",
      iso_3166_2 == "FR-972" ~ "972",
      iso_3166_2 == "FR-976" ~ "976",
      iso_3166_2 == "FR-NC" ~ "988",
      iso_3166_2 == "FR-PF" ~ "987",
      iso_3166_2 == "FR-BL" ~ "977",
      iso_3166_2 == "FR-MF" ~ "978",
      iso_3166_2 == "FR-PM" ~ "975",
      iso_3166_2 == "FR-TF" ~ "984",
      iso_3166_2 == "FR-WF" ~ "986",
      TRUE ~ as.character(iso_3166_2))
    ) %>%
  rename(lib_territoire = nom_subdivision) %>%
  select(iso_territoire_2, lib_territoire, insee_territoire, iso_territoire_3) %>%
  rbind(c("FR", "France métropolitaine", NA, "FRA")) %>%
  arrange(insee_territoire)

save(departement_iso_dep,
     region_iso_reg,
     france,
     file = here("src/script/ngeo-fr/2023/rdata", "etl-db-iso.Rdata"))

