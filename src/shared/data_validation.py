import pandas as pd
import re

def test_length_values(df: pd.DataFrame, column_name: str, lengths: list) -> None:
    """
    Vérifie que toutes les valeurs de la colonne spécifiée ont une longueur de chaîne de caractères donnée ou sont NULL,
    et que les valeurs sont des entiers, des chaînes de caractères ou des chaînes alphanumériques si elles ne sont pas NULL.
    
    Parameters:
    df (pd.DataFrame): Le DataFrame à tester.
    column_name (str): Le nom de la colonne à tester.
    lengths (list): Les longueurs de chaîne de caractères acceptées.
    
    Raises:
    ValueError: Si des valeurs dans la colonne spécifiée n'ont pas une longueur acceptée, ne sont pas des entiers, des chaînes de caractères, des chaînes alphanumériques, ou ne sont pas NULL.
    """
    
    # Filtrer les entrées non-NULL
    non_null_entries = df[df[column_name].notna()].copy()
    
    # Vérifier la validité des valeurs (longueur et type)
    def is_valid(value):
        # Si la valeur est un entier, vérifier la longueur du nombre en le convertissant en chaîne
        if isinstance(value, int):
            return len(str(value)) in lengths
        # Si la valeur est une chaîne de caractères, vérifier sa longueur et son caractère alphanumérique
        elif isinstance(value, str):
            return (len(value) in lengths) and (value.isdigit() or value.isalpha() or value.isalnum())
        # Sinon, valeur non valide
        else:
            return False
    
    # Appliquer la fonction de validation
    invalid_entries = non_null_entries[~non_null_entries[column_name].apply(is_valid)]
    
    # Si des entrées sont invalides, lever une exception
    if not invalid_entries.empty:
        raise ValueError(f"Les entrées suivantes dans la colonne '{column_name}' n'ont pas une longueur acceptée ({lengths}), "
                         f"ne sont pas des entiers, des chaînes de caractères, ou des chaînes alphanumériques:\n"
                         f"{invalid_entries}")




def test_no_null_values(df: pd.DataFrame, column_name: str) -> None:
    """
    Vérifie qu'il n'y a pas de valeurs NULL dans la colonne spécifiée.

    Parameters:
    df (pd.DataFrame): Le DataFrame à tester.
    column_name (str): Le nom de la colonne à tester.

    Raises:
    ValueError: Si des valeurs NULL sont trouvées dans la colonne spécifiée.
    """
    null_entries = df[df[column_name].isnull()]
    if not null_entries.empty:
        raise ValueError(f"Les entrées suivantes dans la colonne '{column_name}' contiennent des valeurs NULL:\n{null_entries}")


def test_date_format(df: pd.DataFrame, column_name: str) -> None:
    """
    Vérifie que toutes les dates dans la colonne spécifiée respectent le format 'YYYY-MM-DD'.
    
    Parameters:
    df (pd.DataFrame): Le DataFrame à tester.
    column_name (str): Le nom de la colonne à tester.
    
    Raises:
    ValueError: Si des valeurs dans la colonne spécifiée ne respectent pas le format de date attendu.
    """
    regex = re.compile(r'^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$')
    
    # Convertir les dates en chaînes si elles ne le sont pas déjà
    df[column_name] = df[column_name].apply(lambda x: x.strftime('%Y-%m-%d') if isinstance(x, pd.Timestamp) else x)
    
    # Filtrer les entrées invalides
    invalid_entries = df[~df[column_name].isnull() & ~df[column_name].apply(lambda x: bool(regex.match(str(x))))]
    
    if not invalid_entries.empty:
        raise ValueError(f"Les entrées suivantes dans la colonne '{column_name}' ne respectent pas le format de date 'YYYY-MM-DD':\n{invalid_entries}")
