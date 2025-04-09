import os
import re
import numpy as np
import pandas as pd
import io
import ast

#=============================== CHARGEMENT ===============================================

def load_files_raw(folder_path : str, file_mapping: dict) -> dict:
    """
    Charge les fichiers csv d'un dossier en les association à des noms de DataFrame défini dans un mapping (schema du dataset).
    Chaque fichier est associé à la clé du `file_mapping` si son nom contient la clé (insensible à la casse).
    Le fichier est alors chargé dans un dictionnaire avec le nom du DataFrame correspondant. 

    Args:
        folder_path (str): chemin du dossier où parcourir les csv
        file_mapping (dict): Dictionnaire de correspondance entre le schema de donnée attendu et les noms de DataFrame
    
    Returns:
        dict: Un dictionnaire de DataFrame avec comme clés les noms définis dans `file_mapping`.
    """
    dataset = {}

    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            matched = False
            for key, df_name in file_mapping.items():
                pattern = re.compile(r".*{}.*".format(re.escape(key)), re.IGNORECASE)
                if pattern.match(filename):
                    df_name = file_mapping[key]
                    file_path = os.path.join(folder_path, filename)
                    try:
                        df = clean_csv_quote_decimal(file_path)
                        dataset[df_name] = df
                        print(f"OK - Chargé : {filename} --> dataset ['{df_name}']")
                        matched = True
                        break
                    except Exception as e:
                        print(f"NOK - Erreur lors du chargement de {filename} : {e}")
                        matched = True
                        break
            if not matched : 
                print(f"NOK - Fichier ignoré (non reconnu) {filename}")

    return dataset


def clean_csv_quote_decimal(file_path : str) -> pd.DataFrame:
    """
    Nettoie un fichier csv mal formaté:
    - supprime les guillements au début et à la fin
    - remplace les virgules par des points dans les champs décimaux encadrés par des doubles-quotes
        - conserve les quotes pour les listes
    - retourne un DataFrame pandas clean

    Args:
        file_path (str): chemin vers le fichier csv
    
    Returns:
        pd.DataFrame: données nettoyées
    """
    lignes_clean = []
    modifications_count = 0 

    with open(file_path, "r", encoding='utf-8') as f:
        for ligne in f:
            ligne = ligne.strip()

            #Cas 1 : ligne encadrée par " au début et à la fin
            if ligne.startswith('"') and ligne.endswith('"'):
                contenu = ligne[1:-1]
                modif_initial = contenu #sauvegarde l'état initial (pour comptage modifs)
            
                # Si le contenu ne contient pas de liste (donc pas de [ ou ])
                if "[" not in contenu and "]" not in contenu:
                    # Remplacer les nombres décimaux entre double-double quotes : ""0,6"" => 0.6
                    contenu = re.sub(r'""(\d+),(\d+)""', lambda m: f"{m.group(1)}.{m.group(2)}", contenu)
            
                #Supprimer les guillemts doubles restants
                contenu = contenu.replace('""', '')
                
                if contenu != modif_initial:
                    modifications_count += 1

                lignes_clean.append(contenu)
                continue

            #Si la ligne ne remplit par les conditions, on la laisse intacte
            lignes_clean.append(ligne)
    
    if modifications_count > 0:
        print(f"    -> Modifications détectées dans '{os.path.basename(file_path)}' : {modifications_count} ligne(s) nettoyée(s)")
            
    concat = io.StringIO("\n".join(lignes_clean))
    df = pd.read_csv(concat, sep=",", encoding="utf-8")

    return df

def fix_encoding(text):
    """
    Corrige les erreurs d'encodage de type 'DiarrhÃ©e' en 'Diarrhée'.
    
    Cette fonction tente de réparer les chaînes mal décodées lorsque du texte UTF-8
    a été interprété à tort en latin1 (ISO-8859-1), entraînant des caractères erronés.
    
    Si le texte est une chaîne, il est réencodé en latin1 puis redécodé en UTF-8.
    Si le texte est une liste de chaînes, la correction est appliquée à chaque élément.
    Les autres types sont retournés inchangés.
    """
    if isinstance(text, str):
        try:
            return text.encode('latin1').decode('utf-8')
        except:
            return text
    elif isinstance(text, list):
        return [fix_encoding(item) for item in text]
    return text

def load_files_parquet(folder_path: str, file_mapping: dict) -> dict:
    """
    Charge les fichiers parquet d'un dossier en les associant à des noms de DataFrame définis dans un mapping.

    Args:
        folder_path (str): Chemin du dossier où parcourir les fichiers .parquet
        file_mapping (dict): Dictionnaire de correspondance entre le nom de fichier attendu et le nom de DataFrame

    Returns:
        dict: Un dictionnaire de DataFrames avec les noms définis dans `file_mapping`
    """
    dataset = {}

    for filename in os.listdir(folder_path):
        if filename.endswith(".parquet"):
            matched = False
            for key, df_name in file_mapping.items():
                pattern = re.compile(r".*{}.*".format(re.escape(key)), re.IGNORECASE)
                if pattern.match(filename):
                    df_name = file_mapping[key]
                    file_path = os.path.join(folder_path, filename)
                    try:
                        df = pd.read_parquet(file_path)
                        dataset[df_name] = df
                        print(f"✅ Chargé : {filename} --> dataset['{df_name}']")
                        matched = True
                        break
                    except Exception as e:
                        print(f"❌ Erreur lors du chargement de {filename} : {e}")
                        matched = True
                        break
            if not matched:
                print(f"⚠️ Fichier ignoré (non reconnu) : {filename}")

    return dataset

#=============================== NETTOYAGE ===============================================

def convert_date(name: str, df: pd.DataFrame) -> tuple:
    """
    Convertir en datetime les champs date parsés en objets par Pandas. 
    Identifier les colonnes dont les libellés contiennent 'date'. 
        - Support des formats ISO (yyyy-mm-dd ou avec timestamps)
        - Support des formats français (dd/mm/yyyy)
    Collecter les anomalies détectées (ex: 0221/11/12 ou format invalide)

    Args:
        name (str): clé du dataset, correspondant au nom du DataFrame
        df (pd.DataFrame): DataFrame

    Returns:
        df (pd.DataFrame): DataFrame avec les dates converties
        anomalies_dates_df (pd.DataFrame) : Détail des anomalies relevées 
    """
    nb_colonnes_converties = 0
    colonnes_converties = []
    anomalies_dates = []

    for col in df.columns:
        if df[col].dtype == 'object' and 'date' in col.lower():
            serie = df[col].astype(str).copy()
            converted = None

            # Cas 1 : format français détecté (présence de '/')
            if serie.str.contains('/').any():
                try:
                    converted = pd.to_datetime(serie, errors='coerce', dayfirst=True)
                except Exception:
                    pass
            else:
                # Cas 2 : format ISO ou mixte (laisser pandas deviner)
                try:
                    converted = pd.to_datetime(serie, errors='coerce', dayfirst=False)
                except Exception:
                    pass

            if converted is not None and converted.notna().sum() > 0:
                # flag des dates invalides
                invalid_mask = converted.isna() & serie.notna()

                for idx in df[invalid_mask].index:
                    anomalies_dates.append({
                        'table': name,
                        'column': col,
                        'index': idx,
                        'type': 'incorrect_format_date'
                    })

                if invalid_mask.sum() > 0:
                    print(f"    ⚠️ {name} -> Colonne '{col}' : {invalid_mask.sum()} dates invalides détectées")
                
                df[col] = converted
                nb_colonnes_converties += 1
                colonnes_converties.append(col)

    if colonnes_converties:
        print(f"    ✅ {name} -> {nb_colonnes_converties} colonnes Dates converties : {', '.join(colonnes_converties)}")
    
    anomalies_dates_df = pd.DataFrame(anomalies_dates)
    return df, anomalies_dates_df


def detect_clean_list_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Détecte les colonnes de strings contenant des virgules.
    Pour les colonnes identifiées (hors exclusions explicites), on applique la fonction `clean_string_to_list` pour transformer en liste de string nettoyées. 

    Args:
        df (pd.DataFrame): DataFrame à transformer

    Returns:
        pd.DataFrame: DataFrame modifié
    """
    excluded_cols = ['petName']

    for col in df.columns:
        #identifier les colonnes qui contiennent des strings avec des virgules
        if col not in excluded_cols:
            if df[col].dropna().astype(str).str.contains(',').any():
                print(f"    >> Colonne '{col}' identifiée comme liste potentielle.")
                df[col] = df[col].apply(clean_string_to_list)

    return df
    
def clean_string_to_list(val) -> list:
    """
    Convertit une chaine de contenant des virgules en liste.
    - Si NaN ou vide -> retourne []
    - Si string représentant une liste (ex: '["A", "B"]') -> parse en liste
    - Si string avec virgules -> split sur virgules
    - Si déjà liste -> nettoie chaque élément
    - Sinon -> retourne [élément cleané]

    Args:
        val (str): valeur de la colonne à modifier

    Returns:
        list: liste
    """
    #gestion des nan
    if pd.isna(val):
        return []
    
    # Si string représentant une liste → ex: '["Arthrose","Cystite"]'
    if isinstance(val, str) and val.strip().startswith('[') and val.strip().endswith(']'):
        try:
            parsed = ast.literal_eval(val)
            if isinstance(parsed, list):
                return [str(x).strip() for x in parsed if str(x).strip()]
        except Exception:
            pass  # fallback ci-dessous

    # Si string contenant des virgules → split classique
    if isinstance(val, str) and ',' in val:
        return [x.strip() for x in val.split(',') if x.strip()]

    # Si déjà une liste
    if isinstance(val, list):
        return [str(x).strip() for x in val if str(x).strip()]

    # Autres cas
    return [str(val).strip()]

def detect_and_clean_boolean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Détecte les colonnes contenant des booléens encodés sous forme de chaînes
    (comme "True", "False", "Vrai", "Faux") et les convertit en booléens natifs,
    en excluant certaines colonnes spécifiées.

    Args:
        df (pd.DataFrame): DataFrame à nettoyer

    Returns:
        pd.DataFrame: DataFrame avec colonnes nettoyées et castées en booléens
    """
    excluded_columns = {
        'franchise',
        'liabilityPremiumInclTax',
        'deductibleLimit',
        'guarantee',
        'claimOutstanding'
    }

    true_values = {"true", "vrai", "1", "yes", "oui"}
    false_values = {"false", "faux", "0", "no", "non"}

    for col in df.columns:
        if col in excluded_columns:
            continue  # on saute les colonnes exclues

        unique_values = set(df[col].dropna().astype(str).str.lower().unique())

        if unique_values.issubset(true_values.union(false_values)):
            df[col] = df[col].astype(str).str.lower().map(
                lambda x: True if x in true_values else False
            )
            df[col] = df[col].astype(bool)

    return df

def detect_clean_percentage_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Détecte les colonnes de pourcentage (avec '%')
    et les convertit en float entre 0 et 1.

    Args:
        df (pd.DataFrame): DataFrame à transformer

    Returns:
        pd.DataFrame: DataFrame modifié
    """
    for col in df.columns:
        # Colonnes avec le symbole %
        contains_percent = df[col].dropna().astype(str).str.contains('%').any()

        if contains_percent:
            try:
                df[col] = df[col].astype(str).str.replace(',', '.').astype(float) / 100
            except Exception:
                df[col] = df[col].apply(convert_percent_to_float)

    return df

def convert_percent_to_float(val) -> float:
    """
    Convertit une chaîne de type '80%' en float 0.8.
    - Si vide ou NaN, renvoie np.nan
    - Si la valeur est déjà numérique, elle est renvoyée telle quelle.

    Args:
        val (str): valeur de la colonne à modifier

    Returns:
        float: valeur modifiée
    """
    if pd.isna(val):
        return np.nan
    try:
        if isinstance(val, str) and '%' in val:
            return float(val.strip().replace('%', '')) / 100
        else:
            return float(val)
    except Exception:
        return np.nan 

def detect_missing_values(name: str, df: pd.DataFrame) -> pd.DataFrame:
    """Identifier les valeurs manquantes et les stocker dans un DataFrame d'anomalies. 

    Args:
        name (str): nom du DataFrame (clé du dataset)
        df (pd.DataFrame): DataFrame

    Returns:
        pd.DataFrame: DataFrame de valeurs manquantes 
    """
    anomalies = []
    for col in df.columns:
        null_mask = df[col].isna()
        for idx in df[null_mask].index:
            anomalies.append({
                'table': name,
                'column': col,
                'index': idx,
                'type': 'missing_value'
            })

    return pd.DataFrame(anomalies)

def convert_object_to_string(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convertit toutes les colonnes de type 'object' d'un DataFrame en type 'string' (pd.StringDtype).

    Cette conversion est utile pour uniformiser les types de données, 
    notamment avant des opérations de nettoyage ou d'analyse textuelle.

    Args:
        df (pd.DataFrame): DataFrame à traiter

    Returns:
        pd.DataFrame: DataFrame avec les colonnes 'object' converties en 'string'
    """
    df = df.copy()
    object_cols = df.select_dtypes(include=['object']).columns
    df[object_cols] = df[object_cols].astype('string')
    return df

def anomalies_detail(df_anomalies: pd.DataFrame, dataset: dict) -> pd.DataFrame:
    """
    Enrichit un DataFrame d'anomalies en ajoutant la valeur originale présente 
    dans les DataFrames sources référencés dans `dataset`.

    Corrige aussi les anomalies de type 'incorrect_format_date' lorsque la valeur originale est vide,
    en les reclassant comme 'missing_value'.

    Args:
        df_anomalies (pd.DataFrame): DataFrame contenant les anomalies détectées.
        dataset (dict): Dictionnaire contenant les DataFrames sources accessibles par leur nom.

    Returns:
        pd.DataFrame: DataFrame enrichi avec une colonne 'original_value' et correction du type.
    """
    anomalies_detail = []

    for _, row in df_anomalies.iterrows():
        table_name = row['table']
        col = row['column']
        idx = row['index']

        original_value = None
        if table_name in dataset and col in dataset[table_name].columns:
            try:
                original_value = dataset[table_name].loc[idx, col]
            except KeyError:
                pass  # index introuvable dans le dataframe source

        anomaly_type = row.get('type', None)

        # Correction du type si la valeur est vide ou nulle
        if anomaly_type == 'incorrect_format_date' and (pd.isna(original_value) or str(original_value).strip() == ''):
            anomaly_type = 'missing_value'

        anomalies_detail.append({
            'table': table_name,
            'column': col,
            'index': idx,
            'original_value': original_value,
            'type': anomaly_type
        })

    return pd.DataFrame(anomalies_detail)

def harmonize_animals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Harmonise les valeurs de la colonne 'animal' en anglais si elle existe.
    Remplace 'chat' par 'cat' et 'chien' par 'dog'.

    Args:
        df (pd.DataFrame): Le DataFrame à traiter.

    Returns:
        pd.DataFrame: Le DataFrame modifié.
    """
    if 'animal' in df.columns:
        df['animal'] = df['animal'].replace({'Chat': 'cat', 'Chien': 'dog'})
    return df

def drop_death_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Supprime les colonnes contenant 'death' dans leur nom (insensible à la casse).

    Args:
        df (pd.DataFrame): DataFrame à nettoyer.

    Returns:
        pd.DataFrame: DataFrame sans les colonnes 'death'.
    """
    cols_to_drop = [col for col in df.columns if 'death' in col.lower()]
    if cols_to_drop:
        print(f"    ->> Suppression des colonnes contenant 'death' : {cols_to_drop}")
    return df.drop(columns=cols_to_drop)

def clean_coverref_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """
    Supprime les espaces en début et fin de chaîne dans la colonne 'coverRef' si elle existe.

    Args:
        df (pd.DataFrame): Le DataFrame à nettoyer.

    Returns:
        pd.DataFrame: Le DataFrame avec 'coverRef' nettoyée.
    """
    if 'coverRef' in df.columns:
        df['coverRef'] = df['coverRef'].astype(str).str.strip()
    return df

def run_pipeline(dataset: dict) -> tuple:
    """
    Exécute le pipeline de nettoyage sur un dictionnaire de DataFrames.

    Étapes :
        - Conversion des colonnes de dates
        - Nettoyage des listes encodées
        - Conversion des pourcentages en float
        - Détection des valeurs manquantes
        - Consolidation des anomalies, enrichies avec les valeurs originales
        - Harmonisation des noms d'animaux
        - suppression des colonnes relative à la police Death
        - Export des DataFrames nettoyés dans 'data/processed/'
        - Export des anomalies nettoyées dans 'outputs/'

    Args:
        dataset (dict): Dictionnaire contenant les DataFrames à nettoyer.

    Returns:
        tuple:
            - dataset_clean (dict): Dictionnaire des DataFrames nettoyés.
            - df_anomalies_detail (pd.DataFrame): Anomalies détectées, enrichies des valeurs originales.
    """
    print("⚙️ Début du pipeline.")
    print(f"    ->> conversion des colonnes dates")
    print(f"    ->> conversion des colonnes pourcentage")
    print(f"    ->> conversion et harmonisation des booléens")
    print(f"    ->> identification des valeurs manquantes")

    dataset_clean = {}
    all_anomalies = []

    for name, df in dataset.items():
        df = df.copy()
        print(f"Nettoyage de la table '{name}'...")

        # Nettoyage des espaces dans coverRef
        if name in ["df_contrats", "df_sinistres", "df_quittances"]:
            df = clean_coverref_whitespace(df)

        # Suppression des colonnes liées à 'death' dans df_contrats
        if name == "df_contrats" or name == "df_quittances" :
            df = drop_death_columns(df)

        # Harmonisation spécifique à df_tarifs
        if name == "df_tarifs":
            df = harmonize_animals(df)

        df, df_anomalies_date = convert_date(name, df)
        df = detect_clean_list_columns(df)
        df = detect_clean_percentage_columns(df)
        df = detect_and_clean_boolean_columns(df)
        df = convert_object_to_string(df)
        df_anomalies_missing = detect_missing_values(name, df)

        dataset_clean[name] = df
        
        # ajouter les anomalies non vides à la liste globale
        for df_anomalies in [df_anomalies_date, df_anomalies_missing]:
            if not df_anomalies.empty:
                all_anomalies.append(df_anomalies)
    
    # concaténer les anomalies
    df_anomalies = (
        pd.concat(all_anomalies, ignore_index=True)
          .drop_duplicates(subset=['table', 'column', 'index'], keep='first')
        if all_anomalies else pd.DataFrame()
    )

    # enrichir avec les valeurs originales
    df_anomalies_detail = anomalies_detail(df_anomalies, dataset)

    # Exporter les DataFrames nettoyés
    if not os.path.exists('../data/processed'):
        os.makedirs('../data/processed')
    
    for name, df in dataset_clean.items():
        output_path = f"../data/processed/{name}.parquet"
        df.to_parquet(output_path, index=False)
        print(f"    ->> Export du DataFrame '{name}' vers '{output_path}'")

    # Exporter le DataFrame des anomalies détaillées
    if not os.path.exists('../outputs'):
        os.makedirs('../outputs')
    
    anomalies_path = '../outputs/anomalies_cleaning.csv'
    df_anomalies_detail.to_csv(anomalies_path, index=False)
    print(f"    ->> Export des anomalies nettoyées vers '{anomalies_path}'")

    print("✅ Pipeline terminé.")
    print(f"📌 {len(df_anomalies_detail)} anomalies détectées lors du nettoyage.")

    return dataset_clean, df_anomalies_detail

