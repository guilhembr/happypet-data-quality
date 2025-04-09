import os
import numpy as np
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt
import re


#=============================== DOUBLONS et IDs ===============================================

def check_bijectivity_between_columns(df: pd.DataFrame, col_a: str, col_b: str, anomaly_label: str = "") -> pd.DataFrame:
    """
    Vérifie la bijectivité entre deux colonnes (col_a <-> col_b) dans un DataFrame.
    Retourne les lignes non bijectives avec un tag 'anomaly_type' et ajoute les montants si disponibles.
    
    Args:
        df: DataFrame à analyser
        col_a: Première colonne (ex: coverId)
        col_b: Deuxième colonne (ex: coverRef)
        anomaly_label: Suffixe pour le type d’anomalie (ex: 'coverId_multiple_coverRef')
    
    Returns:
        DataFrame contenant les paires non bijectives avec colonnes pertinentes et 'anomaly_type'
    """
    print(f"\n🔍 Vérification de la bijectivité entre {col_a} et {col_b}...")

    grouped_a = df.groupby(col_a)[col_b].nunique()
    non_bij_a = grouped_a[grouped_a > 1]
    print(f"Nb de {col_a} associés à plusieurs {col_b} : {len(non_bij_a)}")
    if not non_bij_a.empty:
        print(f"⚠️ La relation {col_a} → {col_b} n'est PAS bijective.")
    else:
        print(f"✅ La relation {col_a} → {col_b} est bijective (1:1).")

    grouped_b = df.groupby(col_b)[col_a].nunique()
    non_bij_b = grouped_b[grouped_b > 1]
    print(f"Nb de {col_b} associés à plusieurs {col_a} : {len(non_bij_b)}")
    if not non_bij_b.empty:
        print(f"⚠️ La relation {col_b} → {col_a} n'est PAS bijective.")
    else:
        print(f"✅ La relation {col_b} → {col_a} est bijective (1:1).")

    if not non_bij_a.empty:
        # Colonnes à conserver
        cols_to_include = [col_a, col_b]
        for montant_col in ['healthPremiumInclTax', 'claimPaid', 'totalClaimPaid']:
            if montant_col in df.columns:
                cols_to_include.append(montant_col)

        anomalies = (
            df[df[col_a].isin(non_bij_a.index)][cols_to_include]
            .drop_duplicates()
            .sort_values(by=[col_a, col_b])
            .copy()
        )
        anomalies['anomaly_type'] = anomaly_label or f"{col_a}_multiple_{col_b}"
        return anomalies

    return pd.DataFrame()

def check_all_duplicates_with_keys(datasets, primary_keys=None, max_rows=10):
    """
    Contrôle les doublons complets et les violations de clés primaires pour chaque DataFrame.

    Parameters:
    ----------
    datasets : dict
        Dictionnaire {nom: DataFrame}.
    primary_keys : dict
        Dictionnaire {nom: liste des colonnes utilisées comme clé primaire}.
    max_rows : int
        Nombre maximum de lignes dupliquées à afficher.

    Returns:
    -------
    dict
        Dictionnaire contenant les doublons détectés (complets et/ou sur clés primaires).
    """
    duplicates_report = {}

    for name, df in datasets.items():
        print(f"🔍 {name} : détection des doublons")

        report = {}

        # 🔁 Doublons complets
        nb_full_dups = df.duplicated().sum()
        print(f"➡️ {nb_full_dups} doublons complets détectés.")

        if nb_full_dups > 0:
            full_dups = df[df.duplicated(keep=False)].sort_values(by=df.columns.tolist())
            display(full_dups.head(max_rows))
            print(f"⚠️ Affichage limité à {max_rows} lignes pour les doublons complets.\n")
            report["full_duplicates"] = full_dups
        else:
            print("✅ Aucun doublon complet détecté.\n")

        # 🔑 Doublons sur clés primaires
        if primary_keys and name in primary_keys:
            key_cols = primary_keys[name]
            nb_pk_dups = df.duplicated(subset=key_cols).sum()
            print(f"➡️ {nb_pk_dups} violations de la clé primaire {key_cols}.")

            if nb_pk_dups > 0:
                pk_dups = df[df.duplicated(subset=key_cols, keep=False)].sort_values(by=key_cols)
                display(pk_dups.head(max_rows))
                print(f"⚠️ Affichage limité à {max_rows} lignes pour les violations de clé primaire.\n")
                report["primary_key_violations"] = pk_dups
            else:
                print("✅ Aucun doublon sur la clé primaire.\n")

        print("------------------------------------------------------------\n")

        if report:
            duplicates_report[name] = report

    return duplicates_report

def check_id_format_anomalies(df: pd.DataFrame, table_name: str, export: bool = False, exclude_cols: list = None) -> pd.DataFrame:
    """
    Détecte les valeurs anormales dans les colonnes contenant 'ID' ou 'Ref' dans leur nom.
    Une valeur est considérée comme anormale si elle est composée uniquement de lettres (a-z, A-Z),
    ce qui suggère une valeur de test ou un placeholder ('chips', 'demo', etc.).

    Params:
    - df : DataFrame à analyser
    - table_name : nom du dataset pour reporting/export
    - export : si True, exporte les anomalies dans un CSV

    Returns:
    - DataFrame des anomalies avec tag 'anomaly_type' et colonne concernée
    """
    if exclude_cols is None:
        exclude_cols = []
    anomalies_list = []

    id_cols = [
        col for col in df.columns 
        if ('ID' in col or 'Ref' in col or 'uuid' in col.lower())
        and col not in exclude_cols    
    ]

    for col in id_cols:
        if df[col].isnull().all():
            continue

        df[col] = df[col].astype(str)
        is_alpha_only = df[col].str.isalpha()
        invalid = df[is_alpha_only].copy()

        n_anomalies = len(invalid)
        
        if n_anomalies > 0:
            print(f"\n🆔 Analyse de la colonne '{col}'")
            print(f"🔍 Nombre de valeurs entièrement alphabétiques : {n_anomalies}")
            print("⚠️ Exemples de valeurs suspectes :")
            print(invalid[col].unique()[:5])

            invalid['anomaly_type'] = f"alpha_only_in_{col}"
            invalid['column_checked'] = col
            anomalies_list.append(invalid)

            if export:
                filename = f"../outputs/anomalies_{table_name}_format_{col}.csv"
                invalid.to_csv(filename, index=False)
                print(f"📁 Anomalies exportées vers {filename}")
        else:
            continue

    if anomalies_list:
        return pd.concat(anomalies_list, ignore_index=True)
    else:
        return pd.DataFrame()

def scan_all_datasets_for_id_anomalies(datasets: dict, export: bool = True) -> pd.DataFrame:
    """
    Scanne tous les jeux de données d'un dictionnaire pour identifier les anomalies dans les colonnes ID/Ref
    en utilisant la fonction check_id_format_anomalies (valeurs entièrement alphabétiques).

    Params:
    - datasets : dictionnaire {nom_table: dataframe}
    - export : si True, exporte les anomalies individuelles et globales

    Returns:
    - DataFrame concaténé des anomalies détectées sur l'ensemble des datasets
    """
    all_anomalies = []

    for name, df in datasets.items():
        print(f"\n🔍 Analyse du dataset : {name}")
        anomalies = check_id_format_anomalies(df, table_name=name, export=False, exclude_cols=['petUuidType'])

        if not anomalies.empty:
            all_anomalies.append(anomalies)

    if all_anomalies:
        anomalies_global = pd.concat(all_anomalies, ignore_index=True)
        if export:
            anomalies_global.to_csv("../outputs/anomalies_ids_format_global.csv", index=False)
            print("\n✅ Anomalies globales exportées vers 'anomalies_ids_format_global.csv'")
        return anomalies_global
    else:
        print("\n🎉 Aucune anomalie détectée dans tous les jeux de données.")
        return pd.DataFrame()
    
#=============================== Contrôle de cohérence ===============================================

def check_quittance_have_matching_contracts(quittances: pd.DataFrame, contrats: pd.DataFrame) -> pd.DataFrame:
    """
    Vérifie que chaque quittance est liée à un contrat existant.
    Renvoie les quittances invalides avec un tag 'anomaly_type' pour reporting.
    Affiche aussi le nombre d'anomalies, le montant total concerné,
    le nombre de contrats distincts et la liste des coverRef concernés.
    """
    merged = quittances.merge(contrats[['coverRef']], on='coverRef', how='left', indicator=True)
    invalid = merged[merged['_merge'] == 'left_only'].copy()

    n_anomalies = len(invalid)
    n_coverRef_uniques = invalid['coverRef'].nunique()
    total_amount = invalid['healthPremiumInclTax'].sum() if 'healthPremiumInclTax' in invalid.columns else None
    coverRef_list = invalid['coverRef'].dropna().unique().tolist()

    print(f"[check_quittance_have_matching_contracts] Nombre de Quittances en Anomalies détectées : {n_anomalies}")
    print(f"[check_quittance_have_matching_contracts] Nombre de coverRef distincts sans contrat : {n_coverRef_uniques}")
    
    if total_amount is not None:
        print(f"[check_quittance_have_matching_contracts] Total Primes des quittances concernées : {total_amount:,.2f} €")
    
    print(f"[check_quittance_have_matching_contracts] coverRef concernés : {coverRef_list}")

    if n_anomalies == 0:
        return pd.DataFrame()

    invalid['anomaly_type'] = "quittance_without_contract"
    return invalid.drop(columns=['_merge'])

def check_sinistres_have_matching_contracts(sinistres: pd.DataFrame, contrats: pd.DataFrame) -> pd.DataFrame:
    """
    Vérifie que chaque sinistre est lié à un contrat existant.
    Renvoie les sinistres invalides avec un tag 'anomaly_type' pour reporting.
    Affiche aussi :
    - le nombre d’anomalies,
    - le montant total d’indemnisation (claimPaid),
    - le nombre de contrats distincts concernés,
    - la liste des coverRef concernés.
    """
    merged = sinistres.merge(contrats[['coverRef']], on='coverRef', how='left', indicator=True)
    invalid = merged[merged['_merge'] == 'left_only'].copy()

    n_anomalies = len(invalid)
    n_coverRef_uniques = invalid['coverRef'].nunique()
    total_indemnisation = invalid['claimPaid'].sum() if 'claimPaid' in invalid.columns else None
    coverRef_list = invalid['coverRef'].dropna().unique().tolist()

    print(f"[check_sinistres_have_matching_contracts] Nombre de sinistres en anomalies détectées : {n_anomalies}")
    print(f"[check_sinistres_have_matching_contracts] Nombre de coverRef distincts sans sinistres : {n_coverRef_uniques}")
    
    if total_indemnisation is not None:
        print(f"[check_sinistres_have_matching_contracts] Total des indemnisations concernées : {total_indemnisation:,.2f} €")
    
    print(f"[check_sinistres_have_matching_contracts] coverRefs concernés : {coverRef_list}")

    if n_anomalies == 0:
        return pd.DataFrame()

    invalid['anomaly_type'] = "sinistre_without_contract"
    return invalid.drop(columns=['_merge'])

def check_contracts_have_quittance(contrats: pd.DataFrame, quittances: pd.DataFrame) -> pd.DataFrame:
    """
    Vérifie que chaque contrat a au moins une quittance associée.
    Renvoie les contrats sans quittance avec un tag 'anomaly_type' pour reporting.
    Affiche aussi :
    - le nombre d’anomalies,
    - le montant total des primes concernées (healthPremiumInclTax),
    - le nombre de contrats distincts concernés,
    - la liste des coverRef concernés.
    """
    merged = contrats.merge(quittances[['coverRef']], on='coverRef', how='left', indicator=True)
    invalid = merged[merged['_merge'] == 'left_only'].copy()

    n_anomalies = len(invalid)
    n_coverRef_uniques = invalid['coverRef'].nunique()
    total_prime = invalid['healthPremiumInclTax'].sum() if 'healthPremiumInclTax' in invalid.columns else None
    coverRef_list = invalid['coverRef'].dropna().unique().tolist()

    print(f"[check_contracts_have_quittance] Nombre de contrats sans quittance détectés : {n_anomalies}")
    print(f"[check_contracts_have_quittance] Nombre de coverRef distincts concernés : {n_coverRef_uniques}")

    if total_prime is not None:
        print(f"[check_contracts_have_quittance] Montant total des primes concernées : {total_prime:,.2f} €")

    print(f"[check_contracts_have_quittance] coverRefs concernés : {coverRef_list}")

    if n_anomalies == 0:
        return pd.DataFrame()

    invalid['anomaly_type'] = "contract_without_quittance"
    return invalid.drop(columns=['_merge'])

def check_sinistres_avant_date_souscription(sinistres: pd.DataFrame, contrats: pd.DataFrame) -> pd.DataFrame:
    """
    Vérifie qu'aucun sinistre n'est antérieur à la date de début de souscription du contrat.
    Renvoie les sinistres invalides avec un tag 'anomaly_type' pour reporting.
    Affiche aussi :
    - le nombre d’anomalies,
    - le montant total d’indemnisation concerné (claimPaid),
    - le nombre de contrats distincts concernés,
    - la liste des coverRef concernés.
    """
    merged = sinistres.merge(contrats[['coverRef', 'coverStartDate']], on='coverRef', how='left')

    if 'incidentDate' not in merged.columns or 'coverStartDate' not in merged.columns:
        print("[check_sinistres_avant_date_souscription] Colonnes manquantes : 'incidentDate' ou 'startDate'")
        return pd.DataFrame()

    # Convertir les dates si ce n'est pas déjà fait
    merged['incidentDate'] = pd.to_datetime(merged['incidentDate'], errors='coerce')
    merged['coverStartDate'] = pd.to_datetime(merged['coverStartDate'], errors='coerce')

    # Détection des anomalies
    invalid = merged[merged['incidentDate'] < merged['coverStartDate']].copy()

    n_anomalies = len(invalid)
    n_coverRef_uniques = invalid['coverRef'].nunique()
    total_indemnisation = invalid['claimPaid'].sum() if 'claimPaid' in invalid.columns else None
    coverRef_list = invalid['coverRef'].dropna().unique().tolist()

    print(f"[check_sinistres_avant_date_souscription] Nombre de sinistres avant souscription détectés : {n_anomalies}")
    print(f"[check_sinistres_avant_date_souscription] Nombre de coverRef distincts concernés : {n_coverRef_uniques}")

    if total_indemnisation is not None:
        print(f"[check_sinistres_avant_date_souscription] Montant total des indemnisations concernées : {total_indemnisation:,.2f} €")

    print(f"[check_sinistres_avant_date_souscription] coverRefs concernés : {coverRef_list}")

    if n_anomalies == 0:
        return pd.DataFrame()

    invalid['anomaly_type'] = "sinistre_before_contract_start"
    return invalid

def check_sinistres_apres_date_fin_contrat(sinistres: pd.DataFrame, contrats: pd.DataFrame) -> pd.DataFrame:
    """
    Vérifie qu’aucun sinistre ne survient après la date de fin de contrat.
    Renvoie les sinistres invalides avec un tag 'anomaly_type'.
    Affiche :
    - le nombre d’anomalies,
    - le total des indemnisations concernées (claimPaid),
    - le nombre de contrats distincts,
    - les coverRef concernés.
    """
    merged = sinistres.merge(contrats[['coverRef', 'coverEndDate']], on='coverRef', how='left')

    # Sécurisation des dates
    merged['incidentDate'] = pd.to_datetime(merged['incidentDate'], errors='coerce')
    merged['coverEndDate'] = pd.to_datetime(merged['coverEndDate'], errors='coerce')

    invalid = merged[merged['incidentDate'] > merged['coverEndDate']].copy()

    n_anomalies = len(invalid)
    n_coverRef = invalid['coverRef'].nunique()
    total_claims = invalid['claimPaid'].sum() if 'claimPaid' in invalid.columns else None
    cover_refs = invalid['coverRef'].dropna().unique().tolist()

    print(f"[check_sinistres_apres_date_fin_contrat] Nombre de sinistres après fin de contrat : {n_anomalies}")
    print(f"[check_sinistres_apres_date_fin_contrat] Nombre de coverRef concernés : {n_coverRef}")
    
    if total_claims is not None:
        print(f"[check_sinistres_apres_date_fin_contrat] Montant total des indemnisations : {total_claims:,.2f} €")

    print(f"[check_sinistres_apres_date_fin_contrat] coverRefs concernés : {cover_refs}")

    if n_anomalies == 0:
        return pd.DataFrame()

    invalid['anomaly_type'] = "sinistre_after_contract_end"
    return invalid




def run_all_consistency_checks(df_contrats, df_quittances, df_sinistres, output_path="../outputs"):
    """
    Exécute tous les contrôles de cohérence et exporte les anomalies détectées dans des fichiers CSV.
    """
    import os
    os.makedirs(output_path, exist_ok=True)

    anomalies_to_export = []

    print("\n========== 📄 LANCEMENT DES CONTRÔLES DE COHÉRENCE ==========\n")

    print("🔹 1. Contrats sans quittance")
    df_anomalies_contrats = check_contracts_have_quittance(df_contrats, df_quittances)
    anomalies_to_export.append(("anomalies_contrats_sans_quittance.csv", df_anomalies_contrats))
    print("------------------------------------------------------------\n")

    print("🔹 2. Quittances sans contrat")
    df_anomalies_quittances = check_quittance_have_matching_contracts(df_quittances, df_contrats)
    anomalies_to_export.append(("anomalies_quittances_sans_contrat.csv", df_anomalies_quittances))
    print("------------------------------------------------------------\n")

    print("🔹 3. Sinistres sans contrat (remboursements fantômes)")
    df_anomalies_sinistres = check_sinistres_have_matching_contracts(df_sinistres, df_contrats)
    anomalies_to_export.append(("anomalies_sinistres_sans_contrat.csv", df_anomalies_sinistres))
    print("------------------------------------------------------------\n")

    print("🔹 4. Sinistres avant date de souscription")
    df_anomalies_sinistres_pre = check_sinistres_avant_date_souscription(df_sinistres, df_contrats)
    anomalies_to_export.append(("anomalies_sinistres_avant_souscription.csv", df_anomalies_sinistres_pre))
    print("------------------------------------------------------------\n")

    print("🔹 5. Sinistres après fin de couverture")
    df_anomalies_sinistres_post = check_sinistres_apres_date_fin_contrat(df_sinistres, df_contrats)
    anomalies_to_export.append(("anomalies_sinistres_apres_souscription.csv", df_anomalies_sinistres_post))
    print("------------------------------------------------------------\n")

    print("📤 Export des anomalies détectées :\n")

    for filename, df in anomalies_to_export:
        if not df.empty:
            full_path = os.path.join(output_path, filename)
            df.to_csv(full_path, index=False)
            print(f"🟠 Exporté : {full_path}")
        else:
            print(f"🟢 Aucun problème détecté → {filename}")

    print("\n========== ✅ CONTRÔLES COHERENCE TERMINÉS ==========\n")

#=============================== Règles métiers  ===============================================

def check_contract_duration(df_contrats: pd.DataFrame, anomalies_path: str = "../outputs/anomalies_cleaning.csv") -> pd.DataFrame:
    """
    Vérifie que la durée du contrat est bien d’un an : coverEndDate = coverStartDate + 1 an.
    Ignore les lignes ayant déjà été identifiées comme anomalies sur les colonnes coverStartDate ou coverEndDate.
    Renvoie les contrats invalides avec un tag 'anomaly_type'.

    Affiche :
    - le nombre d’anomalies,
    - le nombre de coverRef distincts concernés,
    - les coverRefs concernés.
    """
    # Charger les anomalies déjà identifiées
    anomalies = pd.read_csv(anomalies_path)

    # Filtrer les anomalies de type date pour df_contrats
    anomalies_dates = anomalies[
        (anomalies['table'] == 'df_contrats') &
        (anomalies['column'].isin(['coverStartDate', 'coverEndDate']))
    ]
    
    # Récupérer les index à exclure
    indices_exclus = anomalies_dates['index'].dropna().astype(int).unique()
    
    # Exclure les lignes avec valeurs manquantes ou corrompues
    df_filtered = df_contrats.drop(index=indices_exclus, errors='ignore').copy()

    # Sécurisation des dates
    df_filtered['coverStartDate'] = pd.to_datetime(df_filtered['coverStartDate'], errors='coerce')
    df_filtered['coverEndDate'] = pd.to_datetime(df_filtered['coverEndDate'], errors='coerce')

    # Contrôle : coverEndDate = coverStartDate + 1 an
    erreurs = df_filtered[df_filtered['coverEndDate'] != df_filtered['coverStartDate'] + pd.DateOffset(years=1)].copy()

    n_anomalies = len(erreurs)
    n_coverRef = erreurs['coverRef'].nunique()
    cover_refs = erreurs['coverRef'].dropna().unique().tolist()

    print(f"[check_contract_duration] Nombre de contrats avec durée ≠ 1 an : {n_anomalies}")
    print(f"[check_contract_duration] Nombre de coverRef concernés : {n_coverRef}")
    print(f"[check_contract_duration] coverRefs concernés : {cover_refs}")

    if n_anomalies == 0:
        return pd.DataFrame()

    erreurs['anomaly_type'] = "contract_duration_not_1_year"
    return erreurs

def check_eligibilite_animaux(df_contrats: pd.DataFrame):
    """
    Vérifie les conditions d’éligibilité des animaux à l’assurance :
    - Âge au jour du contrôle : > 3 mois et < 9 ans
    - Présence du numéro de puce ou tatouage
    - Format valide de l’identifiant selon le type (chip ou tatoo) et l’espèce (chat ou chien)

    Retourne :
    - un DataFrame des lignes non conformes avec 'anomaly_type' et 'eligibility_reason'
    - un DataFrame résumé du nombre d’anomalies par cause
    """

    df = df_contrats.copy()

    # Nettoyage du champ UUID
    df['petUuid'] = df['petUuid'].astype(str).str.upper().str.replace(" ", "", regex=False).str.strip()

    # Dates au bon format
    df['petBirthday'] = pd.to_datetime(df['petBirthday'], errors='coerce')
    today = pd.Timestamp.now()

    # Calcul de l'âge actuel
    df['age_today'] = ((today - df['petBirthday']).dt.days / 365.25)

    # Détection des règles invalides
    df['age_invalid'] = (df['age_today'] < 0.25) | (df['age_today'] > 9)
    df['uuid_missing'] = df['petUuidType'].isna() | df['petUuidType'].eq("")

    # Format UUID
    def check_chip_format(uuid: str) -> bool:
        return isinstance(uuid, str) and len(uuid) == 15 and bool(re.match(r"^[A-Z0-9]{3}[A-Z0-9]{2}[A-Z0-9]{2}[A-Z0-9]{8}$", uuid))

    def check_tatoo_format(uuid: str, species: str) -> bool:
        if not isinstance(uuid, str):
            return False
        species = species.lower().strip()
        if species in ["cat", "chat"]:
            return bool(re.match(r"^\d{3}[A-Z]{3}$", uuid) or re.match(r"^[A-Z]{3}\d{3}$", uuid))
        elif species in ["dog", "chien"]:
            return bool(re.match(r"^\d{3}[A-Z]{3}$", uuid) or re.match(r"^2[A-Z]{3}\d{3}$", uuid))
        return False

    df['uuid_format_invalid'] = False
    chip_mask = df['petUuidType'] == 'chip'
    tatoo_mask = df['petUuidType'] == 'tatoo'

    df.loc[chip_mask, 'uuid_format_invalid'] = ~df.loc[chip_mask, 'petUuid'].apply(check_chip_format)
    df.loc[tatoo_mask, 'uuid_format_invalid'] = ~df.loc[tatoo_mask].apply(
        lambda row: check_tatoo_format(row['petUuid'], row['petType']), axis=1)

    # Création de la colonne eligibility_reason
    def build_reason(row):
        reasons = []
        if row['age_invalid']:
            reasons.append("age_out_of_bounds")
        if row['uuid_missing']:
            reasons.append("missing_uuid_type")
        if row['uuid_format_invalid']:
            reasons.append("invalid_uuid_format")
        return ", ".join(reasons) if reasons else None

    df['eligibility_reason'] = df.apply(build_reason, axis=1)

    # Sélection des lignes non éligibles
    invalid = df[df['eligibility_reason'].notna()].copy()
    invalid['anomaly_type'] = "animal_not_eligible"

    # Résumé pour affichage
    summary = (
        invalid['eligibility_reason']
        .str.get_dummies(sep=", ")
        .sum()
        .reset_index()
        .rename(columns={"index": "cause", 0: "nombre_d_animaux"})
        .sort_values(by="nombre_d_animaux", ascending=False)
        .reset_index(drop=True)
    )

    n_anomalies = len(invalid)
    n_customers = invalid['customerId'].nunique()
    coverRefs = invalid['coverRef'].dropna().unique().tolist()

    print(f"[check_eligibilite_animaux] Nombre d’animaux non éligibles : {n_anomalies}")
    print(f"[check_eligibilite_animaux] Nombre de customerId distincts concernés : {n_customers}")
    print(f"[check_eligibilite_animaux] coverRefs concernés : {coverRefs}")

    return invalid, summary

def check_tarif_prevention(df_contrats: pd.DataFrame) -> pd.DataFrame:
    """
    Vérifie l'application du tarif Prévention dans les contrats :
    - 99.96 € si preventionLimit == 100
    - 50.05 € si preventionLimit == 50

    Retourne les lignes non conformes avec un champ 'anomaly_type'.
    """

    df = df_contrats.copy()

    # Filtrage : ne garder que les lignes où une garantie prévention est présente
    df = df[df['preventionLimit'].isin([50, 100])].copy()

    # Calcul du tarif attendu
    df['expected_preventionHthc'] = df['preventionLimit'].map({100: 99.96, 50: 50.05})

    # Comparaison avec le tarif observé (avec tolérance flottante de 1 centime)
    df['delta_tarif_prevention'] = df['preventionHthc'] - df['expected_preventionHthc']
    anomalies = df[~np.isclose(df['preventionHthc'], df['expected_preventionHthc'], atol=0.01)].copy()

    anomalies['anomaly_type'] = 'incorrect_prevention_tarif'

    n_anomalies = len(anomalies)
    n_customers = anomalies['customerId'].nunique()
    coverRefs = anomalies['coverRef'].dropna().unique().tolist()

    print(f"[check_tarif_prevention] Nombre d’anomalies tarifaires Prévention : {n_anomalies}")
    print(f"[check_tarif_prevention] Nombre de customerId concernés : {n_customers}")
    print(f"[check_tarif_prevention] coverRefs concernés : {coverRefs}")

    if n_anomalies == 0:
        return pd.DataFrame()

    return anomalies

def check_tarif_health(df_contrats: pd.DataFrame, df_tarifs: pd.DataFrame) -> pd.DataFrame:
    """
    Vérifie la correcte application du tarif Health (taux, plafond, tarif mensuel),
    incluant la réduction multi-contrats.
    Retourne les lignes non conformes.
    """

    # Vérification des colonnes essentielles pour identifier le bon tarif
    required_columns = {'CoverRef', 'CoverId', 'PetRace'}
    missing_columns = required_columns - set(df_tarifs.columns)

    if missing_columns:
        print(f"[check_tarif_health] Contrôle non réalisable : colonnes manquantes dans df_tarifs -> {missing_columns}")
        return pd.DataFrame()  # Pas de contrôle possible

    df_matched = match_tarif_health(df_contrats, df_tarifs)
    df_discounted = apply_multi_contrat_discount(df_matched)

    # Comparaison
    df_discounted['delta_tarif'] = df_discounted['healthHthc'] - df_discounted['expected_healthHthc_discounted']
    anomalies = df_discounted[~np.isclose(df_discounted['healthHthc'], df_discounted['expected_healthHthc_discounted'], atol=0.01)].copy()

    n_anomalies = len(anomalies)
    n_customers = anomalies['customerId'].nunique()
    cover_refs = anomalies['coverRef'].dropna().unique().tolist()

    print(f"[check_tarif_health] Nombre d’anomalies tarifaires Health : {n_anomalies}")
    print(f"[check_tarif_health] Nombre de customerId concernés : {n_customers}")
    print(f"[check_tarif_health] coverRefs concernés : {cover_refs}")

    if n_anomalies == 0:
        return pd.DataFrame()

    anomalies['anomaly_type'] = 'incorrect_health_tarif_or_discount'
    return anomalies

def check_arithmetic_consistency_quittances(df_quittances, tol=1.0):
    """
    Vérifie que healthPremiumInclTax = healthTax + healthBrokerFee + healthHthc
    et retourne les lignes où ce n'est pas le cas (écart ≥ tolérance).

    Args:
        df_quittances (pd.DataFrame): table des quittances
        tol (float): tolérance absolue sur l'écart autorisé, par défaut 1.0€ (non significatif en dessous)

    Returns:
        pd.DataFrame: lignes non conformes avec colonnes coverRef, receiptId, 
                      healthPremiumInclTax, healthTax, healthBrokerFee, healthHthc, 
                      expected_premium, diff
    """
    df = df_quittances.copy()

    # Remplacement des NaN par 0 pour éviter les erreurs de calcul
    df['healthTax'] = df['healthTax'].fillna(0)
    df['healthBrokerFee'] = df['healthBrokerFee'].fillna(0)
    df['healthHthc'] = df['healthHthc'].fillna(0)
    df['healthPremiumInclTax'] = df['healthPremiumInclTax'].fillna(0)

    # Calcul de la prime attendue
    df['expected_premium'] = df['healthTax'] + df['healthBrokerFee'] + df['healthHthc']

    # Calcul de la différence absolue
    df['diff'] = (df['healthPremiumInclTax'] - df['expected_premium']).abs()

    # Sélection des lignes avec écart significatif
    df_invalid = df[df['diff'] >= tol].copy()

    return df_invalid[['coverRef', 'receiptId', 'healthPremiumInclTax', 'healthTax', 
                       'healthBrokerFee', 'healthHthc', 'expected_premium', 'diff']]

def check_arithmetic_consistency_contrats(df_contrats, tol=1.0):
    """
    Vérifie que healthPremiumInclTax = healthBrokerFee + healthHthc + healthTax
    pour les contrats, et retourne les lignes où ce n'est pas respecté.

    Args:
        df_contrats (pd.DataFrame): table des contrats
        tol (float): tolérance absolue sur l'écart autorisé (par défaut 1.0€)

    Returns:
        pd.DataFrame: lignes non conformes avec les colonnes coverRef, 
                      healthPremiumInclTax, healthBrokerFee, healthHthc, healthTax, 
                      expected_premium, diff
    """
    df = df_contrats.copy()

    # Gestion des valeurs manquantes
    df['healthTax'] = df['healthTax'].fillna(0)
    df['healthBrokerFee'] = df['healthBrokerFee'].fillna(0)
    df['healthHthc'] = df['healthHthc'].fillna(0)
    df['healthPremiumInclTax'] = df['healthPremiumInclTax'].fillna(0)

    # Calcul de la prime attendue
    df['expected_premium'] = df['healthTax'] + df['healthBrokerFee'] + df['healthHthc']

    # Calcul de l’écart absolu
    df['diff'] = (df['healthPremiumInclTax'] - df['expected_premium']).abs()

    # Sélection des lignes où l’écart est significatif
    df_invalid = df[df['diff'] >= tol].copy()

    return df_invalid[['coverRef', 'healthPremiumInclTax', 'healthBrokerFee', 
                       'healthHthc', 'healthTax', 'expected_premium', 'diff']]

def check_reimbursement_limits_with_contracts(df_sinistres, df_contrats, tol=1.0):
    """
    Vérifie que les remboursements santé et prévention ne dépassent pas les plafonds annuels
    définis dans la table contrats.

    Args:
        df_sinistres (pd.DataFrame): sinistres avec claimPaid, actCategory, actDate, coverRef
        df_contrats (pd.DataFrame): contrats avec coverRef, healthLimit, preventionLimit
        tol (float): tolérance en euros (par défaut 1.0€)

    Returns:
        pd.DataFrame: lignes où les remboursements dépassent les plafonds
    """
    df_sin = df_sinistres.copy()
    df_contrats = df_contrats.copy()

    # Nettoyage / sécurité
    df_sin['claimPaid'] = df_sin['claimPaid'].fillna(0)
    df_sin['actCategory'] = df_sin['actCategory'].fillna('unknown')
    df_sin['year'] = pd.to_datetime(df_sin['actDate'], errors='coerce').dt.year

    # Agrégation des remboursements par coverRef / année / type
    agg = df_sin.groupby(['coverRef', 'year', 'actCategory'], as_index=False).agg({
        'claimPaid': 'sum'
    })

    # Jointure avec les plafonds issus des contrats
    df_joined = agg.merge(df_contrats[['coverRef', 'healthLimit', 'preventionLimit']], on='coverRef', how='left')

    # Calcul du plafond en fonction du type de garantie
    df_joined['limit'] = df_joined.apply(
        lambda row: row['healthLimit'] if row['actCategory'] in ('MALADIE', 'ACCIDENT', 'ACCIDENTO')
        else row['preventionLimit'] if row['actCategory'] == 'PREVENTION'
        else 0,
        axis=1
    )

    # Calcul du dépassement
    df_joined['overLimit'] = df_joined['claimPaid'] - df_joined['limit']

    # Dépassements significatifs uniquement
    df_exceeded = df_joined[df_joined['overLimit'] > tol].copy()

    # Renommage final
    df_exceeded = df_exceeded.rename(columns={'claimPaid': 'totalClaimPaid'})[[
        'coverRef', 'year', 'actCategory', 'totalClaimPaid', 'limit', 'overLimit'
    ]]

    return df_exceeded

def check_taux_remboursement(df_sinistres: pd.DataFrame, df_contrats: pd.DataFrame) -> pd.DataFrame:
    """
    Vérifie que le taux de remboursement (coverRate) est correctement appliqué
    pour les soins de type MALADIE, ACCIDENT, ACCIDENTO.

    Condition vérifiée :
        claimPaid ≈ actValue * coverRate (tolérance de 1 centime)

    La jointure se fait sur coverRef.
    Retourne un DataFrame des anomalies avec le détail du delta.
    """

    df_sinistres = df_sinistres.copy()
    df_contrats = df_contrats[['coverRef', 'coverRate']].copy()

    # Filtrer les sinistres concernés
    df_sinistres = df_sinistres[df_sinistres['actCategory'].isin(['MALADIE', 'ACCIDENT', 'ACCIDENTO'])]

    # Jointure pour récupérer le coverRate
    merged = df_sinistres.merge(df_contrats, on='coverRef', how='left')

    # Calcul du montant attendu
    merged['expected_claimPaid'] = merged['actValue'] * merged['coverRate']
    merged['delta'] = merged['claimPaid'] - merged['expected_claimPaid']

    # Détection des anomalies
    anomalies = merged[~np.isclose(merged['claimPaid'], merged['expected_claimPaid'], atol=0.01)].copy()
    anomalies['anomaly_type'] = 'incorrect_reimbursement_rate'

    print(f"[check_taux_remboursement] Nombre d’anomalies de remboursement : {len(anomalies)}")
    print(f"[check_taux_remboursement] Nombre de coverRefs concernés : {anomalies['coverRef'].nunique()}")

    return anomalies[['coverRef', 'actCategory', 'actValue', 'coverRate', 'claimPaid', 'expected_claimPaid', 'delta', 'anomaly_type']]

def check_delai_carence(df_sinistres: pd.DataFrame, df_contrats: pd.DataFrame) -> pd.DataFrame:
    """
    Vérifie la correcte application des délais de carence :
    - ACCIDENT / ACCIDENTO : 2 jours
    - MALADIE + actType = HOSP : 120 jours
    - MALADIE + autre type ou PREVENTION : 45 jours

    Compare la date de l'acte (incidentDate) avec la date de souscription + carence.

    Retourne un DataFrame des anomalies avec le nombre de jours de décalage constaté.
    """

    df_sinistres = df_sinistres.copy()
    df_contrats = df_contrats[['coverRef', 'coverStartDate']].copy()

    # Convertir les dates
    df_sinistres['incidentDate'] = pd.to_datetime(df_sinistres['incidentDate'], errors='coerce')
    df_contrats['coverStartDate'] = pd.to_datetime(df_contrats['coverStartDate'], errors='coerce')

    # Jointure
    merged = df_sinistres.merge(df_contrats, on='coverRef', how='left')

    # Attribution du délai de carence
    def get_carence(row):
        if row['actCategory'] in ['ACCIDENT', 'ACCIDENTO']:
            return 2
        elif row['actCategory'] == 'MALADIE' and row.get('actType') == 'HOSP':
            return 120
        elif row['actCategory'] in ['MALADIE']:
            return 45
        else:
            return 0  # Par défaut, on ignore le reste

    merged['carence_days'] = merged.apply(get_carence, axis=1)

    # Date de début de couverture effective
    merged['carence_end_date'] = merged['coverStartDate'] + pd.to_timedelta(merged['carence_days'], unit='D')

    # Anomalies : sinistres trop précoces
    anomalies = merged[merged['incidentDate'] < merged['carence_end_date']].copy()
    anomalies['days_before_eligibility'] = (merged['carence_end_date'] - merged['incidentDate']).dt.days
    anomalies['anomaly_type'] = 'reimbursement_before_carence'

    print(f"[check_delai_carence] Nombre d’anomalies de carence détectées : {len(anomalies)}")
    print(f"[check_delai_carence] Nombre de coverRefs concernés : {anomalies['coverRef'].nunique()}")

    return anomalies[[
        'coverRef', 'actCategory', 'actType', 'incidentDate',
        'coverStartDate', 'carence_days', 'carence_end_date',
        'days_before_eligibility', 'claimPaid',  # ← ajouté ici
        'anomaly_type'
    ]]

def check_negative_values(df: pd.DataFrame, df_name: str = "dataset") -> pd.DataFrame:
    """
    Vérifie l'absence de valeurs négatives dans toutes les colonnes numériques.
    Affiche un résumé clair et retourne un DataFrame des anomalies détectées.
    
    Args:
        df (pd.DataFrame): Le DataFrame à contrôler
        df_name (str): Nom du dataset pour affichage
    
    Returns:
        pd.DataFrame: Tableau des lignes avec valeurs négatives (colonnes + valeurs)
    """
    numeric_cols = df.select_dtypes(include=["number"]).columns
    anomalies = []

    print(f"\n🔎 Contrôle des valeurs négatives dans '{df_name}'")

    for col in numeric_cols:
        negatives = df[df[col] < 0]
        n_neg = len(negatives)

        if n_neg > 0:
            print(f"⚠️  {col} contient {n_neg} valeur(s) négative(s)")
            sample_values = negatives[col].head(5).tolist()
            print(f"    Exemples : {sample_values}")
            
            tmp = negatives.copy()
            tmp["anomaly_column"] = col
            tmp["anomaly_value"] = tmp[col]
            anomalies.append(tmp)

    if not anomalies:
        print(f"✅ Aucune valeur négative trouvée dans '{df_name}'")
        return pd.DataFrame()

    df_anomalies = pd.concat(anomalies, ignore_index=True)
    df_anomalies["anomaly_type"] = "negative_value"
    return df_anomalies

def check_negative_values_on_all_datasets(datasets: dict) -> pd.DataFrame:
    """
    Applique la détection de valeurs négatives sur un ensemble de DataFrames.
    
    Args:
        datasets (dict): Un dictionnaire de type {'nom_dataset': df}
        
    Returns:
        pd.DataFrame: Toutes les anomalies détectées, concaténées avec leur source.
    """
    all_anomalies = []

    for name, df in datasets.items():
        df_anomalies = check_negative_values(df, df_name=name)

        if not df_anomalies.empty:
            df_anomalies["dataset_source"] = name
            all_anomalies.append(df_anomalies)

    if all_anomalies:
        df_all = pd.concat(all_anomalies, ignore_index=True)
        print(f"\n📦 Total anomalies négatives détectées : {len(df_all)} lignes")
        return df_all

    print("\n✅ Aucun problème de valeur négative détecté dans l’ensemble des datasets.")
    return pd.DataFrame()



def run_quality_pipeline_part1(df_contrats, df_quittances, df_tarifs, anomalies_path="../outputs/anomalies_cleaning.csv"):
    """Première partie du pipeline : contrats, prévention, health, arithmétique."""
    results = {}
    anomalies_to_export = []

    print("\n========== 📋 LANCEMENT DU PIPELINE QUALITÉ CONTRATS — PARTIE 1 ==========")

    print("\n🔹 1. Durée des contrats — Vérifie que chaque contrat dure exactement 1 an")
    results["contract_duration"] = check_contract_duration(df_contrats, anomalies_path)
    anomalies_to_export.append(("anomalies_contract_duration.csv", results["contract_duration"]))

    print("\n🔹 2. Éligibilité des animaux — Vérifie l'âge, la présence et le format du numéro d'identification")
    anomalies_elig, summary_elig = check_eligibilite_animaux(df_contrats)
    results["eligibilite_animaux"] = anomalies_elig
    results["summary_eligibilite"] = summary_elig
    anomalies_to_export.append(("anomalies_eligibilite_animaux.csv", anomalies_elig))

    print("\n🔹 3. Tarif Prévention — Vérifie que le tarif est conforme au plafond choisi (50 ou 100€)")
    results["tarif_prevention"] = check_tarif_prevention(df_contrats)
    if results["tarif_prevention"].empty:
        print("[check_tarif_prevention] ✅ Aucun écart détecté")
    anomalies_to_export.append(("anomalies_tarif_prevention.csv", results["tarif_prevention"]))

    print("\n🔹 4. Tarif Health — Vérifie le tarif standard santé en fonction des caractéristiques de l’animal et la réduction multi-contrats")
    results["tarif_health"] = check_tarif_health(df_contrats, df_tarifs)
    if results["tarif_health"].empty and {'CoverRef', 'CoverId', 'PetRace'} - set(df_tarifs.columns):
        print("[check_tarif_health] ⚠️ Contrôle non réalisable : colonnes manquantes dans df_tarifs")
    anomalies_to_export.append(("anomalies_tarif_health.csv", results["tarif_health"]))

    print("\n🔹 5. Arithmétique contrats — Vérifie que la somme des composants = prime annuelle TTC")
    results["arithmetique_contrats"] = check_arithmetic_consistency_contrats(df_contrats)
    print(f"[check_arithmetic_consistency_contrats] Nombre d’anomalies détectées : {len(results['arithmetique_contrats'])}")
    anomalies_to_export.append(("anomalies_arithmetique_contrats.csv", results["arithmetique_contrats"]))

    print("\n🔹 6. Arithmétique quittances — Vérifie que la somme des composants = montant de la quittance")
    results["arithmetique_quittances"] = check_arithmetic_consistency_quittances(df_quittances)
    print(f"[check_arithmetic_consistency_quittances] Nombre d’anomalies détectées : {len(results['arithmetique_quittances'])}")
    anomalies_to_export.append(("anomalies_arithmetique_quittances.csv", results["arithmetique_quittances"]))

    return results, anomalies_to_export

def run_quality_pipeline_part2(df_contrats, df_quittances, df_sinistres, results, anomalies_to_export):
    """Deuxième partie du pipeline : plafonds, remboursements, carence, valeurs négatives."""
    print("\n========== 📋 SUITE DU PIPELINE QUALITÉ CONTRATS — PARTIE 2 ==========")

    print("\n🔹 7. Dépassement des plafonds — Vérifie que les remboursements ne dépassent pas les limites prévues au contrat")
    results["remboursement_vs_plafond"] = check_reimbursement_limits_with_contracts(df_sinistres, df_contrats)
    if results["remboursement_vs_plafond"].empty:
        print("[check_reimbursement_limits_with_contracts] ✅ Aucun dépassement détecté")
    else:
        print(f"[check_reimbursement_limits_with_contracts] ⚠️ {len(results['remboursement_vs_plafond'])} dépassement(s) détecté(s)")
    anomalies_to_export.append(("anomalies_plafond_remboursement.csv", results["remboursement_vs_plafond"]))

    print("\n🔹 8. Taux de remboursement — Vérifie que claimPaid = actValue * coverRate pour certaines catégories d’actes")
    results["taux_remboursement"] = check_taux_remboursement(df_sinistres, df_contrats)
    anomalies_to_export.append(("anomalies_taux_remboursement.csv", results["taux_remboursement"]))

    print("\n🔹 9. Délai de carence — Vérifie que les remboursements ont lieu après les délais de carence contractuels")
    results["delai_carence"] = check_delai_carence(df_sinistres, df_contrats)
    anomalies_to_export.append(("anomalies_delai_carence.csv", results["delai_carence"]))

    print("\n🔹 10. Valeurs négatives — Vérifie qu’aucune valeur numérique n’est strictement négative dans les jeux de données")
    datasets = {
        "df_contrats": df_contrats,
        "df_quittances": df_quittances,
        "df_sinistres": df_sinistres
    }
    results["valeurs_negatives"] = check_negative_values_on_all_datasets(datasets)
    anomalies_to_export.append(("anomalies_valeurs_negatives.csv", results["valeurs_negatives"]))

    return results, anomalies_to_export

def export_pipeline_anomalies(anomalies_to_export, output_path="../outputs"):
    """Troisième partie : export des anomalies dans les fichiers CSV."""
    import os
    os.makedirs(output_path, exist_ok=True)

    print("\n📤 Export des anomalies détectées :")
    for filename, df in anomalies_to_export:
        if df is not None and not df.empty:
            full_path = os.path.join(output_path, filename)
            df.to_csv(full_path, index=False)
            print(f"🟠 Exporté : {full_path}")
        else:
            print(f"🟢 Aucun problème détecté → {filename}")

    print("\n========== ✅ EXPORT TERMINÉ ==========")

#=============================== Synthèse ===============================================

def export_all_anomalies_to_excel(output_dir: str, output_excel_path: str) -> pd.DataFrame:
    """
    Concatène tous les fichiers CSV d’anomalies dans un fichier Excel multi-feuilles.
    Ajoute une feuille 'recap' avec un tableau de synthèse des anomalies par type.
    Affiche et retourne le DataFrame de récapitulatif pour affichage dans le notebook.
    
    Returns:
        pd.DataFrame: tableau de synthèse des anomalies
    """
    files = [f for f in os.listdir(output_dir) if f.startswith("anomalies") and f.endswith(".csv")]
    
    if not files:
        print("❌ Aucun fichier anomalies trouvé.")
        return pd.DataFrame()

    recap_data = []

    with pd.ExcelWriter(output_excel_path, engine='xlsxwriter') as writer:
        for file in files:
            sheet_name = file.replace("anomalies_", "").replace(".csv", "")[:31]
            file_path = os.path.join(output_dir, file)
            df = pd.read_csv(file_path)

            if df.empty:
                print(f"⚪ Fichier vide : {file} — ignoré")
                continue

            df.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"✅ Exporté : {file} → feuille '{sheet_name}'")

            # Agrégation
            anomaly_type = sheet_name
            nb_lignes = len(df)
            nb_cover_id = (
                df['coverRef'].nunique()
                if 'coverRef' in df.columns else
                df['coverId'].nunique()
                if 'coverId' in df.columns else "-"
            )

            agrégats_possibles = ['claimPaid', 'totalClaimPaid', 'overLimit', 'healthPremiumInclTax', 'anomaly_value']
            for col in agrégats_possibles:
                if col in df.columns:
                    montant_total = df[col].sum()
                    agregat = col
                    break
            else:
                montant_total = "-"
                agregat = "-"

            recap_data.append({
                "Type d'anomalie": anomaly_type,
                "Nb de lignes": nb_lignes,
                "Nb de contrats uniques": nb_cover_id,
                "Montant total concerné (€)": montant_total,
                "Agrégat concerné": agregat
            })

        # Ajout de la feuille de synthèse
        if recap_data:
            df_recap = pd.DataFrame(recap_data)
            df_recap.to_excel(writer, sheet_name="recap", index=False)
            print("\n📊 Feuille de synthèse ajoutée sous 'recap'.")

    print(f"\n📦 Fichier Excel généré : {output_excel_path}")

    return df_recap

def plot_bubble_anomalies(
    df,
    montant_col="Montant total concerné (€)",
    contrats_col="Nb de contrats uniques",
    lignes_col="Nb de lignes",
    type_col="Type d'anomalie",
    agregat_col="Agrégat concerné",
    anomalies_a_exclure=None,
    title="Figure 2.1 - Gravité des anomalies"
):
    """
    Crée un bubble plot des anomalies à partir d'un DataFrame.
    
    Parameters:
    - df : DataFrame d'entrée avec les anomalies
    - montant_col : nom de la colonne du montant
    - contrats_col : nom de la colonne des contrats uniques
    - lignes_col : nom de la colonne du nombre de lignes
    - type_col : nom de la colonne du type d'anomalie
    - agregat_col : nom de la colonne de l'agrégat
    - anomalies_a_exclure : liste d'anomalies à exclure du graphe
    - title : titre du graphique
    """
    
    anomalies_a_exclure = anomalies_a_exclure or []

    # Copie sans les lignes incomplètes
    df_bubble = df.dropna(subset=[montant_col, contrats_col]).copy()

    # Nettoyage des colonnes
    df_bubble[montant_col] = (
        df_bubble[montant_col].replace("-", np.nan).astype(float)
    )

    df_bubble[contrats_col] = pd.to_numeric(
        df_bubble[contrats_col], errors="coerce"
    ).fillna(0).astype(int)

    # Filtrage des anomalies à exclure
    df_bubble = df_bubble[~df_bubble[type_col].isin(anomalies_a_exclure)]

    # Graphe
    plt.figure(figsize=(12, 8))
    scatter = plt.scatter(
        df_bubble[contrats_col],
        df_bubble[lignes_col],
        s=df_bubble[montant_col].abs() / 10 + 50,
        c=df_bubble[agregat_col].astype("category").cat.codes,
        cmap="Set2",
        alpha=0.7,
        edgecolors="w",
        linewidth=0.5
    )

    # Étiquettes
    for i, row in df_bubble.iterrows():
        plt.text(
            row[contrats_col] + 2,
            row[lignes_col],
            row[type_col],
            fontsize=8
        )

    plt.xlabel("Nombre de contrats uniques")
    plt.ylabel("Nombre de lignes")
    plt.title(title)
    plt.grid(True)
    plt.tight_layout()
    plt.show()
#=============================== Next Steps ===============================================

def check_quittances_contrats(df_contrats: pd.DataFrame, df_quittances: pd.DataFrame) -> pd.DataFrame:
    """
    Vérifie :
    1. Que chaque mois entre coverStartDate et 31/12/2021 est bien couvert par une quittance.
    2. Que la somme des quittances (avec réduction multi-contrats) est cohérente avec la prime annuelle Health attendue.

    Retourne un DataFrame avec les anomalies détectées et un type d’anomalie :
    - 'missing_quittance_month'
    - 'incorrect_quittance_amount'
    """

    df_contrats = df_contrats.copy()
    df_quittances = df_quittances.copy()

    # Limite temporelle : pas de contrôle après le 31/12/2021
    date_limite = pd.Timestamp("2021-12-31")
    df_contrats['coverStartDate'] = pd.to_datetime(df_contrats['coverStartDate'], errors='coerce')
    df_quittances['issuanceDate'] = pd.to_datetime(df_quittances['issuanceDate'], errors='coerce')

    # Construction du calendrier attendu de quittances par contrat
    all_expected_quittances = []

    for _, row in df_contrats.iterrows():
        start = row['coverStartDate']
        end = min(date_limite, pd.Timestamp.today())
        if pd.isna(start) or start > end:
            continue

        months = pd.date_range(start, end, freq='MS')
        for date in months:
            all_expected_quittances.append({
                'coverRef': row['coverRef'],
                'issuanceDate': date,
                'customerId': row['customerId']
            })

    df_expected = pd.DataFrame(all_expected_quittances)

    # Jointure avec les quittances existantes
    df_check = df_expected.merge(
        df_quittances[['coverRef', 'issuanceDate', 'healthPremiumInclTax']],
        on=['coverRef', 'issuanceDate'],
        how='left',
        indicator=True
    )

    # Anomalies 1 : mois manquants
    anomalies_missing = df_check[df_check['_merge'] == 'left_only'].copy()
    anomalies_missing['anomaly_type'] = 'missing_quittance_month'

    # Anomalies 2 : incohérence de montant (après réduction multi-contrats)
    # Étape 1 : somme des quittances par contrat
    quittances_sum = df_quittances[df_quittances['issuanceDate'] <= date_limite] \
        .groupby('coverRef')['healthPremiumInclTax'].sum().reset_index()
    quittances_sum = quittances_sum.rename(columns={'healthPremiumInclTax': 'sum_quittances'})

    # Étape 2 : jointure avec les contrats pour récupérer prime attendue
    df_cont = df_contrats.merge(quittances_sum, on='coverRef', how='left')

    # Étape 3 : appliquer la réduction multi-contrats
    def apply_discount(group):
        group = group.sort_values(by='healthPremiumInclTax', ascending=False)
        if len(group) > 1:
            group.loc[group.index[1:], 'expected_discounted'] = group.loc[group.index[1:], 'healthPremiumInclTax'] * 0.85
            group.loc[group.index[0], 'expected_discounted'] = group.loc[group.index[0], 'healthPremiumInclTax']
        else:
            group['expected_discounted'] = group['healthPremiumInclTax']
        return group

    df_cont = df_cont.groupby('customerId').apply(apply_discount).reset_index(drop=True)

    # Étape 4 : comparaison
    df_cont['delta'] = df_cont['sum_quittances'] - df_cont['expected_discounted']
    anomalies_amount = df_cont[~np.isclose(df_cont['sum_quittances'], df_cont['expected_discounted'], atol=0.01)].copy()
    anomalies_amount['anomaly_type'] = 'incorrect_quittance_amount'

    # Colonnes utiles
    anomalies_missing = anomalies_missing[['coverRef', 'issuanceDate', 'customerId', 'anomaly_type']]
    anomalies_amount = anomalies_amount[['coverRef', 'customerId', 'healthPremiumInclTax', 'sum_quittances', 'expected_discounted', 'delta', 'anomaly_type']]

    # Fusion des anomalies
    anomalies_all = pd.concat([anomalies_missing, anomalies_amount], ignore_index=True)

    print(f"[check_quittances_contrats] Nombre total d’anomalies : {len(anomalies_all)}")
    print(f"[check_quittances_contrats] - Manque de quittances mensuelles : {len(anomalies_missing)}")
    print(f"[check_quittances_contrats] - Écarts sur le montant total annuel : {len(anomalies_amount)}")

    return anomalies_all

def match_tarif_health(df_contrats: pd.DataFrame, df_tarifs: pd.DataFrame) -> pd.DataFrame:
    """
    Associe chaque contrat à son tarif Health standard (sans réduction),
    basé sur l’âge de l’animal, son espèce, son état de santé, le plafond et le taux de couverture.
    """
    df = df_contrats.copy()

    # Calcule l'âge de l’animal en années, arrondi à l’entier supérieur
    df['petBirthday'] = pd.to_datetime(df['petBirthday'], errors='coerce')
    df['age'] = ((df['coverStartDate'] - df['petBirthday']).dt.days / 365.25).astype(int)

    # Filtrage : on ne garde que les animaux en bonne santé
    df = df[df['petSick'] == 'healthy']

    # Jointure avec df_tarifs
    df_merged = df.merge(
        df_tarifs,
        left_on=['petType', 'age', 'coverRate', 'healthLimit'],
        right_on=['animal', 'age', 'taux', 'healthLimit'],
        how='left',
        suffixes=('', '_tarif')
    )

    # Renommage pour plus de clarté
    df_merged = df_merged.rename(columns={'healthHthcMonthly': 'expected_healthHthc_monthly'})

    # On suppose ici que healthHthc dans contrats est annuel → donc on multiplie par 12
    df_merged['expected_healthHthc'] = df_merged['expected_healthHthc_monthly'] * 12

    return df_merged

def apply_multi_contrat_discount(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applique la règle de réduction multi-contrats Health (15% sur tous sauf le plus cher).
    Seule la garantie Health est concernée.
    """
    df = df.copy()

    # Initialisation de la colonne finale avec la valeur standard
    df['expected_healthHthc_discounted'] = df['expected_healthHthc']

    # Grouper par customerId
    def apply_discount(group):
        group = group.sort_values(by='expected_healthHthc', ascending=False)
        if len(group) > 1:
            group.iloc[1:, group.columns.get_loc('expected_healthHthc_discounted')] *= 0.85
        return group

    df = df.groupby('customerId').apply(apply_discount).reset_index(drop=True)
    return df
