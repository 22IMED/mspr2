"""
transform.py
Étape 2 du pipeline ETL : Transformation et nettoyage des données, création des variables explicatives.
"""
import pandas as pd
from typing import List, Dict, Any

# =============================================================================
# CONSTANTES DE CONFIGURATION - À MODIFIER SELON LES BESOINS
# =============================================================================

# Colonnes critiques (obligatoires pour le traitement)
CRITICAL_COLUMNS = ['user_id', 'event_type']

# Colonnes pour lesquelles on supprime les lignes avec valeurs manquantes
COLUMNS_TO_DROP_NA = ['user_id']

# Colonnes où on remplit les valeurs manquantes avec une valeur par défaut
COLUMNS_TO_FILL_NA = {
    'brand': 'unknown',
    'category_code': 'unknown',
    'user_session': 'unknown'
}

# Colonnes à convertir en datetime
DATETIME_COLUMNS = ['event_time']

# Colonnes à convertir en numérique
NUMERIC_COLUMNS = ['price']

# Colonnes à exclure du traitement (optionnel)
COLUMNS_TO_EXCLUDE = []

# Paramètres de suppression des doublons
REMOVE_DUPLICATES = True
DUPLICATE_SUBSET = ['user_id', 'event_time', 'event_type', 'product_id']

# Paramètres de filtrage des utilisateurs
FILTER_INACTIVE_USERS = True
MIN_USER_ACTIVITY = 5

# Paramètres de création de features
MIN_EVENTS_THRESHOLD = 10
EVENT_TYPES_FOR_CONVERSION = ['purchase', 'view']
FEATURE_COLUMNS = ['category_id', 'price', 'brand']

# Paramètres de nettoyage des données
CLEAN_PRICE_OUTLIERS = True
PRICE_MIN_THRESHOLD = 0.01
PRICE_MAX_THRESHOLD = 10000.0

# Paramètres de validation des données
VALIDATE_EVENT_TYPES = True
VALID_EVENT_TYPES = ['view', 'cart', 'remove_from_cart', 'purchase']

# =============================================================================
# FONCTIONS DE TRANSFORMATION
# =============================================================================

def validate_data_structure(df: pd.DataFrame) -> bool:
    """
    Valide que le DataFrame contient les colonnes requises.
    """
    missing_columns = [col for col in CRITICAL_COLUMNS if col not in df.columns]
    if missing_columns:
        print(f"⚠️ Colonnes manquantes : {missing_columns}")
        return False
    return True

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie les données brutes avec les paramètres configurés.
    """
    df = df.copy()
    
    # Validation de la structure
    if not validate_data_structure(df):
        return pd.DataFrame()
    
    print(f"🧹 Nettoyage des données : {len(df)} lignes initiales")
    
    # Suppression des lignes avec valeurs manquantes dans les colonnes critiques
    if COLUMNS_TO_DROP_NA:
        initial_rows = len(df)
        df = df.dropna(subset=COLUMNS_TO_DROP_NA)
        dropped_rows = initial_rows - len(df)
        if dropped_rows > 0:
            print(f"   - Supprimé {dropped_rows} lignes avec valeurs manquantes dans {COLUMNS_TO_DROP_NA}")
    
    # Remplissage des valeurs manquantes
    if COLUMNS_TO_FILL_NA:
        for column, default_value in COLUMNS_TO_FILL_NA.items():
            if column in df.columns:
                filled_count = df[column].isna().sum()
                df[column] = df[column].fillna(default_value)
                if filled_count > 0:
                    print(f"   - Rempli {filled_count} valeurs manquantes dans '{column}' avec '{default_value}'")
    
    # Conversion des types de colonnes
    if DATETIME_COLUMNS:
        for column in DATETIME_COLUMNS:
            if column in df.columns:
                df[column] = pd.to_datetime(df[column], errors='coerce')
                print(f"   - Converti '{column}' en datetime")
    
    if NUMERIC_COLUMNS:
        for column in NUMERIC_COLUMNS:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors='coerce')
                print(f"   - Converti '{column}' en numérique")
    
    # Suppression des doublons
    if REMOVE_DUPLICATES and DUPLICATE_SUBSET:
        initial_rows = len(df)
        df = df.drop_duplicates(subset=DUPLICATE_SUBSET)
        removed_duplicates = initial_rows - len(df)
        if removed_duplicates > 0:
            print(f"   - Supprimé {removed_duplicates} doublons")
    
    # Nettoyage des valeurs aberrantes pour le prix
    if CLEAN_PRICE_OUTLIERS and 'price' in df.columns:
        initial_rows = len(df)
        df = df[
            (df['price'] >= PRICE_MIN_THRESHOLD) & 
            (df['price'] <= PRICE_MAX_THRESHOLD)
        ]
        removed_outliers = initial_rows - len(df)
        if removed_outliers > 0:
            print(f"   - Supprimé {removed_outliers} lignes avec prix aberrant")
    
    # Validation des types d'événements
    if VALIDATE_EVENT_TYPES and 'event_type' in df.columns:
        invalid_events = df[~df['event_type'].isin(VALID_EVENT_TYPES)]
        if len(invalid_events) > 0:
            print(f"   - ⚠️ {len(invalid_events)} événements avec type invalide trouvés")
    
    print(f"✅ Nettoyage terminé : {len(df)} lignes restantes")
    return df

def create_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crée les variables explicatives pour chaque utilisateur avec les paramètres configurés.
    """
    if not validate_data_structure(df):
        return pd.DataFrame()
    
    print(f"🔧 Création des features pour {df['user_id'].nunique()} utilisateurs")
    
    # Nombre d'événements par utilisateur et par type
    event_counts = df.pivot_table(
        index='user_id', 
        columns='event_type', 
        values='event_time', 
        aggfunc='count', 
        fill_value=0
    )
    
    # Montant total dépensé et prix moyen
    spent = pd.Series(dtype=float, name='total_spent')
    avg_price = pd.Series(dtype=float, name='avg_purchase_price')
    
    if 'price' in df.columns and 'event_type' in df.columns:
        purchase_data = df[df['event_type'] == 'purchase']
        if len(purchase_data) > 0:
            spent = purchase_data.groupby('user_id')['price'].sum().rename('total_spent')
            avg_price = purchase_data.groupby('user_id')['price'].mean().rename('avg_purchase_price')
    
    # Nombre de catégories différentes visitées
    n_cat = pd.Series(dtype=int, name='unique_categories')
    if 'category_id' in df.columns:
        n_cat = df.groupby('user_id')['category_id'].nunique().rename('unique_categories')
    
    # Nombre de marques différentes visitées
    n_brands = pd.Series(dtype=int, name='unique_brands')
    if 'brand' in df.columns:
        n_brands = df.groupby('user_id')['brand'].nunique().rename('unique_brands')
    
    # Taux de conversion (achats / visites)
    conv = pd.Series(dtype=float, name='conversion_rate')
    if 'purchase' in event_counts.columns and 'view' in event_counts.columns:
        purchases = event_counts['purchase']
        views = event_counts['view']
        conv = (purchases / views).rename('conversion_rate')
        # Gérer la division par zéro
        conv = conv.fillna(0)
        conv = conv.replace([float('inf')], 0)
    
    # Fusion des features
    features_list = [event_counts, spent, n_cat, n_brands, avg_price, conv]
    features = pd.concat([f for f in features_list if not f.empty], axis=1)
    
    # Remplacement des NaN par 0
    features = features.fillna(0)
    
    # Filtrage des utilisateurs peu actifs
    if FILTER_INACTIVE_USERS:
        initial_users = len(features)
        features = features[features.sum(axis=1) >= MIN_USER_ACTIVITY]
        filtered_users = initial_users - len(features)
        if filtered_users > 0:
            print(f"   - Filtré {filtered_users} utilisateurs inactifs (< {MIN_USER_ACTIVITY} événements)")
    
    # Filtrage par seuil minimum d'événements
    if MIN_EVENTS_THRESHOLD > 0:
        initial_users = len(features)
        features = features[features.sum(axis=1) >= MIN_EVENTS_THRESHOLD]
        filtered_users = initial_users - len(features)
        if filtered_users > 0:
            print(f"   - Filtré {filtered_users} utilisateurs (< {MIN_EVENTS_THRESHOLD} événements totaux)")
    
    print(f"✅ Features créées pour {len(features)} utilisateurs")
    return features.reset_index()

def get_transformation_summary(df_original: pd.DataFrame, df_cleaned: pd.DataFrame, df_features: pd.DataFrame) -> Dict[str, Any]:
    """
    Retourne un résumé des transformations effectuées.
    """
    return {
        "lignes_originales": len(df_original),
        "lignes_apres_nettoyage": len(df_cleaned),
        "utilisateurs_uniques_originaux": df_original['user_id'].nunique() if 'user_id' in df_original.columns else 0,
        "utilisateurs_apres_features": len(df_features),
        "taux_reduction_lignes": (len(df_original) - len(df_cleaned)) / len(df_original) * 100 if len(df_original) > 0 else 0,
        "taux_reduction_utilisateurs": (df_original['user_id'].nunique() - len(df_features)) / df_original['user_id'].nunique() * 100 if 'user_id' in df_original.columns and df_original['user_id'].nunique() > 0 else 0
    }