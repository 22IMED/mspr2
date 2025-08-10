from __future__ import annotations

from typing import List
import pandas as pd

from mspr2_kedro.etl_steps.extract import list_csv_files, extract_data_in_chunks
from mspr2_kedro.etl_steps.transform import clean_data, create_features


def generate_features(datasets_dir: str, chunk_size: int) -> pd.DataFrame:
    """Generate user-level features from raw CSV files.

    Args:
        datasets_dir: Directory containing raw CSV files.
        chunk_size: Number of rows per chunk when reading large CSVs.
    Returns:
        A DataFrame with one row per user and engineered features.
    """
    all_features: List[pd.DataFrame] = []
    csv_files = list_csv_files(datasets_dir)
    if not csv_files:
        return pd.DataFrame()

    for csv_file in csv_files:
        for chunk in extract_data_in_chunks(csv_file, chunk_size=chunk_size):
            cleaned = clean_data(chunk)
            features = create_features(cleaned)
            if not features.empty:
                all_features.append(features)

    if not all_features:
        return pd.DataFrame()

    features_df = pd.concat(all_features, ignore_index=True)
    return features_df


def pass_through(df: pd.DataFrame) -> pd.DataFrame:
    return df