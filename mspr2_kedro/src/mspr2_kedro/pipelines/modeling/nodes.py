from __future__ import annotations

import os
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score


def preprocess_features(features: pd.DataFrame) -> pd.DataFrame:
    df = features.copy()
    numeric_cols = [col for col in df.select_dtypes(include=["number"]).columns if col != "user_id"]
    if not numeric_cols:
        return df
    for col in numeric_cols:
        mean = df[col].mean()
        std = df[col].std()
        df[col] = df[col].clip(lower=mean - 5 * std, upper=mean + 5 * std)
    scaler = StandardScaler()
    df[numeric_cols] = scaler.fit_transform(df[numeric_cols])
    return df


def run_pca(df: pd.DataFrame, variance_threshold: float = 0.9) -> pd.DataFrame:
    user_ids = df["user_id"] if "user_id" in df.columns else None
    X = df.drop(columns=["user_id"]) if "user_id" in df.columns else df
    if X.empty:
        return df
    pca = PCA(n_components=variance_threshold, svd_solver="full")
    X_pca = pca.fit_transform(X)
    df_pca = pd.DataFrame(X_pca, columns=[f"PC{i+1}" for i in range(X_pca.shape[1])])
    if user_ids is not None:
        df_pca.insert(0, "user_id", user_ids)
    return df_pca


def choose_best_k_and_cluster(df_pca: pd.DataFrame, min_k: int = 2, max_k: int = 10) -> pd.DataFrame:
    X = df_pca.drop(columns=["user_id"]) if "user_id" in df_pca.columns else df_pca
    if X.empty:
        return df_pca
    inertias = []
    silhouettes = []
    k_range = range(min_k, max_k + 1)
    for k in k_range:
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = kmeans.fit_predict(X)
        inertias.append(kmeans.inertia_)
        silhouettes.append(silhouette_score(X, labels))
    best_k = k_range[silhouettes.index(max(silhouettes))]
    kmeans = KMeans(n_clusters=best_k, random_state=42, n_init=10)
    df_out = df_pca.copy()
    df_out["cluster"] = kmeans.fit_predict(X)
    return df_out


def analyze_clusters(df_clusters: pd.DataFrame) -> str:
    if "cluster" not in df_clusters.columns:
        return "Aucune colonne 'cluster' trouvée."
    clusters = sorted(df_clusters["cluster"].unique())
    lines = [f"Nombre total de clusters : {len(clusters)}\n"]
    for cluster in clusters:
        group = df_clusters[df_clusters["cluster"] == cluster]
        size = len(group)
        lines.append(f"\n--- Cluster {cluster} ---")
        lines.append(f"Taille du cluster : {size}")
        means = group.mean(numeric_only=True)
        lines.append("Moyennes des variables principales :")
        for col, val in means.items():
            if col not in ["cluster", "user_id"]:
                lines.append(f"  {col} : {val:.2f}")
        top_features = means.drop(["cluster"], errors="ignore").sort_values(ascending=False).head(3)
        lines.append("Caractéristiques principales du groupe :")
        for feat, val in top_features.items():
            lines.append(f"  {feat} (moyenne : {val:.2f})")
        if len(top_features) and top_features.index[0] == "total_spent":
            name = "Gros acheteurs"
        elif len(top_features) and top_features.index[0] == "conversion_rate":
            name = "Acheteurs efficaces"
        elif "view" in top_features.index:
            name = "Curieux/Explorateurs"
        else:
            name = f"Cluster {cluster}"
        lines.append(f"Nom proposé : {name}\n")
    return "\n".join(lines)