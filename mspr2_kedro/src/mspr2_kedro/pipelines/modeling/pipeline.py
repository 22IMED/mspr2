from __future__ import annotations

from kedro.pipeline import Pipeline, node
from .nodes import preprocess_features, run_pca, choose_best_k_and_cluster, analyze_clusters


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline([
        node(
            func=preprocess_features,
            inputs="features_all_users_memory",
            outputs="features_normalized",
            name="preprocess_features_node",
        ),
        node(
            func=run_pca,
            inputs={
                "df": "features_normalized",
                "variance_threshold": "params:pca_variance_threshold",
            },
            outputs="features_pca",
            name="pca_node",
        ),
        node(
            func=choose_best_k_and_cluster,
            inputs={
                "df_pca": "features_pca",
                "min_k": "params:min_k",
                "max_k": "params:max_k",
            },
            outputs="features_clusters",
            name="clustering_node",
        ),
        node(
            func=analyze_clusters,
            inputs="features_clusters",
            outputs="clusters_analysis_report",
            name="analyze_clusters_node",
        ),
    ])