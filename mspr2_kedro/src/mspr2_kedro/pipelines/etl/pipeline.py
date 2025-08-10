from __future__ import annotations

from kedro.pipeline import Pipeline, node
from .nodes import generate_features, pass_through


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline([
        node(
            func=generate_features,
            inputs={
                "datasets_dir": "params:datasets_dir",
                "chunk_size": "params:etl_chunk_size",
            },
            outputs="features_all_users_memory",
            name="generate_features_node",
        ),
        node(
            func=pass_through,
            inputs="features_all_users_memory",
            outputs="features_all_users",
            name="persist_features_node",
        ),
    ])