from __future__ import annotations

import os
from typing import Any
import pandas as pd
from kedro.io import AbstractDataset


class CSVDataset(AbstractDataset[pd.DataFrame, pd.DataFrame]):
    def __init__(self, filepath: str, load_args: dict[str, Any] | None = None, save_args: dict[str, Any] | None = None):
        self._filepath = filepath
        self._load_args = load_args or {}
        self._save_args = save_args or {}

    def _load(self) -> pd.DataFrame:
        return pd.read_csv(self._filepath, **self._load_args)

    def _save(self, data: pd.DataFrame) -> None:
        os.makedirs(os.path.dirname(self._filepath), exist_ok=True)
        data.to_csv(self._filepath, **self._save_args)

    def _describe(self) -> dict[str, Any]:
        return {"filepath": self._filepath}

    def _exists(self) -> bool:
        return os.path.exists(self._filepath)


class TextDataset(AbstractDataset[str, str]):
    def __init__(self, filepath: str, load_args: dict[str, Any] | None = None, save_args: dict[str, Any] | None = None, encoding: str = "utf-8"):
        self._filepath = filepath
        self._load_args = load_args or {}
        self._save_args = save_args or {}
        self._encoding = encoding

    def _load(self) -> str:
        with open(self._filepath, "r", encoding=self._encoding) as f:
            return f.read()

    def _save(self, data: str) -> None:
        os.makedirs(os.path.dirname(self._filepath), exist_ok=True)
        with open(self._filepath, "w", encoding=self._encoding) as f:
            f.write(data)

    def _describe(self) -> dict[str, Any]:
        return {"filepath": self._filepath}

    def _exists(self) -> bool:
        return os.path.exists(self._filepath)