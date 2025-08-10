# Projet IA - Catégorisation des clients d'Amazing

## Présentation
Ce projet catégorise les clients d'une marketplace (Amazing) selon leurs comportements d'achat et de navigation, à l'aide d'un pipeline ETL et d'un pipeline IA (clustering non supervisé).

Depuis la dernière mise à jour, le projet a été converti en structure Kedro pour améliorer la robustesse, la modularité et la reproductibilité.

- **Données** : Fichiers d'événements utilisateurs (CSV volumineux)
- **Objectif** : Identifier des groupes de clients pour adapter l'offre, le marketing et la personnalisation
- **Respect RGPD** : Données anonymisées, aucun traitement nominatif

## Organisation du projet
```
mspr2/
│
├── mspr2_kedro/           # Projet Kedro (NOUVEAU)
│   ├── conf/              # Config Kedro (catalog, params)
│   ├── notebooks/
│   ├── src/mspr2_kedro/
│   │   ├── datasets/      # Datasets personnalisés (CSV, texte)
│   │   ├── etl_steps/     # Utilitaires ETL (extract, transform, load)
│   │   └── pipelines/
│   │       ├── etl/       # Pipeline ETL (nodes + pipeline)
│   │       └── modeling/  # Pipeline IA (preprocess, PCA, clustering, analyse)
│   └── pyproject.toml
│
├── datasets/              # Fichiers CSV bruts (non versionnés)
├── output/                # Sorties ETL (features) (non versionnées)
├── model_ia_steps/        # Sorties IA (normalisation, PCA, clustering, rapports) (non versionnées)
├── extracted_csv/         # (optionnel) fichiers extraits (non versionnés)
├── exploration_results/   # Rapports d'exploration (non versionnés)
│
├── exploration_csv.py     # Script d'exploration rapide des CSV bruts (legacy)
├── main_etl.py            # Legacy (remplacé par Kedro)
├── main_model.py          # Legacy (remplacé par Kedro)
├── main_all.py            # Legacy (remplacé par Kedro)
└── requirements.txt       # Dépendances legacy (pré-Kedro)
```

## Prérequis
- Python 3.10 ou 3.11 recommandé
- Installer Kedro et les dépendances du projet Kedro:
  - Kedro: `pip install --user "kedro==0.19.*"`
  - Dépendances: `pip install -r mspr2_kedro/requirements.txt`
  - Si `kedro` n'est pas dans le PATH: utiliser `~/.local/bin/kedro`

- Placer les fichiers de données CSV dans le dossier `datasets/`

## Exécution avec Kedro (recommandé)
Se placer dans le dossier du projet Kedro:

```bash
cd mspr2_kedro
# Pipeline complet (ETL + IA)
kedro run
# ou (si kedro n'est pas dans PATH)
~/.local/bin/kedro run
```

- Sorties générées:
  - ETL: `output/features_all_users.csv`
  - IA: `model_ia_steps/features_normalized.csv`, `model_ia_steps/features_pca.csv`, `model_ia_steps/features_clusters.csv`, `model_ia_steps/clusters_analysis_report.txt`

Exécuter seulement une partie du pipeline:
```bash
# ETL seulement
kedro run --pipeline etl
# IA seulement (à partir des features déjà présentes)
kedro run --pipeline modeling
```

Paramètres clés (configurables dans `mspr2_kedro/conf/base/parameters.yml`):
- `datasets_dir`: chemin des CSV bruts (par défaut `datasets/`)
- `etl_chunk_size`: taille des chunks de lecture CSV
- `pca_variance_threshold`: variance expliquée cible pour la PCA
- `min_k`, `max_k`: plage de recherche du nombre de clusters

## Exécution legacy (déconseillé, pour compatibilité)
Ces scripts sont conservés à titre transitoire. Préférez Kedro.

```bash
# Pipeline complet (ETL + IA)
python main_all.py
# Pipeline ETL seul
python main_etl.py
# Pipeline IA seul
python main_model.py
```

## Détail des étapes IA
1. **Exploration** : Statistiques, valeurs manquantes, outliers
2. **Prétraitement** : Normalisation, gestion des extrêmes
3. **PCA** : Réduction de dimensionnalité, visualisation
4. **Clustering** : K-Means, choix du nombre de groupes (coude, silhouette)
5. **Analyse** : Description, nommage et rapport sur chaque cluster

## Respect du RGPD
- Les données sont anonymisées (`user_id` non nominatif)
- Aucun traitement nominatif ou sensible
- Les scripts sont conçus pour garantir la confidentialité et la sécurité des données

## Auteurs
- Projet réalisé dans le cadre de la certification Chef.fe de projet expert en IA (RNCP36582) 