# CIDSEC_CONST — Pipeline Data Medallion & Analytics IA

Projet de fin d'études (CESI / CIDSEC Consulting, Luxembourg — 21/09/2026 → 12/02/2027) :
conception et mise en œuvre d'un pipeline de données de bout en bout
(**ingestion → transformation → stockage → visualisation**) avec composants IA/ML.

## Architecture en un coup d'œil

**Medallion Bronze → Silver → Gold**, conteneurisée sous Docker :

- **Ingestion** : CSV (Kaggle Olist, Cymbal Investments) + API REST
- **Stockage** : HDFS (Parquet) + Apache Hive (catalogue SQL)
- **Transformation** : Apache Spark (PySpark + écriture JDBC)
- **Orchestration** : Apache Airflow (planification, alertes, qualité de données)
- **Service BI** : PostgreSQL → **Power BI**
- **ML** : Prophet / XGBoost (prévision CA, prédiction de retard)

## Contenu du dépôt

| Dossier | Contenu | État |
|---|---|---|
| `docs/` | 01 Audit · 02 Architecture · 03 Plan · 04 Guide d'installation · 06 Phase 3 ML · 07 Guide Power BI | ✅ |
| `docker/` | `docker-compose.yml` (13 services), configs Hadoop/Hive/Spark, `init-hdfs.sh`, image Airflow enrichie (Java + PySpark) | ✅ |
| `spark_jobs/` | Jobs PySpark : `bronze_ingest`, `silver_olist`, `silver_cymbal`, `silver_iot`, `silver_api`, `conformed_build`, `gold_build`, `data_quality/checks` + `common` | ✅ |
| `dags/` | DAGs Airflow : `dag_01_ingest_bronze`, `dag_04_silver_transform`, `dag_05_gold_build`, `dag_06_data_quality`, `dag_07_ml_train` | ✅ |
| `ml/` | Phase 3 ML : `train_pred_retard` (XGBoost), `train_forecast_ca` (Prophet), `train_forecast_iot` (Prophet), `requirements.txt`, `modeles/` | ✅ |
| `ingestion/` | Scripts API REST (Open-Meteo, Frankfurter) — Python pur, exécutables à la main | ✅ |
| `sql/` | Vues Hive (`01_views_silver.hql`) + DDL PostgreSQL (`01_gold_ddl.sql`, `02_dq_ddl.sql`, `03_powerbi_views.sql`) | ✅ |
| `powerbi/` | Livrable `CIDSEC.pbix` — à construire via `docs/07_powerbi_guide.md` (guide pas-à-pas) | 📋 |
| `data_quality/` | Contrôles qualité (41 contrôles, 6 familles) + alertes — `checks.py`, `dag_06` | ✅ |
| `Données/` | Sources locales (non versionnées) | ✅ |

## Démarrage rapide

Le guide complet (Pourquoi / Comment / Vérification, étape par étape) est dans
**`docs/04_guide_installation.md`**. En résumé :

```bash
# 0. Docker Desktop doit être démarré : docker ps doit répondre
cp docker/.env.example docker/.env

# 1. Lancer la plateforme (11 services : HDFS, Hive, Spark, Airflow, PostgreSQL)
cd docker && docker compose up -d && docker compose ps

# 2. Vérifications de santé
docker exec cidsec-namenode hdfs dfs -ls /data
docker exec cidsec-hive-server beeline -u jdbc:hive2://localhost:10000 -e "SHOW DATABASES;"

# 3. Pipeline : à la main (étape 7 du guide) ou via Airflow (http://localhost:8280)
```

## Conventions

- Code : Python 3.11, commentaires en français, commits conventionnels (`feat:`, `fix:`, `docs:`).
- Git : `main` ← `develop` ← `feature/*`. **Aucune donnée ni secret dans Git** (`.gitignore`).
- Toute étape du pipeline doit être réalisable **à la main** (commandes documentées) en plus
  d'être automatisée par Airflow.
