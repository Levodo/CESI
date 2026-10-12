# 03 — Plan de projet détaillé (phases, étapes, livrables)

**Calendrier aligné sur la fiche de validation du stage** (21/09/2026 → 12/02/2027).
Chaque phase liste ses étapes **réalisables à la main** et le code qu'on produira.

---

## Phase 0 — Mise en place de l'environnement (semaine du 05/10/2026)

| # | Étape | Action concrète | Résultat attendu |
|---|---|---|---|
| 0.1 | Créer le dépôt GitHub | `git init`, `git remote add origin …`, premier commit de la structure | Dépôt distant accessible à l'équipe |
| 0.2 | Écrire `docker/docker-compose.yml` | Définir les 9 services : `postgres`, `namenode`, `datanode`, `hive-metastore`, `hive-server2`, `spark-master`, `spark-worker`, `airflow-webserver`, `airflow-scheduler` | Fichier Compose validé (`docker compose config`) |
| 0.3 | Écrire les fichiers de config | `docker/hadoop/*.xml` (core-site, hdfs-site), `docker/hive/hive-site.xml`, `docker/airflow/airflow.cfg`, `.env.example` | Config versionnée |
| 0.4 | Démarrer la plateforme | Démarrer Docker Desktop → `docker compose up -d` → vérifier chaque port | 9 conteneurs `healthy` |
| 0.5 | Initialiser HDFS | `hdfs dfs -mkdir -p /data/{landing,bronze,silver,gold,models,dq}` | Arborescence du lac créée |
| 0.6 | Initialiser PostgreSQL | `createdb` : `airflow_db`, `hive_metastore`, `gold_db` ; créer les rôles `airflow`, `hive`, `spark`, `powerbi` | Bases et utilisateurs prêts |
| 0.7 | Charger les données en landing | Décompresser les zips → `hdfs dfs -put` vers `/data/landing/…` | 10 fichiers visibles dans HDFS |
| 0.8 | Vérification | `docker compose ps`, `hdfs dfs -ls -R /data`, UI Airflow sur `localhost:8180` | ✅ plateforme opérationnelle |

**Livrable** : plateforme démarrable par `docker compose up -d` par n'importe qui.

## Phase 1 — Audit & architecture *(en cours — livrables produits le 02/10/2026)*

| # | Étape | État |
|---|---|---|
| 1.1 | Inventorier et profiler les sources | ✅ `docs/01_rapport_audit_donnees.md` |
| 1.2 | Mesurer l'intégrité référentielle | ✅ 0 clé orpheline |
| 1.3 | Recenser les anomalies + règles de correction | ✅ 13 anomalies (A1-A13) |
| 1.4 | Concevoir l'architecture cible | ✅ `docs/02_architecture_cible.md` |
| 1.5 | Valider avec le tuteur + plan projet | ✅ ce document |

**Livrable contractuel (20/10/2026)** : rapport d'audit + document d'architecture.

## Phase 2 — Pipeline ETL/ELT (23/10 → 15/11/2026)

| # | Étape | Code produit |
|---|---|---|
| 2.1 | Ingestion Bronze : CSV → Parquet daté | `spark_jobs/bronze_ingest.py` (PySpark : lecture `utf-8-sig`, ajout `_ingestion_ts`, `_source_file`, `_load_date`, écriture Parquet partitionné) |
| 2.2 | Créer les tables Hive Bronze | `sql/bronze_ddl.hql` (`CREATE EXTERNAL TABLE … STORED AS PARQUET`) |
| 2.3 | Nettoyage Silver (règles A1-A13) | `spark_jobs/silver_olist.py`, `spark_jobs/silver_cymbal.py` (typage, déduplication, enrichissements : `delivery_delay_days`, `is_late`, `order_month`, jointure traduction) |
| 2.4 | Tables Hive Silver + vue métier | `sql/silver_ddl.hql` + `sql/silver_views.hql` (`v_commandes_complete`) |
| 2.5 | Agrégats Gold | `spark_jobs/gold_build.py` (KPI CA jour/catégorie/état, délais de livraison, satisfaction ; tables étoile `dim_*`, `fact_commande`) |
| 2.6 | Export PostgreSQL (**Spark JDBC**) | même job : `df.write.format("jdbc").option("url","jdbc:postgresql://postgres:5432/gold_db")…` |
| 2.7 | Ingestion multicanale (API + IoT) | `ingestion/ingest_api_meteo.py`, `ingestion/ingest_api_fx.py` (REST → JSON daté archivé → Bronze → Silver, avec *forward-fill* des jours ouvrés) · `spark_jobs/silver_iot.py` (OPSD : sélection DE/FR/BE/NL/AT · **UCI** : lecture `sep=';'`, `decimal=','`, `dateFormat='dd/MM/yyyy'`, `nullValue='?'` + agrégations 1 h / 1 j) |
| 2.8 | Jeux enrichis multi-sources | `spark_jobs/conformed_build.py` (jointures commandes ⋈ météo, trades ⋈ FX → `silver/conformed/`) |
| 2.9 | Orchestration Airflow | `dags/dag_01_ingest_olist.py` … `dag_05_gold_build.py`, `dag_03b_ingest_iot.py` (SparkSubmitOperator, dépendances, retries, alertes) |
| 2.10 | Tests manuels de bout en bout | Procédure documentée : vider une partition, relancer, vérifier idempotence |

**Livrable contractuel (15/11/2026)** : pipeline fonctionnel, rejouable à la main ou par Airflow.

## Phase 3 — ML & analytics prédictive (20/11 → 20/12/2026)

| # | Étape | Code produit |
|---|---|---|
| 3.1 | Feature engineering | `spark_jobs/features_ml.py` (séries CA, saisonnalité, features livraison **+ météo**) |
| 3.2 | Prévision CA quotidien | `ml/forecast_ca.py` (Prophet, puis challenger XGBoost) → `/data/models` |
| 3.3 | Classification retard de livraison | `ml/predict_delay.py` (XGBoost, métriques AUC/recall) |
| 3.4 | Prévisions IoT (charge + foyer) | `ml/forecast_load.py` (Prophet/XGBoost : `silver.iot.energie` horizon 7 j · `silver.iot.foyer_1h` horizon 48 h) |
| 3.5 | Prédictions vers Gold/PostgreSQL | écriture `gold.forecast_ca`, `gold.pred_retard`, `gold.forecast_charge` |
| 3.6 | Insights automatiques | `ml/insights.py` (variations atypiques, top/flop, résumés textuels) |
| 3.7 | DAG `dag_07_ml_training` | ré-entraînement hebdomadaire + suivi des métriques |

**Livrable contractuel (20/12/2026)** : module ML intégré, prédictions visibles en BI.

## Phase 4 — Dashboard & qualité de données (22/12 → 20/01/2027)

| # | Étape | Résultat |
|---|---|---|
| 4.1 | Module DQ | `data_quality/checks.py` + table `dq.dq_checks` (7 types de contrôles, §8 architecture) |
| 4.2 | Alerting | `dags/dag_06_data_quality.py` (échec du DAG + notification si seuil dépassé) |
| 4.3 | Modèle Power BI | `powerbi/CIDSEC.pbix` : modèle étoile, 6 pages, mesures DAX (CA, marge, taux de retard, NPS) |
| 4.4 | Connexion & rafraîchissement | connexion PostgreSQL `gold_db`, documentation du rafraîchissement |
| 4.5 | Recette utilisateur | scénarios de test avec les parties prenantes (Finance, Opérations) |

**Livrable contractuel (20/01/2027)** : dashboard interactif + monitoring qualité.

## Phase 5 — Tests, documentation & soutenance (22/01 → 12/02/2027)

| # | Étape | Livrable |
|---|---|---|
| 5.1 | Tests d'intégration complets | jeu de tests reproductible (rejeu depuis zéro sur une machine neuve) |
| 5.2 | Rapport technique final | `docs/05_rapport_final.md` (architecture, décisions, résultats, limites, perspectives) |
| 5.3 | Guide d'utilisation & formation | `docs/04_guide_installation.md` + `docs/06_guide_utilisateur.md` (Power BI pour les métiers) |
| 5.4 | Présentation de soutenance | slides + démo live (pipeline complet : CSV → dashboard) |

**Livrable contractuel (12/02/2027)** : rapport + présentation + dépôt GitHub complet.

---

## Estimation de charge (par phase)

| Phase | Charge estimée | Point de contrôle |
|---|---|---|
| 0 — Environnement | 3-5 jours | plateforme démarrée sans erreur |
| 1 — Audit & architecture | ✅ fait | validation tuteur |
| 2 — Pipeline ETL/ELT | 4 semaines | démo pipeline complet |
| 3 — ML | 4 semaines | démo prédictions |
| 4 — BI & DQ | 3 semaines | démo dashboard |
| 5 — Clôture | 3 semaines | soutenance |

## Risques identifiés

| Risque | Impact | Parade |
|---|---|---|
| RAM insuffisante pour Hive + Spark + Airflow | blocage d'exécution | limiter la mémoire Spark (`--driver-memory 1g`), fermer les apps, augmenter le swap Docker |
| Premier `docker compose up` long (téléchargement images) | perte de temps | lancer tôt, en Wi-Fi stable |
| Spark/Hive : versions incompatibles entre images | erreurs au runtime | **épingler les versions** dans docker-compose (documentées) |
| Perte des données locales (Données/) | re-téléchargement | conserver les zips sources ; les données ne sont pas dans Git (volontairement) |
| Glissement de planning (phases 2-3) | livrables en retard | découper en sous-tâches hebdo ; prioriser le cœur du pipeline avant le ML |

## Prochaine étape immédiate

Validation de l'architecture (`docs/02`) par l'équipe, puis **génération du code complet
de la Phase 0 + Phase 2** (docker-compose, DDL Hive, jobs PySpark, DAGs Airflow) —
chaque fichier commenté et chaque commande de vérification documentée.
