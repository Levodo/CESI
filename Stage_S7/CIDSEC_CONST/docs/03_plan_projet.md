# 03 — Plan de projet & journal de réalisation (phases, étapes, livrables)

**Calendrier aligné sur la fiche de validation du stage** (21/09/2026 → 12/02/2027).
Chaque phase liste ses étapes **réalisables à la main** et le code produit.

> **État du projet au 19/11/2026** : Phases 0 ✅ · 1 ✅ · 2 ✅ · **3 ✅ (ML complète)** ·
> 4 🔄 (Power BI : vues + guide faits, `.pbix` à monter à la main) · 5 📋
> Détails : ce document · Résultats ML : `docs/06_phase3_ml.md` · Guide Power BI : `docs/07_powerbi_guide.md`.

---

## Phase 0 — Mise en place de l'environnement *(✅ faite — octobre 2026)*

| # | Étape | Action concrète | Résultat attendu |
|---|---|---|---|
| 0.1 | Créer le dépôt GitHub | `git init`, `git remote add origin …`, premier commit de la structure | Dépôt distant accessible à l'équipe |
| 0.2 | Écrire `docker/docker-compose.yml` | Définir les 9 services : `postgres`, `namenode`, `datanode`, `hive-metastore`, `hive-server2`, `spark-master`, `spark-worker`, `airflow-webserver`, `airflow-scheduler` | Fichier Compose validé (`docker compose config`) |
| 0.3 | Écrire les fichiers de config | `docker/hadoop/*.xml` (core-site, hdfs-site), `docker/hive/hive-site.xml`, `docker/airflow/airflow.cfg`, `.env.example` | Config versionnée |
| 0.4 | Démarrer la plateforme | Démarrer Docker Desktop → `docker compose up -d` → vérifier chaque port | 9 conteneurs `healthy` |
| 0.5 | Initialiser HDFS | `hdfs dfs -mkdir -p /data/{landing,bronze,silver,gold,models,dq}` | Arborescence du lac créée |
| 0.6 | Initialiser PostgreSQL | `createdb` : `airflow_db`, `hive_metastore`, `gold_db` ; créer les rôles `airflow`, `hive`, `spark`, `powerbi` | Bases et utilisateurs prêts |
| 0.7 | Charger les données en landing | Décompresser les zips → `hdfs dfs -put` vers `/data/landing/…` | 10 fichiers visibles dans HDFS |
| 0.8 | Vérification | `docker compose ps`, `hdfs dfs -ls -R /data`, UI Airflow sur `localhost:8280` | ✅ plateforme opérationnelle |

**Livrable** : plateforme démarrable par `docker compose up -d` par n'importe qui. ✅

**Écarts avec le plan initial** : **13 services** au lieu de 9 (ajout Adminer,
airflow-init) ; **Airflow sur le port `8280`** au lieu de 8080 (occupé par un logiciel
EDB local — `docs/04`) ; incidents Docker Desktop/WSL résolus (`wsl --update`).

## Phase 1 — Audit & architecture *(✅ faite — livrée le 02/10/2026)*

| # | Étape | État |
|---|---|---|
| 1.1 | Inventorier et profiler les sources | ✅ `docs/01_rapport_audit_donnees.md` |
| 1.2 | Mesurer l'intégrité référentielle | ✅ 0 clé orpheline |
| 1.3 | Recenser les anomalies + règles de correction | ✅ **22 anomalies** (A1-A13 e-commerce, B1-B8 IoT/API) — étendu après ajout des 4 sources |
| 1.4 | Concevoir l'architecture cible | ✅ `docs/02_architecture_cible.md` |
| 1.5 | Valider avec le tuteur + plan projet | ✅ ce document |

**Livrable contractuel (20/10/2026)** : rapport d'audit + document d'architecture.

## Phase 2 — Pipeline ETL/ELT *(✅ faite — validée le 21/11/2026 : CHAINE-COMPLETE-SUCCES)*

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

**Livrable contractuel (15/11/2026)** : pipeline fonctionnel, rejouable à la main ou par
Airflow. ✅ **validé le 21/11/2026** (rejeu complet : Silver ✅ Gold ✅ Qualité 41/41 ✅).

**Écarts notables** : `dag_01_ingest_bronze` en pause (courses de runs) ; règle d'or
**« la Silver ne consomme que le dernier snapshot Bronze »** (`read_latest_snapshot`,
bug racine du 18/11) ; règle A14 ajoutée (scores hors bornes → NULL) ; tables finales :
14 Bronze, 17 Silver, 19 Gold/PostgreSQL.

## Phase 3 — ML & analytics prédictive *(✅ faite — livrée le 19/11/2026, en avance)*

> **Écarts avec le plan initial** : noms de fichiers simplifiés (`forecast_ca.py` →
> `train_forecast_ca.py`, `predict_delay.py` → `train_pred_retard.py`, `forecast_load.py`
> → `train_forecast_iot.py`) ; l'étape « insights automatiques » (`ml/insights.py`) est
> **reportée en Phase 5** (non contractuelle). Résultats chiffrés : `docs/06_phase3_ml.md`.

| # | Étape | Code produit | Statut & résultats |
|---|---|---|---|
| 3.1 | Feature engineering | `silver_db.conformed_commandes_enrichies` (météo intégrée) | ✅ |
| 3.2 | Classification retards (XGBoost) | `ml/train_pred_retard.py` | ✅ **AUC = 0,73** ; top features : saisonnalité, météo, fret |
| 3.3 | Prévision CA par catégories (Prophet) | `ml/train_forecast_ca.py` | ✅ 6 séries, 90 j ; **WAPE 36 %** (grandes séries) |
| 3.4 | Prévisions IoT (Prophet) | `ml/train_forecast_iot.py` | ✅ 4 séries ; **WAPE 1,6-33,5 %** ; 4/4 baselines battues |
| 3.5 | Prédictions vers Gold/PostgreSQL | `gold.ml_pred_retard_*`, `gold.ml_forecast_ca*`, `gold.ml_forecast_iot*` (7 tables) | ✅ Hive + PostgreSQL |
| 3.6 | Green Book ML (fuites / colinéarité) | `docs/06_phase3_ml.md` §3.3 | ✅ |
| 3.7 | DAG `dag_07_ml_train` | ré-entraînement hebdo (dimanche 06:00) + contrôle de fumée | ✅ validé 4/4 vert ; `max_active_runs=1` (anti-SIGTERM) |

**Livrable contractuel (20/12/2026)** : module ML intégré, prédictions visibles en BI. ✅ **atteint le 19/11**

## Phase 4 — Dashboard & qualité de données *(🔄 en cours)*

| # | Étape | Résultat | Statut |
|---|---|---|---|
| 4.1 | Module DQ | `data_quality/checks.py` + `dq.dq_checks` (41 contrôles, 6 familles) | ✅ **fait en Phase 2** |
| 4.2 | Alerting | `dags/dag_06_data_quality.py` (échec DAG si contrôle KO) | ✅ fait en Phase 2 |
| 4.3 | Vues Power BI | `sql/postgres/03_powerbi_views.sql` (5 vues `vue_pbi_*` testées en base) | ✅ 19/11 |
| 4.4 | Guide de construction | `docs/07_powerbi_guide.md` (pas-à-pas, DAX prêt, 6 pages) | ✅ 19/11 |
| 4.5 | Modèle Power BI | `powerbi/CIDSEC.pbix` (6 pages) — **à monter à la main via le guide, ~45 min** | 📋 |
| 4.6 | Recette utilisateur | scénarios de test avec les parties prenantes (Finance, Opérations) | 📋 |

**Livrable contractuel (20/01/2027)** : dashboard interactif + monitoring qualité.

## Phase 5 — Tests, documentation & soutenance *(📋 à venir — 22/01 → 12/02/2027)*

| # | Étape | Livrable | Statut |
|---|---|---|---|
| 5.1 | Tests d'intégration complets | rejeu depuis zéro sur machine neuve (procédure `docs/04`) | 📋 |
| 5.2 | Rapport technique final | `docs/05_rapport_final.md` (architecture, décisions, résultats, limites) | 📋 |
| 5.3 | Guide d'utilisation & formation | `docs/04_guide_installation.md` + guide Power BI pour les métiers | 📋 |
| 5.4 | Insights automatiques (reporté de la Phase 3) | `ml/insights.py` (variations atypiques, résumés) | 📋 optionnel |
| 5.5 | Présentation de soutenance | slides + démo live (CSV → dashboard) | 📋 |

**Livrable contractuel (12/02/2027)** : rapport + présentation + dépôt GitHub complet.

---

## Journal de réalisation (chronologie) / Build log

| Date | Étape réalisée | Preuve / résultat |
|---|---|---|
| 02/10/2026 | Phase 1 livrée | `docs/01`, `docs/02`, ce plan |
| 05-07/10/2026 | Phase 0 : plateforme + sources étendues | 13 conteneurs, 6 sources, 22 anomalies |
| 07-21/10/2026 | Phase 2 : jobs Bronze/Silver/Gold/DQ écrits, ~15 bugs de rejeu corrigés et documentés (FR/EN) | commits GitHub |
| 12/10/2026 | Première chaîne Silver→Gold verte | `fact_commande = 99 441` |
| 18/11/2026 | Bug racine : mélange de snapshots Bronze → règle `read_latest_snapshot` | `fact_trades = 1 222 553` ✅ |
| 21/11/2026 | **CHAINE-COMPLETE-SUCCES** : Silver ✅ Gold ✅ Qualité ✅ (41/41) | push `5fe6cb5` |
| 19/11/2026 | Phase 3 : 3 modèles ML entraînés et validés | AUC 0,73 · WAPE 36 % · WAPE 1,6 % |
| 19/11/2026 | `dag_07_ml_train` validé 4/4 vert | push `8cc9971` |
| 19/11/2026 | Phase 4 : 5 vues Power BI + guide manuel complet | push `b752dfc` |

---

## Estimation de charge (par phase)

| Phase | Charge estimée | Statut réel |
|---|---|---|
| 0 — Environnement | 3-5 jours | ✅ fait |
| 1 — Audit & architecture | 2 semaines | ✅ fait (livré en avance) |
| 2 — Pipeline ETL/ELT | 4 semaines | ✅ fait (validé le 21/11) |
| 3 — ML | 4 semaines | ✅ fait **en avance** (19/11 vs échéance 20/12) |
| 4 — BI & DQ | 3 semaines | 🔄 ~80 % (reste le `.pbix` + recette) |
| 5 — Clôture | 3 semaines | 📋 à venir |

## Risques identifiés

| Risque | Impact | Parade | Bilan |
|---|---|---|---|
| RAM insuffisante (Hive+Spark+Airflow) | blocage | `--driver-memory 1g`, swap Docker | ✅ tenu |
| Versions incompatibles entre images | erreurs runtime | épingler les versions | ✅ tenu |
| Perte des données locales (`Données/`) | re-téléchargement | conserver les zips (hors Git, volontaire) | ✅ |
| Instabilité Docker Desktop / WSL | arrêts de jobs | `wsl --update`, redémarrage moteur | ✅ réglé |
| Courses de runs Airflow (SIGTERM) | jobs tués | pause `dag_01`, `max_active_runs=1`, 3 tâches max | ✅ réglé |
| Glissement de planning | livrables en retard | prioriser le cœur avant le ML | ✅ **en avance** |

## Prochaine étape immédiate

1. **Construire `powerbi/CIDSEC.pbix`** en suivant `docs/07_powerbi_guide.md` (~45 min à la main).
2. Recette utilisateur (page par page avec les métiers).
3. Phase 5 : tests de rejeu sur machine neuve + rapport final + soutenance.
