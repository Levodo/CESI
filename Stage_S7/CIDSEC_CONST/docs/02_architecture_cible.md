# 02 — Architecture cible (Data Pipeline Medallion)

**Projet** : CIDSEC_CONST — Pipeline de données de bout en bout + analytics pilotée par IA
**Auteur** : Assistant Data Engineer / Cloud Solution Architect
**Date** : 02/10/2026 — **Statut** : v1.0 (à valider avant implémentation)

---

## 1. Vue d'ensemble

L'architecture suit le modèle **Medallion** (Bronze → Silver → Gold), conteneurisé avec
**Docker**, et orchestré par **Airflow**. Les données transitent par **HDFS** (stockage
fichiers), **Apache Hive** (catalogue de tables interrogeables), **Apache Spark**
(transformation, y compris les écritures vers PostgreSQL via **JDBC**), et aboutissent
dans **PostgreSQL** (couche de service) consommée par **Power BI**.

```
                    ┌──────────────────────────────────────────────────────────┐
   SOURCES          │                     PIPELINE (Docker)                    │       CONSOMMATEURS
┌─────────────┐     │  ┌─────────┐   ┌──────────┐   ┌──────────┐   ┌────────┐  │     ┌──────────┐
│ CSV Kaggle  │────▶│  │ Landing │──▶│  BRONZE  │──▶│  SILVER  │──▶│  GOLD  │──┼────▶│ Power BI │
│ (Olist,     │     │  │  Zone   │   │ (HDFS +  │   │ (HDFS +  │   │ HDFS + │  │     └──────────┘
│  Cymbal)    │     │  │ /landing│   │  Hive)   │   │  Hive)   │   │ Hive + │  │     ┌──────────┐
├─────────────┤     │  └─────────┘   └──────────┘   └──────────┘   │ Postgr.│──┼────▶│ ML / IA  │
│ API REST    │────▶│       ▲              ▲              ▲         └────────┘  │     └──────────┘
│ (météo...)  │     │       │              │              │              │      │
└─────────────┘     │  ┌─────────────────────────────────────────────┐ │      │
                    │  │        AIRFLOW : orchestration + DQ          │◀┘      │
                    │  └─────────────────────────────────────────────┘         │
                    └──────────────────────────────────────────────────────────┘
```

---

## 2. Les 3 couches Medallion — qui fait quoi

| Couche | Rôle | Règle d'or | Format / Emplacement |
|---|---|---|---|
| **Bronze** | Copie fidèle de la source, datée, immutable | **Aucune modification des données** — on ajoute seulement des colonnes techniques | Parquet partitionné dans HDFS `/bronze/…`, tables Hive externes |
| **Silver** | Données propres, typées, dédupliquées, conformes | Réutilisable par tout le monde ; une ligne = une entité métier propre | Parquet dans HDFS `/silver/…`, tables Hive |
| **Gold** | Agrégats métier + features ML prêts à consommer | Orienté décision : KPI, séries, tables étoile pour Power BI | Parquet Hive `/gold/…` **et** tables PostgreSQL `gold.*` |

Colonnes techniques ajoutées dès la Bronze (traçabilité) :
`_ingestion_ts`, `_source_file`, `_load_date` (partition), `_record_hash`.

### Croisements multi-sources (cohérence)

La couche Silver produit en `silver/conformed/` des jeux enrichis qui relient chaque
source batch à sa **source pivot** (voir §8.2 du rapport d'audit) :

| Jeu enrichi | Jointure | Usage |
|---|---|---|
| `conformed.commandes_enrichies` | commandes Olist ⋈ météo São Paulo (sur date de commande et date de livraison) | analyse et ML « impact météo sur les retards » |
| `conformed.trades_enrichis` | trades Cymbal ⋈ taux de change (sur `TradeDate`) | valorisation multi-devise, analyses de marché |
| `conformed.energie_pivot` | série OPSD agrégée jour/pays | contexte macro + ML forecasting |

---

## 3. Composants techniques et justification

| Composant | Version cible | Rôle dans le projet | Pourquoi il est là |
|---|---|---|---|
| Docker + Docker Compose | v27+ | Conteneurisation de toute la plateforme | Reproductibilité : « ça marche sur ma machine » devient « ça marche partout » |
| HDFS (NameNode + DataNode) | Hadoop 3.3.x | Stockage des fichiers Bronze/Silver/Gold | C'est le stockage distribué demandé ; simule un vrai data lake |
| Apache Hive (Metastore + Server2) | 3.1.x / 4.0 | Catalogue de tables + requêtes SQL (beeline) | Permet de requêter le lac en SQL sans re-copier les données |
| PostgreSQL | 16 | 3 bases : `airflow_db`, `hive_metastore`, `gold_db` | Métadonnées Hive + orchestration + **couche de service pour Power BI** |
| Apache Spark (Master + Worker) | 3.5.x | Toutes les transformations (lecture/écriture JDBC incluse) | Moteur distribué demandé (« Spark JDBC ») |
| Apache Airflow | 2.10.x | Ordonnancement des pipelines, monitoring, alertes | Planification, reprise sur erreur, visibilité des exécutions |
| Power BI Desktop | dernière | Tableau de bord interactif | Outil BI imposé ; se connecte à PostgreSQL |
| GitHub | — | Versioning du code et de la doc | Collaboration, historique, revue |
| Python | 3.11 (dans les conteneurs) | Jobs PySpark, scripts d'ingestion API, DQ | Langue pivot de l'écosystème data |

### Ressources machine minimales
- **RAM** : 8 Go alloués à Docker (16 Go conseillés) — l'empilement Hive + Spark + Airflow est gourmand.
- **Disque** : 25 Go libres (images Docker ~12 Go + données ~2 Go).
- **CPU** : 4 cœurs minimum.
- Docker Desktop **démarré** avant toute manipulation (vérifier avec `docker ps`).

---

## 4. Arborescence HDFS (Data Lake)

```
/data
├── landing/            # dépose brute des fichiers sources (non versionné, non modifié)
│   ├── olist/          # les 9 CSV Olist
│   ├── cymbal/         # le CSV trade capture
│   ├── iot/            # séries temporelles : UCI foyer (minute) + OPSD pays (horaire)
│   ├── api/            # réponses API datées (météo, taux de change…)
│   └── readme.txt      # provenance et date de chaque dépôt (traçabilité)
├── bronze/
│   ├── olist/<table>/_load_date=AAAA-MM-JJ/*.parquet
│   ├── cymbal/<table>/_load_date=AAAA-MM-JJ/*.parquet
│   ├── iot/<serie>/_load_date=AAAA-MM-JJ/*.parquet
│   └── api/<flux>/_load_date=AAAA-MM-JJ/*.parquet
├── silver/
│   ├── olist/<table>/...      # mêmes tables, nettoyées
│   ├── cymbal/<table>/...
│   ├── iot/<serie>/...        # série par pays, sous-ensemble de colonnes
│   ├── api/meteo_quotidien/...  api/taux_change/...
│   └── conformed/...          # entités intégrées multi-sources (croisements)
├── gold/
│   ├── kpi_ca_quotidien/...
│   ├── kpi_livraison/...
│   ├── dim_client/...  dim_produit/...  dim_vendeur/...  dim_date/...
│   ├── fact_commande/...
│   └── features_ml/...
├── models/                    # modèles ML entraînés (sérialisés)
└── dq/                        # rapports de qualité de données (JSON)
```

**Pourquoi des partitions `_load_date`** : chaque exécution journalière écrit sa propre
partition. Une re-exécution (rattrapage) ne détruit jamais les données d'hier →
**idempotence** et **historique immuable** (principe Bronze).

---

## 5. Bases de données Hive (catalogue sur les fichiers HDFS)

| Base Hive | Contenu | Type de table |
|---|---|---|
| `bronze_db` | Miroir 1:1 des sources (9 tables Olist, 1 table Cymbal, tables API) | EXTERNAL, format Parquet |
| `silver_db` | Tables nettoyées + vues métier enrichies (ex: `v_commandes_complete`) | EXTERNAL, format Parquet |
| `gold_db` | KPI et agrégats finaux | EXTERNAL, format Parquet |

> Hive ne **possède** pas les données : il décrit les fichiers Parquet de HDFS.
> Supprimer une table Hive ne supprime pas les fichiers (et inversement).

---

## 6. PostgreSQL — base de service

| Base / Schéma | Usage | Consommateur |
|---|---|---|
| `airflow_db` | Métadonnées Airflow (DAGs, runs, variables) | Airflow |
| `hive_metastore` | Catalogue Hive | Hive Metastore |
| `gold_db` (schéma `gold`) | Tables agrégées alimentées par **Spark JDBC** | **Power BI**, requêtes SQL ad hoc |
| `gold_db` (schéma `dq`) | Résultats des contrôles qualité (`dq_checks`) | Airflow (alertes), Power BI (page monitoring) |

**Flux Spark JDBC** : à la fin de la couche Gold, Spark écrit les tables finales vers
PostgreSQL (`df.write.format("jdbc")`) — c'est le point d'intégration « Spark JDBC »
demandé. Power BI lit ensuite PostgreSQL (mode Import recommandé).

---

## 7. Ordonnancement Airflow

| DAG | Fréquence | Contenu |
|---|---|---|
| `dag_01_ingest_bronze` | journalier (02:00) | API (météo, FX) + **toutes** les sources statiques → Bronze (12 tables + 2 JSON) |
| `dag_04_silver_transform` | journalier (03:30) | Bronze → Silver (4 jobs parallèles) puis `conformed_build` (jeux enrichis croisés) |
| `dag_05_gold_build` | journalier (04:30) | Silver → Gold (étoile) → PostgreSQL via JDBC + contrôle de fumée |
| `dag_06_data_quality` | journalier (05:00) | contrôles qualité (6 familles) → table `dq.dq_checks` → alerte si échec |
| `dag_07_ml_training` | hebdomadaire *(phase 3)* | entraînement modèles → `/data/models` → prédictions vers Gold |

> Implémentation réelle : l'ingestion de **toutes** les sources (y compris API et IoT)
> est regroupée dans `dag_01_ingest_bronze` — un fichier en échec ne bloque pas les
> autres (une tâche par fichier), et l'ordre d'exécution reste garanti par les
> dépendances inter-DAG (01 → 04 → 05 → 06).

Chaîne de dépendances : `ingest → silver → gold → ml`, chaque étape déclenchée par la
fin de la précédente (sensors ou chaînage inter-DAG).

---

## 8. Qualité des données & alertes

Module `data_quality/` (Python, exécuté par Airflow) : chaque contrôle écrit une ligne
dans `dq_checks` :

| Contrôle | Exemple | Seuil d'alerte |
|---|---|---|
| Fraîcheur | `_load_date` du jour présent en Bronze | > 24 h d'écart = alerte |
| Comptage | nb lignes Silver > 0 et ±30 % vs veille | variation > 30 % = alerte |
| Complétude | taux de NULL sur `order_delivered_customer_date` | > 5 % = alerte |
| Validité | `review_score` ∈ [1,5], `price` > 0 | toute anomalie = alerte |
| Unicité | `order_id` unique dans `silver.orders` | tout doublon = alerte |
| Intégrité | aucune clé orpheline enfant → parent | toute orpheline = alerte |

**Alerting** : notification Airflow (onglet Alerts + callback `on_failure`) ;
extension possible email/Slack (à paramétrer dans `.env`).

---

## 9. Composant ML (phase 3)

| Cas d'usage | Données | Modèle | Sortie |
|---|---|---|---|
| Prévision du CA quotidien | `gold.kpi_ca_quotidien` | Prophet (et XGBoost en challenger) | `gold.forecast_ca` (30 j) |
| Prédiction du retard de livraison | `silver.v_commandes_complete` **+ météo São Paulo** | XGBoost classification | `gold.pred_retard` |
| Prévision de charge électrique (IoT) | `silver.iot.energie` (OPSD, 50 k points horaires) | Prophet / XGBoost régression | `gold.forecast_charge` (7 j) |
| Prévision de consommation foyer | `silver.iot.foyer_1h` (UCI, 2 M points à la minute) | Prophet / XGBoost régression | `gold.forecast_foyer` (48 h) |
| Insight automatique | agrégats Gold | règles + résumés générés | page « Insights » Power BI |

Entraînement dans le conteneur Spark/Python, modèle sérialisé sur `/data/models`,
prédictions écrites en Gold puis propagées vers PostgreSQL pour Power BI.

---

## 10. Power BI

- **Connexion** : PostgreSQL (`localhost:5432`, base `gold_db`, utilisateur dédié `powerbi`).
- **Modèle en étoile** : `fact_commande` + `dim_date`, `dim_client`, `dim_produit`, `dim_vendeur`.
- **Pages** : Vue d'ensemble CA — Analyse produits/catégories — Performance livraison —
  Satisfaction client — Prévisions (ML) — Monitoring qualité des données.
- **Rafraîchissement** : manuel sur Desktop ; planifié ultérieurement via Gateway si besoin.

---

## 11. GitHub — conventions

```
CIDSEC_CONST/
├── README.md                  # démarrage rapide
├── docker/                    # docker-compose.yml, Dockerfiles, configs (hdfs, hive, airflow)
├── dags/                      # DAGs Airflow
├── spark_jobs/                # jobs PySpark (bronze, silver, gold, ml)
├── sql/                       # scripts HiveQL / PostgreSQL (DDL, vues)
├── data_quality/              # module de contrôles qualité
├── ingestion/                 # scripts d'ingestion API
├── powerbi/                   # fichier .pbix + dictionnaire de données
├── docs/                      # ce dossier
├── .gitignore                 # exclut Données/, *.csv, *.zip, .env
└── .env.example               # variables d'environnement commentées
```

Workflow : `main` (stable) ← `develop` ← branches `feature/*` ; commits conventionnels
(`feat:`, `fix:`, `docs:`) ; **aucune donnée** ni secret dans Git.

---

## 12. Décisions d'architecture (simplifiées)

| Décision | Retenue | Alternative écartée | Raison |
|---|---|---|---|
| Modèle de données | Medallion Bronze/Silver/Gold | ETL direct source → BI | Traçabilité, rejeu possible, clarté pour l'équipe |
| Stockage des données | HDFS + Parquet + Hive | Tout dans PostgreSQL | HDFS/Hive imposés ; PostgreSQL gardé en couche de service |
| Consommation BI | PostgreSQL via Spark JDBC | ODBC Hive vers Power BI | JDBC simple, robuste, documenté ; Power BI adore PostgreSQL |
| Ordonnancement | Airflow | Cron / scripts manuels | Monitoring, reprise, dépendances, UI |
| DQ | module Python interne | Great Expectations | Léger, transparent, pédagogique ; GX possible en extension |
| Conteneurs | Docker Compose | Kubernetes | Sur-mesure pour une équipe et une machine ; K8s = surdimensionné |

---

## 13. Points d'attention connus

1. **RAM** : Hive Metastore + Spark + Airflow ≈ 6-8 Go. Fermer les apps lourdes ; ajuster
   `driver_memory` / `executor_memory` dans les jobs si échecs OOM.
2. **Windows** : les chemins Docker sont montés via `./data:/data` (chemins relatifs,
   jamais `C:\...` dans docker-compose).
3. **Docker Desktop doit être démarré** avant `docker compose up` (contrôler `docker ps`).
4. **Premier démarrage** : téléchargement des images (~20 min) puis initialisation Hive
   (schéma de métadonnées créé automatiquement au premier lancement).
5. **Pas de données dans Git** : seul le chemin local `Données/` est utilisé, décrit dans
   la documentation (et le zip d'origine conservé tel quel).
