# -*- coding: utf-8 -*-
"""
=====================================================================================
dag_01_ingest_bronze.py — DAG Airflow : INGESTION vers la couche Bronze
                           Airflow DAG: INGESTION into the Bronze layer

POURQUOI / WHY :
  Ce DAG automatise ce qu'un humain pourrait faire a la main (d'ou la documentation
  des commandes equivalentes ci-dessous) : charger chaque source dans la Bronze,
  dans un ordre garanti, avec reprise automatique en cas d'echec.
  This DAG automates what a human could do by hand (hence the equivalent manual
  commands below): load every source into Bronze, in a guaranteed order, with
  automatic retry on failure.

COMMENT / HOW :
  3 groupes de taches / 3 task groups :
    1. API      : appels REST (PythonOperator, bibliotheque standard) -> JSON archives
    2. STATIQUE : les 10 fichiers CSV/TXT (SparkSubmitOperator -> bronze_ingest.py)
    3. IoT      : OPSD + UCI (SparkSubmitOperator -> bronze_ingest.py)
  Toutes les taches Spark soumettent au cluster spark://spark-master:7077.
  All Spark tasks submit to the spark://spark-master:7077 cluster.

EQUIVALENT A LA MAIN / MANUAL EQUIVALENT (a comprendre et tester avant Airflow) :
  # API :
  python /opt/project/ingestion/ingest_api_meteo.py
  python /opt/project/ingestion/ingest_api_fx.py
  # Un fichier (exemple commandes Olist) :
  spark-submit --master spark://spark-master:7077 \
      /opt/project/spark_jobs/bronze_ingest.py \
      --source olist --table olist_orders_dataset \
      --input /landing/E-Commerce/olist_orders_dataset.csv --sep ,

VERIFICATION :
  Airflow UI http://localhost:8180 -> DAG "dag_01_ingest_bronze" -> toutes les taches vertes
  beeline -u jdbc:hive2://localhost:10000 -e "SHOW TABLES IN bronze_db;"
=====================================================================================
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# --- Parametres communs aux soumissions Spark / shared Spark submit parameters -----
SPARK_MASTER = "spark://spark-master:7077"
SPARK_CONF = {
    # Memoire explicite : protege le poste de developpement (cf. docs/02 §13)
    # Explicit memory: protects the dev machine (see docs/02 §13)
    "spark.driver.memory": "1g",
    "spark.executor.memory": "1g",
}

# --- Registre des sources statiques / static source registry ----------------------
# POURQUOI / WHY : declarer les sources dans un tableau -> ajouter un fichier = ajouter
# une ligne, jamais modifier de logique. C'est le principe "configuration sur code".
# Declaring sources in a table -> adding a file = adding one row, never touching
# logic. "Configuration over code" principle.
STATIC_SOURCES = [
    # (groupe, table cible, fichier source, separateur)
    # (group, target table, source file, separator)
    ("olist", "olist_customers_dataset", "/landing/E-Commerce/olist_customers_dataset.csv", ","),
    ("olist", "olist_geolocation_dataset", "/landing/E-Commerce/olist_geolocation_dataset.csv", ","),
    ("olist", "olist_order_items_dataset", "/landing/E-Commerce/olist_order_items_dataset.csv", ","),
    ("olist", "olist_order_payments_dataset", "/landing/E-Commerce/olist_order_payments_dataset.csv", ","),
    ("olist", "olist_order_reviews_dataset", "/landing/E-Commerce/olist_order_reviews_dataset.csv", ","),
    ("olist", "olist_orders_dataset", "/landing/E-Commerce/olist_orders_dataset.csv", ","),
    ("olist", "olist_products_dataset", "/landing/E-Commerce/olist_products_dataset.csv", ","),
    ("olist", "olist_sellers_dataset", "/landing/E-Commerce/olist_sellers_dataset.csv", ","),
    ("olist", "product_category_name_translation", "/landing/E-Commerce/product_category_name_translation.csv", ","),
    ("cymbal", "cymbal_investments_trade_capture_report",
     "/landing/Investissment_Cymball/cymbal_investments_trade_capture_report.csv", ","),
    ("iot", "opsd_time_series_60min", "/landing/IoT_Energie/opsd_time_series_60min.csv", ","),
    ("iot", "uci_household_power", "/landing/UCI_power/household_power_consumption.txt", ";"),
]

# --- Fonctions d'ingestion API / API ingestion functions --------------------------
def _run_script(script, extra_args=None):
    """
    Execute un script d'ingestion API dans le meme processus Python.
    Runs an API ingestion script in the same Python process.

    POURQUOI / WHY : les scripts sont ecrits pour etre executables A LA MAIN aussi
    (python ingest_api_meteo.py). Les importer ici garantit UN SEUL code, deux usages.
    The scripts are written to ALSO run manually. Importing them here guarantees
    ONE code, two uses.
    """
    import runpy
    import sys
    sys.argv = [script] + (extra_args or [])
    runpy.run_path(script, run_name="__main__")


def ingest_meteo():
    """Appel API meteo + archivage JSON / weather API call + JSON archiving."""
    _run_script("/opt/project/ingestion/ingest_api_meteo.py",
                ["--start", "2016-09-01", "--end", "2018-10-31", "--out-dir", "/landing_api"])


def ingest_fx():
    """Appel API taux de change + archivage JSON / FX API call + JSON archiving."""
    _run_script("/opt/project/ingestion/ingest_api_fx.py",
                ["--start", "2020-05-18", "--end", "2020-11-23", "--out-dir", "/landing_api"])


def bronze_task(group, table, input_path, sep, fmt="csv"):
    """
    Fabrique une tache Spark de chargement Bronze / builds one Bronze Spark load task.
    POURQUOI : une tache par fichier -> un fichier en erreur n'empeche pas les autres.
    WHY     : one task per file -> one failing file does not block the others.
    """
    return SparkSubmitOperator(
        task_id=f"bronze_{table}",
        application="/opt/project/spark_jobs/bronze_ingest.py",
        conn_id="spark_default",
        name=f"bronze_{table}",
        conf=SPARK_CONF,
        application_args=[
            "--source", group,
            "--table", table,
            "--input", input_path,
            "--sep", sep,
            "--format", fmt,
        ],
        verbose=True,   # logs detailles dans Airflow / detailed logs in Airflow
    )


# --- Definition du DAG / DAG definition ------------------------------------------
default_args = {
    "owner": "cidsec-data-team",
    "depends_on_past": False,
    "retries": 2,                                  # 2 nouvelles tentatives / 2 retries
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_01_ingest_bronze",
    description="Ingestion de toutes les sources vers la Bronze / ingest all sources into Bronze",
    default_args=default_args,
    # POURQUOI max_active_tasks=3 : 16 taches Spark en parallele saturent la machine
    # (2 cœurs) -> le scheduler Airflow perd ses heartbeats -> les taches se tuent
    # elles-memes ("Heartbeat time limit exceeded", observe le 07/10/2026).
    # 3 par 3 : meme resultat, 5x moins de pression ressource.
    # WHY max_active_tasks=3: 16 parallel Spark tasks saturate the machine (2 cores)
    # -> Airflow scheduler loses heartbeats -> tasks kill themselves. 3 at a time:
    # same result, 5x less resource pressure.
    max_active_tasks=3,
    # Quotidien a 02:00, avec en plus un "catchup" desecactive (pas de rejeu historique
    # automatique : les donnees sources sont figees).
    # Daily at 02:00, catchup disabled (no historical replay: source data is frozen).
    start_date=datetime(2026, 10, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["bronze", "ingestion"],
) as dag:

    # --- Groupe 1 : API (appels REST) / API group (REST calls) -------------------
    t_meteo = PythonOperator(task_id="api_fetch_meteo", python_callable=ingest_meteo)
    t_fx = PythonOperator(task_id="api_fetch_fx", python_callable=ingest_fx)

    # --- Groupe 2+3 : chargement Bronze / Bronze load ---------------------------
    tasks = {}
    for group, table, path, sep in STATIC_SOURCES:
        tasks[table] = bronze_task(group, table, path, sep)

    # Les API doivent etre appellees AVANT leur chargement Bronze.
    # APIs must be called BEFORE their Bronze load.
    # --format json : la Bronze archive le JSON BRUT (cf. bronze_ingest.py).
    t_bronze_meteo = bronze_task("api", "api_meteo_sp_daily", "/landing_api/openmeteo_*.json", ",", fmt="json")
    t_bronze_fx = bronze_task("api", "api_taux_change", "/landing_api/frankfurter_*.json", ",", fmt="json")

    # Enchaine / chain :
    # 1) Les fichiers statiques se chargent en parallele (aucune dependance entre eux)
    #    Static files load in parallel (no dependency between them)
    # 2) API fetch -> API bronze
    t_meteo >> t_bronze_meteo
    t_fx >> t_bronze_fx
