# -*- coding: utf-8 -*-
"""
=====================================================================================
dag_04_silver_transform.py — DAG Airflow : TRANSFORMATION Bronze -> Silver
                              Airflow DAG: TRANSFORMATION Bronze -> Silver

POURQUOI / WHY :
  La Silver est la couche de CONFIANCE : ce DAG applique, dans un ordre libre entre
  jobs (aucune dependance inter-sources), les regles de l'audit (A1-A13, B1-B8).
  The Silver layer is the TRUST layer: this DAG applies, in any order between jobs
  (no inter-source dependency), the audit rules (A1-A13, B1-B8).

COMMENT / HOW :
  5 taches Spark / 5 Spark tasks :
    silver_olist.py, silver_cymbal.py, silver_iot.py, silver_api.py   (paralleles)
    conformed_build.py (APRES olist + cymbal + api : il croise leurs sorties)
  Chaque job lit la Bronze (bronze_db.*) et ecrit la Silver (silver_db.*).

EQUIVALENT A LA MAIN / MANUAL EQUIVALENT :
  spark-submit --master spark://spark-master:7077 /opt/project/spark_jobs/silver_olist.py
  spark-submit --master spark://spark-master:7077 /opt/project/spark_jobs/silver_cymbal.py
  spark-submit --master spark://spark-master:7077 /opt/project/spark_jobs/silver_iot.py
  spark-submit --master spark://spark-master:7077 /opt/project/spark_jobs/silver_api.py

VERIFICATION :
  beeline -u jdbc:hive2://localhost:10000 -e "SHOW TABLES IN silver_db;"
  SELECT count(*) FROM silver_db.olist_order_reviews;   -- < 99 224 (dedup A4)
=====================================================================================
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_CONF = {
    "spark.driver.memory": "1g",
    "spark.executor.memory": "2g",   # UCI = 2M lignes : un peu plus de memoire
}

default_args = {
    "owner": "cidsec-data-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def silver_task(task_id, script, name):
    """Tache de transformation Silver / one Silver transformation task."""
    return SparkSubmitOperator(
        task_id=task_id,
        application=f"/opt/project/spark_jobs/{script}",
        conn_id="spark_default",
        name=name,
        conf=SPARK_CONF,
        verbose=True,
    )


with DAG(
    dag_id="dag_04_silver_transform",
    description="Nettoyage/conformisation Bronze -> Silver / cleaning Bronze -> Silver",
    default_args=default_args,
    # Meme raison que dag_01 : limiter la pression sur le scheduler (2 cœurs).
    # Same reason as dag_01: limit pressure on the scheduler (2 cores).
    max_active_tasks=2,
    start_date=datetime(2026, 10, 1),
    schedule_interval="30 3 * * *",   # apres l'ingestion Bronze (02:00-03:30)
    catchup=False,
    tags=["silver", "transformation"],
) as dag:

    # POURQUOI l'ordre libre ? Aucun job Silver ne lit une table ecrite par un autre :
    # ils sont donc en parallele, et un echec isole ne bloque pas les autres.
    # WHY free ordering? No Silver job reads another's output: they run in parallel,
    # and one isolated failure does not block the others.
    t_olist = silver_task("silver_olist", "silver_olist.py", "silver_olist")
    t_cymbal = silver_task("silver_cymbal", "silver_cymbal.py", "silver_cymbal")
    t_iot = silver_task("silver_iot", "silver_iot.py", "silver_iot")
    t_api = silver_task("silver_api", "silver_api.py", "silver_api")

    # --- Jeux enrichis multi-sources / multi-source enriched datasets ---------------
    # POURQUOI / WHY : conformed_build CROISE les sorties (commandes x meteo,
    # trades x taux, energie agregee). Il doit donc attendre les 4 jobs sources.
    # conformed_build CROSSES outputs (orders x weather, trades x FX, aggregated
    # energy). It must wait for the 4 source jobs.
    t_conformed = silver_task("conformed_build", "conformed_build.py", "conformed_build")
    [t_olist, t_cymbal, t_api, t_iot] >> t_conformed
