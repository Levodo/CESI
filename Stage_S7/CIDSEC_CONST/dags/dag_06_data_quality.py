# -*- coding: utf-8 -*-
"""
=====================================================================================
dag_06_data_quality.py — DAG Airflow : SUPERVISION de la qualité des données
                         Airflow DAG: DATA QUALITY supervision

POURQUOI / WHY :
  La qualité est mesurée APRÈS les transformations (Silver/Gold), pas avant : c'est
  la donnée consommée par les métiers qui doit être fiable. Ce DAG déclenche les
  contrôles, trace leurs résultats dans PostgreSQL, et ALERTE (échec du DAG) dès
  qu'un contrôle échoue — c'est le canal d'alerte exigé par le sujet.
  Quality is measured AFTER transformations (Silver/Gold), not before: it is the
  data consumed by the business that must be reliable. This DAG triggers the checks,
  traces results in PostgreSQL, and ALERTS (DAG failure) as soon as one check fails.

COMMENT / HOW :
  3 taches enchaînées / 3 chained tasks:
    1. ensure_dq_schema  : crée dq.dq_checks si absent (psycopg2, idempotent)
    2. run_checks        : spark-submit data_quality/checks.py --layer all --fail-on-error
    3. report            : relit les résultats, affiche un résumé, liste les échecs

EQUIVALENT A LA MAIN / MANUAL EQUIVALENT :
  docker exec -i cidsec-postgres psql -U postgres -d gold_db < sql/postgres/02_dq_ddl.sql
  docker exec cidsec-airflow-sched spark-submit --master spark://spark-master:7077 \
      /opt/project/data_quality/checks.py --layer all --fail-on-error

VERIFICATION :
  Airflow UI -> dag_06_data_quality (vert si tous les contrôles passent)
  docker exec cidsec-postgres psql -U spark -d gold_db \
      -c "SELECT run_ts, table_name, check_name, passed FROM dq.dq_checks ORDER BY run_ts DESC LIMIT 20;"
=====================================================================================
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "cidsec-data-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def ensure_dq_schema():
    """
    Crée le schéma dq + la table dq_checks si absents / create dq schema + table if missing.
    POURQUOI / WHY : rendre le DAG autonome — un nouvel arrivant n'a pas à se souvenir
    d'exécuter un script SQL manuel avant la première exécution.
    Make the DAG self-sufficient: a newcomer does not have to remember running a SQL
    script before the first run.
    COMMENT : psycopg2 est déjà présent dans l'image Airflow (cf. dag_05).
    """
    import psycopg2
    conn = psycopg2.connect(host="postgres", port=5432, dbname="gold_db",
                            user="spark", password="spark")
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS dq;")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS dq.dq_checks (
                    run_ts timestamp NOT NULL, layer text NOT NULL,
                    table_name text NOT NULL, check_name text NOT NULL,
                    observed text, expected text, passed boolean NOT NULL,
                    details text);
            """)
        conn.commit()
        print("[dq] schéma/table dq.dq_checks prêt / dq.dq_checks ready")
    finally:
        conn.close()


def report():
    """
    Lit les résultats dans PostgreSQL et échoue si un contrôle du dernier run échoue.
    Reads results from PostgreSQL and fails if any check of the last run failed.

    POURQUOI / WHY : --fail-on-error fait déjà échouer la tache Spark, mais ce rapport
    centralise la LISTE lisible des problèmes dans les logs Airflow : le message
    d'alerte dit quoi corriger, pas seulement « c'est cassé ».
    --fail-on-error already fails the Spark task, but this report centralizes the
    readable LIST of problems in Airflow logs: the alert says WHAT to fix, not just
    "it's broken".
    """
    import psycopg2
    conn = psycopg2.connect(host="postgres", port=5432, dbname="gold_db",
                            user="spark", password="spark")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name, check_name, observed, expected
                FROM dq.dq_checks
                WHERE run_ts = (SELECT MAX(run_ts) FROM dq.dq_checks)
                  AND passed = false
                ORDER BY table_name, check_name;
            """)
            failures = cur.fetchall()
    finally:
        conn.close()

    if failures:
        lignes = "\n".join(f"  - {t}.{c}: vu {o} | attendu {e}" for t, c, o, e in failures)
        raise AirflowException(
            f"{len(failures)} contrôle(s) de qualité en échec / quality check(s) failed:\n{lignes}")
    print("[dq] tous les contrôles sont verts / all checks green")


with DAG(
    dag_id="dag_06_data_quality",
    description="Contrôles qualité + alerte / quality checks + alerting",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule_interval="0 5 * * *",   # après la Gold (04:30) / after Gold (04:30)
    catchup=False,
    tags=["data-quality", "monitoring"],
) as dag:

    t_schema = PythonOperator(task_id="ensure_dq_schema", python_callable=ensure_dq_schema)

    t_checks = SparkSubmitOperator(
        task_id="run_checks",
        application="/opt/project/data_quality/checks.py",
        conn_id="spark_default",
        name="data_quality_checks",
        conf={"spark.driver.memory": "1g", "spark.executor.memory": "1g"},
        application_args=["--layer", "all", "--fail-on-error"],
        verbose=True,
    )

    t_report = PythonOperator(task_id="report_failures", python_callable=report)

    # Enchaînement : schéma -> contrôles -> rapport
    # Chaining: schema -> checks -> report
    t_schema >> t_checks >> t_report
