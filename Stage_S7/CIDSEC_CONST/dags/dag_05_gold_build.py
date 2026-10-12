# -*- coding: utf-8 -*-
"""
=====================================================================================
dag_05_gold_build.py — DAG Airflow : CONSTRUCTION de la couche Gold + export JDBC
                        Airflow DAG: GOLD layer build + JDBC export

POURQUOI / WHY :
  Etape finale du pipeline batch : construit le modele en etoile (faits + dimensions)
  et le recopie dans PostgreSQL, la seule source que Power BI consomme. Ce DAG
  DOIT attendre la fin de la Silver (dependance metier explicite ci-dessous).
  Final step of the batch pipeline: builds the star schema (facts + dimensions) and
  copies it into PostgreSQL, the only source Power BI consumes. This DAG MUST wait
  for Silver completion (explicit business dependency below).

COMMENT / HOW :
  Une tache Spark (gold_build.py) + un controle de fumee SQL qui verifie que les
  tables attendues existent bien dans PostgreSQL apres l'export.
  One Spark task (gold_build.py) + a SQL smoke check verifying expected tables do
  exist in PostgreSQL after the export.

EQUIVALENT A LA MAIN / MANUAL EQUIVALENT :
  spark-submit --master spark://spark-master:7077 /opt/project/spark_jobs/gold_build.py
  docker exec cidsec-postgres psql -U spark -d gold_db -c "\\dt gold.*"

VERIFICATION :
  docker exec cidsec-postgres psql -U spark -d gold_db \
      -c "SELECT * FROM gold.kpi_ca_quotidien ORDER BY jour DESC LIMIT 5;"
  Power BI : se connecter a postgresql://localhost:5432, base gold_db, schema gold
=====================================================================================
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_CONF = {
    "spark.driver.memory": "1g",
    "spark.executor.memory": "2g",
}

default_args = {
    "owner": "cidsec-data-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="dag_05_gold_build",
    description="Modele en etoile Gold + export PostgreSQL / Gold star schema + PostgreSQL export",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule_interval="30 4 * * *",   # apres la Silver (03:30)
    catchup=False,
    tags=["gold", "bi"],
) as dag:

    t_gold = SparkSubmitOperator(
        task_id="gold_build",
        application="/opt/project/spark_jobs/gold_build.py",
        conn_id="spark_default",
        name="gold_build",
        conf=SPARK_CONF,
        verbose=True,
    )

    # Controle de fumee / smoke check :
    # POURQUOI : un job Spark "vert" ne garantit pas que Power BI voit les donnees.
    # Ce controle verifie le VRAI point de consommation : la base PostgreSQL.
    # WHY: a "green" Spark job does not guarantee Power BI sees the data. This check
    # verifies the REAL consumption point: the PostgreSQL database.
    # COMMENT : psycopg2 est deja installe dans l'image Airflow ; on interroge
    #            postgres:5432 depuis le reseau Docker (pas besoin de docker CLI).
    #            psycopg2 ships with the Airflow image; queries postgres:5432 over
    #            the Docker network (no docker CLI needed).
    def check_postgres_gold():
        import psycopg2
        conn = psycopg2.connect(host="postgres", port=5432, dbname="gold_db",
                                user="spark", password="spark")
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema='gold' ORDER BY table_name")
                tables = [row[0] for row in cur.fetchall()]
                print("[check] tables du schema gold / gold schema tables:", tables)
                attendues = {"fact_commande", "fact_trades", "kpi_ca_quotidien", "dim_date"}
                manquantes = attendues - set(tables)
                assert not manquantes, f"Tables absentes de PostgreSQL / missing tables: {manquantes}"
        finally:
            conn.close()

    t_check = PythonOperator(
        task_id="check_postgres_gold",
        python_callable=check_postgres_gold,
    )

    t_gold >> t_check
