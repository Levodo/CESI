# -*- coding: utf-8 -*-
"""
=====================================================================================
dag_07_ml_train.py — DAG Airflow : ENTRAÎNEMENT des 3 modèles ML (Phase 3)
                        Airflow DAG: TRAINING of the 3 ML models (Phase 3)

POURQUOI / WHY :
  Orchestre les 3 modèles de la Phase 3 (retards/XGBoost, CA/Prophet, IoT/Prophet) en
  UNE tâche planifiée hebdomadaire, avec un contrôle de fumee final. Sans ce DAG, il
  faudrait lancer les scripts à la main (voir docs/06_phase3_ml.md).
  Orchestrates the 3 Phase 3 models (delays/XGBoost, revenue/Prophet, IoT/Prophet) in
  ONE weekly scheduled task, with a final smoke check. Without this DAG, scripts would
  have to be run by hand (see docs/06_phase3_ml.md).

COMMENT / HOW :
  3 tâches INDÉPENDANTES lancées en parallèle (limite connue : max 3 tâches actives
  simultanément, sinon le scheduler Airflow perd le heartbeat et tue les jobs — réglage
  documenté dans le projet), puis un contrôle SQL qui vérifie que les tables ml_* sont
  bien présentes dans PostgreSQL (schéma gold).
  3 INDEPENDENT tasks run in parallel (known limit: max 3 concurrently active tasks,
  else the Airflow scheduler loses heartbeat and kills jobs — documented project-wide
  setting), then a SQL check verifying the ml_* tables exist in PostgreSQL (gold schema).

  Choix technique / technical choice : BashOperator avec la commande `python ...`
  EXACTEMENT identique à la validation manuelle (docs/06), plutôt que SparkSubmitOperator,
  pour garantir que la commande planifiée = la commande testée.
  BashOperator with the exact `python ...` command used in manual validation (docs/06),
  rather than SparkSubmitOperator, to guarantee scheduled command = tested command.

EQUIVALENT A LA MAIN / MANUAL EQUIVALENT :
  docker exec cidsec-airflow-sched python /opt/project/ml/train_pred_retard.py
  docker exec cidsec-airflow-sched python /opt/project/ml/train_forecast_ca.py
  docker exec cidsec-airflow-sched python /opt/project/ml/train_forecast_iot.py

VERIFICATION :
  docker exec cidsec-postgres psql -U spark -d gold_db \
      -c "SELECT * FROM gold.ml_pred_retard_metriques ORDER BY run_ts DESC LIMIT 1;"
  docker exec cidsec-postgres psql -U spark -d gold_db \
      -c "SELECT serie, wape_test_pct FROM gold.ml_forecast_iot_metriques ORDER BY wape_test_pct;"
=====================================================================================
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "cidsec-data-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="dag_07_ml_train",
    description="Entrainement des 3 modeles ML / Training of the 3 ML models",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule_interval="0 6 * * 0",   # dimanche 06:00 — apres le batch / Sunday 06:00 — after the batch
    catchup=False,
    # POURQUOI max_active_runs=1 : DEUX runs simultanés x 3 tâches = 6 tâches actives,
    #            au-delà de la limite connue (3) — le scheduler perd le heartbeat et
    #            tue les jobs (SIGTERM, constaté le 19/11/2026). Un seul run à la fois.
    # WHY max_active_runs=1: TWO concurrent runs x 3 tasks = 6 active tasks, beyond the
    #      known limit (3) — the scheduler loses heartbeat and kills jobs (SIGTERM,
    #      observed 19/11/2026). One run at a time.
    max_active_runs=1,
    tags=["ml", "ia", "phase3"],
) as dag:

    # --- Modèle 1 : prédiction des retards (XGBoost) / delay prediction --------------
    # POURQUOI ce modèle en premier : il est le plus rapide (~2 min) et ses sorties
    #            alimentent la page « monitoring qualité » de Power BI (Phase 4).
    # WHY this model first: it is the fastest (~2 min) and its outputs feed the Power BI
    #      'quality monitoring' page (Phase 4).
    t_pred_retard = BashOperator(
        task_id="ml_pred_retard",
        bash_command="python /opt/project/ml/train_pred_retard.py",
    )

    # --- Modèle 2 : prévisions CA par catégories (Prophet) / revenue forecast --------
    t_forecast_ca = BashOperator(
        task_id="ml_forecast_ca",
        bash_command="python /opt/project/ml/train_forecast_ca.py",
    )

    # --- Modèle 3 : prévisions IoT (Prophet) / IoT power forecast --------------------
    t_forecast_iot = BashOperator(
        task_id="ml_forecast_iot",
        bash_command="python /opt/project/ml/train_forecast_iot.py",
    )

    # Controle de fumee final / final smoke check :
    # POURQUOI : un script « vert » ne garantit pas que les tables soient visibles dans
    #            PostgreSQL (l'export JDBC est la dernière étape de chaque script).
    #            On vérifie donc la présence des 6 tables ml_* après coup.
    # WHY: a 'green' script does not guarantee the tables are visible in PostgreSQL
    #      (the JDBC export is each script's last step). We verify the 6 ml_* tables.
    # COMMENT : psycopg2 est déjà installé dans l'image Airflow ; on interroge
    #            postgres:5432 depuis le réseau Docker (pas de docker CLI dans le
    #            conteneur) — même pattern que dag_05_gold_build.py.
    # HOW: psycopg2 ships with the Airflow image; query postgres:5432 over the Docker
    #      network (no docker CLI inside the container) — same pattern as dag_05.
    def check_postgres_ml():
        import psycopg2
        conn = psycopg2.connect(host="postgres", port=5432, dbname="gold_db",
                                user="spark", password="spark")
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema='gold' AND table_name LIKE 'ml_%' ORDER BY table_name")
                tables = [row[0] for row in cur.fetchall()]
                print("[check] tables ml_* dans PostgreSQL / ml_* tables in PostgreSQL:", tables)
                attendues = {
                    "ml_pred_retard_metriques", "ml_pred_retard_importance",
                    "ml_pred_retard_predictions", "ml_forecast_ca",
                    "ml_forecast_ca_metriques", "ml_forecast_iot",
                    "ml_forecast_iot_metriques",
                }
                manquantes = attendues - set(tables)
                assert not manquantes, f"Tables ml_* absentes / missing ml_* tables: {manquantes}"
        finally:
            conn.close()

    t_controle = PythonOperator(
        task_id="ml_controle_tables",
        python_callable=check_postgres_ml,
    )

    # Les 3 modèles sont INDÉPENDANTS : parallélisés (max 3 actifs = limite connue).
    # The 3 models are INDEPENDENT: parallelized (max 3 active = known limit).
    [t_pred_retard, t_forecast_ca, t_forecast_iot] >> t_controle
