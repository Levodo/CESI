# -*- coding: utf-8 -*-
"""
=====================================================================================
checks.py — CONTRÔLE DE LA QUALITÉ DES DONNÉES (Data Quality)
            DATA QUALITY CHECKS

POURQUOI / WHY :
  Un pipeline qui "tourne" n'est pas un pipeline FIDELE. Sans controle automatise,
  une anomalie (doublon, colonne vide, cle orpheline) passe inapercue jusqu'au
  tableau de bord, ou elle devient une DECISION erronee. Ce module mesure la qualite
  a chaque execution et trace le resultat dans PostgreSQL (table dq.dq_checks) :
  historique consultable dans Power BI + alerte Airflow si un controle echoue.
  A pipeline that "runs" is not a RELIABLE pipeline. Without automated checks, an
  anomaly (duplicate, empty column, orphan key) goes unnoticed until the dashboard,
  where it becomes a WRONG DECISION. This module measures quality at every run and
  traces the result in PostgreSQL (dq.dq_checks table): history viewable in Power BI
  + Airflow alert if a check fails.

COMMENT / HOW :
  6 familles de controles (cf. docs/02 §8) :
    1. EXISTENCE    : la table est-elle interrogeable et non vide ?
    2. COMPTAGE     : le nombre de lignes est-il plausible (> 0) ?
    3. COMPLETUDE   : les colonnes cles sont-elles remplies ?
    4. VALIDITE     : les valeurs respectent-elles leurs bornes metier ?
    5. UNICITE      : la cle primaire est-elle unique ?
    6. INTEGRITE    : aucune cle enfant orpheline de sa table parent ?
  Chaque controle produit UNE ligne de resultat ; le job quitte avec le code 1 si
  --fail-on-error est pose et qu'au moins un controle echoue (=> DAG en echec => alerte).
  Each check produces ONE result row; the job exits with code 1 if --fail-on-error
  is set and at least one check fails (=> DAG failure => alert).

EQUIVALENT A LA MAIN / MANUAL EQUIVALENT :
  spark-submit --master spark://spark-master:7077 /opt/project/data_quality/checks.py --layer all
  # Puis consultation / then browse results :
  docker exec cidsec-postgres psql -U spark -d gold_db \
      -c "SELECT * FROM dq.dq_checks WHERE passed = false ORDER BY run_ts DESC;"

VERIFICATION :
  docker exec cidsec-postgres psql -U spark -d gold_db \
      -c "SELECT layer, table_name, check_name, observed, expected, passed FROM dq.dq_checks;"
=====================================================================================
"""

import argparse
import sys
from datetime import datetime

sys.path.insert(0, "/opt/project/spark_jobs")  # "import common" / allows "import common"

from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import CONFIG, get_spark

# Schema des resultats (EXPLICITE : jamais d'inference sur 15 lignes de resultats)
# Results schema (EXPLICIT: never infer on 15 result rows)
RESULT_SCHEMA = T.StructType([
    T.StructField("run_ts", T.TimestampType()),
    T.StructField("layer", T.StringType()),
    T.StructField("table_name", T.StringType()),
    T.StructField("check_name", T.StringType()),
    T.StructField("observed", T.StringType()),
    T.StructField("expected", T.StringType()),
    T.StructField("passed", T.BooleanType()),
    T.StructField("details", T.StringType()),
])

results = []   # accumulateur de resultats / results accumulator


def record(layer, table, check, observed, expected, passed, details=""):
    """Enregistre le resultat d'un controle / record one check result."""
    results.append({
        "run_ts": datetime.now(),
        "layer": layer, "table_name": table, "check_name": check,
        "observed": str(observed), "expected": str(expected),
        "passed": bool(passed), "details": details,
    })
    # Affichage immediat : le log Airflow montre chaque controle au fil de l'eau
    # Immediate print: Airflow log shows each check as it happens
    statut = "OK " if passed else "ECHEC"
    print(f"[dq] {statut} | {layer}.{table} | {check} | vu: {observed} | attendu: {expected}")


def check_table(spark, layer, table):
    """
    Controles 1-2 : EXISTENCE + COMPTAGE.
    Checks 1-2 : EXISTENCE + ROW COUNT.
    """
    try:
        n = spark.table(f"{layer}.{table}").count()
        record(layer, table, "existence_comptage", n, "> 0", n > 0,
               "la table est interrogeable / table is queryable")
    except Exception as exc:   # table absente = probleme grave mais pas de crash du job
        record(layer, table, "existence_comptage", "ABSENTE", "table presente", False,
               str(exc)[:300])


def check_completeness(spark, layer, table, columns, max_null_rate=0.05):
    """
    Controle 3 : COMPLETUDE (taux de NULL sous le seuil).
    Check 3 : COMPLETENESS (NULL rate under threshold).
    """
    df = spark.table(f"{layer}.{table}")
    n = df.count()
    if n == 0:
        return   # deja signale par le comptage / already flagged by the count check
    for col in columns:
        nulls = df.filter(F.col(col).isNull()).count()
        rate = nulls / n
        record(layer, table, f"completude:{col}", f"{rate:.2%} NULL",
               f"<= {max_null_rate:.0%}", rate <= max_null_rate,
               f"{nulls} lignes nulles sur {n} / null rows")


def check_validity(spark, layer, table, column, predicate, expected_label):
    """
    Controle 4 : VALIDITE (une expression SQL doit etre vraie pour toutes les lignes).
    Check 4 : VALIDITY (a SQL expression must hold for every row).
    """
    df = spark.table(f"{layer}.{table}")
    invalid = df.filter(f"NOT ({predicate})").count()
    record(layer, table, f"validite:{column}", f"{invalid} lignes invalides",
           f"0 | {expected_label}", invalid == 0, predicate)


def check_uniqueness(spark, layer, table, key_columns):
    """
    Controle 5 : UNICITE de la cle.
    Check 5 : KEY UNIQUENESS.
    """
    df = spark.table(f"{layer}.{table}")
    n = df.count()
    n_keys = df.select(*key_columns).distinct().count()
    record(layer, table, f"unicite:{'+'.join(key_columns)}", f"{n_keys} cles distinctes",
           f"= {n} lignes", n_keys == n,
           "tout doublon de cle fausse les agregats / any key duplicate corrupts aggregates")


def check_referential_integrity(spark, layer, child_table, child_col, parent_table, parent_col):
    """
    Controle 6 : INTEGRITE REFERENTIELLE (aucune cle orpheline).
    Check 6 : REFERENTIAL INTEGRITY (no orphan key).
    """
    child = spark.table(f"{layer}.{child_table}").select(child_col).distinct()
    parent = spark.table(f"{layer}.{parent_table}").select(parent_col).distinct()
    orphans = child.join(parent, child[child_col] == parent[parent_col], "left_anti").count()
    record(layer, child_table, f"integrite:{child_col}->{parent_table}.{parent_col}",
           f"{orphans} orphelines", "0", orphans == 0,
           "jointures silencieusement perdues / silently lost joins")


# ===================================================================================
# Catalogue des controles / checks catalogue
# POURQUOI un catalogue declaratif : ajouter un controle = ajouter une ligne,
# sans jamais toucher a la logique ci-dessous.
# WHY a declarative catalog: adding a check = adding one row, never touching logic.
# ===================================================================================
def run_silver_checks(spark):
    """Controles de la couche Silver / Silver layer checks."""
    S = "silver_db"

    # Comptages cles / key row counts
    for t in ["olist_orders", "olist_order_items", "olist_order_payments",
              "olist_order_reviews", "olist_products", "olist_customers",
              "olist_sellers", "cymbal_trades", "meteo_quotidien", "taux_change",
              "iot_energie_pays", "iot_foyer_1h",
              "conformed_commandes_enrichies", "conformed_trades_enrichis"]:
        check_table(spark, S, t)

    # Completeness (colonnes cles / key columns)
    check_completeness(spark, S, "olist_orders", ["order_id", "customer_id", "order_purchase_timestamp"])
    check_completeness(spark, S, "cymbal_trades", ["tradereportid", "trade_date", "last_px"])
    check_completeness(spark, S, "meteo_quotidien", ["jour", "temp_moy_c"], max_null_rate=0.0)

    # Validite (bornes metier mesurees par l'audit / business bounds measured by audit)
    check_validity(spark, S, "olist_order_reviews", "review_score",
                   "review_score BETWEEN 1 AND 5", "score dans [1,5]")
    check_validity(spark, S, "olist_order_items", "price",
                   "price > 0", "prix strictement positif")
    check_validity(spark, S, "meteo_quotidien", "precipitations_mm",
                   "precipitations_mm >= 0", "precipitations non negatives")

    # Unicite / uniqueness
    check_uniqueness(spark, S, "olist_orders", ["order_id"])
    check_uniqueness(spark, S, "cymbal_trades", ["tradereportid"])     # apres dedup A11
    check_uniqueness(spark, S, "olist_order_reviews", ["review_id"])   # apres dedup A4

    # Integrite referentielle / referential integrity
    check_referential_integrity(spark, S, "olist_order_items", "order_id", "olist_orders", "order_id")
    check_referential_integrity(spark, S, "olist_order_payments", "order_id", "olist_orders", "order_id")


def run_gold_checks(spark):
    """Controles de la couche Gold / Gold layer checks."""
    G = "gold_db"
    for t in ["dim_date", "dim_client", "dim_produit", "dim_vendeur", "dim_pays",
              "fact_commande", "fact_trades", "fact_energie_jour", "kpi_ca_quotidien"]:
        check_table(spark, G, t)

    # Grain des faits / fact grain
    check_uniqueness(spark, G, "fact_commande", ["order_id"])
    check_uniqueness(spark, G, "fact_trades", ["trade_report_id"])


def persist_results(spark):
    """
    Ecrit les resultats dans PostgreSQL (dq.dq_checks) / write results to PostgreSQL.
    POURQUOI / WHY : un controle dont le resultat n'est pas trace n'existe pas pour
    l'exploitation. PostgreSQL alimente la page "Monitoring qualite" de Power BI.
    A check whose result is not traced does not exist for operations. PostgreSQL feeds
    the Power BI "Quality monitoring" page.
    """
    if not results:
        print("[dq] aucun resultat a ecrire / no result to write")
        return
    df = spark.createDataFrame(results, schema=RESULT_SCHEMA)
    (df.write
      .mode("append")            # on AJOUTE l'historique / APPEND to the history
      .format("jdbc")
      .option("url", CONFIG["pg_url"])
      .option("dbtable", "dq.dq_checks")
      .option("user", CONFIG["pg_user"])
      .option("password", CONFIG["pg_password"])
      .option("driver", "org.postgresql.Driver")
      .save())
    print(f"[dq] {len(results)} resultats ecrits dans dq.dq_checks / results written")


def main():
    p = argparse.ArgumentParser(description="Data quality checks / controles qualite")
    p.add_argument("--layer", default="all", choices=["silver", "gold", "all"],
                   help="Couche a controler / layer to check")
    p.add_argument("--fail-on-error", action="store_true",
                   help="Quitter en code 1 si un controle echoue / exit 1 on failure")
    args = p.parse_args()

    spark = get_spark("data_quality_checks")
    if args.layer in ("silver", "all"):
        run_silver_checks(spark)
    if args.layer in ("gold", "all"):
        run_gold_checks(spark)

    persist_results(spark)

    nb_ko = sum(1 for r in results if not r["passed"])
    print(f"[dq] === {len(results)} controles, {nb_ko} echec(s) / checks, {nb_ko} failure(s) ===")
    spark.stop()

    if args.fail_on_error and nb_ko > 0:
        # POURQUOI : un code retour non nul = Airflow marque la tache en echec =
        # notification dans l'onglet Alerts. C'est le canal d'alerte minimal.
        # WHY: non-zero return code = Airflow marks the task failed = notification in
        # the Alerts tab. This is the minimal alerting channel.
        sys.exit(1)


if __name__ == "__main__":
    main()
