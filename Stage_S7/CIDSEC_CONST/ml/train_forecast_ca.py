#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ml/train_forecast_ca.py — Modèle 2 : prévision du CA par catégories (Prophet) / Model 2: revenue forecast by category
======================================================================================================================
POURQUOI ce fichier : Phase 3 (ML) du projet CIDSEC_CONST. Ce script produit une
PRÉVISION à 90 jours du chiffre d'affaires par catégorie de produits, à partir de
l'historique journalier de la Gold (`fact_commande` ⋈ `dim_produit`). Objectif
opérationnel : anticiper la saisonnalité des ventes pour le pilotage commercial.
WHY this file: Phase 3 (ML) of the CIDSEC_CONST project. This script produces a
90-DAY forecast of revenue per product category from the daily Gold history
(`fact_commande` joined with `dim_produit`). Operational goal: anticipate sales
seasonality for commercial steering.

COMMENT ce fichier marche / HOW this file works :
1) Agrège le CA journalier par catégorie (et le CA total) depuis la couche Gold.
   Aggregates daily revenue per category (and total) from the Gold layer.
2) Pour chaque série : découpe 30 derniers jours en « test » (évaluation), entraîne
   Prophet (saisonnalité hebdo + annuelle), prévoit 90 jours.
   For each series: last 30 days held out as 'test', Prophet is trained (weekly +
   yearly seasonality), forecasts 90 days.
3) Évalue avec MAPE / MAE sur le test (plus la baseline « valeur de la veille »).
   Evaluates with MAPE / MAE on the test (plus a 'previous value' baseline).
4) Persiste en Gold (Hive + PostgreSQL) : prévisions + métriques, et sauvegarde les
   modèles Prophet en JSON.
   Persists to Gold (Hive + PostgreSQL): forecasts + metrics, and saves the Prophet
   models as JSON.

VÉRIFICATION / VERIFICATION :
- `SELECT * FROM gold.ml_forecast_ca_metriques;` → MAPE par catégorie (plus bas = mieux).
- `SELECT * FROM gold.ml_forecast_ca WHERE type_ligne='prevision';` → les 90 jours prévus.
- Le rapport final s'affiche dans les logs / terminal.

ÉQUIVALENT À LA MAIN / MANUAL EQUIVALENT :
docs/06_phase3_ml.md, section « rejouer à la main » (commandes une par une).
"""

# --- Imports ---------------------------------------------------------------------------
# POURQUOI sys.path.append : common.py (spark_jobs/) n'est pas un paquet installable.
# WHY sys.path.append: common.py (spark_jobs/) is not an installed package.
import json
import os
import sys

sys.path.append("/opt/project/spark_jobs")

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DoubleType, IntegerType, StringType, StructField, StructType)

from common import CONFIG, get_spark, read_latest_snapshot, write_postgres, write_table  # noqa: E402

# --- Correctif de compatibilité NumPy / NumPy compatibility shim ------------------------
# POURQUOI : PySpark 3.x appelle np.bool dans toPandas() (supprimé dans numpy>=1.24).
#            On rétablit l'alias — voir ml/train_pred_retard.py pour le détail.
# WHY: PySpark 3.x calls np.bool in toPandas() (removed in numpy>=1.24). We restore the
#      alias — see ml/train_pred_retard.py for details.
if not hasattr(np, "bool"):
    np.bool = bool  # type: ignore[attr-defined]

# --- Configuration / Configuration ------------------------------------------------------
DST = CONFIG["db_gold"]          # gold_db
HDFS_GOLD = CONFIG["hdfs_gold"]  # hdfs://namenode:9000/data/gold

HORIZON_JOURS = 90       # horizon de prévision / forecast horizon
TEST_JOURS = 30          # derniers jours gardés pour l'évaluation / last days held out
TOP_CATEGORIES = 5       # catégories suivies individuellement / categories tracked individually
SEED = 42

EXIGE_MIN_JOURS = 90     # minimum d'historique pour entraîner Prophet / minimum history
                         # (Prophet a besoin d'assez de points pour la saisonnalité)


def charger_series(spark):
    """Construit les séries temporelles CA journalier par catégorie + la série TOTAL.
    Builds daily revenue time series per category + the TOTAL series.

    POURQUOI cette jointure : fact_commande est au grain COMMANDE (pas de clé produit) ;
    la catégorie vit dans le Silver (items ⋈ products). On agrège donc le CA journalier
    par catégorie depuis le grain ITEM (le plus fin) ⋈ orders (pour la date d'achat).
    WHY this join: fact_commande is at ORDER grain (no product key); the category lives
    in Silver (items joined with products). We aggregate daily revenue per category from
    the ITEM grain (finest) joined with orders (for the purchase date).
    """
    # Noms des tables SILVER (releves par SHOW TABLES IN silver_db / actual names from
    # SHOW TABLES IN silver_db).
    silver = CONFIG["db_silver"]
    items = read_latest_snapshot(spark, f"{silver}.olist_order_items")   # order_id, product_id, price
    prod = read_latest_snapshot(spark, f"{silver}.olist_products")       # product_id, product_category_name
    orders = read_latest_snapshot(spark, f"{silver}.olist_orders")       # order_id, order_purchase_timestamp

    df = (items
          .join(orders.select("order_id", "order_purchase_timestamp"), on="order_id", how="inner")
          .join(prod.select("product_id", "product_category_name"), on="product_id", how="inner")
          .select(F.to_date(F.col("order_purchase_timestamp")).alias("ds"),
                  F.col("price").cast("double").alias("ca"),
                  F.col("product_category_name").alias("categorie")))

    # Série 1 : CA TOTAL toutes catégories / Series 1: TOTAL revenue across categories
    total = (df.groupBy("ds").agg(F.sum("ca").alias("y"))
             .withColumn("serie", F.lit("TOTAL")))

    # Séries 2..N : CA par catégorie / Series 2..N: revenue per category
    par_cat = (df.groupBy("ds", "categorie")
               .agg(F.sum("ca").alias("y"))
               .withColumn("serie", F.col("categorie")))

    # POURQUOI toPandas : Prophet travaille en pandas ; le volume est faible
    #            (séries agrégées ~1 000 points x quelques séries).
    # WHY toPandas: Prophet works on pandas; the volume is small (aggregated series).
    pdf_total = total.select("ds", "y", "serie").toPandas()
    pdf_cat = par_cat.select("ds", "y", "serie").toPandas()

    # Top N catégories par CA cumulé / Top N categories by cumulative revenue
    top = (pdf_cat.groupby("serie")["y"].sum()
           .sort_values(ascending=False)
           .head(TOP_CATEGORIES).index.tolist())

    series = {"TOTAL": pdf_total}
    for cat in top:
        series[cat] = pdf_cat[pdf_cat["serie"] == cat][["ds", "y", "serie"]].copy()
    return series


def entrainer_prophet(pdf, nom):
    """Entraîne Prophet sur une série et renvoie prévisions + métriques de test.
    Trains Prophet on one series and returns forecasts + test metrics.

    POURQUOI découper sur les 30 derniers jours : sans jeu de test, le MAPE serait
    mesuré sur les données d'entraînement (trop optimiste). La baseline « valeur
    de la veille » montre la valeur ajoutée réelle du modèle.
    WHY split on the last 30 days: without a test set, MAPE would be measured on
    training data (too optimistic). The 'previous value' baseline shows the model's
    real added value.
    """
    from prophet import Prophet
    from sklearn.metrics import mean_absolute_error

    pdf = pdf.sort_values("ds").reset_index(drop=True)
    pdf["ds"] = pd.to_datetime(pdf["ds"])
    if len(pdf) < EXIGE_MIN_JOURS:
        print(f"[ml][{nom}] serie trop courte ({len(pdf)} jours) — passee / skipped")
        return None, None

    train = pdf.iloc[:-TEST_JOURS]
    test = pdf.iloc[-TEST_JOURS:]

    # POURQUOI weekly_seasonality=True : les ventes ont un rythme hebdomadaire
    #            (pics en semaine) ; yearly_seasonality=True : saisonnalité annuelle
    #            (fêtes, saison). daily_seasonality=False : données JOURNALIÈRES.
    # WHY weekly_seasonality=True: sales have a weekly rhythm; yearly_seasonality=True:
    #      annual season (holidays, seasons). daily_seasonality=False: DAILY data.
    m = Prophet(weekly_seasonality=True, yearly_seasonality=True,
                daily_seasonality=False, seasonality_mode="multiplicative")
    m.fit(train.rename(columns={"ds": "ds", "y": "y"}))

    # Évaluation : prévision sur la période de test / Evaluation: forecast the test period
    fut_test = m.make_future_dataframe(periods=TEST_JOURS, include_history=False)
    pred_test = m.predict(fut_test)
    yhat_test = pred_test["yhat"].values[:len(test)]

    mape = float(np.mean(np.abs((test["y"].values - yhat_test) /
                                np.maximum(np.abs(test["y"].values), 1.0))) * 100)
    mae = float(mean_absolute_error(test["y"].values, yhat_test))
    # WAPE = somme des erreurs / somme des ventes — ROBUSTE aux jours quasi-nuls,
    #        contrairement au MAPE qui explose quand y ≈ 0. Métrique de référence.
    # WAPE = sum of errors / sum of sales — ROBUST to near-zero days, unlike MAPE
    #        which explodes when y ≈ 0. Reference metric.
    wape = float(np.sum(np.abs(test["y"].values - yhat_test)) /
                 max(float(np.sum(np.abs(test["y"].values))), 1.0) * 100)
    # Baselines : « valeur de la veille » ET « même jour semaine précédent » (saison).
    # Baselines: 'previous value' AND 'same weekday last week' (seasonal).
    base = float(np.mean(np.abs(test["y"].values - train["y"].values[-1])))
    base_saison = float(np.mean(np.abs(test["y"].values - train["y"].values[-7])))

    metriques = {
        "serie": nom,
        "mape_test_pct": round(mape, 2),
        "wape_test_pct": round(wape, 2),
        "mae_test": round(mae, 2),
        "baseline_mae_veille": round(base, 2),
        "baseline_mae_saison": round(base_saison, 2),
        "n_jours_train": int(len(train)),
        "n_jours_test": int(len(test)),
    }

    # Prévision finale sur l'horizon complet (historique + 90 jours futurs).
    # Final forecast over the full horizon (history + 90 future days).
    fut = m.make_future_dataframe(periods=HORIZON_JOURS, include_history=True)
    fc = m.predict(fut)
    derniere_date = pdf["ds"].max()
    fc["type_ligne"] = np.where(fc["ds"] > derniere_date, "prevision", "historique_ajuste")

    a_venir = fc[["ds", "yhat", "yhat_lower", "yhat_upper", "type_ligne"]].copy()
    a_venir["serie"] = nom

    # Sauvegarde du modèle Prophet en JSON (rejeu sans ré-entraînement).
    # Save the Prophet model as JSON (replay without retraining).
    from prophet.serialize import model_to_json
    dossier = os.environ.get("ML_MODEL_DIR", "/opt/project/ml/modeles")
    os.makedirs(dossier, exist_ok=True)
    with open(f"{dossier}/forecast_ca_{nom.replace(' ', '_')}.json", "w") as f:
        json.dump(model_to_json(m), f)

    return a_venir, metriques


def persister(spark, previsions, metriques_liste):
    """Écrit prévisions et métriques en Gold (Hive + PostgreSQL).
    Writes forecasts and metrics to Gold (Hive + PostgreSQL).
    """
    from datetime import datetime
    ts = datetime.now().isoformat(timespec="seconds")

    # 1) Prévisions / Forecasts
    rows = [{
        "run_ts": ts, "serie": r["serie"],
        "jour": r["ds"].strftime("%Y-%m-%d"),
        "yhat": round(float(r["yhat"]), 2),
        "yhat_bas": round(float(r["yhat_lower"]), 2),
        "yhat_haut": round(float(r["yhat_upper"]), 2),
        "type_ligne": r["type_ligne"],
    } for _, r in previsions.iterrows()]
    schema = StructType([
        StructField("run_ts", StringType()), StructField("serie", StringType()),
        StructField("jour", StringType()), StructField("yhat", DoubleType()),
        StructField("yhat_bas", DoubleType()), StructField("yhat_haut", DoubleType()),
        StructField("type_ligne", StringType())])
    df = spark.createDataFrame(rows, schema=schema)
    # Signature write_table : (df, database, table, hdfs_path) — 4 arguments.
    write_table(df, DST, "ml_forecast_ca", f"{HDFS_GOLD}/ml_forecast_ca")
    write_postgres(df, "ml_forecast_ca")

    # 2) Métriques / Metrics
    mrows = [dict(run_ts=ts, **m) for m in metriques_liste]
    m_schema = StructType([
        StructField("run_ts", StringType()), StructField("serie", StringType()),
        StructField("mape_test_pct", DoubleType()), StructField("wape_test_pct", DoubleType()),
        StructField("mae_test", DoubleType()),
        StructField("baseline_mae_veille", DoubleType()),
        StructField("baseline_mae_saison", DoubleType()),
        StructField("n_jours_train", IntegerType()), StructField("n_jours_test", IntegerType())])
    mdf = spark.createDataFrame(mrows, schema=m_schema)
    write_table(mdf, DST, "ml_forecast_ca_metriques", f"{HDFS_GOLD}/ml_forecast_ca_metriques")
    write_postgres(mdf, "ml_forecast_ca_metriques")
    print(f"[ml] {len(rows)} lignes de previsions et {len(mrows)} series persistees "
          f"/ {len(rows)} forecast rows and {len(mrows)} series persisted")


def main():
    spark = get_spark("train_forecast_ca")
    print("[ml] === Modèle 2 : prévision du CA par catégories / Model 2: revenue forecast ===")

    series = charger_series(spark)
    print(f"[ml] series chargees / series loaded: {', '.join(series.keys())}")

    previsions, metriques_liste = [], []
    for nom, pdf in series.items():
        print(f"[ml] --- serie: {nom} ({len(pdf)} jours) ---")
        a_venir, metriques = entrainer_prophet(pdf, nom)
        if a_venir is not None:
            previsions.append(a_venir)
            metriques_liste.append(metriques)
            print(f"[ml][{nom}] WAPE test = {metriques['wape_test_pct']} % | "
                  f"MAPE = {metriques['mape_test_pct']} % | "
                  f"MAE = {metriques['mae_test']} (baseline saison = {metriques['baseline_mae_saison']})")

    if previsions:
        toutes = pd.concat(previsions, ignore_index=True)
        persister(spark, toutes, metriques_liste)

        # --- VÉRIFICATION / VERIFICATION ------------------------------------
        for r in (spark.table(f"{DST}.ml_forecast_ca_metriques")
                  .orderBy(F.col("mape_test_pct")).limit(3).collect()):
            print(f"[ml] VERIF serie={r['serie']} mape={r['mape_test_pct']}% mae={r['mae_test']}")
    spark.stop()
    print("[ml] TERMINE / DONE")


if __name__ == "__main__":
    main()
