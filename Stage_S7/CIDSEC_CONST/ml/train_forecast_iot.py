#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ml/train_forecast_iot.py — Modèle 3 : prévision de consommation électrique (IoT) / Model 3: power consumption forecast
=======================================================================================================================
POURQUOI ce fichier : Phase 3 (ML) du projet CIDSEC_CONST. Ce script prévoit la
consommation électrique à 90 jours, sur 2 familles de séries issues de la couche Silver :
  1) Foyer UCI (`iot_foyer_1j`) : consommation journalière totale (3 sous-compteurs).
  2) Pays européens (`iot_energie_pays`) : charge électrique moyenne journalière par pays.
Objectif opérationnel : anticiper la demande électrique (dimensionnement, achat d'énergie).
WHY this file: Phase 3 (ML). This script forecasts power consumption 90 days ahead on
2 series families from the Silver layer:
  1) UCI household (`iot_foyer_1j`): total daily consumption (3 sub-meters).
  2) European countries (`iot_energie_pays`): average daily grid load per country.
Operational goal: anticipate power demand (sizing, energy purchasing).

COMMENT ce fichier marche / HOW this file works :
1) Agrège les séries journalières (foyer = somme des sous-compteurs ; pays = moyenne de
   la charge horaire par jour).
   Aggregates daily series (household = sum of sub-meters; country = daily mean load).
2) Pour chaque série : 30 derniers jours en test, Prophet (hebdo + annuelle), 90 jours.
   For each series: last 30 days as test, Prophet (weekly + yearly), 90 days.
3) Métriques HONNÊTES : WAPE (référence), MAPE, MAE vs 2 baselines (veille + saison).
   HONEST metrics: WAPE (reference), MAPE, MAE vs 2 baselines (previous value + seasonal).
4) Persistance Gold (Hive + PostgreSQL) + modèles Prophet sauvegardés en JSON.

VÉRIFICATION / VERIFICATION :
- `SELECT * FROM gold.ml_forecast_iot_metriques ORDER BY wape_test_pct;`
- `SELECT * FROM gold.ml_forecast_iot WHERE type_ligne='prevision' LIMIT 10;`

ÉQUIVALENT À LA MAIN / MANUAL EQUIVALENT :
docs/06_phase3_ml.md, section « rejouer à la main ».
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
# WHY: PySpark 3.x calls np.bool in toPandas() (removed in numpy>=1.24).
if not hasattr(np, "bool"):
    np.bool = bool  # type: ignore[attr-defined]

# --- Configuration / Configuration ------------------------------------------------------
DST = CONFIG["db_gold"]
HDFS_GOLD = CONFIG["hdfs_gold"]

HORIZON_JOURS = 90      # horizon de prévision / forecast horizon
TEST_JOURS = 30         # jours gardés en test / days held out
EXIGE_MIN_JOURS = 90    # historique minimum pour Prophet / minimum history
TOP_PAYS = 3            # pays suivis individuellement / countries tracked individually


def charger_series(spark):
    """Construit les séries journalières : Foyer (UCI) + pays (OPSD).
    Builds daily series: Household (UCI) + countries (OPSD).

    POURQUOI 2 familles : le sujet contractuel demande des prévisions IoT ; le foyer
    (UCI) et les pays (OPSD) couvrent les 2 jeux de données IoT disponibles en Silver.
    WHY 2 families: the contract requires IoT forecasts; the household (UCI) and the
    countries (OPSD) cover both IoT datasets available in Silver.
    """
    silver = CONFIG["db_silver"]
    series = {}

    # --- Série 1 : Foyer UCI — consommation totale = somme des 3 sous-compteurs (kWh/j)
    # Series 1: UCI household — total consumption = sum of the 3 sub-meters (kWh/day)
    fo = read_latest_snapshot(spark, f"{silver}.iot_foyer_1j")
    pdf_fo = (fo
              .select(F.col("jour").alias("ds"),
                      (F.col("sous_compteur_1_kwh") + F.col("sous_compteur_2_kwh")
                       + F.col("sous_compteur_3_kwh")).alias("y"))
              .orderBy("ds").toPandas())
    series["FOYER_UCI"] = pdf_fo

    # --- Séries 2..N : Pays OPSD — charge moyenne journalière par pays / country daily mean load
    pa = read_latest_snapshot(spark, f"{silver}.iot_energie_pays")
    pdf_pa = (pa
              .select(F.to_date(F.col("ts")).alias("ds"), F.col("pays"),
                      F.col("charge").cast("double").alias("y"))
              .groupBy("ds", "pays").agg(F.avg("y").alias("y"))
              .toPandas())
    # Top N pays par nombre de jours d'historique (régularité) / top N countries by
    # number of history days (regularity).
    top = (pdf_pa.groupby("pays")["ds"].nunique()
           .sort_values(ascending=False).head(TOP_PAYS).index.tolist())
    for p in top:
        series[f"PAYS_{p}"] = pdf_pa[pdf_pa["pays"] == p][["ds", "y"]].copy()
    return series


def entrainer_prophet(pdf, nom):
    """Entraîne Prophet sur une série (test 30 j, horizon 90 j) + métriques honnêtes.
    Trains Prophet on one series (30-day test, 90-day horizon) + honest metrics.

    NOTE PÉDAGOGIQUE / TEACHING NOTE : les fonctions métriques sont volontairement
    dupliquées avec train_forecast_ca.py pour que chaque script se lise SEUL, sans
    aller chercher un autre fichier. C'est un choix de clarté pour un projet de stage.
    METRIC functions are deliberately DUPLICATED with train_forecast_ca.py so each
    script reads ALONE. A clarity choice for an internship project.
    """
    from prophet import Prophet
    from sklearn.metrics import mean_absolute_error

    pdf = pdf.sort_values("ds").reset_index(drop=True)
    pdf["ds"] = pd.to_datetime(pdf["ds"])
    # B1 (cf. docs/01) : les sources IoT ont des TROUS (jours sans mesure). Prophet ne
    #                    travaille pas sur des NaN — on retire ces jours et on l'annonce.
    # B1 (see docs/01): the IoT sources have GAPS (days without measurement). Prophet
    #      cannot handle NaN — those days are dropped, and this is printed out.
    avant = len(pdf)
    pdf = pdf.dropna(subset=["y"])
    if avant != len(pdf):
        print(f"[ml][{nom}] {avant - len(pdf)} jours sans mesure retires (regle B1) / "
              f"{avant - len(pdf)} empty days dropped (rule B1)")
    if len(pdf) < EXIGE_MIN_JOURS:
        print(f"[ml][{nom}] serie trop courte ({len(pdf)} jours) — passee / skipped")
        return None, None

    train = pdf.iloc[:-TEST_JOURS]
    test = pdf.iloc[-TEST_JOURS:]

    m = Prophet(weekly_seasonality=True, yearly_seasonality=True,
                daily_seasonality=False, seasonality_mode="multiplicative")
    m.fit(train)

    fut_test = m.make_future_dataframe(periods=TEST_JOURS, include_history=False)
    pred_test = m.predict(fut_test)
    yhat_test = pred_test["yhat"].values[:len(test)]

    # Métriques / metrics
    mape = float(np.mean(np.abs((test["y"].values - yhat_test) /
                                np.maximum(np.abs(test["y"].values), 1.0))) * 100)
    mae = float(mean_absolute_error(test["y"].values, yhat_test))
    # WAPE = métrique de référence (robuste aux valeurs faibles) / reference metric
    wape = float(np.sum(np.abs(test["y"].values - yhat_test)) /
                 max(float(np.sum(np.abs(test["y"].values))), 1.0) * 100)
    base = float(np.mean(np.abs(test["y"].values - train["y"].values[-1])))
    base_saison = float(np.mean(np.abs(test["y"].values - train["y"].values[-7])))

    metriques = {
        "serie": nom,
        "wape_test_pct": round(wape, 2),
        "mape_test_pct": round(mape, 2),
        "mae_test": round(mae, 2),
        "baseline_mae_veille": round(base, 2),
        "baseline_mae_saison": round(base_saison, 2),
        "n_jours_train": int(len(train)),
        "n_jours_test": int(len(test)),
    }

    # Prévision finale / final forecast
    fut = m.make_future_dataframe(periods=HORIZON_JOURS, include_history=True)
    fc = m.predict(fut)
    derniere_date = pdf["ds"].max()
    fc["type_ligne"] = np.where(fc["ds"] > derniere_date, "prevision", "historique_ajuste")
    a_venir = fc[["ds", "yhat", "yhat_lower", "yhat_upper", "type_ligne"]].copy()
    a_venir["serie"] = nom

    # Sauvegarde du modèle / model save
    from prophet.serialize import model_to_json
    dossier = os.environ.get("ML_MODEL_DIR", "/opt/project/ml/modeles")
    os.makedirs(dossier, exist_ok=True)
    with open(f"{dossier}/forecast_iot_{nom.replace(' ', '_')}.json", "w") as f:
        json.dump(model_to_json(m), f)

    return a_venir, metriques


def persister(spark, previsions, metriques_liste):
    """Écrit prévisions et métriques en Gold (Hive + PostgreSQL).
    Writes forecasts and metrics to Gold (Hive + PostgreSQL).
    """
    from datetime import datetime
    ts = datetime.now().isoformat(timespec="seconds")

    rows = [{
        "run_ts": ts, "serie": r["serie"], "jour": r["ds"].strftime("%Y-%m-%d"),
        "yhat": round(float(r["yhat"]), 4),
        "yhat_bas": round(float(r["yhat_lower"]), 4),
        "yhat_haut": round(float(r["yhat_upper"]), 4),
        "type_ligne": r["type_ligne"],
    } for _, r in previsions.iterrows()]
    schema = StructType([
        StructField("run_ts", StringType()), StructField("serie", StringType()),
        StructField("jour", StringType()), StructField("yhat", DoubleType()),
        StructField("yhat_bas", DoubleType()), StructField("yhat_haut", DoubleType()),
        StructField("type_ligne", StringType())])
    df = spark.createDataFrame(rows, schema=schema)
    # Signature write_table : (df, database, table, hdfs_path) — 4 arguments.
    write_table(df, DST, "ml_forecast_iot", f"{HDFS_GOLD}/ml_forecast_iot")
    write_postgres(df, "ml_forecast_iot")

    mrows = [dict(run_ts=ts, **m) for m in metriques_liste]
    m_schema = StructType([
        StructField("run_ts", StringType()), StructField("serie", StringType()),
        StructField("wape_test_pct", DoubleType()), StructField("mape_test_pct", DoubleType()),
        StructField("mae_test", DoubleType()),
        StructField("baseline_mae_veille", DoubleType()),
        StructField("baseline_mae_saison", DoubleType()),
        StructField("n_jours_train", IntegerType()), StructField("n_jours_test", IntegerType())])
    mdf = spark.createDataFrame(mrows, schema=m_schema)
    write_table(mdf, DST, "ml_forecast_iot_metriques", f"{HDFS_GOLD}/ml_forecast_iot_metriques")
    write_postgres(mdf, "ml_forecast_iot_metriques")
    print(f"[ml] {len(rows)} lignes de previsions et {len(mrows)} series persistees "
          f"/ {len(rows)} forecast rows and {len(mrows)} series persisted")


def main():
    spark = get_spark("train_forecast_iot")
    print("[ml] === Modèle 3 : prévision IoT (consommation électrique) / Model 3: IoT forecast ===")

    series = charger_series(spark)
    print(f"[ml] series chargees / series loaded: "
          + ", ".join(f"{k} ({len(v)} j)" for k, v in series.items()))

    previsions, metriques_liste = [], []
    for nom, pdf in series.items():
        print(f"[ml] --- serie: {nom} ({len(pdf)} jours) ---")
        a_venir, metriques = entrainer_prophet(pdf, nom)
        if a_venir is not None:
            previsions.append(a_venir)
            metriques_liste.append(metriques)
            bat = "OUI" if metriques["mae_test"] < metriques["baseline_mae_saison"] else "non"
            print(f"[ml][{nom}] WAPE = {metriques['wape_test_pct']} % | "
                  f"MAE = {metriques['mae_test']} vs baseline saison {metriques['baseline_mae_saison']} "
                  f"-> bat la baseline ? {bat}")

    if previsions:
        toutes = pd.concat(previsions, ignore_index=True)
        persister(spark, toutes, metriques_liste)

        # --- VÉRIFICATION / VERIFICATION ------------------------------------
        for r in (spark.table(f"{DST}.ml_forecast_iot_metriques")
                  .orderBy(F.col("wape_test_pct")).limit(3).collect()):
            print(f"[ml] VERIF serie={r['serie']} wape={r['wape_test_pct']}% mae={r['mae_test']}")
    spark.stop()
    print("[ml] TERMINE / DONE")


if __name__ == "__main__":
    main()
