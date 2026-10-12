# -*- coding: utf-8 -*-
"""
=====================================================================================
silver_api.py — Couche SILVER : flux API (meteo Open-Meteo, taux de change Frankfurter)
                SILVER layer: API feeds (Open-Meteo weather, Frankfurter FX rates)

POURQUOI / WHY :
  Les API livrent du JSON imbrique. La Bronze garde le JSON brut (une ligne = un
  appel). La Silver l'explose en tables plates et typées, jointes au reste du projet
  (météo <-> commandes, taux <-> trades) : c'est la que les sources deviennent
  COHERENTES entre elles.
  APIs deliver nested JSON. Bronze keeps the raw payload (one row = one call).
  Silver explodes it into flat, typed tables joined with the rest of the project
  (weather <-> orders, FX <-> trades): this is where sources become COHERENT.

REGLES APPLIQUEES / RULES APPLIED (cf. docs/01 §8.3) :
  B3 : taux de change = jours ouvres uniquement -> FORWARD-FILL sur calendrier complet
       (dernier taux connu reporte, regle standard de marche)
  B5 : chaque payload est date et archive (cote Bronze), d'ou l'argument --load-date

VERIFICATION :
  SELECT * FROM silver_db.meteo_quotidien WHERE jour BETWEEN '2016-09-01' AND '2016-09-05';
  SELECT count(*) FROM silver_db.taux_change;   -- 136 jours ouvres -> + week-ends completes
=====================================================================================
"""

import argparse
import sys

sys.path.insert(0, "/opt/project/spark_jobs")

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import (CONFIG, add_technical_columns, ensure_databases, get_spark,
                    write_table)

DST = CONFIG["db_silver"]
SRC = CONFIG["db_bronze"]

# --- Schemas JSON explicites / explicit JSON schemas ------------------------------
# POURQUOI / WHY : laisser Spark "deviner" le schema d'un JSON API est fragile
# (champs absents = colonnes disparues). Le schema explicite fige le contrat.
# Letting Spark "guess" an API JSON schema is fragile (missing fields = missing
# columns). The explicit schema freezes the contract.
METEO_SCHEMA = T.StructType([
    T.StructField("latitude", T.DoubleType()),
    T.StructField("longitude", T.DoubleType()),
    T.StructField("timezone", T.StringType()),
    T.StructField("daily", T.StructType([
        T.StructField("time", T.ArrayType(T.StringType())),
        T.StructField("temperature_2m_mean", T.ArrayType(T.DoubleType())),
        T.StructField("temperature_2m_max", T.ArrayType(T.DoubleType())),
        T.StructField("temperature_2m_min", T.ArrayType(T.DoubleType())),
        T.StructField("precipitation_sum", T.ArrayType(T.DoubleType())),
        T.StructField("windspeed_10m_max", T.ArrayType(T.DoubleType())),
    ])),
])

# Le JSON Frankfurter est un OBJET {base, amount, start_date, end_date, rates} :
# les sous-cles "base" etc. ne sont PAS des maps -> parser la map directement renvoie
# NULL partout (sequence(NULL,NULL) observe le 08/10/2026). On declare donc la vraie
# structure, et on n'utilisera que p.rates.
# The Frankfurter JSON is an OBJECT {base, amount, start_date, end_date, rates}:
# keys like "base" are NOT maps -> parsing as a bare map returns NULL everywhere.
# We declare the real structure and only use p.rates.
FX_SCHEMA = T.StructType([
    T.StructField("base", T.StringType()),
    T.StructField("amount", T.DoubleType()),
    T.StructField("start_date", T.StringType()),
    T.StructField("end_date", T.StringType()),
    T.StructField("rates", T.MapType(T.StringType(), T.MapType(T.StringType(), T.DoubleType()))),
])


def silver_meteo(spark, table="api_meteo_sp_daily"):
    """
    Meteo quotidienne Sao Paulo / Sao Paulo daily weather.
    Une ligne = un jour / one row = one day.
    """
    df = spark.table(f"{SRC}.{table}").select(
        F.from_json(F.col("raw_json"), METEO_SCHEMA).alias("p"))

    df = (df
          .select(
              F.col("p.latitude").alias("latitude"),
              F.col("p.longitude").alias("longitude"),
              # arrays_zip + explode : les tableaux paralleles deviennent des lignes
              # parallel arrays become rows
              F.explode(
                  F.arrays_zip(
                      F.col("p.daily.time"),
                      F.col("p.daily.temperature_2m_mean"),
                      F.col("p.daily.temperature_2m_max"),
                      F.col("p.daily.temperature_2m_min"),
                      F.col("p.daily.precipitation_sum"),
                      F.col("p.daily.windspeed_10m_max"),
                  )
              ).alias("j"))
          .select(
              "latitude", "longitude",
              F.to_date(F.col("j.time")).alias("jour"),
              F.col("j.temperature_2m_mean").alias("temp_moy_c"),
              F.col("j.temperature_2m_max").alias("temp_max_c"),
              F.col("j.temperature_2m_min").alias("temp_min_c"),
              F.col("j.precipitation_sum").alias("precipitations_mm"),
              F.col("j.windspeed_10m_max").alias("vent_max_kmh"),
          ))
    df = add_technical_columns(df, source_file="openmeteo_sp_daily_2016_2018.json")
    write_table(df, DST, "meteo_quotidien", f"{CONFIG['hdfs_silver']}/api/meteo_quotidien")


def silver_fx(spark, table="api_taux_change"):
    """
    Taux de change USD -> devises / USD FX rates.
    Regle B3 : forward-fill sur calendrier complet (remplissage des week-ends).
    Rule B3: forward-fill over a complete calendar (weekend backfill).
    """
    df = spark.table(f"{SRC}.{table}").select(
        F.from_json(F.col("raw_json"), FX_SCHEMA).alias("taux"))

    # explode de la map date -> (devise -> taux) / explode map date -> (currency -> rate)
    # NOTE : "taux" est desormais la STRUCT complete ; c'est son champ .rates qui est
    #        la map a exploser (voir FX_SCHEMA).
    # NOTE: "taux" is now the full STRUCT; its .rates field is the map to explode.
    df = (df
          .select(F.explode(F.col("taux.rates")).alias("jour_str", "m"))
          .select(
              F.to_date("jour_str").alias("jour"),
              F.col("m")["BRL"].alias("usd_vers_brl"),
              F.col("m")["EUR"].alias("usd_vers_eur"),
              F.col("m")["GBP"].alias("usd_vers_gbp"),
              F.col("m")["JPY"].alias("usd_vers_jpy"),
          ))

    # --- B3 : calendrier complet + forward-fill ------------------------------------
    # POURQUOI : les marches sont fermes le week-end ; sans remplissage, toute
    # jointure avec un evenement du samedi perdrait son taux.
    # WHY: markets are closed on weekends; without backfill, any Saturday join
    # would lose its rate.
    bounds = df.agg(F.min("jour").alias("mn"), F.max("jour").alias("mx")).first()
    cal = spark.range(1).select(
        F.explode(F.sequence(F.lit(bounds["mn"]), F.lit(bounds["mx"]), F.expr("interval 1 day")))
        .alias("jour"))
    w = Window.orderBy("jour")
    df = (cal.join(df, "jour", "left")
          .withColumn("usd_vers_brl", F.last("usd_vers_brl", ignorenulls=True).over(w))
          .withColumn("usd_vers_eur", F.last("usd_vers_eur", ignorenulls=True).over(w))
          .withColumn("usd_vers_gbp", F.last("usd_vers_gbp", ignorenulls=True).over(w))
          .withColumn("usd_vers_jpy", F.last("usd_vers_jpy", ignorenulls=True).over(w))
          .withColumn("est_jour_ouvre", F.date_format("jour", "E").isin(["Sat", "Sun"]) == False))

    df = add_technical_columns(df, source_file="frankfurter_usd_2020.json")
    write_table(df, DST, "taux_change", f"{CONFIG['hdfs_silver']}/api/taux_change")


def parse_args():
    p = argparse.ArgumentParser(description="Silver API / API cleaning")
    p.add_argument("--meteo-table", default="api_meteo_sp_daily")
    p.add_argument("--fx-table", default="api_taux_change")
    p.add_argument("--skip", choices=["meteo", "fx"], help="Sauter une source / skip one source")
    return p.parse_args()


def main():
    args = parse_args()
    spark = get_spark("silver_api")
    ensure_databases(spark, [DST])
    print("[silver] === regles API : B3, B5 (cf. docs/01) ===")
    if args.skip != "meteo":
        silver_meteo(spark, args.meteo_table)
    if args.skip != "fx":
        silver_fx(spark, args.fx_table)
    print("[silver] API termine / API done")
    spark.stop()


if __name__ == "__main__":
    main()
