# -*- coding: utf-8 -*-
"""
=====================================================================================
silver_iot.py — Couche SILVER : series temporelles IoT (UCI foyer + OPSD pays)
                SILVER layer: IoT time series (UCI household + OPSD countries)

POURQUOI / WHY :
  Ces deux sources sont les "capteurs" du projet : formats piegeux (separateur ';',
  virgule decimale, '?'), trous de mesure, 300 colonnes inutiles. La Silver les rend
  exploitables : un timestamp propre, des nombres, une table LONGUE (une ligne =
  un pays a un instant) qui se rejoint facilement avec le reste du projet.
  These two sources are the project's "sensors": tricky formats (';' separator,
  decimal comma, '?'), measurement gaps, 300 useless columns. Silver makes them
  usable: clean timestamps, numbers, a LONG table (one row = one country at one
  instant) that joins easily with the rest of the project.

REGLES APPLIQUEES / RULES APPLIED (cf. docs/01 §8.3) :
  B1 : trous OPCD combles par les voisins immediats (max 1 point), sinon NULL + flag
  B2 : sous-ensemble de pays/colonnes pertinent (AT, BE, DE, FR, NL)
  B6 : lecture UCI : sep=';' , virgule decimale, date jj/mm/aaaa, '?' = NULL
  B7 : lignes UCI incompletes EXCLUES + comptees pour la qualite
  B8 : UCI : agregation 1 minute -> 1 heure et 1 jour (la minute reste en Bronze)

VERIFICATION :
  SELECT * FROM silver_db.iot_energie_pays LIMIT 10;
  SELECT count(*) FROM silver_db.iot_foyer_1h;   -- ~ 47 mois x 730 h
=====================================================================================
"""

import sys

sys.path.insert(0, "/opt/project/spark_jobs")

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import (CONFIG, add_technical_columns, ensure_databases, get_spark,
                    read_latest_snapshot, write_table)

DST = CONFIG["db_silver"]
SRC = CONFIG["db_bronze"]

# Sous-ensemble de pays retenu (regle B2) / selected country subset (rule B2)
COUNTRIES = ["AT", "BE", "DE", "FR", "NL"]


def _find_col(columns, country, semantic):
    """
    Trouve la colonne OPSD d'un pays pour une signification (charge, prix...).
    Finds the OPSD column of a country for one semantic (load, price...).

    POURQUOI / WHY : les colonnes OPSD ne suivent pas un schema unique par pays
    (ex: BE n'a pas de prix day-ahead). On cherche par motif plutot que par nom
    fige -> le job survit a une source qui evolue.
    OPSD columns are not uniform across countries (e.g. BE has no day-ahead price).
    Pattern matching instead of hard-coded names -> the job survives source changes.
    """
    patterns = {
        "charge": f"{country}_load_actual_entsoe_transparency",
        "prix": f"{country}_price_day_ahead",
        "solaire": f"{country}_solar_generation_actual",
        "eolien": f"{country}_wind_onshore_generation_actual",
    }
    return patterns[semantic] if patterns[semantic] in columns else None


def silver_opsd(spark):
    """
    OPSD : 300 colonnes -> table longue pays x heure / 300 columns -> long table country x hour.
    Regles / rules : B1 (trous), B2 (sous-ensemble).
    """
    cols = read_latest_snapshot(spark, f"{SRC}.opsd_time_series_60min").columns

    # Version UNION ALL : un SELECT par pays, fusionnes avec unionByName.
    # POURQUOI ne pas utiliser stack() : selon la version de pyspark/Spark, le
    #            generateur stack() dans selectExpr est traite comme UNE SEULE colonne
    #            ("IllegalArgumentException: Old column names (1): col0", constate le
    #            12/10/2026). L'UNION ALL est trivial, lisible et fonctionne partout.
    # WHY not stack(): depending on the pyspark/Spark version pair, the stack()
    #     generator in selectExpr is treated as ONE column. UNION ALL is trivial,
    #     readable and works everywhere (5 scans of a 50k-row table = negligible).
    # Chaque select : (pays, ts, charge, prix, solaire, eolien).
    dfs = []
    for c in COUNTRIES:
        exprs = [f"'{c}' AS pays", "to_timestamp(utc_timestamp) AS ts"]
        for semantic in ["charge", "prix", "solaire", "eolien"]:
            src_col = _find_col(cols, c, semantic)
            val = f"CAST({src_col} AS DOUBLE)" if src_col else "CAST(NULL AS DOUBLE)"
            exprs.append(f"{val} AS {semantic}")
        dfs.append(read_latest_snapshot(spark, f"{SRC}.opsd_time_series_60min").selectExpr(*exprs))

    long_df = dfs[0]
    for d in dfs[1:]:
        long_df = long_df.unionByName(d)

    # B1 : comblement des trous isoles (moyenne des voisins) sur chaque serie pays
    # B1 : fill isolated gaps (neighbor average) on each country series
    w = Window.partitionBy("pays").orderBy("ts")
    for semantic in ["charge", "prix", "solaire", "eolien"]:
        prev_c, next_c = f"_p_{semantic}", f"_n_{semantic}"
        long_df = (long_df
                   .withColumn(prev_c, F.lag(semantic).over(w))
                   .withColumn(next_c, F.lead(semantic).over(w))
                   .withColumn(semantic,
                               F.coalesce(F.col(semantic),
                                          F.when(F.col(prev_c).isNotNull() & F.col(next_c).isNotNull(),
                                                 (F.col(prev_c) + F.col(next_c)) / 2)
                                           .otherwise(F.col(prev_c))))
                   .drop(prev_c, next_c))

    long_df = add_technical_columns(long_df, source_file="opsd_time_series_60min.csv")
    write_table(long_df, DST, "iot_energie_pays", f"{CONFIG['hdfs_silver']}/iot/energie_pays")


def silver_uci(spark):
    """
    UCI : telemetrie minute -> tables 1h et 1j / minute telemetry -> 1h and 1d tables.
    Regles / rules : B6 (format), B7 (lignes incompletes), B8 (agregation).
    """
    df = read_latest_snapshot(spark, f"{SRC}.uci_household_power")

    # B6 : virgule decimale -> point, puis cast double ; '?' deja devenu NULL en Bronze
    # B6 : decimal comma -> dot, then cast double; '?' already became NULL in Bronze
    num_cols = ["global_active_power", "global_reactive_power", "voltage",
                "global_intensity", "sub_metering_1", "sub_metering_2", "sub_metering_3"]
    for c in num_cols:
        df = df.withColumn(c, F.regexp_replace(F.col(c), ",", ".").cast("double"))

    # Horodatage = date (j/m/aaaa) + heure / timestamp = date + time
    # POURQUOI "d/M/yyyy" et pas "dd/MM/yyyy" : le fichier UCI contient des dates a
    #            CHIFFRE UNIQUE ("1/1/2010") ; le parseur strict de Spark 3 refuse
    #            "dd/MM/yyyy" sur ces valeurs (DateTimeParseException, constate le
    #            13/10/2026). "d" et "M" acceptent 1 ou 2 chiffres.
    # WHY "d/M/yyyy" not "dd/MM/yyyy": the UCI file contains SINGLE-DIGIT dates
    #     ("1/1/2010"); Spark 3's strict parser rejects "dd/MM/yyyy" on those.
    #     "d" and "M" accept 1 or 2 digits.
    df = df.withColumn(
        "ts",
        F.to_timestamp(F.concat_ws(" ", F.col("date"), F.col("time")), "d/M/yyyy HH:mm:ss"),
    )

    total = df.count()
    df = df.filter(F.col("global_active_power").isNotNull())     # B7 : exclusion des lignes trouees
    print(f"[silver-uci] lignes exclues (trous) / excluded rows (gaps): {total - df.count()}")

    df = df.drop("date", "time")

    # B8 : agregations / aggregations
    hourly = (df
              .groupBy(F.window("ts", "1 hour"))
              .agg(
                  F.round(F.avg("global_active_power"), 3).alias("puissance_active_moy_kw"),
                  F.round(F.avg("voltage"), 2).alias("tension_moy_v"),
                  F.round(F.avg("global_intensity"), 3).alias("intensite_moy_a"),
                  F.round(F.sum("sub_metering_1"), 2).alias("sous_compteur_1_kwh"),
                  F.round(F.sum("sub_metering_2"), 2).alias("sous_compteur_2_kwh"),
                  F.round(F.sum("sub_metering_3"), 2).alias("sous_compteur_3_kwh"),
                  F.count("*").alias("nb_mesures"),
              )
              .select(F.col("window.start").alias("heure"), "*")
              .drop("window"))
    hourly = add_technical_columns(hourly, source_file="household_power_consumption.txt")

    daily = (df
             .groupBy(F.to_date("ts").alias("jour"))
             .agg(
                 F.round(F.avg("global_active_power"), 3).alias("puissance_active_moy_kw"),
                 F.round(F.max("global_active_power"), 3).alias("puissance_active_max_kw"),
                 F.round(F.sum("sub_metering_1"), 2).alias("sous_compteur_1_kwh"),
                 F.round(F.sum("sub_metering_2"), 2).alias("sous_compteur_2_kwh"),
                 F.round(F.sum("sub_metering_3"), 2).alias("sous_compteur_3_kwh"),
                 F.count("*").alias("nb_mesures"),
             ))
    daily = add_technical_columns(daily, source_file="household_power_consumption.txt")

    write_table(hourly, DST, "iot_foyer_1h", f"{CONFIG['hdfs_silver']}/iot/foyer_1h")
    write_table(daily, DST, "iot_foyer_1j", f"{CONFIG['hdfs_silver']}/iot/foyer_1j")


def main():
    spark = get_spark("silver_iot")
    ensure_databases(spark, [DST])
    print("[silver] === regles IoT : B1, B2, B6, B7, B8 (cf. docs/01) ===")
    silver_opsd(spark)
    silver_uci(spark)
    print("[silver] IoT termine / IoT done")
    spark.stop()


if __name__ == "__main__":
    main()
