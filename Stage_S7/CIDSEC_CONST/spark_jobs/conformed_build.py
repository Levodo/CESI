# -*- coding: utf-8 -*-
"""
=====================================================================================
conformed_build.py — Couche SILVER : jeux ENRICHIS multi-sources (silver/conformed/)
                     SILVER layer: multi-source ENRICHED datasets (silver/conformed/)

POURQUOI / WHY :
  Les sources sont propres mais encore ISOLEES en Silver. La valeur metier nait du
  CROISEMENT : la meteo explique les retards de livraison, les taux de change
  valorisent les trades en devise. Ce job construit ces jeux "conformed" (conformes),
  point d'entree unique des analyses et du ML qui melangent plusieurs sources.
  Sources are clean but still ISOLATED in Silver. Business value comes from
  CROSSING them: weather explains delivery delays, FX rates value trades in local
  currency. This job builds those "conformed" datasets, the single entry point for
  analyses and ML mixing several sources.

COMMENT / HOW :
  3 jeux ecrits dans silver_db (fichiers dans /data/silver/conformed/) :
    1. conformed_commandes_enrichies : commandes Olist + montants + METEO du jour
    2. conformed_trades_enrichis     : trades Cymbal + TAUX DE CHANGE du jour
    3. conformed_energie_pivot       : serie energie OPSD agregee jour x pays
  Toutes les jointures sont des LEFT JOIN : une source manquante n'efface jamais une
  ligne metier (les colonnes enrichies deviennent NULL, pas la ligne).
  All joins are LEFT JOINs: a missing source never erases a business row.

EQUIVALENT A LA MAIN / MANUAL EQUIVALENT :
  spark-submit --master spark://spark-master:7077 /opt/project/spark_jobs/conformed_build.py

VERIFICATION :
  SELECT count(*) FROM silver_db.conformed_commandes_enrichies;   -- 99 441 commandes
  SELECT order_id, delivery_delay_days, precipitations_mm
    FROM silver_db.conformed_commandes_enrichies WHERE precipitations_mm > 10 LIMIT 5;
  SELECT * FROM silver_db.conformed_trades_enrichis LIMIT 5;      -- valeur_brl remplie
=====================================================================================
"""

import sys

sys.path.insert(0, "/opt/project/spark_jobs")  # Permet "import common" / allows "import common"

from pyspark.sql import functions as F

from common import CONFIG, ensure_databases, get_spark, write_table

DST = CONFIG["db_silver"]


# ===================================================================================
# 1. Commandes Olist enrichies de la meteo Sao Paulo
#    Olist orders enriched with Sao Paulo weather
# POURQUOI / WHY : c'est le jeu d'entrainement du modele "pred_retard" (phase 3) :
#    la pluie et le vent le jour d'achat sont des candidates explicatives du retard.
#    This is the training set for the "pred_retard" model (phase 3): rain and wind on
#    the purchase day are candidate explanations for late delivery.
# ===================================================================================
def conformed_commandes(spark):
    """
    1 ligne = 1 COMMANDE, enrichie meteo / 1 row = 1 ORDER, weather-enriched.
    Grain : order_id (identique a fact_commande en Gold, pour comparaison directe).
    """
    orders = spark.table(f"{DST}.olist_orders")
    items = spark.table(f"{DST}.olist_order_items")
    meteo = spark.table(f"{DST}.meteo_quotidien")

    # --- Montants par commande (agregation des lignes) / per-order amounts ----------
    # POURQUOI : le grain cible est la COMMANDE ; les lignes d'articles sont donc
    # agregees avant jointure pour ne pas dupliquer les commandes.
    # WHY: the target grain is the ORDER; item rows are aggregated before the join
    # so orders do not get duplicated.
    montants = (items.groupBy("order_id")
                .agg(
                    F.round(F.sum("price"), 2).alias("montant_articles"),
                    F.round(F.sum("freight_value"), 2).alias("montant_fret"),
                    F.count("*").alias("nb_articles"),
                ))
    montants = montants.withColumn(
        "montant_total",
        F.round(F.col("montant_articles") + F.col("montant_fret"), 2))

    # --- Jointure commandes x montants / orders x amounts ---------------------------
    df = orders.join(montants, "order_id", "left")

    # --- Enrichissement meteo sur le JOUR D'ACHAT / weather on PURCHASE DAY ----------
    # COMMENT / HOW : on deduit la date d'achat, puis LEFT JOIN sur la meteo du jour.
    # La meteo couvre exactement la periode Olist (cf. docs/01 §8.2) : aucune ligne
    # ne devrait etre orpheline, mais le LEFT JOIN protege quoi qu'il arrive.
    df = df.withColumn("jour_achat", F.to_date("order_purchase_timestamp"))
    df = (df.join(meteo.select(
                    "jour",
                    "temp_moy_c", "temp_max_c", "temp_min_c",
                    "precipitations_mm", "vent_max_kmh"),
                  df["jour_achat"] == meteo["jour"], "left")
          .drop("jour"))

    # Feature derivee : jour de pluie (binaire, lisible par un modele ET par un humain)
    # Derived feature: rainy day (binary, readable by a model AND by a human)
    df = df.withColumn(
        "is_jour_pluie",
        F.when(F.col("precipitations_mm") >= 1.0, F.lit(True)).otherwise(F.lit(False)))

    write_table(df, DST, "conformed_commandes_enrichies",
                f"{CONFIG['hdfs_silver']}/conformed/commandes_enrichies")


# ===================================================================================
# 2. Trades Cymbal enrichis des taux de change
#    Cymbal trades enriched with FX rates
# POURQUOI / WHY : les prix de trade sont en USD ; les parties prenantes europeennes
#    raisonnent en EUR/BRL. Valoriser chaque trade au taux de son jour est une
#    regle de marche standard (et le taux est deja "forward-fill", cf. regle B3).
#    Trade prices are in USD; European stakeholders think in EUR/BRL. Valuing each
#    trade at its day rate is standard market practice (rate already forward-filled).
# ===================================================================================
def conformed_trades(spark):
    """
    1 ligne = 1 TRADE, valorise en BRL/EUR / 1 row = 1 TRADE, valued in BRL/EUR.
    Grain : trade_report_id.
    """
    trades = spark.table(f"{DST}.cymbal_trades")
    fx = spark.table(f"{DST}.taux_change")

    df = trades.withColumn("jour", F.col("trade_date"))

    # LEFT JOIN sur le jour : le taux est connu pour toute la periode Cymbal (docs/01 §8.2)
    # LEFT JOIN on the day: rate known for the whole Cymbal period
    df = (df.join(fx.select("jour", "usd_vers_brl", "usd_vers_eur", "est_jour_ouvre"),
                  "jour", "left"))

    # Valorisation / valuation : prix x taux du jour
    df = (df
          .withColumn("valeur_brl", F.round(F.col("last_px") * F.col("usd_vers_brl"), 2))
          .withColumn("valeur_eur", F.round(F.col("last_px") * F.col("usd_vers_eur"), 2)))

    write_table(df, DST, "conformed_trades_enrichis",
                f"{CONFIG['hdfs_silver']}/conformed/trades_enrichis")


# ===================================================================================
# 3. Pivot energie : OPSD agregé jour x pays
#    Energy pivot: OPSD aggregated day x country
# POURQUOI / WHY : la serie brute est horaire x pays x 4 semantiques. Les analyses et
#    le ML quotidien ont besoin d'une forme "pivot" lisible : 1 ligne = 1 pays x 1 jour.
#    The raw series is hourly x country x 4 semantics. Daily analysis and ML need a
#    readable "pivot" shape: 1 row = 1 country x 1 day.
# ===================================================================================
def conformed_energie(spark):
    """
    1 ligne = 1 PAYS x 1 JOUR / 1 row = 1 COUNTRY x 1 DAY.
    """
    df = spark.table(f"{DST}.iot_energie_pays")

    df = (df
          .withColumn("jour", F.to_date("ts"))
          .groupBy("pays", "jour")
          .agg(
              F.round(F.avg("charge"), 2).alias("charge_moy_mw"),
              F.round(F.max("charge"), 2).alias("charge_max_mw"),
              F.round(F.avg("prix"), 2).alias("prix_moy"),
              F.round(F.avg("solaire"), 2).alias("solaire_moy_mw"),
              F.round(F.avg("eolien"), 2).alias("eolien_moy_mw"),
              F.count("*").alias("nb_points"),   # controle : 24 points = jour complet
          ))

    write_table(df, DST, "conformed_energie_pivot",
                f"{CONFIG['hdfs_silver']}/conformed/energie_pivot")


def main():
    spark = get_spark("conformed_build")
    ensure_databases(spark, [DST])
    print("[conformed] === jeux enrichis multi-sources (cf. docs/02, croisements) ===")
    conformed_commandes(spark)
    conformed_trades(spark)
    conformed_energie(spark)
    print("[conformed] termine / done")
    spark.stop()


if __name__ == "__main__":
    main()
