# -*- coding: utf-8 -*-
"""
=====================================================================================
gold_build.py — Couche GOLD : modele dimensionnel (faits + dimensions) + export JDBC
                GOLD layer: dimensional model (facts + dimensions) + JDBC export

POURQUOI / WHY :
  La Gold transforme des donnees "propres" en donnees "decidees" : un modele en
  etoile (une table de faits au centre, des dimensions autour) est LE format que
  Power BI et les utilisateurs comprennent sans formation. Chaque table a un GRAIN
  (granularite) fige et documente — la decision la plus importante du modele.
  Gold turns "clean" data into "decision-ready" data: a star schema (one fact table
  at the centre, dimensions around) is THE format Power BI and business users
  understand without training. Each table has a fixed, documented GRAIN — the most
  important decision of the model.

  Puis, copie vers PostgreSQL via JDBC : c'est la porte d'entree de Power BI.
  Then, copy to PostgreSQL via JDBC: this is Power BI's entry door.

COMMENT / HOW :
  spark-submit --master spark://spark-master:7077 /opt/project/spark_jobs/gold_build.py
  (ou automatise par le DAG dag_05_gold_build / or automated by the dag_05_gold_build DAG)

GRAINS DES TABLES DE FAITS / FACT TABLE GRAINS :
  fact_commande      : 1 ligne = 1 COMMANDE (order_id)
  fact_trades        : 1 ligne = 1 TRADE (trade_report_id)
  fact_energie_jour  : 1 ligne = 1 PAYS x 1 JOUR
  fact_foyer_jour    : 1 ligne = 1 FOYER x 1 JOUR (le foyer est unique ici)

VERIFICATION :
  SELECT * FROM gold_db.kpi_ca_quotidien ORDER BY jour DESC LIMIT 10;
  docker exec cidsec-postgres psql -U spark -d gold_db -c "SELECT count(*) FROM gold.fact_commande;"
=====================================================================================
"""

import sys

sys.path.insert(0, "/opt/project/spark_jobs")

from pyspark.sql import functions as F

from common import (CONFIG, ensure_databases, get_spark, write_postgres, write_table)

SRC = CONFIG["db_silver"]
DST = CONFIG["db_gold"]


# ==================================================================================
# DIMENSIONS
# ==================================================================================
def dim_date(spark):
    """
    Dimension calendrier partagee / shared calendar dimension (conformed).
    POURQUOI / WHY : une SEULE dimension date pour toutes les etoiles -> les regles
    de decoupage (trimestre, semaine...) sont coherentes partout, et Power BI
    peut filtrer toutes les faits d'un meme coup.
    ONE date dimension for all stars -> time-slicing rules are consistent everywhere.
    """
    dates = spark.range(1).select(
        F.explode(F.sequence(F.to_date(F.lit("2006-01-01")),
                             F.to_date(F.lit("2021-12-31")),
                             F.expr("interval 1 day"))).alias("jour"))
    df = (dates
          .withColumn("annee", F.year("jour"))
          .withColumn("trimestre", F.quarter("jour"))
          .withColumn("mois", F.month("jour"))
          .withColumn("nom_mois", F.date_format("jour", "MMMM"))
          .withColumn("semaine", F.weekofyear("jour"))
          .withColumn("jour_semaine", F.dayofweek("jour"))          # 1 = dimanche
          .withColumn("nom_jour", F.date_format("jour", "EEEE"))
          .withColumn("est_weekend", F.col("jour_semaine").isin([1, 7])))
    write_table(df, DST, "dim_date", f"{CONFIG['hdfs_gold']}/dim_date")
    return df


def dim_client(spark):
    """1 ligne = 1 CLIENT / 1 row = 1 CUSTOMER. Cle = customer_id."""
    df = spark.table(f"{SRC}.olist_customers")
    write_table(df, DST, "dim_client", f"{CONFIG['hdfs_gold']}/dim_client")
    return df


def dim_produit(spark):
    """1 ligne = 1 PRODUIT / 1 row = 1 PRODUCT. Cle = product_id (categories A1-A3 deja appliquees)."""
    df = spark.table(f"{SRC}.olist_products")
    write_table(df, DST, "dim_produit", f"{CONFIG['hdfs_gold']}/dim_produit")
    return df


def dim_vendeur(spark):
    """1 ligne = 1 VENDEUR / 1 row = 1 SELLER. Cle = seller_id."""
    df = spark.table(f"{SRC}.olist_sellers")
    write_table(df, DST, "dim_vendeur", f"{CONFIG['hdfs_gold']}/dim_vendeur")
    return df


def dim_geographie(spark):
    """1 ligne = 1 CODE POSTAL / 1 row = 1 ZIP CODE (coordonnees moyennes, regle A7)."""
    df = spark.table(f"{SRC}.olist_geolocation")
    write_table(df, DST, "dim_geographie", f"{CONFIG['hdfs_gold']}/dim_geographie")
    return df


def dim_pays(spark):
    """1 ligne = 1 PAYS du reseau energetique / 1 row = 1 energy-network COUNTRY."""
    df = (spark.table(f"{SRC}.iot_energie_pays")
          .select("pays").distinct()
          .withColumn("nom_pays", F.when(F.col("pays") == "AT", F.lit("Autriche"))
                                   .when(F.col("pays") == "BE", F.lit("Belgique"))
                                   .when(F.col("pays") == "DE", F.lit("Allemagne"))
                                   .when(F.col("pays") == "FR", F.lit("France"))
                                   .when(F.col("pays") == "NL", F.lit("Pays-Bas"))
                                   .otherwise(F.col("pays"))))
    write_table(df, DST, "dim_pays", f"{CONFIG['hdfs_gold']}/dim_pays")
    return df


# ==================================================================================
# TABLES DE FAITS / FACT TABLES
# ==================================================================================
def fact_commande(spark):
    """
    1 ligne = 1 COMMANDE / 1 row = 1 ORDER (grain: order_id).

    POURQUOI / WHY :
      Ce grain (et non la ligne de commande) est choisi car la question metier
      principale est "comment se portent les COMMANDES ?" (CA, delais, satisfaction).
      Chaque mesure est ADDITIVE (on peut sommer sur les dimensions) — regle d'or
      des mesures de faits.
      This grain (not the order line) is chosen because the main business question
      is "how are ORDERS doing?". Every measure is ADDITIVE — golden rule of
      fact measures.

    Mesures / measures : montant_articles, frais_port, montant_total, nb_articles,
                         montant_paye, delai_livraison_jours, is_late, review_score
    Cles etrangeres / foreign keys : customer_id, jour_achat (-> dim_date)
    """
    orders = spark.table(f"{SRC}.olist_orders")
    items = spark.table(f"{SRC}.olist_order_items")
    payments = spark.table(f"{SRC}.olist_order_payments")
    reviews = spark.table(f"{SRC}.olist_order_reviews")

    items_agg = (items.groupBy("order_id").agg(
        F.round(F.sum("price"), 2).alias("montant_articles"),
        F.round(F.sum("freight_value"), 2).alias("frais_port"),
        F.count("*").alias("nb_articles")))

    pay_agg = (payments.groupBy("order_id").agg(
        F.round(F.sum("payment_value"), 2).alias("montant_paye"),
        F.count("*").alias("nb_paiements")))

    rev_agg = reviews.groupBy("order_id").agg(
        F.max("review_score").alias("review_score"))

    df = (orders
          .join(items_agg, "order_id", "left")
          .join(pay_agg, "order_id", "left")
          .join(rev_agg, "order_id", "left")
          .select(
              "order_id", "customer_id", "order_status",
              F.to_date("order_purchase_timestamp").alias("jour_achat"),
              F.col("montant_articles"),
              F.col("frais_port"),
              F.round(F.coalesce(F.col("montant_articles"), F.lit(0))
                      + F.coalesce(F.col("frais_port"), F.lit(0)), 2).alias("montant_total"),
              F.col("nb_articles"), F.col("montant_paye"), F.col("nb_paiements"),
              F.col("delivery_delay_days").alias("delai_livraison_jours"),
              "is_late", "is_approved", "review_score",
          ))
    write_table(df, DST, "fact_commande", f"{CONFIG['hdfs_gold']}/fact_commande")
    return df


def fact_trades(spark):
    """
    1 ligne = 1 TRADE / 1 row = 1 TRADE (grain: trade_report_id).
    Mesures : quantite, dernier_prix, strike, valeur notionnelle en USD et EUR.
    Regle de coherence : jointure avec la table taux_change (Silver API) sur la date
    -> chaque trade est valorise dans sa devise de marche ET en EUR.
    Coherence rule: join with the taux_change table (Silver API) on the trade date
    -> each trade is valued in its market currency AND in EUR.
    """
    trades = spark.table(f"{SRC}.cymbal_trades")
    fx = spark.table(f"{SRC}.taux_change").select("jour", "usd_vers_eur", "usd_vers_brl")

    df = (trades
          .join(fx, trades["trade_date"] == fx["jour"], "left")
          .select(
              "tradereportid", "order_id", "symbol", "side", "partyid",
              "trade_date", "sending_time", "transact_time",
              "quantity", "last_px", "strike_price",
              F.round(F.col("last_px") * F.col("quantity"), 2).alias("valeur_usd"),
              F.round(F.col("last_px") * F.col("quantity") * F.col("usd_vers_eur"), 2).alias("valeur_eur"),
              F.round(F.col("last_px") * F.col("quantity") * F.col("usd_vers_brl"), 2).alias("valeur_brl"),
          ))
    write_table(df, DST, "fact_trades", f"{CONFIG['hdfs_gold']}/fact_trades")
    return df


def fact_energie_jour(spark):
    """
    1 ligne = 1 PAYS x 1 JOUR / 1 row = 1 COUNTRY x 1 DAY (grain: pays + jour).
    Mesures : charge moyenne/max, prix moyen, production solaire/eolienne cumulee.
    """
    df = (spark.table(f"{SRC}.iot_energie_pays")
          .withColumn("jour", F.to_date("ts"))
          .groupBy("pays", "jour")
          .agg(
              F.round(F.avg("charge"), 2).alias("charge_moy_mw"),
              F.round(F.max("charge"), 2).alias("charge_max_mw"),
              F.round(F.avg("prix"), 2).alias("prix_moy"),
              F.round(F.sum("solaire"), 2).alias("solaire_total_mwh"),
              F.round(F.sum("eolien"), 2).alias("eolien_total_mwh"),
          ))
    write_table(df, DST, "fact_energie_jour", f"{CONFIG['hdfs_gold']}/fact_energie_jour")
    return df


def fact_foyer_jour(spark):
    """
    1 ligne = 1 JOUR pour le foyer capte / 1 row = 1 DAY for the monitored household.
    Mesures : consommation moyenne/max, sous-compteurs (cuisine, buanderie, clim).
    """
    df = spark.table(f"{SRC}.iot_foyer_1j").select(
        F.col("jour"), "puissance_active_moy_kw", "puissance_active_max_kw",
        "sous_compteur_1_kwh", "sous_compteur_2_kwh", "sous_compteur_3_kwh", "nb_mesures")
    write_table(df, DST, "fact_foyer_jour", f"{CONFIG['hdfs_gold']}/fact_foyer_jour")
    return df


def kpi_ca_quotidien(spark, fact_commande_df):
    """
    Table d'agregat metier : CA quotidien (c'est LA table que Power BI affichera).
    Business aggregate table: daily revenue (the table Power BI will display).
    1 ligne = 1 JOUR / 1 row = 1 DAY.
    """
    df = (fact_commande_df
          .groupBy("jour_achat")
          .agg(
              F.round(F.sum("montant_total"), 2).alias("ca_total"),
              F.count("*").alias("nb_commandes"),
              F.round(F.avg("montant_total"), 2).alias("panier_moyen"),
              F.round(F.avg(F.when(F.col("is_late"), 1).otherwise(0)), 4).alias("taux_retard"),
              F.round(F.avg("review_score"), 2).alias("note_moyenne"),
          )
          .withColumnRenamed("jour_achat", "jour")
          .orderBy("jour"))
    write_table(df, DST, "kpi_ca_quotidien", f"{CONFIG['hdfs_gold']}/kpi_ca_quotidien")
    return df


def main():
    spark = get_spark("gold_build")
    ensure_databases(spark, [DST])
    print("[gold] === construction du modele en etoile / star schema build ===")

    # Dimensions d'abord (elles servent de cibles de jointure), puis les faits.
    # Dimensions first (they are join targets), then facts.
    dim_date(spark)
    dim_client(spark)
    dim_produit(spark)
    dim_vendeur(spark)
    dim_geographie(spark)
    dim_pays(spark)

    fc = fact_commande(spark)
    fact_trades(spark)
    fact_energie_jour(spark)
    fact_foyer_jour(spark)
    kpi = kpi_ca_quotidien(spark, fc)

    # --- Export PostgreSQL (Spark JDBC) : porte d'entree Power BI ------------------
    # --- PostgreSQL export (Spark JDBC): Power BI entry door ----------------------
    print("[gold] === export JDBC vers PostgreSQL / JDBC export to PostgreSQL ===")
    for table, df in [
        ("dim_date", spark.table(f"{DST}.dim_date")),
        ("dim_client", spark.table(f"{DST}.dim_client")),
        ("dim_produit", spark.table(f"{DST}.dim_produit")),
        ("dim_vendeur", spark.table(f"{DST}.dim_vendeur")),
        ("dim_geographie", spark.table(f"{DST}.dim_geographie")),
        ("dim_pays", spark.table(f"{DST}.dim_pays")),
        ("fact_commande", fc),
        ("fact_trades", spark.table(f"{DST}.fact_trades")),
        ("fact_energie_jour", spark.table(f"{DST}.fact_energie_jour")),
        ("fact_foyer_jour", spark.table(f"{DST}.fact_foyer_jour")),
        ("kpi_ca_quotidien", kpi),
    ]:
        write_postgres(df, table)

    print("[gold] Gold termine / Gold done")
    spark.stop()


if __name__ == "__main__":
    main()
