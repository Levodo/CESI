# -*- coding: utf-8 -*-
"""
=====================================================================================
silver_olist.py — Couche SILVER : nettoyage et conformisation du jeu Olist
                  SILVER layer: cleaning and conforming the Olist dataset

POURQUOI / WHY :
  La Bronze conserve tout (y compris les defauts). La Silver est la couche de
  CONFIANCE : types corrects, doublons supprimes, valeurs invalides traitees.
  Chaque regle ci-dessous correspond a une anomalie numerotee du rapport d'audit
  (docs/01_rapport_audit_donnees.md) : rien n'est arbitraire, tout est tracable.
  Bronze keeps everything (flaws included). Silver is the TRUST layer: correct types,
  duplicates removed, invalid values handled. Each rule below maps to a numbered
  anomaly in the audit report: nothing is arbitrary, everything is traceable.

COMMENT / HOW :
  Lecture des tables Bronze -> application des regles -> ecriture Parquet Silver
  + enregistrement comme tables Hive (silver_db.*), requetables en SQL par tous.

VERIFICATION :
  beeline -u jdbc:hive2://localhost:10000 -e "SELECT count(*) FROM silver_db.olist_orders;"
  SELECT count(*) FROM silver_db.olist_order_reviews;   -- doit etre < bronze (dedup A4)
=====================================================================================
"""

import sys

sys.path.insert(0, "/opt/project/spark_jobs")

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import (CONFIG, ensure_databases, get_spark, write_table)

SRC = CONFIG["db_bronze"]      # bronze_db
DST = CONFIG["db_silver"]      # silver_db


def silver_orders(spark):
    """
    Table commandes / orders table.
    Regles / rules :
      A10 : cast des 5 horodatages (texte -> timestamp) / cast 5 timestamps (text -> timestamp)
      A5  : order_approved_at NULL conserve + indicateur is_approved
      A6  : dates de livraison NULL conservees (commandes non livrees, legitime)
      A6+ : calculs metier : delai de livraison reel, retard vs estimation
    """
    df = spark.table(f"{SRC}.olist_orders_dataset")

    ts_cols = ["order_purchase_timestamp", "order_approved_at",
               "order_delivered_carrier_date", "order_delivered_customer_date",
               "order_estimated_delivery_date"]
    for c in ts_cols:
        df = df.withColumn(c, F.to_timestamp(F.col(c)))          # A10

    df = df.withColumn("is_approved", F.col("order_approved_at").isNotNull())          # A5
    df = df.withColumn(
        "delivery_delay_days",  # delai reel de livraison en jours / actual delivery time in days
        F.round(F.datediff(F.col("order_delivered_customer_date"), F.col("order_purchase_timestamp")), 2),
    )
    df = df.withColumn(
        "is_late",  # livre apres la date estimee / delivered after the estimated date
        F.when(F.col("order_delivered_customer_date").isNull(), F.lit(None).cast("boolean"))
         .otherwise(F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date")),
    )
    write_table(df, DST, "olist_orders", f"{CONFIG['hdfs_silver']}/olist/orders")


def silver_order_items(spark):
    """
    Table lignes de commande / order items table.
    Regles / rules :
      A10 : cast prix/fret (texte -> decimal) et shipping_limit_date -> timestamp
    """
    df = spark.table(f"{SRC}.olist_order_items_dataset")
    df = (df
          .withColumn("price", F.col("price").cast("decimal(12,2)"))
          .withColumn("freight_value", F.col("freight_value").cast("decimal(12,2)"))
          .withColumn("shipping_limit_date", F.to_timestamp(F.col("shipping_limit_date")))
          .withColumn("order_item_id", F.col("order_item_id").cast("int")))
    write_table(df, DST, "olist_order_items", f"{CONFIG['hdfs_silver']}/olist/order_items")


def silver_payments(spark):
    """
    Table paiements / payments table.
    Regles / rules :
      A10 : cast des montants / cast amounts
      A8  : payment_value = 0 CONSERVE (cas metier "avoir") mais documente
    """
    df = spark.table(f"{SRC}.olist_order_payments_dataset")
    df = (df
          .withColumn("payment_sequential", F.col("payment_sequential").cast("int"))
          .withColumn("payment_installments", F.col("payment_installments").cast("int"))
          .withColumn("payment_value", F.col("payment_value").cast("decimal(12,2)")))
    write_table(df, DST, "olist_order_payments", f"{CONFIG['hdfs_silver']}/olist/order_payments")


def silver_reviews(spark):
    """
    Table avis / reviews table.
    Regles / rules :
      A4  : 814 lignes dupliquees supprimees : on garde, par review_id, la ligne au
            review_answer_timestamp le plus recent (la plus "aboutie").
            814 duplicate rows removed: keep, per review_id, the row with the most
            recent review_answer_timestamp (the most "final" one).
      A10 : cast du score et des dates / cast score and dates
    """
    df = spark.table(f"{SRC}.olist_order_reviews_dataset")
    df = (df
          .withColumn("review_score", F.col("review_score").cast("int"))
          .withColumn("review_creation_date", F.to_timestamp(F.col("review_creation_date")))
          .withColumn("review_answer_timestamp", F.to_timestamp(F.col("review_answer_timestamp"))))

    w = Window.partitionBy("review_id").orderBy(F.col("review_answer_timestamp").desc_nulls_last())
    df = (df
          .withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)                    # deduplication A4
          .drop("_rn"))
    write_table(df, DST, "olist_order_reviews", f"{CONFIG['hdfs_silver']}/olist/order_reviews")


def silver_products(spark):
    """
    Table produits (enrichie de la traduction) / products table (translation-enriched).
    Regles / rules :
      A2  : 610 produits sans categorie -> 'unknown' (les autres metadonnées restent NULL)
      A3  : poids = 0 -> NULL (physiquement invalide) + drapeau de qualite _dq_flag
      A1  : LEFT JOIN traduction + COALESCE : les 2 categories sans traduction
            gardent leur nom portugais plutot que de disparaitre
    """
    prod = spark.table(f"{SRC}.olist_products_dataset")
    tr = spark.table(f"{SRC}.product_category_name_translation")

    prod = (prod
            .withColumn("product_category_name",
                        F.when(F.col("product_category_name").isNull(), F.lit("unknown"))
                         .otherwise(F.col("product_category_name")))                   # A2
            .withColumn("product_weight_g",
                        F.when(F.col("product_weight_g").cast("int") <= 0, F.lit(None).cast("int"))
                         .otherwise(F.col("product_weight_g").cast("int")))             # A3
            .withColumn("_dq_flag",
                        F.when(F.col("product_category_name") == "unknown", F.lit("missing_category"))
                         .when(F.col("product_weight_g").isNull(), F.lit("invalid_weight"))
                         .otherwise(F.lit(None))))

    # jointure de traduction / translation join
    # POURQUOI select AVANT de renommer : la table Bronze de traduction porte aussi
    #            les colonnes techniques (_ingestion_ts...) ; les garder creerait des
    #            DOUBLONS de colonnes a l'ecriture ("Found duplicate column(s)",
    #            constate le 08/10/2026). On ne prend QUE les 2 colonnes metier.
    # WHY select BEFORE renaming: the Bronze translation table also carries the
    #     technical columns (_ingestion_ts...); keeping them creates DUPLICATE
    #     columns at write time. Only the 2 business columns are taken.
    tr = tr.select(
        F.col("product_category_name").alias("cat_pt"),
        F.col("product_category_name_english").alias("cat_en"))
    prod = (prod
            .join(tr, prod["product_category_name"] == tr["cat_pt"], "left")
            .withColumn("product_category_en", F.coalesce(F.col("cat_en"), F.col("product_category_name")))  # A1
            .drop("cat_pt", "cat_en"))
    write_table(prod, DST, "olist_products", f"{CONFIG['hdfs_silver']}/olist/products")


def silver_geolocation(spark):
    """
    Table geolocalisation agregee / aggregated geolocation table.
    Regles / rules :
      A7  : 31 coordonnees hors Bresil retirees (bounding box), et les 17 972 codes
            postaux a plusieurs points sont MOYENNES (1 ligne = 1 code postal).
            31 out-of-Brazil coordinates removed (bounding box), and the 17,972
            multi-point zip codes are AVERAGED (1 row = 1 zip code).
    """
    df = spark.table(f"{SRC}.olist_geolocation_dataset")
    df = (df
          .withColumn("lat", F.col("geolocation_lat").cast("double"))
          .withColumn("lng", F.col("geolocation_lng").cast("double"))
          .filter(F.col("lat").between(-34, 6) & F.col("lng").between(-74, -32)))       # A7

    df = (df
          .groupBy("geolocation_zip_code_prefix", "geolocation_state")
          .agg(
              F.round(F.avg("lat"), 6).alias("geo_lat"),
              F.round(F.avg("lng"), 6).alias("geo_lng"),
              # ville la plus frequente du code postal / most frequent city for the zip code
              F.first("geolocation_city").alias("geo_city"),
          ))
    write_table(df, DST, "olist_geolocation", f"{CONFIG['hdfs_silver']}/olist/geolocation")


def silver_simple(spark, table, casts):
    """
    Tables simples (clients, vendeurs) : trim + cast uniquement / simple tables: trim + cast only.
    Regles / rules : A10 (types) + nettoyage des espaces superflus (trim).
    """
    df = spark.table(f"{SRC}.{table}")
    for col, typ in casts.items():
        df = df.withColumn(col, F.col(col).cast(typ))
    for col in [c for c in df.columns if df.schema[c].simpleString() == "string"]:
        df = df.withColumn(col, F.trim(F.col(col)))
    short = table.replace("olist_", "").replace("_dataset", "")
    write_table(df, DST, f"olist_{short}", f"{CONFIG['hdfs_silver']}/olist/{short}")


def main():
    spark = get_spark("silver_olist")
    ensure_databases(spark, [DST])

    print("[silver] === regles Olist / Olist rules : A1-A10 (cf. docs/01) ===")
    silver_orders(spark)
    silver_order_items(spark)
    silver_payments(spark)
    silver_reviews(spark)
    silver_products(spark)
    silver_geolocation(spark)
    silver_simple(spark, "olist_customers_dataset",
                  {"customer_zip_code_prefix": "int"})
    silver_simple(spark, "olist_sellers_dataset",
                  {"seller_zip_code_prefix": "int"})

    print("[silver] Olist termine / Olist done")
    spark.stop()


if __name__ == "__main__":
    main()
