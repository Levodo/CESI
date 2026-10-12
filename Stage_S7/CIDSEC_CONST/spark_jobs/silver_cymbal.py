# -*- coding: utf-8 -*-
"""
=====================================================================================
silver_cymbal.py — Couche SILVER : transactions financieres Cymbal Investments
                   SILVER layer: Cymbal Investments financial trades

POURQUOI / WHY :
  Le fichier de trade capture est propre mais "brut" : doublons de TradeReportID,
  colonnes constantes sans interet analytique, types texte. La Silver le transforme
  en table de faits financiers exploitable (une ligne = un trade unique).
  The trade capture file is clean but "raw": TradeReportID duplicates, constant
  columns of no analytical value, text types. Silver turns it into a usable
  financial fact table (one row = one unique trade).

REGLES APPLIQUEES / RULES APPLIED (cf. docs/01 §8.3) :
  A10 : cast des types (texte -> timestamp/decimal/int)
  A11 : 9 doublons de TradeReportID supprimes (on garde le SendingTime le plus recent)
  A13 : colonnes constantes retirees du modele (Quantity, CFICode, SenderCompID,
        PartyIDSource, PartyRole) — elles restent en Bronze

VERIFICATION :
  SELECT count(*) FROM silver_db.cymbal_trades;   -- = 1 222 553 (apres dedup A11)
  SELECT count(DISTINCT trade_report_id) FROM silver_db.cymbal_trades;
=====================================================================================
"""

import sys

sys.path.insert(0, "/opt/project/spark_jobs")

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import (CONFIG, ensure_databases, get_spark, write_table)

DST = CONFIG["db_silver"]
SRC = CONFIG["db_bronze"]

# Colonnes constantes (A13), mesurees par l'audit / constant columns, measured by the audit
CONSTANT_COLS = ["quantity", "cficode", "sendercompid", "partyidsource", "partyrole"]


def main():
    spark = get_spark("silver_cymbal")
    ensure_databases(spark, [DST])
    print("[silver] === regles Cymbal : A10, A11, A13 (cf. docs/01) ===")

    df = spark.table(f"{SRC}.cymbal_investments_trade_capture_report")

    # --- A10 : typage / typing ------------------------------------------------------
    df = (df
          .withColumn("sending_time", F.to_timestamp("sendingtime"))
          .withColumn("transact_time", F.to_timestamp("transacttime"))
          .withColumn("trade_date", F.to_date("tradedate"))
          .withColumn("maturity_date", F.to_timestamp("maturitydate"))   # champ artificiel (A12)
          .withColumn("strike_price", F.col("strikeprice").cast("decimal(14,3)"))
          .withColumn("last_px", F.col("lastpx").cast("decimal(14,3)"))
          .withColumn("quantity", F.col("quantity").cast("int")))

    # --- A11 : deduplication / deduplication ---------------------------------------
    w = Window.partitionBy("tradereportid").orderBy(F.col("sending_time").desc_nulls_last())
    before = df.count()
    df = (df
          .withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn"))
    print(f"[silver] doublons supprimes / duplicates removed: {before - df.count()}")

    # --- A13 : retrait des colonnes constantes / drop constant columns --------------
    df = df.drop(*[c for c in CONSTANT_COLS if c in df.columns])

    write_table(df, DST, "cymbal_trades", f"{CONFIG['hdfs_silver']}/cymbal/trades")
    print("[silver] Cymbal termine / Cymbal done")
    spark.stop()


if __name__ == "__main__":
    main()
