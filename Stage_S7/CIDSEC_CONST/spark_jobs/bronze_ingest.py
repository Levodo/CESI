# -*- coding: utf-8 -*-
"""
=====================================================================================
bronze_ingest.py — Couche BRONZE : copie fidele et immutable des sources
                   BRONZE layer: faithful, immutable copy of the sources

POURQUOI / WHY :
  La Bronze est le "filet de securite" du projet : quoi qu'il arrive aux traitements
  en aval (bug, mauvaise regle), la donnee d'origine reste disponible, verbatim.
  AUCUNE transformation metier n'est appliquee ici — on ajoute seulement les colonnes
  techniques de tracabilite. C'est la regle d'or Medallion.
  Bronze is the project's "safety net": whatever happens downstream (bug, wrong rule),
  the original data remains available, verbatim. NO business transformation here —
  only technical traceability columns. This is the Medallion golden rule.

COMMENT / HOW :
  Lecture du fichier source (CSV/JSON/TXT) TOUT EN CHAINE DE CARACTERES (string) pour
  ne rien perdre ni rien convertir, puis ecriture Parquet partitionnee par jour.
  Reads the source file ALL AS STRINGS so nothing is lost or converted, then writes
  day-partitioned Parquet.

  Exemples d'appel / call examples :
    python bronze_ingest.py --source olist  --table olist_orders_dataset \
        --input /landing/E-Commerce/olist_orders_dataset.csv --sep ,
    python bronze_ingest.py --source iot    --table uci_household_power \
        --input /landing/UCI_power/household_power_consumption.txt --sep ";"
    python bronze_ingest.py --source api    --table meteo_sp_daily \
        --input /landing_api/openmeteo_sp_daily_2016_2018.json --format json

VERIFICATION :
  hdfs dfs -ls /data/bronze/<source>/<table>
  beeline -u jdbc:hive2://localhost:10000 -e "SELECT count(*) FROM bronze_db.<table>;"
=====================================================================================
"""

import argparse
import sys

sys.path.insert(0, "/opt/project/spark_jobs")  # Permet "import common" / allows "import common"

from pyspark.sql import functions as F

from common import (CONFIG, add_technical_columns, ensure_databases, get_spark,
                    normalize_column_names, write_table)


def parse_args():
    """Arguments de la ligne de commande / command-line arguments."""
    p = argparse.ArgumentParser(description="Ingestion Bronze / Bronze ingestion")
    p.add_argument("--source", required=True,
                   help="Famille de source : olist | cymbal | iot | api")
    p.add_argument("--table", required=True,
                   help="Nom de la table cible (sans prefixe) / target table name")
    p.add_argument("--input", required=True,
                   help="Chemin LOCAL du fichier (montage Docker) / local file path")
    p.add_argument("--format", default="csv", choices=["csv", "json", "txt"],
                   help="Format du fichier / file format (defaut: csv)")
    p.add_argument("--sep", default=",",
                   help="Separateur CSV / CSV separator (defaut: , ; UCI utilise ;)")
    p.add_argument("--load-date", default=None,
                   help="Date de partition forcee (AAAA-MM-JJ) / forced partition date")
    return p.parse_args()


def read_source(spark, args):
    """
    Lit le fichier source SANS le transformer / Reads the source file WITHOUT transforming it.

    POURQUOI / WHY :
      - header=True : la 1re ligne contient les noms de colonnes.
      - inferSchema=False (defaut) : TOUT reste string en Bronze. C'est volontaire :
        typage = responsabilite de Silver (regle A10 de l'audit).
      - inferSchema=False (default): EVERYTHING stays string in Bronze. On purpose:
        typing is Silver's responsibility (audit rule A10).
    COMMENT / HOW :
      Les options varient selon le format : sep pour CSV/TXT, multiLine pour JSON.

    POURQUOI LE PREFIXE file:// / WHY THE file:// PREFIX :
      Le core-site.xml definit fs.defaultFS=hdfs://namenode:9000 : tout chemin NU
      ("/landing/...") est resolu DANS HDFS, pas dans le conteneur ("Path does not
      exist"). Un chemin local doit porter explicitement le scheme file://.
      core-site.xml sets fs.defaultFS=hdfs://namenode:9000: any BARE path is resolved
      INSIDE HDFS, not in the container. A local path must carry the file:// scheme.
    """
    src = args.input
    if not src.startswith(("hdfs://", "file://", "s3a://")):
        src = "file://" + src
        print(f"[bronze] chemin local -> scheme explicite / local path -> explicit scheme: {src}")

    if args.format == "json":
        # POURQUOI / WHY : la Bronze archive le JSON BRUT (une ligne = le payload entier).
        # Aucune interpretation au chargement : si l'API change de structure, aucune
        # donnee n'est perdue. Silver fera le parsing avec un schema explicite.
        # Bronze archives the RAW JSON (one row = whole payload). No parsing at load
        # time: if the API structure changes, no data is lost. Silver parses it
        # with an explicit schema.
        return normalize_column_names(spark.read.text(src)).withColumnRenamed("value", "raw_json")
    else:
        df = (
            spark.read
            .option("header", "true")
            .option("sep", args.sep)
            .option("quote", '"')
            .option("escape", '"')
            .option("encoding", "UTF-8")
            .option("mode", "PERMISSIVE")   # Ligne mal formee -> colonne _corrupt_tolerated
            .csv(src)
        )
    return normalize_column_names(df)


def main():
    args = parse_args()
    spark = get_spark(f"bronze_{args.source}_{args.table}")
    ensure_databases(spark, [CONFIG["db_bronze"]])

    print(f"[bronze] lecture / reading: {args.input}")
    df = read_source(spark, args)

    # Tracabilite : d'ou vient cette ligne ? / traceability: where does this row come from?
    df = add_technical_columns(df, source_file=args.input, load_date=args.load_date)

    hdfs_path = f"{CONFIG['hdfs_bronze']}/{args.source}/{args.table}"
    write_table(df, CONFIG["db_bronze"], args.table, hdfs_path)

    # Petit rapport de controle / small control report
    total = df.count()
    print(f"[bronze] {total} lignes chargees dans {args.table} / rows loaded")
    print(f"[bronze] colonnes / columns: {df.columns}")
    spark.stop()


if __name__ == "__main__":
    main()
