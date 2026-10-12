# -*- coding: utf-8 -*-
"""
=====================================================================================
common.py — Elements partages par tous les jobs PySpark
            Shared building blocks for all PySpark jobs

POURQUOI / WHY :
  Tous les jobs ont besoin des memes choses : une session Spark connectee au catalogue
  Hive, des chemins HDFS stables, des colonnes techniques de tracabilite. Centraliser
  evite la duplication et garantit l'identite des regles entre les jobs.
  Every job needs the same things: a Spark session wired to the Hive catalog, stable
  HDFS paths, technical traceability columns. Centralizing avoids duplication and
  guarantees identical rules across jobs.

COMMENT / HOW :
  from common import get_spark, CONFIG, add_technical_columns, normalize_column_names

VERIFICATION :
  spark.sql("SHOW DATABASES") doit lister bronze_db, silver_db, gold_db apres le 1er job.
=====================================================================================
"""

import hashlib
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ---------------------------------------------------------------------------------
# Configuration centrale / Central configuration
# POURQUOI : un seul endroit a modifier si l'arborescence change.
# WHY     : single place to edit if the directory tree changes.
# ---------------------------------------------------------------------------------
CONFIG = {
    # Sources locales (montages Docker) / local sources (Docker mounts)
    "landing": "/landing",             # Donnees statiques (CSV/zip extraits)
    "landing_api": "/landing_api",     # Reponses API capturees par Airflow

    # Data Lake HDFS / HDFS Data Lake
    "hdfs_bronze": "hdfs://namenode:9000/data/bronze",
    "hdfs_silver": "hdfs://namenode:9000/data/silver",
    "hdfs_gold": "hdfs://namenode:9000/data/gold",

    # Bases Hive / Hive databases
    "db_bronze": "bronze_db",
    "db_silver": "silver_db",
    "db_gold": "gold_db",

    # PostgreSQL couche de service / PostgreSQL serving layer
    "pg_url": "jdbc:postgresql://postgres:5432/gold_db",
    "pg_user": "spark",
    "pg_password": "spark",            # LAB ONLY — cf. docs/04, securite
    "pg_schema": "gold",

    # Divers / misc
    # ATTENTION / WARNING : cette valeur est utilisee par strftime() de PYTHON
    # (date.today().strftime(...)) — le format doit donc etre PYTHON ("%Y-%m-%d"),
    # PAS un format Java/Spark ("yyyy-MM-dd"). Avec un format Java, strftime renvoie
    # le texte litteral "yyyy-MM-dd" comme valeur de partition (bug corrige le
    # 06/10/2026, vu dans les logs HDFS : "_load_date=yyyy-MM-dd").
    # This value is used by PYTHON's strftime(): the format must be PYTHON
    # ("%Y-%m-%d"), NOT a Java/Spark format ("yyyy-MM-dd"). With a Java format,
    # strftime returns the literal text "yyyy-MM-dd" as the partition value.
    "load_date_format": "%Y-%m-%d",
}


def get_spark(app_name: str) -> SparkSession:
    """
    Construit la session Spark du projet / Build the project's Spark session.

    POURQUOI / WHY :
      - enableHiveSupport() : les tables ecrites (saveAsTable) apparaissent dans Hive
        et sont requetables en SQL par toute l'equipe (beeline, Power BI via vues).
      - partitionOverwriteMode=dynamic : re-executer un job ne remplace QUE la
        partition du jour -> rejeu sans perte (idempotence).
      - enableHiveSupport(): written tables appear in Hive, queryable in SQL.
      - dynamic partition overwrite: rerunning a job replaces ONLY the day's
        partition -> replay without data loss (idempotence).
    COMMENT / HOW :
      Les options de config restantes (Parquet snappy, JDBC jar, memoire) viennent de
      docker/spark/spark-defaults.conf, monte dans le conteneur.
      Remaining options come from docker/spark/spark-defaults.conf, mounted in the container.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .enableHiveSupport()
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def ensure_databases(spark: SparkSession, databases=None) -> None:
    """
    Cree les bases Hive si elles n'existent pas / Create Hive databases if missing.
    POURQUOI : idempotence — le job doit pouvoir tourner sur une plateforme neuve.
    WHY     : idempotence — the job must run on a brand-new platform.
    """
    for db in (databases or [CONFIG["db_bronze"], CONFIG["db_silver"], CONFIG["db_gold"]]):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
        print(f"[common] base Hive garantie / Hive database ensured: {db}")


def normalize_column_names(df: DataFrame) -> DataFrame:
    """
    Nettoie les noms de colonnes / Clean column names.
    POURQUOI / WHY :
      Certains CSV ont un BOM UTF-8 (fichier de traduction), des espaces ou des
      caracteres accentues dans les en-tetes. Les noms normalises evitent des erreurs
      mysterieuses plus tard (regle A9 de l'audit).
      Some CSVs carry a UTF-8 BOM (translation file), spaces or accented characters
      in headers. Normalized names avoid mysterious failures later (audit rule A9).
    COMMENT / HOW :
      minuscules, sans BOM, lettres/chiffres/underscore uniquement.
      lowercase, BOM stripped, letters/digits/underscore only.
    """
    import re
    clean = lambda c: re.sub(r"[^0-9a-zA-Z_]", "_", c.replace("\ufeff", "")).strip("_").lower()
    return df.toDF(*[clean(c) for c in df.columns])


def add_technical_columns(df: DataFrame, source_file: str, load_date: str = None) -> DataFrame:
    """
    Ajoute les colonnes techniques de tracabilite / Add technical traceability columns.

    POURQUOI / WHY :
      Savoir D'OU vient chaque ligne et QUAND elle a ete chargee est indispensable
      pour auditer, rejeuer et diagnostiquer les problemes de qualite. C'est la regle
      d'or de la couche Bronze.
      Knowing WHERE each row comes from and WHEN it was loaded is essential to audit,
      replay and diagnose quality issues. This is the golden rule of the Bronze layer.

    Colonnes ajoutees / added columns :
      _ingestion_ts  : horodatage de chargement / load timestamp
      _source_file   : fichier d'origine / origin file
      _load_date     : partition logique (jour) / logical partition (day)
      _record_hash   : empreinte de la ligne (detection de doublons) / row fingerprint
    """
    ld = load_date or date.today().strftime(CONFIG["load_date_format"])
    df = df.withColumn("_ingestion_ts", F.current_timestamp())
    df = df.withColumn("_source_file", F.lit(source_file))
    df = df.withColumn("_load_date", F.lit(ld))
    # hash md5 de la concatenation des colonnes metier (hors colonnes techniques)
    # md5 hash of business columns concatenated (technical columns excluded)
    business_cols = [c for c in df.columns if not c.startswith("_")]
    df = df.withColumn(
        "_record_hash",
        F.md5(F.concat_ws("||", *[F.coalesce(F.col(c).cast(T.StringType()), F.lit("")) for c in business_cols])),
    )
    return df


def write_table(df: DataFrame, database: str, table: str, hdfs_path: str,
                partition_col: str = "_load_date") -> None:
    """
    Ecrit une table Hive externe + ses fichiers Parquet / Write a Hive external table + its Parquet files.

    POURQUOI / WHY :
      "EXTERNE" (via option path) : les fichiers restent dans mon arborescence HDFS,
      supprimer la table ne supprime PAS les donnees -> securite et tracabilite.
      "EXTERNAL" (via path option): files stay in my HDFS tree, dropping the table
      does NOT delete the data -> safety and traceability.
    COMMENT / HOW :
      write.mode("overwrite") + partitionOverwriteMode=dynamic : seules les partitions
      presentes dans le DataFrame sont remplacees.
      Only partitions present in the DataFrame are replaced.
    VERIFICATION :
      hdfs dfs -ls <hdfs_path> et "SELECT count(*) FROM <database>.<table>;"
    """
    full_name = f"{database}.{table}"
    # POURQUOI ce garde-fou : write_table partitionne par _load_date, mais certains
    #            jobs (agregations, conformed, gold) produisent des DataFrames SANS
    #            colonnes techniques -> "partition column _load_date is not defined"
    #            (constate le 08/10/2026 sur olist_geolocation). On ajoute la colonne
    #            si elle manque, avec la date du jour : aucun job ne peut plus echouer
    #            sur ce piege.
    # WHY this guard: write_table partitions by _load_date, but some jobs (aggregates,
    #      conformed, gold) produce DataFrames WITHOUT technical columns -> partition
    #      error. The column is added when missing, with today's date: no job can
    #      trip this trap anymore.
    if partition_col not in df.columns:
        df = df.withColumn(partition_col, F.lit(date.today().strftime(CONFIG["load_date_format"])))
        print(f"[common] colonne de partition ajoutee / partition column added: {partition_col}")
    (
        df.write
        .mode("overwrite")
        .format("parquet")
        .option("path", hdfs_path)
        .partitionBy(partition_col)
        .saveAsTable(full_name)
    )
    print(f"[common] table ecrite / table written: {full_name} -> {hdfs_path}")


def write_postgres(df: DataFrame, table: str, mode: str = "overwrite") -> None:
    """
    Exporte une table vers PostgreSQL via JDBC / Export a table to PostgreSQL via JDBC.

    POURQUOI / WHY :
      C'est LE point d'integration "Spark JDBC" : Power BI ne parle pas HDFS/Hive de
      facon fiable, mais PostgreSQL oui. La couche Gold est donc doublee en BDD.
      This is THE "Spark JDBC" integration point: Power BI does not talk to HDFS/Hive
      reliably, but PostgreSQL it does. The Gold layer is therefore mirrored in the DB.
    COMMENT / HOW :
      write.format("jdbc") + properties user/password (fournis par CONFIG).
    VERIFICATION :
      docker exec cidsec-postgres psql -U spark -d gold_db -c "SELECT count(*) FROM gold.<table>;"
    """
    props = {
        "user": CONFIG["pg_user"],
        "password": CONFIG["pg_password"],
        "driver": "org.postgresql.Driver",
    }
    (
        df.write
        .mode(mode)
        .format("jdbc")
        .option("url", CONFIG["pg_url"])
        .option("dbtable", f"{CONFIG['pg_schema']}.{table}")
        .options(**props)
        .save()
    )
    print(f"[common] export JDBC OK / JDBC export OK: {CONFIG['pg_schema']}.{table}")
