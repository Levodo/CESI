# =====================================================================================
# Dockerfile — Image Airflow enrichie (Java + PySpark + connecteur Spark)
#              Enriched Airflow image (Java + PySpark + Spark connector)
#
# POURQUOI / WHY :
#   L'image officielle d'Airflow ne contient NI Java NI PySpark. Or Airflow doit
#   soumettre des jobs PySpark au cluster Spark ("spark-submit"), ce qui exige :
#   1) une JVM (le driver Spark tourne dans Airflow, mode "client"),
#   2) PySpark (meme version que le cluster : 3.3.0),
#   3) le fournisseur Airflow pour Spark (SparkSubmitOperator).
#   The official Airflow image has NO Java and NO PySpark. But Airflow must submit
#   PySpark jobs to the Spark cluster ("spark-submit"), which requires a JVM,
#   PySpark (same version as the cluster), and the Airflow Spark provider.
#
# COMMENT / HOW :
#   Construit automatiquement par docker-compose (service airflow-init, "build:").
#   Built automatically by docker-compose (airflow-init service, "build:").
#
# VERIFICATION :
#   docker exec cidsec-airflow-sched java -version     -> OpenJDK 17
#   docker exec cidsec-airflow-sched spark-submit --version -> version 3.3.0
# =====================================================================================

FROM apache/airflow:2.10.4-python3.8
# python3.8 : PySpark 3.3.0 n'est PAS compatible Python 3.11+.
# python3.8: PySpark 3.3.0 is NOT compatible with Python 3.11+.

# --- 1. Java (execute en root : Airflow tourne ensuite avec l'utilisateur "airflow") ---
#        Java (installed as root: Airflow later runs as the "airflow" user)
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# JAVA_HOME est utilise par spark-submit pour lancer le driver.
# JAVA_HOME is used by spark-submit to launch the driver.
# Java 17 : seule version JDK disponible sur Debian bookworm (base de l'image
# apache/airflow), ET supportee par Spark 3.3+.
# Java 17: only JDK available on Debian bookworm (the apache/airflow base),
# AND supported by Spark 3.3+.
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# --- 2. PySpark + connecteur Airflow->Spark (en utilisateur "airflow") ----------------
#        PySpark + Airflow->Spark connector (as the "airflow" user)
USER airflow
# pyspark 3.3.0 = MEME version que les conteneurs bde2020/spark:3.3.0-hadoop3.3.
# pyspark 3.3.0 = SAME version as the bde2020/spark:3.3.0-hadoop3.3 containers.
RUN pip install --no-cache-dir \
        "pyspark==3.3.0" \
        "apache-airflow-providers-apache-spark==4.9.0"
