#!/bin/bash
# =====================================================================================
# init-hdfs.sh — Initialisation de l'arborescence du Data Lake HDFS
#                Data Lake HDFS directory tree initialization
#
# POURQUOI / WHY :
#   Les jobs Spark ecrivent dans /data/bronze|/data/silver|/data/gold : ces repertoires
#   doivent exister avant. Ce script les cree ET archive les sources brutes dans
#   /data/landing (copie immuable, pour la tracabilite).
#   Spark jobs write into /data/bronze|silver|gold: directories must exist first.
#   This script creates them AND archives raw sources into /data/landing
#   (immutable copy, for traceability).
#
# COMMENT / HOW :
#   Service "hdfs-init" de docker-compose (one-shot). Rejouable a l'infini :
#   chaque commande est protegee par un test (hdfs dfs -test -e).
#   "hdfs-init" service in docker-compose (one-shot). Safe to rerun infinitely:
#   every command is guarded by a test.
#
# VERIFICATION :
#   docker exec cidsec-namenode hdfs dfs -ls -R /data   -> l'arborescence + les fichiers
# =====================================================================================
set -e  # Arret immediat en cas d'erreur / stop immediately on any error

echo "=== [1/3] Creation de l'arborescence HDFS / Creating HDFS directory tree ==="
for d in /data /data/landing /data/landing/olist /data/landing/cymbal /data/landing/iot \
         /data/landing/api /data/bronze /data/silver /data/gold /data/warehouse \
         /data/models /data/dq /tmp/hive; do
  hdfs dfs -mkdir -p "$d" 2>/dev/null || true
done
echo "OK : repertoires crees / directories created"

echo "=== [2/3] Archivage des sources brutes dans /data/landing ==="
echo "===         Archiving raw sources into /data/landing               ==="
# On copie SEULEMENT si le fichier n'existe pas deja dans HDFS (idempotence).
# Copy ONLY if the file does not already exist in HDFS (idempotence).
put_if_absent() {
  local src="$1" dst="$2"
  # POURQUOI / WHY : si la destination est un REPERTOIRE (suffixe /), le test porterait
  # sur le repertoire lui-meme (toujours present) -> aucun fichier ne serait jamais
  # copie. On vise donc explicitement <dst>/<nom_du_fichier>.
  # If the destination is a DIRECTORY (trailing /), the test would hit the directory
  # itself (always present) -> nothing would ever be copied. We explicitly target
  # <dst>/<file_name> instead.
  if [[ "$dst" == */ ]]; then
    dst="$dst$(basename "$src")"
  fi
  if hdfs dfs -test -e "$dst" 2>/dev/null; then
    echo "  deja present, ignore / already present, skipped: $dst"
  elif [ -e "$src" ]; then
    hdfs dfs -put "$src" "$dst" && echo "  copie / copied: $dst"
  else
    echo "  source absente localement / local source missing: $src"
  fi
}

# Olist : les 9 CSV du zip (supposes decompreses dans Données/E-Commerce/)
# Olist: the 9 CSVs from the zip (assumed extracted in Données/E-Commerce/)
if [ -d /landing/E-Commerce ]; then
  for f in /landing/E-Commerce/*.csv; do put_if_absent "$f" /data/landing/olist/; done
fi
# Cymbal : rapport de trade capture
if [ -d /landing/Investissment_Cymball ]; then
  for f in /landing/Investissment_Cymball/*.csv; do put_if_absent "$f" /data/landing/cymbal/; done
fi
# IoT : UCI (zip extrait) + OPSD
if [ -d /landing/UCI_power ]; then
  for f in /landing/UCI_power/*.txt /landing/UCI_power/*.csv; do put_if_absent "$f" /data/landing/iot/; done
fi
if [ -d /landing/IoT_Energie ]; then
  for f in /landing/IoT_Energie/*.csv; do put_if_absent "$f" /data/landing/iot/; done
fi
# API : reponses JSON capturees par Airflow
if [ -d /landing_api ]; then
  for f in /landing_api/*.json; do put_if_absent "$f" /data/landing/api/; done
fi

echo "=== [3/3] Contenu final du lac / Final lake content ==="
hdfs dfs -du -h /data 2>/dev/null || true
echo "=== init-hdfs termine / done ==="
