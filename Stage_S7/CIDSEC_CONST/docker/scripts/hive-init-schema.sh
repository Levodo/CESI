#!/usr/bin/env bash
# =====================================================================================
# hive-init-schema.sh — Initialisation du catalogue Hive (UNE seule fois)
#                      Hive catalog schema initialization (ONCE)
#
# POURQUOI / WHY :
#   Le catalogue Hive (table DBS, VERSION, etc.) doit exister dans PostgreSQL AVANT
#   le démarrage du metastore. Sans schéma, le metastore plante en boucle avec
#   "Required table missing : DBS".
#   The Hive catalog (DBS, VERSION tables...) must exist in PostgreSQL BEFORE the
#   metastore starts. Without the schema, the metastore crash-loops with
#   "Required table missing : DBS".
#
#   Ce script est appelé SANS ESPACE dans ses arguments (command: ["bash", "/scripts/..."])
#   car le /entrypoint.sh de l'image bde2020 termine par `exec $@` NON QUOTÉ : toute
#   commande contenant des espaces est découpée par le shell et perd ses arguments
#   (c'est exactement pourquoi "bash -c '... -initSchema'" échouait au démarrage).
#   This script is called with NO SPACES in its arguments because the image's
#   /entrypoint.sh ends with an UNQUOTED `exec $@`: any command containing spaces
#   gets word-split and loses its arguments (exactly why the previous init failed).
#
# COMMENT / HOW :
#   1. `schematool -info` teste si le schéma existe déjà.
#   2. S'il n'existe pas : `schematool -dbType postgres -initSchema` le crée.
#   3. Sortie 0 dans tous les cas (le service doit être "complété" pour Compose),
#      mais un échec réel est affiché explicitement.
#   1. `schematool -info` tests whether the schema already exists.
#   2. If missing: `schematool -dbType postgres -initSchema` creates it.
#   3. Always exit 0 (Compose needs a "completed" service), but real failures
#      are printed explicitly.
#
# VERIFICATION :
#   docker exec cidsec-postgres psql -U hive -d metastore -c "\dt"
#   -> doit lister DBS, VERSION, TBLS, COLUMNS_V2, etc.
# =====================================================================================
set -u

SCHEMA_TOOL=/opt/hive/bin/schematool
# FR: Filtre le bruit SLF4J pour ne garder que les messages utiles.
# EN: Filters SLF4J noise to keep only useful messages.
FILTER='^SLF4J|log4j'

echo "[hive-init] Etat du schema / Checking schema state..."
if "$SCHEMA_TOOL" -dbType postgres -info 2>&1 | grep -Ev "$FILTER"; then
  echo "[hive-init] Schema deja present — rien a faire / Schema already present — nothing to do."
  exit 0
fi

echo "[hive-init] Schema absent — initialisation / Schema missing — initializing..."
if "$SCHEMA_TOOL" -dbType postgres -initSchema 2>&1 | grep -Ev "$FILTER"; then
  echo "[hive-init] SUCCES : schema du catalogue cree / SUCCESS: catalog schema created."
  exit 0
else
  echo "[hive-init] ECHEC de l'initialisation / FAILED to initialize schema."
  echo "[hive-init] Voir docs/04_guide_installation.md (section Depannage)."
  exit 0   # FR: on ne bloque pas `docker compose up`, l'erreur est affichée ci-dessus.
           # EN: do not block `docker compose up`, the error is printed above.
fi
