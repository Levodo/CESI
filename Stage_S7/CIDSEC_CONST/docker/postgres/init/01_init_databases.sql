-- =============================================================================
-- Fichier : docker/postgres/init/01_init_databases.sql
-- -----------------------------------------------------------------------------
-- POURQUOI (WHY) :
--   FR : La plateforme utilise UN SEUL serveur PostgreSQL (légereté, une seule
--        instance à administrer) mais TROIS bases isolées :
--          1. airflow_db       -> métadonnées de l'orchestrateur Airflow
--          2. hive_metastore   -> catalogue de tables Hive (descriptions des
--                                 fichiers Parquet stockés dans HDFS)
--          3. gold_db          -> couche de SERVICE : tables finales lues par
--                                 Power BI (schéma gold) + qualité (schéma dq)
--        Séparer les bases évite qu'un problème dans l'une pollue les autres.
--   EN : The platform uses ONE PostgreSQL server (lightweight, single instance
--        to administer) but THREE isolated databases:
--          1. airflow_db       -> Airflow orchestration metadata
--          2. hive_metastore   -> Hive table catalog (describes Parquet files
--                                 stored in HDFS)
--          3. gold_db          -> SERVING layer: final tables read by Power BI
--                                 (gold schema) + data quality (dq schema)
--        Separate databases prevent one failure from polluting the others.
--
-- COMMENT (HOW) :
--   FR : Ce script est monté dans /docker-entrypoint-initdb.d et exécuté
--        AUTOMATIQUEMENT par l'image postgres au PREMIER démarrage uniquement
--        (quand le volume est vide). Pour le re-exécuter : docker compose down -v
--        (EFFACE les données !) puis docker compose up -d.
--   EN : This script is mounted into /docker-entrypoint-initdb.d and run
--        AUTOMATICALLY by the postgres image on FIRST startup only (empty
--        volume). To re-run it: docker compose down -v (DELETES data!) then
--        docker compose up -d.
-- =============================================================================

-- --- 1. Bases de données -----------------------------------------------------
CREATE DATABASE airflow_db;        -- FR: Airflow / EN: Airflow metadata
CREATE DATABASE hive_metastore;    -- FR: catalogue Hive / EN: Hive catalog
CREATE DATABASE gold_db;           -- FR: service BI   / EN: BI serving layer

-- --- 2. Utilisateurs dédiés (un rôle par consommateur = principe du moindre
--        privilège / one role per consumer = least privilege principle) ------
CREATE USER airflow  WITH PASSWORD 'airflow_change_moi';   -- FR: Airflow       / EN: Airflow
CREATE USER hive     WITH PASSWORD 'hive_change_moi';      -- FR: metastore Hive / EN: Hive metastore
CREATE USER spark    WITH PASSWORD 'spark_change_moi';     -- FR: écriture JDBC  / EN: JDBC writer
CREATE USER powerbi  WITH PASSWORD 'powerbi_change_moi';   -- FR: lecture seule BI / EN: BI read-only

-- --- 3. Droits ---------------------------------------------------------------
GRANT ALL PRIVILEGES ON DATABASE airflow_db     TO airflow;
GRANT ALL PRIVILEGES ON DATABASE hive_metastore TO hive;
GRANT ALL PRIVILEGES ON DATABASE gold_db        TO spark;

-- FR : Power BI ne reçoit que la lecture : une erreur de dashboard ne pourra
--      jamais modifier les données.
-- EN : Power BI gets read-only access: a dashboard mistake can never modify data.
GRANT CONNECT ON DATABASE gold_db TO powerbi;

-- FR : Les schémas (gold, dq) sont créés par les jobs Spark ; on autorise spark
--      à créer des objets dans gold_db et powerbi à les lire.
-- EN : Schemas (gold, dq) are created by Spark jobs; allow spark to create
--      objects in gold_db and powerbi to read them.
\c gold_db;
GRANT ALL ON SCHEMA public TO spark;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO powerbi;
