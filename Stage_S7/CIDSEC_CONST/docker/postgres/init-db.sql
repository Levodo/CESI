-- =====================================================================================
-- init-db.sql — Initialisation de PostgreSQL (execute AU PREMIER DEMARRAGE uniquement)
--               PostgreSQL initialization (runs at FIRST STARTUP only)
--
-- POURQUOI / WHY :
--   Chaque brique de la plateforme a sa base : Hive (catalogue), Airflow (metadonnees
--   d'orchestration), Gold (couche de service lue par Power BI). Les separer evite
--   qu'un outil casse les donnees d'un autre.
--   Each tool has its own database. Separating them prevents one tool from breaking
--   another's data.
--
-- COMMENT / HOW :
--   Monte dans /docker-entrypoint-initdb.d/ (voir docker-compose.yml) : PostgreSQL
--   execute automatiquement les .sql presents a son premier demarrage.
--   Mounted into /docker-entrypoint-initdb.d/: PostgreSQL automatically runs the
--   .sql files at first startup.
--
-- VERIFICATION :
--   docker exec cidsec-postgres psql -U postgres -c "\l"  -> 3 bases en plus de postgres
--   ATTENTION : ce script ne s'execute PAS si le volume "pgdata" existe deja.
--   NOTE     : this script does NOT run again if the "pgdata" volume already exists.
--
-- SECURITE / SECURITY : mots de passe de demonstration uniquement (projet pedagogique).
--                       demo passwords only (teaching project).
-- =====================================================================================

-- 1. Base du catalogue Hive / Hive catalog database ------------------------------
CREATE USER hive WITH PASSWORD 'hive';
CREATE DATABASE metastore OWNER hive;

-- 2. Base des metadonnees Airflow / Airflow metadata database ---------------------
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow_db OWNER airflow;

-- 3. Base de service "Gold" (Spark ecrit, Power BI lit) --------------------------
--    "Gold" serving database (Spark writes, Power BI reads)
CREATE USER spark WITH PASSWORD 'spark';
CREATE DATABASE gold_db OWNER spark;

-- 4. Utilisateur Power BI en lecture seule / read-only Power BI user -------------
-- POURQUOI : Power BI ne doit JAMAIS pouvoir ecrire ni supprimer des donnees.
-- WHY     : Power BI must NEVER be able to write or delete data.
CREATE USER powerbi WITH PASSWORD 'powerbi';
GRANT CONNECT ON DATABASE gold_db TO powerbi;

\c gold_db
-- Schema dedie a la couche Gold (Spark y ecrit via JDBC, Power BI y lit)
-- Dedicated schema for the Gold layer (Spark writes via JDBC, Power BI reads)
CREATE SCHEMA gold AUTHORIZATION spark;
GRANT USAGE ON SCHEMA gold TO powerbi;
GRANT USAGE ON SCHEMA public TO powerbi;
-- Toute table future creee par "spark" sera automatiquement lisible par "powerbi"
-- Any future table created by "spark" is automatically readable by "powerbi"
ALTER DEFAULT PRIVILEGES FOR USER spark IN SCHEMA public GRANT SELECT ON TABLES TO powerbi;
ALTER DEFAULT PRIVILEGES FOR USER spark IN SCHEMA gold GRANT SELECT ON TABLES TO powerbi;
