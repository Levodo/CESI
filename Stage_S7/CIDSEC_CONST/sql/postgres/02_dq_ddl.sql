-- =====================================================================================
-- 02_dq_ddl.sql — Table de traçabilité de la qualité des données
--                 Data quality traceability table
--
-- POURQUOI / WHY :
--   Les résultats des contrôles (data_quality/checks.py) doivent être HISTORISÉS :
--   sans table dédiée, un contrôle ne laisse qu'une ligne de log éphémère.
--   Cette table alimente la page « Monitoring qualité » de Power BI et sert de
--   preuve d'exécution pour les audits.
--   Check results must be HISTORIZED: without a dedicated table, a check leaves only
--   an ephemeral log line. This table feeds the Power BI "Quality monitoring" page
--   and serves as execution evidence for audits.
--
-- COMMENT / HOW :
--   Exécuté UNE fois, à la main ou automatiquement par dag_06 (via psycopg2) :
--     docker exec -i cidsec-postgres psql -U postgres -d gold_db < sql/postgres/02_dq_ddl.sql
--   Run ONCE, manually or automatically by dag_06 (via psycopg2).
--
-- VERIFICATION :
--   docker exec cidsec-postgres psql -U spark -d gold_db -c "\d dq.dq_checks"
-- =====================================================================================

-- Schéma dédié : isole la supervision des données métier (schéma gold)
-- Dedicated schema: isolates supervision from business data (gold schema)
CREATE SCHEMA IF NOT EXISTS dq;

CREATE TABLE IF NOT EXISTS dq.dq_checks (
    run_ts     timestamp   NOT NULL,  -- horodatage du contrôle / check timestamp
    layer      text        NOT NULL,  -- silver_db | gold_db
    table_name text        NOT NULL,  -- table contrôlée / checked table
    check_name text        NOT NULL,  -- famille:controle / family:check
    observed   text,                  -- valeur mesurée / measured value
    expected   text,                  -- valeur attendue / expected value
    passed     boolean     NOT NULL,  -- TRUE = contrôle réussi / check passed
    details    text                   -- contexte libre / free context
);

-- Index : la consultation type est « derniers échecs »
-- Index: the typical query is "latest failures"
CREATE INDEX IF NOT EXISTS idx_dq_checks_passed_ts ON dq.dq_checks (passed, run_ts DESC);

-- Droits : le rôle spark (utilisé par les jobs JDBC) écrit ; powerbi lit
-- Grants: the spark role (used by JDBC jobs) writes; powerbi reads
GRANT USAGE ON SCHEMA dq TO spark, powerbi;
GRANT SELECT, INSERT ON dq.dq_checks TO spark;
GRANT SELECT ON dq.dq_checks TO powerbi;
