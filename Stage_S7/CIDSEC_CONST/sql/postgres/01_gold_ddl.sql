-- =====================================================================================
-- 01_gold_ddl.sql — PostgreSQL schema "gold" : index, vues et documentation
--                    PostgreSQL "gold" schema: indexes, views, documentation
--
-- POURQUOI / WHY :
--   Spark cree les tables via JDBC (overwrite) mais ne cree PAS les index ni les vues
--   de confort. Ce script les ajoute : performances Power BI (filtres/ tris) et
--   surfaces de lecture propres pour les utilisateurs.
--   Spark creates the tables via JDBC (overwrite) but NOT indexes or convenience
--   views. This script adds them: Power BI performance (filters/sorts) and clean
--   reading surfaces for users.
--
-- COMMENT / HOW :
--   Une fois apres le premier export Gold (et apres chaque changement de schema) :
--   docker exec -i cidsec-postgres psql -U spark -d gold_db < sql/postgres/01_gold_ddl.sql
--   (a la main : etape 7 du guide, docs/04)
--
-- VERIFICATION :
--   \d gold.fact_commande   -> les index listes ; \d+ gold.v_bi_commandes -> la vue
--
-- NOTE IMPORTANTE / IMPORTANT NOTE :
--   Ce fichier n'est PAS execute par les jobs : c'est un script d'exploitation,
--   versionne, rejouable, documente. Les donnees sont elles, produites par Spark.
--   This file is NOT run by jobs: it is an ops script, versioned, replayable,
--   documented. The data itself is produced by Spark.
-- =====================================================================================

-- --- Index de performance / performance indexes ----------------------------------
-- POURQUOI : Power BI filtre frequemment sur la date et l'etat de commande ; sans
-- index, PostgreSQL relit toute la table a chaque requete.
-- WHY: Power BI frequently filters on date and order status; without an index,
-- PostgreSQL re-scans the whole table on every query.

CREATE INDEX IF NOT EXISTS idx_fact_commande_jour    ON gold.fact_commande (jour_achat);
CREATE INDEX IF NOT EXISTS idx_fact_commande_status  ON gold.fact_commande (order_status);
CREATE INDEX IF NOT EXISTS idx_fact_trades_date      ON gold.fact_trades (trade_date);
CREATE INDEX IF NOT EXISTS idx_fact_energie_pays_jour ON gold.fact_energie_jour (pays, jour);

-- --- Vue de confort pour Power BI / convenience view for Power BI -----------------
-- POURQUOI : presenter a l'utilisateur metier un jeu de colonnes simple et nomme
-- dans son langage (et non les cles techniques du pipeline).
-- WHY: present business users with a simple, business-named column set (not the
-- pipeline's technical keys).
CREATE OR REPLACE VIEW gold.v_bi_commandes AS
SELECT
    f.order_id,
    f.order_status,
    f.jour_achat,
    d.annee,
    d.mois,
    d.nom_mois,
    f.montant_total,
    f.nb_articles,
    f.montant_paye,
    f.delai_livraison_jours,
    f.is_late,
    f.review_score,
    c.customer_city,
    c.customer_state
FROM gold.fact_commande f
LEFT JOIN gold.dim_date    d ON f.jour_achat = d.jour
LEFT JOIN gold.dim_client  c ON f.customer_id = c.customer_id;

-- --- Droits de lecture Power BI / Power BI read rights ----------------------------
-- (normalement deja accordes par init-db.sql ; reaffirmation explicite ici)
-- (normally already granted by init-db.sql; explicitly reaffirmed here)
GRANT USAGE ON SCHEMA gold TO powerbi;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO powerbi;
