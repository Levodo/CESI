-- =====================================================================================
-- 01_views_silver.hql — Vues metier de la couche Silver / Silver business views
--
-- POURQUOI / WHY :
--   Les tables Silver sont "propres" mais morcelees (une table par source). Ces vues
--   assemblent deja les jointures les plus utiles : l'utilisateur final (ou Power BI)
--   interroge UNE vue plutot que 5 tables.
--   Silver tables are clean but scattered (one per source). These views pre-assemble
--   the most useful joins: the end user (or Power BI) queries ONE view, not 5 tables.
--
-- COMMENT / HOW :
--   Execute via beeline (voir docs/04, etape 6) :
--   beeline -u jdbc:hive2://localhost:10000 -f /opt/project/sql/hive/01_views_silver.hql
--
-- VERIFICATION :
--   beeline -u jdbc:hive2://localhost:10000 -e "SELECT * FROM silver_db.v_commandes_complete LIMIT 5;"
-- =====================================================================================

USE silver_db;

-- ---------------------------------------------------------------------------------
-- Vue : commande complete / complete order view
-- 1 ligne = 1 commande enrichie du client, de sa zone geographique et de la meteo
-- du jour de l'achat (coherence inter-sources, docs/01 §8.2).
-- 1 row = 1 order enriched with its customer, geographic area and the weather of the
-- purchase day (cross-source coherence).
-- ---------------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_commandes_complete AS
SELECT
    o.order_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.delivery_delay_days   AS delai_livraison_jours,
    o.is_late,
    o.is_approved,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    g.geo_lat,
    g.geo_lng,
    m.temp_moy_c,
    m.precipitations_mm,
    m.vent_max_kmh
FROM olist_orders o
LEFT JOIN olist_customers   c ON o.customer_id = c.customer_id
LEFT JOIN olist_geolocation g ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
LEFT JOIN meteo_quotidien   m ON m.jour = to_date(o.order_purchase_timestamp);

-- ---------------------------------------------------------------------------------
-- Vue : note de satisfaction par commande / satisfaction score per order
-- Apres deduplication (A4), une commande a au plus une note exploitable.
-- After dedup (A4), an order has at most one usable score.
-- ---------------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_satisfaction_commande AS
SELECT
    order_id,
    review_score,
    review_creation_date,
    review_answer_timestamp
FROM olist_order_reviews;
