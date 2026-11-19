-- ============================================================================
-- sql/postgres/03_powerbi_views.sql — Vues préparées pour Power BI (Phase 4)
--                                        Prepared views for Power BI (Phase 4)
-- ============================================================================
-- POURQUOI ce fichier : Power BI consomme PostgreSQL directement. Plutôt que de
--            reconstruire les mêmes agrégats dans chaque visuel (risque d'erreur et
--            de divergence entre pages), on fournit des VUES métier prêtes à l'emploi.
--            Chaque vue correspond à une ou deux pages du rapport final.
-- WHY this file: Power BI consumes PostgreSQL directly. Rather than rebuilding the
--      same aggregates in every visual (risk of error and divergence between pages),
--      we provide ready-to-use BUSINESS VIEWS. Each view maps to one or two report
--      pages.
--
-- COMMENT l'utiliser / HOW to use :
--   1. Exécuter ce fichier UNE fois (voir « ÉQUIVALENT À LA MAIN » ci-dessous).
--      Run this file ONCE (see 'MANUAL EQUIVALENT' below).
--   2. Dans Power BI Desktop : Se connecter à PostgreSQL, puis charger les vues
--      `gold.vue_pbi_*` au lieu des tables brutes (plus simples à visualiser).
--      In Power BI Desktop: connect to PostgreSQL, then load the `gold.vue_pbi_*`
--      views instead of raw tables (simpler to visualize).
--
-- ÉQUIVALENT À LA MAIN / MANUAL EQUIVALENT :
--   docker exec -i cidsec-postgres psql -U spark -d gold_db < sql/postgres/03_powerbi_views.sql
--   -- ou via Adminer : coller le contenu de ce fichier et cliquer « Exécuter ».
--
-- VÉRIFICATION / VERIFICATION :
--   docker exec cidsec-postgres psql -U spark -d gold_db -c "\dv gold.vue_pbi_*"
--   -- attendu / expected : 5 vues listées
-- ============================================================================


-- ----------------------------------------------------------------------------
-- Vue 1 : synthèse globale (page « Synthèse » du rapport)
-- View 1: global summary (report 'Summary' page)
-- POURQUOI : une seule ligne de KPI pour les cartes du haut de la page d'accueil.
-- WHY: one KPI row for the cards at the top of the home page.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vue_pbi_synthese AS
SELECT
    (SELECT COUNT(*) FROM gold.fact_commande)                          AS nb_commandes,
    (SELECT SUM(montant_total) FROM gold.fact_commande)                AS ca_total,
    (SELECT ROUND(AVG(delai_livraison_jours)::numeric, 1)
       FROM gold.fact_commande WHERE delai_livraison_jours IS NOT NULL) AS delai_moyen_jours,
    (SELECT ROUND(100.0 * AVG(CASE WHEN is_late THEN 1 ELSE 0 END)::numeric, 1)
       FROM gold.fact_commande)                                        AS taux_retard_pct,
    (SELECT COUNT(*) FROM gold.fact_trades)                            AS nb_trades,
    (SELECT COUNT(*) FROM gold.ml_forecast_ca
      WHERE type_ligne = 'prevision')                                  AS nb_previsions_ca,
    (SELECT ROUND(AVG(wape_test_pct)::numeric, 1)
       FROM gold.ml_forecast_iot_metriques)                            AS wape_moyen_iot;


-- ----------------------------------------------------------------------------
-- Vue 2 : CA par catégorie et par jour (page « Ventes & catégories »)
-- View 2: revenue per category and day (report 'Sales & categories' page)
-- POURQUOI : fact_commande est au grain COMMANDE (sans clé produit) ; l'historique
--            par catégorie le plus fiable en PostgreSQL est l'historique AJUSTÉ par
--            Prophet stocké dans ml_forecast_ca (une ligne par jour et par catégorie).
--            Note honnête : « historique_ajuste » = courbe lissée par Prophet.
-- WHY: fact_commande is at ORDER grain (no product key); the most reliable per-category
--      history in PostgreSQL is Prophet's FITTED history in ml_forecast_ca. Honest note:
--      'historique_ajuste' = Prophet-smoothed curve.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vue_pbi_ventes_categorie AS
SELECT
    serie                         AS categorie,
    jour,
    yhat                          AS ca_ajuste,
    type_ligne
FROM gold.ml_forecast_ca;


-- ----------------------------------------------------------------------------
-- Vue 3 : qualité des données (page « Monitoring qualité »)
-- View 3: data quality (report 'Quality monitoring' page)
-- POURQUOI : la table dq.dq_checks est brute (1 ligne = 1 contrôle) ; cette vue
--            agrège par couche technique (colonne « layer ») pour alimenter un
--            graphe « taux de réussite ».
-- WHY: the dq.dq_checks table is raw (1 row = 1 check); this view aggregates per
--      technical layer (column 'layer') to feed a 'pass rate' chart.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vue_pbi_qualite AS
SELECT
    layer                                          AS couche,
    COUNT(*)                                       AS nb_controles,
    SUM(CASE WHEN passed THEN 1 ELSE 0 END)        AS nb_ok,
    ROUND(100.0 * AVG(CASE WHEN passed THEN 1 ELSE 0 END)::numeric, 1) AS taux_reussite_pct,
    MAX(run_ts)                                    AS dernier_controle
FROM dq.dq_checks
GROUP BY layer;


-- ----------------------------------------------------------------------------
-- Vue 4 : prévisions ML normalisées (page « Prévisions »)
-- View 4: normalized ML forecasts (report 'Forecasts' page)
-- POURQUOI : les prévisions CA (€) et IoT (MW/kWh) sont dans 2 tables avec des
--            unités différentes ; la vue les empile avec un libellé d'unité pour
--            un seul visuel « sélection de série ».
-- WHY: the CA forecasts (€) and IoT forecasts (MW/kWh) live in 2 tables with
--      different units; the view stacks them with a unit label for a single
--      'series selector' visual.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vue_pbi_previsions AS
SELECT 'CA' AS famille, serie, jour, yhat, yhat_bas, yhat_haut, 'EUR' AS unite
  FROM gold.ml_forecast_ca WHERE type_ligne = 'prevision'
UNION ALL
SELECT 'IOT' AS famille, serie, jour, yhat, yhat_bas, yhat_haut, 'kWh/MW' AS unite
  FROM gold.ml_forecast_iot WHERE type_ligne = 'prevision';


-- ----------------------------------------------------------------------------
-- Vue 5 : modèles ML — métriques consolidées (page « Monitoring ML »)
-- View 5: consolidated ML metrics (report 'ML monitoring' page)
-- POURQUOI : 3 tables de métriques séparées (retards/CA/IoT) ; cette vue les
--            consolide pour suivre la DÉRIVE des modèles dans le temps.
-- WHY: 3 separate metrics tables (delays/CA/IoT); this view consolidates them to
--      monitor model DRIFT over time.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vue_pbi_modeles AS
SELECT run_ts, 'retards' AS modele, serie, wape_test_pct AS wape, mae_test AS mae
  FROM gold.ml_forecast_ca_metriques
UNION ALL
SELECT run_ts, 'iot' AS modele, serie, wape_test_pct, mae_test
  FROM gold.ml_forecast_iot_metriques
UNION ALL
-- Les métriques XGBoost n'ont ni wape ni mae (classification) -> NULL explicites.
-- XGBoost metrics have neither wape nor mae (classification) -> explicit NULLs.
SELECT run_ts, 'retards_xgboost' AS modele,
       'classification' AS serie, NULL AS wape, NULL AS mae
  FROM gold.ml_pred_retard_metriques;
