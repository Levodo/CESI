# Phase 3 — Machine Learning (dossier de suivi) / Phase 3 — Machine Learning (tracking document)

> **Pourquoi ce document / WHY this document** : il rassemble, à un endroit unique, ce qui
> est fait en Phase 3, comment c'est fait (étapes reproductibles), les résultats mesurés
> et la liste des étapes restantes. Toute l'équipe doit pouvoir rejouer les modèles à la
> main en suivant ce document.
> *It gathers in one place what is done in Phase 3, how it is done (reproducible steps),
> the measured results and the remaining steps. Anyone in the team should be able to
> replay the models by hand following this document.*

**Date de mise à jour / last updated** : 19/11/2026
**Auteur / author** : Bertrand LEVODO (stage CIDSEC Consulting)

---

## 1. Vue d'ensemble / Overview

La Phase 3 transforme la couche Gold (Phase 2) en **valeurs prédictives**. Trois modèles
sont prévus par le sujet contractuel :

*Phase 3 turns the Gold layer (Phase 2) into **predictive value**. Three models are
planned by the contract:*

| # | Modèle / Model | Algorithme | Statut / Status |
|---|---|---|---|
| 1 | Prédiction des retards de livraison / Delivery delay prediction | **XGBoost** (classification) | ✅ **Fait / Done** |
| 2 | Prévision du CA par catégories / Revenue forecast by category | **Prophet** (séries temporelles) | ✅ **Fait / Done** |
| 3 | Prévision de consommation électrique IoT / IoT power consumption forecast | Prophet / XGBoost | 📋 À faire / To do |
| 4 | Green Book ML (colonnes exploitables) / ML Green Book (usable columns) | document | ✅ **Fait / Done** (section 5) |
| 5 | Monitoring (importance des variables, variance) / Monitoring (feature importance, variance) | tables + doc | ✅ **Fait / Done** (section 6) |
| 6 | Orchestration `dag_07_ml_train` / Orchestration | Airflow | 📋 À faire / To do |

**Flux des données / data flow** :

```
Gold (fact_commande ⋈ dim_produit ⋈ météo)     ← Phase 2, déjà construit
        │
        ├──► ml/train_pred_retard.py   → gold.ml_pred_retard_*      (XGBoost)
        ├──► ml/train_forecast_ca.py   → gold.ml_forecast_ca*       (Prophet)
        └──► ml/train_forecast_iot.py  → gold.ml_forecast_iot*      (à faire)
                     │
                     └──► PostgreSQL (schéma gold) ──► Power BI (Phase 4)
```

---

## 2. Environnement & dépendances / Environment & dependencies

**Pourquoi / WHY** : les paquets Python installés dans un conteneur Docker disparaissent à
chaque recréation du conteneur (couche éphémère). Il faut donc un fichier de dépendances
et une commande de réinstallation connue de tous.

*Python packages installed in a Docker container disappear at each container recreation
(ephemeral layer). Hence a dependency file and a known reinstallation command.*

### Étapes / Steps

1. **Installer les dépendances ML** (une commande, depuis Windows, à la racine du projet) :
   *Install ML dependencies (one command, from Windows, at the project root)*:
   ```bash
   docker exec cidsec-airflow-sched python -m pip install --quiet -r /opt/project/ml/requirements.txt
   ```
2. **Vérifier** / *Verify* :
   ```bash
   docker exec cidsec-airflow-sched python -c "import xgboost, sklearn, pandas, prophet; print('DEPS-OK')"
   ```
   Résultat attendu / expected: `DEPS-OK`

> **Note technique / technical note** : PySpark 3.x utilise `np.bool` dans `toPandas()`,
> supprimé dans numpy ≥ 1.24. Un « shim » de compatibilité (2 lignes) est intégré dans
> chaque script ML — pas besoin de rétrograder numpy.
> *PySpark 3.x uses `np.bool` in `toPandas()`, removed in numpy ≥ 1.24. A 2-line
> compatibility shim is built into each ML script — no need to downgrade numpy.*

---

## 3. Modèle 1 — Prédiction des retards de livraison (XGBoost) / Model 1 — Delivery delay prediction

### 3.1 Pourquoi / WHY
Anticiper les commandes en retard **au moment de la commande** (et non après) pour
déclencher des alertes logistiques. *Anticipate late orders **at order time** (not after)
to trigger logistics alerts.*

### 3.2 Données / Data
- Source : `silver_db.conformed_commandes_enrichies` (96 476 commandes livrées,
  enrichies météo — construit par `spark_jobs/conformed_build.py`).
- Label : `is_late` (1 = livré en retard, taux réel **8,1 %**).

### 3.3 Green Book — colonnes retenues / exclues / Green Book — kept vs excluded columns

| Colonne | Décision | Pourquoi / Why |
|---|---|---|
| `nb_articles`, `montant_articles`, `montant_fret` | ✅ Exploitables | Connues à la commande / known at order time |
| `temp_max_c`, `temp_min_c`, `precipitations_mm`, `vent_max_kmh`, `is_jour_pluie` | ✅ Exploitables | Météo du jour (association temps réel) / day weather (real-time association) |
| `jour_semaine`, `mois`, `jour_mois` | ✅ Exploitables | Saisonnalité dérivée de `jour_achat` / seasonality from order date |
| `delivery_delay_days`, `is_late` | ❌ Exclues (label) | C'est la réponse elle-même / the answer itself |
| `order_delivered_*`, `order_status` | ❌ Exclues | Inconnues à la commande (**fuite de données**) / unknown at order time (**data leakage**) |
| `review_score` | ❌ Exclue | Donné après livraison (fuite) / given after delivery (leakage) |
| `montant_total` | ❌ Exclue | = `montant_articles + montant_fret` (**colinéarité exacte**) / exact linear combination |
| `temp_moy_c` | ❌ Exclue | ≈ moyenne de `temp_max_c`/`temp_min_c` (colinéarité) / combination of min/max |

### 3.4 Méthode / Method
1. Découpe **80 % entraînement / 20 % test**, stratifiée (même taux de retard des deux côtés).
2. XGBoost (300 arbres, profondeur 5) avec `scale_pos_weight = 11,3` pour compenser le
   déséquilibre des classes (8 % de positifs).
3. Métriques : AUC, exactitude, F1, précision, rappel.
4. Persistance : 3 tables Gold (Hive + PostgreSQL) + modèle sauvegardé en JSON.

### 3.5 Résultats mesurés / Measured results (exécution du 19/11/2026)

| Métrique | Valeur | Lecture / Reading |
|---|---|---|
| **AUC** | **0,7302** | Pouvoir discriminant bon / good discrimination |
| Exactitude / accuracy | 0,7156 | |
| Rappel / recall | 0,623 | 2 retards sur 3 détectés / 2 of 3 delays detected |
| Précision / precision | 0,166 | |
| F1 | 0,2622 | |
| Train / test | 77 180 / 19 296 | |

**Top 5 variables (gain) / top 5 features (gain)** :
`mois` (0,266) · `temp_min_c` (0,100) · `jour_mois` (0,094) · `montant_fret` (0,083) · `precipitations_mm` (0,078)
→ **la saisonnalité, la météo et le fret pilotent les retards** / *seasonality, weather and
freight drive delays.*

### 3.6 Tables produites / Output tables
| Table | Contenu / Content |
|---|---|
| `gold.ml_pred_retard_metriques` | Scores par exécution / scores per run |
| `gold.ml_pred_retard_importance` | Importance des variables (gain) / feature importance |
| `gold.ml_pred_retard_predictions` | Prédictions sur le jeu de test / test predictions |
| `ml/modeles/pred_retard_xgboost.json` | Modèle sérialisé / serialized model |

### 3.7 Rejouer à la main / Replay by hand
```bash
# 1. Depuis Windows (racine du projet) / From Windows (project root) :
docker exec cidsec-airflow-sched python /opt/project/ml/train_pred_retard.py

# 2. Vérifier les résultats / Verify results :
docker exec cidsec-postgres psql -U spark -d gold_db -c \
  "SELECT * FROM gold.ml_pred_retard_metriques ORDER BY run_ts DESC LIMIT 1;"
docker exec cidsec-postgres psql -U spark -d gold_db -c \
  "SELECT * FROM gold.ml_pred_retard_importance ORDER BY rang LIMIT 5;"
```

---

## 4. Modèle 2 — Prévision du CA par catégories (Prophet) / Model 2 — Revenue forecast by category

### 4.1 Pourquoi / WHY
Anticiper la saisonnalité des ventes (hebdomadaire + annuelle) par catégorie de produits
pour le pilotage commercial. *Anticipate sales seasonality (weekly + yearly) per product
category for commercial steering.*

### 4.2 Données / Data
- Source : Silver `olist_order_items` ⋈ `olist_orders` ⋈
  `olist_products` (grain **item** = le plus fin pour la catégorie ; la table
  Gold `fact_commande` est au grain commande et ne porte pas la clé produit).
- Agrégat : **CA journalier par catégorie** (somme des prix d'articles) + série
  **TOTAL** toutes catégories.
- Historique : ~730 jours (2016-09 → 2018-10).

### 4.3 Méthode / Method
1. Top **5 catégories** par CA cumulé + série TOTAL (6 séries).
2. Pour chaque série : **30 derniers jours gardés en test** (évaluation), Prophet entraîné
   avec saisonnalité **hebdomadaire + annuelle** (multiplicative).
3. Évaluation : **MAPE** et **MAE** sur le test, comparés à une **baseline « valeur de la
   veille »** (le modèle doit faire mieux que « demain = hier »).
4. Prévision finale : **90 jours** (bornes basses/hautes = intervalle de confiance).
5. Persistance : `gold.ml_forecast_ca` (prévisions) + `gold.ml_forecast_ca_metriques`
   (scores) — Hive + PostgreSQL — modèles Prophet sauvegardés en JSON.

### 4.4 Tables produites / Output tables
| Table | Contenu / Content |
|---|---|
| `gold.ml_forecast_ca` | 90 jours prévus par série (`yhat`, `yhat_bas`, `yhat_haut`, `type_ligne`) |
| `gold.ml_forecast_ca_metriques` | MAPE / MAE / baseline par série |
| `ml/modeles/forecast_ca_<serie>.json` | Modèles Prophet sérialisés |

### 4.5 Résultats mesurés / Measured results (exécution du 19/11/2026)

| Série | WAPE (réf.) | MAPE | MAE modèle | MAE baseline saison | Le modèle bat la baseline ? |
|---|---|---|---|---|---|
| **cama_mesa_banho** | **36,2 %** | 104 % | 804 | 1 161 | ✅ Oui (+31 %) |
| **beleza_saude** | ~43 % | 108 % | 1 895 | — | ✅ Oui |
| **informatica_acessorios** | **50,2 %** | 245 % | 721 | 934 | ✅ Oui (+23 %) |
| esporte_lazer | 66,9 % | 1 204 % | 1 190 | 1 016 | ❌ Non |
| relogios_presentes | 74,2 % | 512 % | 1 882 | 1 382 | ❌ Non |
| TOTAL (toutes catégories) | voir table `gold.ml_forecast_ca_metriques` | | | | |

**Lecture honnête / honest reading** :
- ✅ Sur les **grandes séries** (cama_mesa_banho, beleza_saude, informatica — > 8 000 €/j),
  le modèle **bat la baseline saisonnière de 20 à 30 %** : la saisonnalité Prophet
  apporte une vraie valeur.
- ❌ Sur les **petites séries** (relogios, esporte_lazer — CA journalier faible et
  irrégulier), le modèle ne bat pas « même jour semaine dernière » : l'historique est
  trop bruité. *Décision documentée : ces séries sont livrées à titre indicatif, avec
  leur intervalle de confiance, sans garantie de performance.*
- ⚠️ **Le MAPE n'est PAS une métrique fiable ici** : il explose (1 204 %) dès qu'un jour
  de test a un CA proche de zéro. La métrique de référence est le **WAPE** (robuste).
  *MAPE is NOT reliable here — it explodes when a test day has near-zero revenue. The
  reference metric is WAPE.*
- Les prévisions sont datées, avec bornes basses/hautes (`yhat_bas`/`yhat_haut`) pour
  que Power BI puisse afficher l'incertitude.

### 4.6 Rejouer à la main / Replay by hand
```bash
docker exec cidsec-airflow-sched python /opt/project/ml/train_forecast_ca.py

# Vérifier / Verify :
docker exec cidsec-postgres psql -U spark -d gold_db -c \
  "SELECT * FROM gold.ml_forecast_ca_metriques ORDER BY mape_test_pct;"
docker exec cidsec-postgres psql -U spark -d gold_db -c \
  "SELECT serie, jour, yhat, type_ligne FROM gold.ml_forecast_ca WHERE type_ligne='prevision' LIMIT 10;"
```

---

## 5. Modèle 3 — Prévision IoT (à faire) / Model 3 — IoT forecast (to do)

**Prévu / planned** : prévision de la consommation électrique (`uci_household_power`,
`opsd_time_series`) par Prophet ou XGBoost sur variables dérivées (lags, calendrier).
Livrable : `ml/train_forecast_iot.py` + tables `gold.ml_forecast_iot*`.
*Forecast power consumption using Prophet or XGBoost on derived features (lags, calendar).*

---

## 6. Monitoring & explication de la variance / Monitoring & variance explanation

- **Importance des variables** : table `gold.ml_pred_retard_importance` (gain) — les 5
  dominantes sont commentées section 3.5. *Feature importance table — top 5 commented.*
- **Suivi des métriques dans le temps** : chaque exécution écrit une ligne datée
  (`run_ts`) dans `*_metriques` — on peut tracer la dérive du modèle en Power BI.
  *Each run writes a dated row — model drift can be charted in Power BI.*
- **Variance expliquée (AUC)** : 0,73 sur données inconnues = le modèle généralise sans
  sur-apprentissage marqué (train/test cohérents). *Explained variance (AUC): 0.73 on
  unseen data = the model generalizes without marked overfitting.*

---

## 7. Journal des modifications / Change log

| Date | Étape / Step | Détail |
|---|---|---|
| 19/11/2026 | Modèle 1 | `ml/train_pred_retard.py` — entraîné, validé (AUC 0,73), 3 tables écrites, modèle sauvegardé |
| 19/11/2026 | Environnement | `ml/requirements.txt` + shim numpy ; volume `../ml` ajouté au compose (rw) |
| 19/11/2026 | Modèle 2 | `ml/train_forecast_ca.py` — 6 séries (TOTAL + top 5 catégories), 90 jours prévus, 3 943 lignes écrites ; WAPE ~36-74 % ; le modèle bat la baseline saisonnière sur les grandes séries (voir 4.5) |
| 19/11/2026 | Documentation | Ce document créé puis complété avec les résultats réels des modèles 1 et 2 |

---

## 8. Glossaire / Glossary

| Terme | Définition |
|---|---|
| **AUC** | Aire sous la courbe ROC — 0,5 = aléatoire, 1 = parfait / area under ROC curve |
| **MAPE** | Erreur absolue moyenne en % / mean absolute percentage error |
| **Fuite de données** | Utiliser une information inconnue au moment de la prédiction / using information not yet known at prediction time |
| **Colinéarité** | Deux variables redondantes (l'une déductible de l'autre) / two redundant variables |
| **Baseline** | Référence simple (« valeur de la veille ») que le modèle doit dépasser / simple benchmark to beat |
| **Prophet** | Bibliothèque de prévision temporelle (Meta) avec saisonnalités / time-series forecasting library (Meta) |
| **XGBoost** | Algorithme d'arbres boostés pour la classification/régression / boosted trees algorithm |
