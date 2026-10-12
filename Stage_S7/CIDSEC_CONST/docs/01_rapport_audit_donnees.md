# 01 — Rapport d'audit des données

**Projet** : CIDSEC_CONST — Pipeline Medallion + Analytics IA
**Date de l'audit** : 02/10/2026 — **Méthode** : profiling automatique (script `audit_quality.py`,
résultats bruts dans `docs/data/audit_report.json`) + lecture manuelle des anomalies.

---

## 1. Périmètre audité

Deux jeux de données disponibles en local dans
`C:\Users\Administrateur\Desktop\CESI\Stage_S7\CIDSEC_CONST\Données` :

| Source | Fichier | Contenu | Taille | Lignes | Colonnes |
|---|---|---|---|---|---|
| **Olist** (Kaggle, e-commerce brésilien) | `E-Commerce/archive.zip` | 9 CSV : clients, commandes, articles, paiements, avis, produits, vendeurs, géoloc, traductions | 45 Mo (zip) | **1 350 445** au total | 5 à 9 par table |
| **Cymbal Investments** (rapport de trade capture) | `Investissment_Cymball/archive (1).zip` | 1 CSV : transactions financières (marchés US, 2020) | 61 Mo (zip) | **1 222 562** | 17 |

**Total : 2 573 007 lignes, 10 fichiers, ~128 Mo de CSV.** Volume parfaitement adapté
à un pipeline Spark/HDFS pédagogique et réaliste.

## 2. Inventaire détaillé

### 2.1 Olist (simulation ERP/CRM e-commerce)

| Table | Lignes | Clé | Doublons clé | Valeurs vides notables | Période |
|---|---|---|---|---|---|
| `olist_orders_dataset` | 99 441 | `order_id` (unique) | 0 | `order_approved_at` : 160 · `order_delivered_customer_date` : 2 965 | achat 04/09/2016 → 17/10/2018 |
| `olist_customers_dataset` | 99 441 | `customer_id` (unique) | 0 | — | — |
| `olist_order_items_dataset` | 112 650 | (`order_id`,`order_item_id`) | 9 803 order_id à plusieurs lignes (normal) | — | livraison max 09/04/2020 |
| `olist_order_payments_dataset` | 103 886 | (`order_id`,`payment_sequential`) | 2 961 order_id multi-paiements (normal) | — | — |
| `olist_order_reviews_dataset` | 99 224 | `review_id` | **789 identifiants dupliqués → 814 lignes en trop** | commentaires : 87 658 titres / 58 274 messages vides (normal) | 02/10/2016 → 29/10/2018 |
| `olist_products_dataset` | 32 951 | `product_id` (unique) | 0 | **610 produits sans catégorie** · 2 sans dimensions/poids | — |
| `olist_sellers_dataset` | 3 095 | `seller_id` (unique) | 0 | — | — |
| `olist_geolocation_dataset` | 1 000 163 | `zip_code_prefix` | **17 972 codes postaux sur-représentés** (plusieurs coordonnées) | — | — |
| `product_category_name_translation` | 71 | `product_category_name` (unique) | 0 | — | — |

### 2.2 Cymbal Investments (simulation données financières)

| Table | Lignes | Clé | Contenu |
|---|---|---|---|
| `cymbal_investments_trade_capture_report` | 1 222 562 | `TradeReportID` (1 222 553 distincts → **9 doublons**) | Ordres/transactions sur 13 symboles (ESM0…), 9 contreparties, du **18/05/2020 au 23/11/2020** (188 jours) |

Colonnes à faible variété (à documenter comme constantes métier) :
`Quantity` = 1 toujours · `CFICode` = 1 seule valeur · `SenderCompID` = 1 valeur ·
`PartyIDSource` et `PartyRole` = 1 valeur chacun.

## 3. Intégrité référentielle (Olist) — ✅ excellente

| Relation | Clés enfant | Clés orphelines |
|---|---|---|
| `orders.customer_id` → `customers.customer_id` | 99 441 | **0** |
| `order_items.order_id` → `orders.order_id` | 98 666 | **0** |
| `payments.order_id` → `orders.order_id` | 99 440 | **0** |
| `reviews.order_id` → `orders.order_id` | 98 673 | **0** |
| `order_items.product_id` → `products.product_id` | 32 951 | **0** |
| `order_items.seller_id` → `sellers.seller_id` | 3 095 | **0** |
| `products.category` → `translation.category` | 73 | **2** (voir anomalie A1) |

Aucune clé orpheline sur les relations transactionnelles : le jeu est sain pour un
modèle dimensionnel.

## 4. Anomalies relevées et traitement prévu

> Chaque anomalie est classée (🔴 bloquant / 🟠 à corriger / 🟡 mineur) et **trouvée
> dans la couche Silver** avec la règle exacte appliquée.

| # | Sévérité | Anomalie | Détail mesuré | Règle de traitement (couche Silver) |
|---|---|---|---|---|
| **A1** | 🟠 | 2 catégories produits sans traduction anglaise | `pc_gamer`, `portateis_cozinha_e_preparadores_de_alimentos` | `LEFT JOIN` traduction + valeur d'origine en secours (`COALESCE`) |
| **A2** | 🟠 | 610 produits sans catégorie ni métadonnées de nom/description | 610 lignes avec 4 colonnes vides | Catégorie = `'inconnu'` ; métadonnées laissées à `NULL` |
| **A3** | 🟡 | 4 produits avec `product_weight_g = 0` (2 autres sans poids du tout) | incohérent physiquement | poids → `NULL` (et flag `_dq_flag = 'poids_invalide'`) |
| **A4** | 🟠 | **814 lignes de reviews dupliquées** (789 `review_id` en double) | même avis répété | déduplication : conservation de la ligne au `review_answer_timestamp` le plus récent |
| **A5** | 🟡 | 160 commandes sans `order_approved_at` | commandes `canceled` principalement | conservé à `NULL` + indicateur `is_approved = false` |
| **A6** | 🟡 | 2 965 commandes sans date de livraison client | cohérent : statuts non livrés (625 `canceled`, 609 `unavailable`, 1 107 `shipped`…) | conservé à `NULL` ; les KPIs de livraison ne portent que sur `order_status = 'delivered'` |
| **A7** | 🟠 | Géolocalisation : 31 coordonnées hors Brésil et 17 972 codes postaux avec plusieurs points GPS | lat/lng extrêmes (ex: lat 45, lng 121) | agrégation par code postal (moyenne lat/lng) + exclusion des points hors zone Brésil (bounding box) |
| **A8** | 🟡 | Paiements : `payment_value = 0` et `payment_installments = 0` existants | valeurs min = 0 | conservation (cas métier « acompte/avoir ») mais contrôle qualité DQ-04 documenté |
| **A9** | 🟡 | BOM UTF-8 dans `product_category_name_translation.csv` | 1re colonne lue `\ufeffproduct_category_name` | lecture `utf-8-sig` systématique dans tous les readers |
| **A10** | 🔴 | Tous les CSV : **types texte uniquement** (dates, montants, nombres) | ex: `price = '58.90'` | typage explicite systématique en Silver (`to_timestamp`, `cast`) — **aucun calcul en Bronze** |
| **A11** | 🟡 | Cymbal : 9 `TradeReportID` dupliqués | 1 222 562 lignes / 1 222 553 distincts | déduplication Silver sur `TradeReportID` (garder `SendingTime` le plus récent) |
| **A12** | 🟡 | Cymbal : `MaturityDate` = `TransactTime` + 60 s (valeur dérivée artificielle) | observé sur échantillon | conservé tel quel, mais **exclu des analyses métier** (documenté dans le dictionnaire de données) |
| **A13** | 🟡 | Cymbal : colonnes constantes (`Quantity`, `CFICode`, `SenderCompID`, `PartyIDSource`, `PartyRole`) | 0 variabilité | conservées en Bronze, **écartées du modèle Gold** (gain de place, clarté) |

## 5. Synthèse qualité par source

| Critère | Olist | Cymbal |
|---|---|---|
| Complétude | 🟢 > 99 % sur les colonnes clés | 🟢 100 % (aucun vide) |
| Unicité | 🟠 reviews dupliquées (A4) | 🟠 9 doublons (A11) |
| Intégrité référentielle | 🟢 0 orpheline | 🟢 clé `TradeReportID` quasi unique |
| Validité | 🟠 poids/catégories invalides (A3) | 🟡 colonnes constantes (A13) |
| Fraîcheur | 🟡 données figées 2016-2018 (normal : dataset public) | 🟡 figé 2020 (idem) |
| Typage | 🔴 tout est texte (A10) | 🔴 tout est texte (A10) |

**Verdict** : les données sont **bonnes et exploitables**. Aucune anomalie bloquante ;
les corrections sont standard et se traitent dans la couche Silver avec des règles
tracées et automatisables. Le typage (A10) est le chantier principal mais mécanique.

## 6. Recommandations pour la suite

1. **Conserver les fichiers sources intacts** (principe Bronze) — toute correction est
   faite dans Silver, jamais dans les fichiers d'origine.
2. **Industrialiser les règles du tableau §4** : chaque ligne devient une règle de
   transformation Silver + un test automatisé (couche DQ).
3. **Dictionnaire de données** à produire (livrable phase 2) : pour chaque table, sens
   des colonnes, type, unité, règles de qualité — indispensable pour les utilisateurs finaux.
4. **Ajouter une source API** (météo ou autre) pour démontrer l'ingestion multicanal
   prévue au sujet (fichier JSON → Bronze → Silver).
5. Sur la couche Gold, **ne pas agréger les commandes non livrées** dans les KPIs de
   livraison (A6) pour éviter les biais de reporting.

---

## 7. Adéquation des sources initiales au périmètre du sujet (audit du 05/10/2026)

Le sujet exige un pipeline gérant des sources **hétérogènes** (ERP, CRM, IoT, APIs), une
**ingestion multicanale**, et des modèles ML de **prévision/classification**. Confronté à
ces exigences, le périmètre initial présentait 4 lacunes :

| Exigence du sujet | Couverture par les sources initiales | Verdict |
|---|---|---|
| Sources ERP / CRM hétérogènes | Olist (clients, commandes, produits, vendeurs, paiements) | ✅ couvert |
| Données transactionnelles à fort volume | Cymbal (1,2 M de transactions financières) | ✅ couvert |
| **IoT / séries temporelles** | aucune source | ❌ **manquait** |
| **Ingestion par API REST** | aucune source | ❌ **manquait** |
| **Enrichissement externe analytique** (croisement de sources) | aucun lien métier entre Olist (e-commerce Brésil 2016-2018) et Cymbal (marchés US 2020) | ❌ **manquait** |
| ML : prévision sur séries riches | 2 ans de CA Olist (limité) | 🟡 partiel |

**Conclusion : les sources initiales étaient insuffisantes** (3 lacunes bloquantes pour
les exigences du sujet) **et faiblement cohérentes entre elles** (deux domaines disjoints,
aucune clé de jointure possible). Trois sources compatibles ont donc été intégrées.

## 8. Sources complémentaires intégrées (05/10/2026)

### 8.1 Inventaire et justification

| # | Source | Fichier local | Volume | Période | Rôle dans le projet |
|---|---|---|---|---|---|
| S3 | **Open Power System Data** (IoT / énergie) | `Données/IoT_Energie/opsd_time_series_60min.csv` | 50 401 lignes horaires × 300 colonnes (130 Mo) | 31/12/2014 → 30/09/2020 | Source **IoT / séries temporelles** : charge électrique, production solaire/éolienne, prix day-ahead par pays. Sert à l'ingestion continue simulée et au **forecasting Prophet/XGBoost** |
| S6 | **UCI — Household Power Consumption** (IoT / télémétrie) | `Données/UCI_power/archive.zip` → `household_power_consumption.txt` | **2 075 259 lignes minute par minute** × 9 colonnes (133 Mo) | 16/12/2006 → 26/11/2010 | Source **IoT haute résolution** : consommation électrique d'un foyer (puissance active/réactive, tension, intensité, 3 sous-compteurs). Cas d'usage : prévision de charge à court terme, agrégation horaire/journalière, détection de pics |
| S4 | **Open-Meteo** (API REST, sans clé) | `Données/API_Meteo/openmeteo_sp_daily_2016_2018.json` | 791 jours × 5 variables | 01/09/2016 → 31/10/2018 | Source **API** + **cohérence** : météo de São Paulo **exactement sur la période Olist** → croisement météo / délais de livraison |
| S5 | **Frankfurter** (API REST, sans clé) | `Données/API_FX/frankfurter_usd_2020.json` | 136 jours ouvrés × 4 devises | 18/05/2020 → 23/11/2020 | Source **API** + **cohérence** : taux de change USD→BRL/EUR/GBP/JPY **exactement sur la période Cymbal** → valorisation des trades en devise |

### 8.2 Cohérence obtenue (recouvrement temporel vérifié)

| Domaine métier | Source principale | Source de croisement | Recouvrement | Usage analytique |
|---|---|---|---|---|
| E-commerce | Olist (09/2016 → 10/2018) | Météo São Paulo (09/2016 → 10/2018) | **100 %** | Impact de la pluie/vent sur les retards de livraison → feature ML `pred_retard` |
| Finance | Cymbal (05/2020 → 11/2020) | Taux de change (05/2020 → 11/2020) | **100 %** | Valorisation multi-devise, corrélation volumes/prix |
| Énergie (IoT) | OPSD (2014 → 2020) | — (source pivot ML) | couvre les 2 périodes | Prévision de charge (Prophet/XGBoost), simulation d'ingestion continue |
| Foyer (IoT haute rés.) | UCI Household Power (12/2006 → 11/2010) | — (source pivot ML) | 4 ans de télémétrie à la minute | Prévision de consommation à court terme, agrégations, détection de pics |

Chaque jeu de données batch dispose désormais d'une **source pivot de croisement** :
plus aucune source n'est isolée, et le pipeline couvre les 4 familles exigées
(ERP/CRM, transactionnel, IoT, API).

### 8.3 Profiling des nouvelles sources — anomalies relevées

| # | Source | Anomalie mesurée | Règle de traitement (Silver) |
|---|---|---|---|
| **B1** | 🟠 OPSD | Valeurs manquantes ponctuelles : `FR_load` 44 h, `DE_solar` 104 h, `DE_wind_onshore` 73 h | Interpolation linéaire sur 2 points max + sinon `NULL` + flag `_dq_flag` |
| **B2** | 🟡 OPSD | 300 colonnes (tous pays/mix) — surdimensionné pour le projet | Sélection du sous-ensemble pertinent en Silver (DE, FR, BE, NL, AT : charge, solaire, éolien, prix) |
| **B3** | 🟠 FX | 136 jours ouvrés sur ~190 jours calendaires (week-ends et jours fériés absents) | *Forward-fill* des dernières valeurs connues sur les jours manquants (règle standard de marché) |
| **B4** | 🟢 Météo | Aucune valeur manquante (791/791 jours complets) | Aucune correction nécessaire |
| **B5** | 🟡 Toutes API | Un appel API = un instantané : non reproductible si l'API change | Chaque réponse est **archivée telle quelle en Bronze datée** (principe d'immutabilité) |
| **B6** | 🟠 UCI | **Format piègeux** : séparateur `;`, virgule décimale, dates en `jj/mm/aaaa`, valeurs manquantes codées `?` | Reader Spark dédié (`sep=';'`, `decimal=','`, `dateFormat='dd/MM/yyyy'`, `nullValue='?'`) — vérifié sur échantillon |
| **B7** | 🟠 UCI | 25 979 lignes incomplètes (1,25 %), les 8 mesures vides **simultanément**, réparties sur **82 jours** (trous en rafales, parfois des journées entières) | Exclusion des lignes incomplètes en Silver + flag `_dq_flag='trou_telemetrie'` ; l'interpolation n'est **pas** appliquée (trop de trous consécutifs pour être fiable) |
| **B8** | 🟡 UCI | Résolution à la minute = 2 M de lignes pour 4 ans | Agrégation en Silver (`iot_foyer_1h`, `iot_foyer_1j`) pour l'analytique ; la minute reste en Bronze pour le ML haute résolution |

### 8.4 Note sur la source UCI (récupérée le 05/10/2026)

Le dataset *Individual Household Electric Power Consumption* (UCI) n'avait pas pu être
téléchargé automatiquement depuis ce réseau (connexions interrompues par
`archive.ics.uci.edu`, vérifié sur 2 URL et 8 tentatives). **L'équipe l'a récupéré
manuellement** (`Données/UCI_power/archive.zip`, 19,4 Mo) et il est désormais **intégré
au périmètre comme source S6**, conjointement à OPSD :

| | UCI Household Power | OPSD |
|---|---|---|
| Échelle | **1 foyer**, télémétrie à la **minute** | **Pays** (DE, FR, BE, NL, AT…), marché horaire |
| Période | 2006 → 2010 | 2014 → 2020 |
| Usage | prévision de charge haute résolution, agrégations, détection de pics | contexte marché, forecasting agrégé |

Les deux sources sont **complémentaires** (micro vs macro) et alimentent les mêmes
zones du pipeline (`/data/landing/iot/` → `/data/bronze/iot/` → `/data/silver/iot/`),
sans modification d'architecture.

---

*Annexe : `docs/data/audit_report.json` (profiling brut : vides, cardinalités, min/max par colonne).*
