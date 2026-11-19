# Phase 4 — Power BI : guide de construction manuel du rapport / Phase 4 — Power BI: manual report build guide

> **Pourquoi ce document / WHY this document** : le livrable `powerbi/CIDSEC.pbix` est
> produit par **Power BI Desktop** (application graphique Windows) — il ne peut pas être
> généré par un script. Ce guide détaille CHAQUE clic pour que **n'importe quel membre
> de l'équipe** construise le rapport à la main en ~45 minutes, sans connaissance
> préalable de Power BI.
> *The `powerbi/CIDSEC.pbix` deliverable is produced by **Power BI Desktop** (Windows
> GUI) — it cannot be script-generated. This guide details EVERY click so **any team
> member** can build the report by hand in ~45 minutes, with no prior Power BI knowledge.*

**Date** : 19/11/2026 · **Auteur** : Bertrand LEVODO

---

## 0. Ce qui est déjà prêt / What is already ready

| Élément | État | Où |
|---|---|---|
| Données (schéma en étoile + ML + qualité) | ✅ Dans PostgreSQL | base `gold_db`, schémas `gold` et `dq` |
| Vues métier pour Power BI | ✅ Créées (5 vues `vue_pbi_*`) | `sql/postgres/03_powerbi_views.sql` |
| Modèles ML + métriques | ✅ Entraînés (Phase 3) | tables `gold.ml_*` |
| Ce guide | ✅ | ce document |

**Prérequis / prerequisites** : Power BI Desktop (gratuit, Microsoft Store) + la
plateforme Docker du projet qui tourne (les conteneurs `cidsec-postgres`).

---

## 1. Installer Power BI Desktop / Install Power BI Desktop

1. Ouvrir le **Microsoft Store** sur Windows.
2. Chercher « **Power BI Desktop** » → **Installer** (≈ 500 Mo).
3. L'application est prête quand « Ouvrir » apparaît.
   *Open Microsoft Store → search 'Power BI Desktop' → Install → Open.*

---

## 2. Se connecter à PostgreSQL / Connect to PostgreSQL

1. Lancer Power BI Desktop → **Récupérer des données** (Get data) → **Plus…** →
   rechercher « **PostgreSQL** » → **Connecter**.
2. Remplir la fenêtre :
   | Champ | Valeur |
   |---|---|
   | Serveur / Server | `localhost:5432` |
   | Base de données / Database | `gold_db` |
3. Cliquer **OK** → la fenêtre d'authentification apparaît :
   | Champ | Valeur |
   |---|---|
   | Base de données / Database | `spark` |
   | Mot de passe / Password | `spark` |
4. ⚠️ Si un avertissement de chiffrement apparaît : cocher « **Ignorer les problèmes
   de confidentialité** » (base locale de développement) → **OK**.
   *If a privacy warning appears: check 'Ignore privacy levels' (local dev database).*

---

## 3. Charger les données / Load the data

Dans le **navigateur**, cocher **uniquement** ces éléments (le reste est inutile au
rapport et ralentirait le chargement) :
*In the Navigator, check ONLY these items:*

- [x] `vue_pbi_synthese` — KPI globaux / global KPIs
- [x] `vue_pbi_ventes_categorie` — CA par catégorie / revenue per category
- [x] `vue_pbi_qualite` — taux de réussite des contrôles / quality pass rate
- [x] `vue_pbi_previsions` — prévisions CA + IoT / forecasts
- [x] `vue_pbi_modeles` — métriques des modèles / model metrics
- [x] `fact_commande` — pour les analyses détaillées / detailed analysis
- [x] `dim_date` — table de dates / date table

→ **Charger** (Load). Attendez ~30 s (596 + 3 943 + ~99 000 lignes).

---

## 4. Modèle relationnel / Data model

Power BI crée la plupart des relations automatiquement. **Vérifier** dans le vue
**Modèle** (icône en bas à gauche) :
*Power BI creates most relationships automatically. **Check** in the Model view:*

- `dim_date[jour]` → `fact_commande[jour_achat]` (relation « active », sens étoile)
- `fact_commande` et les vues `vue_pbi_*` sont **indépendantes** (pas de relation) :
  c'est volontaire — les vues sont déjà agrégées.

Si une relation manque : faire glisser la colonne de la dimension vers la table de fait.

---

## 5. Mesures DAX / DAX measures

**Comment créer une mesure / how to create a measure** : clic droit sur la table →
**Nouvelle mesure** → coller la formule → Entrée.
*Right-click a table → New measure → paste the formula → Enter.*

```dax
-- 1. Chiffre d'affaires total / Total revenue
CA Total = SUM ( fact_commande[montant_total] )

-- 2. Nombre de commandes / Order count
Nb Commandes = COUNT ( fact_commande[order_id] )

-- 3. Taux de retard (%) / Late rate (%)
Taux Retard % =
    DIVIDE (
        CALCULATE ( COUNTROWS ( fact_commande ), fact_commande[is_late] = TRUE () ),
        COUNTROWS ( fact_commande )
    ) * 100

-- 4. Délai moyen de livraison (jours) / Average delivery time (days)
Delai Moyen Jours = AVERAGE ( fact_commande[delai_livraison_jours] )

-- 5. CA prévision 90 jours (€) / 90-day forecast revenue (€)
CA Prevision 90j =
    CALCULATE (
        SUM ( vue_pbi_previsions[yhat] ),
        vue_pbi_previsions[famille] = "CA",
        vue_pbi_previsions[type_ligne] = "prevision"
    )
    -- NB : vue_pbi_previsions n'a pas de colonne type_ligne -> utiliser
    --      ml_forecast_ca directement si cette mesure renvoie vide.
    -- NB: vue_pbi_previsions has no type_ligne column -> use ml_forecast_ca
    --     directly if this measure returns blank.

-- 6. WAPE moyen des prévisions IoT / Average IoT forecast WAPE
WAPE Moyen IoT = AVERAGE ( vue_pbi_modeles[wape] )

-- 7. Taux de réussite des contrôles qualité (%) / Quality pass rate (%)
Taux Qualite % = AVERAGE ( vue_pbi_qualite[taux_reussite_pct] )
```

---

## 6. Les 6 pages du rapport / The 6 report pages

Pour ajouter une page : « **+** » en bas → double-clic pour renommer.

### Page 1 — « Synthèse » / Summary
| Visuel | Données |
|---|---|
| 4 **Cartes** (Card) | `CA Total`, `Nb Commandes`, `Taux Retard %`, `Delai Moyen Jours` |
| 1 **Carte** | `WAPE Moyen IoT` |
| 1 **Graphique linéaire** | `vue_pbi_previsions[yhat]` par `jour`, filtre `famille = "CA"` |

### Page 2 — « Ventes & catégories » / Sales & categories
| Visuel | Données |
|---|---|
| **Graphique à barres** | `ca_ajuste` par `categorie` (top 5) |
| **Graphique linéaire** | `ca_ajuste` par `jour` (légende : `categorie`) |
| **Segmentation** (Slicer) | `categorie` |

### Page 3 — « Trades & marchés » / Trades & markets
| Visuel | Données |
|---|---|
| **Carte** | `Nb Commandes` (ou comptage de `fact_trades` si chargé) |
| **Graphique linéaire** | CA par `jour` (source : `vue_pbi_ventes_categorie`, `categorie = "TOTAL"`) |

### Page 4 — « Énergie & IoT » / Energy & IoT
| Visuel | Données |
|---|---|
| **Graphique linéaire** | `vue_pbi_previsions[yhat]` par `jour`, filtre `famille = "IOT"` |
| **Carte** | `WAPE Moyen IoT` |

### Page 5 — « Prévisions ML » / ML forecasts
| Visuel | Données |
|---|---|
| **Graphique linéaire** | `yhat` + `yhat_bas` + `yhat_haut` par `jour` (zone d'incertitude) |
| **Segmentation** | `serie` (choix de la série) |
| **Tableau** | `vue_pbi_modeles` complet |

### Page 6 — « Monitoring qualité » / Quality monitoring
| Visuel | Données |
|---|---|
| **Jauge** (Gauge) | `Taux Qualite %` |
| **Graphique à barres** | `nb_controles` par `couche` (légende : `nb_ok`) |
| **Tableau** | `vue_pbi_modeles` (suivi de la dérive des modèles) |

---

## 7. Enregistrer le livrable / Save the deliverable

1. **Fichier → Enregistrer sous** → naviguer jusqu'à
   `C:\Users\Administrateur\Desktop\CESI\Stage_S7\CIDSEC_CONST\powerbi\`
   (créer le dossier `powerbi` s'il n'existe pas).
2. Nommer le fichier **`CIDSEC.pbix`** → **Enregistrer**.

---

## 8. Vérification finale / Final verification

| Contrôle | Attendu |
|---|---|
| Le fichier `powerbi/CIDSEC.pbix` existe | ✅ |
| Les 6 pages sont présentes en bas de l'écran | ✅ |
| Les cartes de la page 1 affichent des chiffres (pas de blanc) | CA ≈ 13 M R$, 99 441 commandes |
| Rafraîchir les données (Accueil → Actualiser) | aucune erreur |

**Rejouer la connexion à la main / replay the connection by hand** : depuis Windows,
vérifier que la base répond avant d'ouvrir Power BI :
```bash
docker exec cidsec-postgres psql -U spark -d gold_db -c "SELECT * FROM gold.vue_pbi_synthese;"
```

---

## 9. Dépannage / Troubleshooting

| Symptôme | Cause | Solution |
|---|---|---|
| « could not connect » | conteneurs arrêtés | `docker start cidsec-postgres` (ou `docker compose up -d`) |
| Cartes vides | filtres croisés | onglet **Filtres** → « Effacer les filtres » |
| Colonnes absentes dans le navigateur | vues non créées | ré-exécuter `sql/postgres/03_powerbi_views.sql` (section 0) |
| Chiffres différents d'une page à l'autre | unités (R$ vs €) | vérifier la colonne `unite` de `vue_pbi_previsions` |
