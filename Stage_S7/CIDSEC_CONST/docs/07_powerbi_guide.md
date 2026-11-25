# Phase 4 — Power BI : guide de construction manuel du rapport / Phase 4 — Power BI: manual report build guide

> **Pourquoi ce document / WHY this document** : le livrable `powerbi/CIDSEC.pbix` est
> produit par **Power BI Desktop** (application graphique Windows) — il ne peut pas être
> généré par un script. Ce guide détaille CHAQUE clic pour que **n'importe quel membre
> de l'équipe** construise le rapport à la main (~1 h), sans connaissance préalable.
> Il intègre les **retours d'expérience réels** (pièges rencontrés et solutions validées
> le 24/11/2026 lors de la première construction du rapport).
> *This guide details EVERY click and integrates REAL field feedback (traps encountered
> and validated fixes during the first report build on 24/11/2026).*

**Date** : 24/11/2026 (v2 — enrichi des retours terrain) · **Auteur** : Bertrand LEVODO

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

## 2. Se connecter aux données — la voie ODBC *(recommandée, validée sur le terrain)*

> **Pourquoi ODBC et non « PostgreSQL » ?** Le connecteur natif PostgreSQL de Power BI
> exige SSL alors que le serveur du projet est en SSL simple ; il échoue avec
> « Exception while reading from stream ». La voie ODBC (pilote psqlODBC) est **fiable
> et testée**. Le connecteur natif reste possible pour les utilisateurs expérimentés
> (voir §2 bis en fin de section).

### 2.1 Installer le pilote psqlODBC *(5 min, une seule fois)*

1. Télécharger le pilote ODBC PostgreSQL sur le site **EDB** (« ODBC Driver Downloads »)
   — installer la version **64 bits**.
2. Vérification : Démarrer → taper « odbc » → **« Sources de données ODBC (64 bits) »**
   → onglet **Pilotes** → la liste doit contenir **« PostgreSQL Unicode(x64) »**.

### 2.2 Créer le DSN système *(3 min)*

> **Le piège n°1 à connaître** : un DSN n'est enregistré qu'au clic sur **« Save »/« OK »**
> final. Si vous fermez la fenêtre sans sauvegarder (parce que le Test a échoué), le DSN
> n'existe pas et Power BI ne le verra pas.

1. Dans « Sources de données ODBC (64 bits) » → onglet **« DSN système »**
   *(pas « DSN utilisateur »)* → **Ajouter…**
2. Sélectionner **« PostgreSQL Unicode(x64) »** *(pas ANSI, pas une version 32 bits)*
   → **Terminé**.
3. Remplir **exactement** :

| Champ | Valeur | Commentaire |
|---|---|---|
| Data Source Name (DSN) | `CIDSEC` | nom libre, retenu dans Power BI |
| Description | `Base Gold du projet CIDSEC (Docker)` | optionnel |
| **Database** | `gold_db` | ⚠️ **si vide**, erreur `database 'spark' does not exist` |
| Server | `127.0.0.1` | ⚠️ jamais `cidsec-postgres` (nom interne du conteneur) |
| Port | `5432` | |
| User Name | `spark` | |
| Password | `spark` | |
| SSL Mode | `Disable` | déroulant |

4. **Test** → attendu : ✅ **« Connection successful »**
5. **Save** *(c'est CE clic qui enregistre le DSN)* → fermer l'utilitaire.

**Si le Test échoue** : voir §9 (« Software caused connection abort » = redémarrer
Docker Desktop avant de recommencer).

### 2.3 Connecter Power BI au DSN

1. Power BI → **Obtenir des données** → chercher « odbc » → cliquer **« ODBC »**
   *(⚠️ pas « Flux OData », juste au-dessus)* → **Se connecter**.
2. « Nom de source de données (DSN) » : choisir **`CIDSEC`** → **OK**.
3. Niveau de confidentialité : **Ignorer les problèmes de confidentialité**.
4. Si une authentification est demandée : **Défaut** *(le DSN porte déjà les identifiants)*
   ou **Base de données** → `spark` / `spark`.

### 2 bis. Voie alternative : connecteur natif « PostgreSQL »

Seulement si vous préférez le connecteur natif : Obtenir des données → **PostgreSQL** →
Serveur `127.0.0.1`, Base `gold_db` → **Ignorer la confidentialité** → `spark`/`spark`.
Pièges connus : « Hôte inconnu » = mauvais nom d'hôte (utiliser `127.0.0.1`) ;
« Exception while reading from stream » = problème SSL (revenir à la voie ODBC).

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

💡 **Astuce anti-faute de frappe** : dans les formules DAX, n'écrivez **jamais** un nom
de table ou de colonne à la main — tapez les premières lettres puis **double-cliquez**
sur l'élément dans le volet **Données** : Power BI insère le nom exact (y compris quand
le nom affiché est préfixé par le schéma, ex. `gold_vue_pbi_qualite`).

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

### 5.1 Comment créer une mesure (procédure exacte)

1. Dans le ruban en haut : cliquer **« Nouvelle mesure »** *(section Calculs)*.
2. Une **barre de formule** apparaît sous le ruban — effacer le texte par défaut.
3. **Coller** la formule ci-dessous → **Entrée**.
4. La mesure apparaît dans le volet **Données** ✅ → recommencer pour la suivante.

💡 **Popup « Séparateurs DAX »** : cliquer **« Ignorer »** — les formules ci-dessous
utilisent déjà la virgule `,` = le réglage par défaut annoncé. *(Seulement si Power BI
souligne une erreur de syntaxe : remplacer les virgules par des `;`.)*

### 5.2 Les 7 mesures (corrigées et validées sur le terrain)

```dax
-- 1. Chiffre d'affaires historique total / Total historical revenue
-- Attendu : ≈ 15 843 553 R$ (15,84M)
CA Total = SUM ( fact_commande[montant_total] )

-- 2. Nombre de commandes / Order count — Attendu : 99 441
Nb Commandes = COUNT ( fact_commande[order_id] )

-- 3. Taux de retard (%) sur commandes avec verdict / Late rate (%)
-- Attendu : ≈ 8 %
-- POURQUOI cette forme : la colonne is_late est importée en TEXTE ('true'/'false'/'t'…)
--   par le pilote ODBC — la comparaison = TRUE() échoue. De plus les commandes non
--   livrées ont is_late vide : elles sont EXCLUES des deux côtés du rapport.
-- WHY this form: is_late is imported as TEXT by the ODBC driver (= TRUE() fails), and
--   undelivered orders have a blank is_late: excluded from both sides of the ratio.
Taux Retard % =
    DIVIDE (
        CALCULATE (
            COUNTROWS ( fact_commande ),
            LOWER ( fact_commande[is_late] ) IN { "true", "t", "1", "yes" }
        ),
        CALCULATE (
            COUNTROWS ( fact_commande ),
            LOWER ( fact_commande[is_late] ) IN { "true", "t", "1", "yes", "false", "f", "0", "no" }
        )
    ) * 100

-- 4. Délai moyen de livraison (jours) / Average delivery time (days) — Attendu : ≈ 12
Delai Moyen Jours = AVERAGE ( fact_commande[delai_livraison_jours] )

-- 5. CA prévision 90 jours (R$) / 90-day forecast revenue (R$) — Attendu : ≈ 1 994 731
-- POURQUOI le filtre serie = "TOTAL" : la vue contient la série TOTAL ET les 5
--   catégories ; sans ce filtre, la mesure somme les deux = DOUBLE COMPTAGE (2,77M
--   au lieu de 1,99M — constaté le 24/11/2026).
-- WHY serie = "TOTAL": the view holds the TOTAL series AND the 5 categories; without
--   this filter the measure sums both = DOUBLE COUNTING.
-- NB : ne pas filtrer sur type_ligne (colonne absente de la vue qui ne contient que
--      des prévisions ; l'erreur « type_ligne introuvable » vient de là).
CA Prevision 90j = CALCULATE (
    SUM ( vue_pbi_previsions[yhat] ),
    vue_pbi_previsions[famille] = "CA",
    vue_pbi_previsions[serie] = "TOTAL"
)

-- 6. WAPE moyen des prévisions IoT / Average IoT forecast WAPE
WAPE Moyen IoT = AVERAGE ( vue_pbi_modeles[wape] )

-- 7. Taux de réussite des contrôles qualité (%) / Quality pass rate (%) — Attendu : 100
Taux Qualite % = AVERAGE ( vue_pbi_qualite[taux_reussite_pct] )
```

---

## 6. Les 6 pages du rapport / The 6 report pages

**Créer un visuel** : dans le volet **Visualisations** (droite), cliquer l'icône du
visuel (ex. **Carte**) → un visuel vide apparaît sur le canevas → cocher le champ ou la
mesure dans le volet **Données** → il se remplit. Pour les graphiques : glisser les
champs dans les puits « Axe » et « Valeurs ».

### Page 1 — « Synthèse »
| Visuel | Champs |
|---|---|
| **Carte** × 5 | `CA Total`, `Nb Commandes`, `Taux Retard %`, `Delai Moyen Jours`, `CA Prevision 90j` |
| **Graphique linéaire** | `vue_pbi_previsions[yhat]` en Valeurs · `jour` en Axe · filtre (volet Filtres) : `famille = "CA"` ET `serie = "TOTAL"` |

### Page 2 — « Ventes & catégories »
| Visuel | Champs |
|---|---|
| **Graphique à barres** | `ca_ajuste` en Valeurs · `categorie` en Axe |
| **Graphique linéaire** | `ca_ajuste` en Valeurs · `jour` en Axe · `categorie` en Légende |
| **Segmentation** | `categorie` |

### Page 3 — « Trades & marchés »
| Visuel | Champs |
|---|---|
| **Carte** | `Nb Commandes` |
| **Graphique linéaire** | `ca_ajuste` par `jour` (source `vue_pbi_ventes_categorie`, filtre `categorie = "TOTAL"` — ⚠️ sinon double comptage) |

### Page 4 — « Énergie & IoT »
| Visuel | Champs |
|---|---|
| **Graphique linéaire** | `vue_pbi_previsions[yhat]` par `jour`, filtre `famille = "IOT"` |
| **Carte** | `WAPE Moyen IoT` |

### Page 5 — « Prévisions ML »
| Visuel | Champs |
|---|---|
| **Graphique linéaire** | `yhat` + `yhat_bas` + `yhat_haut` par `jour` (zone d'incertitude) |
| **Segmentation** | `serie` (choix de la série) |
| **Tableau** | table `vue_pbi_modeles` entière |

### Page 6 — « Monitoring qualité »
| Visuel | Champs |
|---|---|
| **Jauge** | `Taux Qualite %` |
| **Graphique à barres** | `nb_controles` en Valeurs · `couche` en Axe · `nb_ok` en Légende |
| **Tableau** | `vue_pbi_modeles` |

---

## 7. Enregistrer le livrable / Save the deliverable

1. **Fichier → Enregistrer sous** → naviguer jusqu'à
   `C:\Users\Administrateur\Desktop\CESI\Stage_S7\CIDSEC_CONST\powerbi\`
   (créer le dossier `powerbi` s'il n'existe pas).
2. Nommer le fichier **`CIDSEC.pbix`** → **Enregistrer**.

---

## 8. Vérification finale / Final verification

| Contrôle | Attendu (**valeurs mesurées en base**) |
|---|---|
| Carte `Nb Commandes` | **99 441** |
| Carte `CA Total` | **≈ 15,84 M R$** (15 843 553) |
| Carte `Taux Retard %` | **≈ 8 %** |
| Carte `CA Prevision 90j` | **≈ 1,99 M R$** (1 994 731) |
| Les 6 pages existent en bas de l'écran | ✅ |
| **Accueil → Actualiser** | aucune erreur |

**Rejouer la connexion à la main / replay the connection by hand** : depuis Windows,
vérifier que la base répond avant d'ouvrir Power BI :
```bash
docker exec cidsec-postgres psql -U spark -d gold_db -c "SELECT * FROM gold.vue_pbi_synthese;"
```

---

## 9. Dépannage / Troubleshooting

| Symptôme | Cause | Solution |
|---|---|---|
| « Hôte inconnu » (connecteur PostgreSQL) | mauvais nom d'hôte | utiliser `127.0.0.1` (jamais `cidsec-postgres`) |
| « Exception while reading from stream » | SSL / proxy Docker | **voie ODBC** (§2) ; si persiste : `docker desktop restart` |
| « Software caused connection abort » (Test DSN) | proxy réseau Docker corrompu | `docker desktop restart` (2 min) puis refaire le Test |
| `database 'spark' does not exist` | champ **Database** vide dans le DSN | mettre `gold_db` dans le champ Database |
| Le DSN `CIDSEC` n'apparaît pas dans Power BI | DSN non enregistré (Test raté sans Save) ou pilote 32 bits | refaire §2.2 en terminant par **Save** ; pilote **x64** |
| Erreur DAX « comparaison Text avec True/False » | `is_late` importé en texte | utiliser `LOWER(...) IN {...}` (mesure 3) |
| Erreur DAX « type_ligne introuvable » | colonne absente de la vue | retirer ce filtre (mesure 5) |
| Une mesure affiche « (Vide) » | table non chargée ou dénominateur à 0 | vérifier la table dans le volet Données ; créer `Test Nb = COUNTROWS(fact_commande)` pour diagnostiquer |
| Double comptage dans un total | filtre `serie` manquant sur `vue_pbi_previsions` | ajouter `serie = "TOTAL"` (ou la catégorie voulue) |
| Noms de tables préfixés (`gold_…`) | schéma inclus au chargement | double-cliquer l'élément dans le volet Données au lieu de taper |
| Popup « Séparateurs DAX » | information | **Ignorer** (la virgule convient) |
| Cartes vides après filtrage | filtres croisés | volet **Filtres** → « Effacer les filtres » |
| Rien ne répond plus au niveau SQL | plateforme arrêtée | `docker compose up -d` dans `docker/` |
| Conteneurs absents après redémarrage machine | Docker Desktop non lancé | lancer Docker Desktop → les conteneurs `restart: unless-stopped` repartent |
