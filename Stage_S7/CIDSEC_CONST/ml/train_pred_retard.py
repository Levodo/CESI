#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ml/train_pred_retard.py — Modèle 1 : prédiction des retards de livraison / Model 1: delivery delay prediction
=============================================================================================================
POURQUOI ce fichier : dans le cadre de la Phase 3 (ML) du projet CIDSEC_CONST, ce script
entraîne un modèle de CLASSIFICATION binaire prédisant si une commande sera livrée EN RETARD
(is_late = 1) à partir de ce qu'on sait AU MOMENT DE LA COMMANDE (montants, météo du jour,
saisonnalité). Objectif opérationnel : anticiper les retards pour déclencher des alertes
logistiques AVANT qu'ils ne se produisent.
WHY this file: Phase 3 (ML) of the CIDSEC_CONST project. This script trains a BINARY
CLASSIFICATION model predicting whether an order will be delivered LATE (is_late = 1),
using only information KNOWN AT ORDER TIME (amounts, weather that day, seasonality).
Operational goal: anticipate delays to trigger logistics alerts BEFORE they happen.

COMMENT ce fichier marche / HOW this file works :
1) Lit le dernier snapshot de `silver_db.conformed_commandes_enrichies` (commandes
   enrichies météo — construit par conformed_build.py).
   Reads the latest snapshot of the enriched orders table (weather-enriched, built by
   conformed_build.py).
2) Prépare X (features) et y (label is_late) — le GREEN BOOK (docs/06_green_book_ml.md)
   justifie chaque colonne retenue ou exclue (fuites de données / colinéarité).
   Prepares X (features) and y (label is_late) — the GREEN BOOK justifies every kept or
   excluded column (data leakage / collinearity).
3) Découpe 80 % entraînement / 20 % test (stratifié), entraîne un XGBoost
   (scale_pos_weight = classes déséquilibrées), calcule AUC / F1 / exactitude.
   80/20 stratified split, trains XGBoost (scale_pos_weight for imbalanced classes),
   computes AUC / F1 / accuracy.
4) Persiste en Gold : métriques, importance des variables, prédictions — côté Hive ET
   PostgreSQL (pour Power BI à la Phase 4).
   Persists to Gold: metrics, feature importance, predictions — to Hive AND PostgreSQL
   (for Power BI in Phase 4).

VÉRIFICATION / VERIFICATION :
- `SELECT * FROM gold.ml_pred_retard_metriques;` → la dernière ligne donne les scores
  (AUC attendu > 0.60 pour être utile).
- `SELECT * FROM gold.ml_pred_retard_importance;` → l'importance des features (gain).
- L'exécution imprime un rapport final dans les logs Airflow / le terminal.

ÉQUIVALENT À LA MAIN / MANUAL EQUIVALENT :
Tout est décrit dans docs/06_green_book_ml.md, section « rejouer le modèle à la main ».
"""

# --- Imports ---------------------------------------------------------------------------
# POURQUOI sys.path.append : les scripts spark_jobs/ (common.py) ne sont pas dans un
#            paquet installable ; on ajoute leur dossier au chemin d'import.
# WHY sys.path.append: spark_jobs/ (common.py) is not an installed package; we add its
#      folder to the import path.
import os
import sys

sys.path.append("/opt/project/spark_jobs")

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402  (chargé après sys.path volontairement / loaded after sys.path on purpose)
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DoubleType, IntegerType, StringType, StructField, StructType)

from common import CONFIG, get_spark, read_latest_snapshot, write_postgres, write_table  # noqa: E402

# --- Correctif de compatibilité NumPy / NumPy compatibility shim ------------------------
# POURQUOI : PySpark 3.x appelle np.bool dans toPandas(), alias SUPPRIMÉ dans numpy>=1.24.
#            Au lieu de rétrograder numpy (ce qui casse les autres librairies), on rétablit
#            l'alias manquant — 2 lignes, zéro dépendance, survit aux recréations de conteneur.
# WHY: PySpark 3.x calls np.bool in toPandas(), an alias REMOVED in numpy>=1.24. Instead
#      of downgrading numpy (which breaks other libraries), we restore the missing alias —
#      2 lines, no dependency, survives container recreations.
if not hasattr(np, "bool"):
    np.bool = bool  # type: ignore[attr-defined]  # shim de compat / compatibility shim

# --- Configuration du modèle / Model configuration -------------------------------------
# POURQUOI exclure ces colonnes :
#   - delivery_delay_days, is_late, dates de livraison, order_status : INCONNUES au moment
#     de la commande (ce serait « tricher » — fuite de données / data leakage).
#   - montant_total : combinaison linéaire de montant_articles + montant_fret (colinéarité
#     exacte, documentée dans le Green Book).
#   - temp_moy_c : combinaison de temp_max_c / temp_min_c (colinéarité).
#   - review_score : donné APRÈS la livraison (fuite).
# WHY exclude these columns: unknown at order time (data leakage), or exact linear
# combinations of other features (collinearity) — see the Green Book.
FEATURES = [
    "nb_articles",          # nombre d'articles / number of items
    "montant_articles",     # total produits / items total
    "montant_fret",         # frais de port / freight cost
    "temp_max_c",           # météo du jour / day weather
    "temp_min_c",
    "precipitations_mm",    # pluie / rainfall
    "vent_max_kmh",         # vent / wind
    "is_jour_pluie",        # booléen pluie / rainy day flag
    "jour_semaine",         # saisonnalité / seasonality (0=lundi)
    "mois",                 # saisonnalité / seasonality
    "jour_mois",            # saisonnalité / seasonality
]
LABEL = "is_late"           # 1 = livré en retard / delivered late

SRC = f"{CONFIG['db_silver']}.conformed_commandes_enrichies"   # silver_db.conformed_commandes_enrichies
DST = CONFIG["db_gold"]                                          # gold_db

SEED = 42                  # reproductibilité / reproducibility
TEST_RATIO = 0.2           # 20 % des données gardées pour le test / 20% held out for test


def charger_donnees(spark):
    """Charge les commandes enrichies et prépare X, y.
    Loads enriched orders and prepares X, y.

    POURQUOI filtrer sur is_late NOT NULL : seules les commandes LIVRÉES ont un label
    connu ; les commandes en cours/annulées n'ont pas de vérité terrain.
    WHY filter is_late NOT NULL: only DELIVERED orders have a known label; pending or
    cancelled orders have no ground truth.
    """
    df = read_latest_snapshot(spark, SRC)

    # Ajout des composantes de date à partir de jour_achat (acheté à la commande — pas de fuite).
    # Date parts from jour_achat (known at order time — no leakage).
    # IMPORTANT : les colonnes derivees sont creees AVANT le select final (sinon elles
    #             n'existent pas encore — bug corrige le 19/11/2026).
    # IMPORTANT: derived columns are created BEFORE the final select (otherwise they do
    #             not exist yet — bug fixed on 19/11/2026).
    df = (df
          .withColumn("jour_semaine", F.dayofweek(F.col("jour_achat")) - 1)   # 0 = lundi / Monday
          .withColumn("mois", F.month(F.col("jour_achat")))
          .withColumn("jour_mois", F.dayofmonth(F.col("jour_achat"))))

    cols = FEATURES + [LABEL, "order_id", "jour_achat"]
    df = df.select(*cols).where(F.col(LABEL).isNotNull())

    # POURQUOI toPandas : le jeu fait ~95 000 lignes x ~12 colonnes (quelques Mo) ;
    #            XGBoost travaille nativement en pandas/numpy, sans passer par Spark MLlib.
    # WHY toPandas: the dataset is ~95k rows x ~12 columns (a few MB); XGBoost works
    #      natively on pandas/numpy, no need to go through Spark MLlib.
    pdf = df.toPandas()
    pdf[FEATURES] = pdf[FEATURES].apply(pd.to_numeric, errors="coerce").fillna(0)
    pdf[LABEL] = pdf[LABEL].astype(int)
    return pdf


def entrainer_modele(pdf):
    """Entraîne XGBoost et renvoie le modèle + les métriques + l'importance des variables.
    Trains XGBoost and returns the model + metrics + feature importance.
    """
    # POURQUOI ces imports ici : échouer tôt (et clairement) si les librairies manquent,
    #            plutôt qu'une erreur obscure en cours de calcul.
    # WHY these imports here: fail early and clearly if libraries are missing.
    import xgboost as xgb
    from sklearn.metrics import (accuracy_score, f1_score, precision_score, recall_score,
                                 roc_auc_score)
    from sklearn.model_selection import train_test_split

    X = pdf[FEATURES]
    y = pdf[LABEL]

    X_tr, X_te, y_tr, y_te = train_test_split(
        X, y, test_size=TEST_RATIO, random_state=SEED, stratify=y)  # stratifié / stratified

    # POURQUOI scale_pos_weight : les retards sont minoritaires (~10 %) ; sans correction,
    #            le modèle prédirait « jamais en retard » et aurait une belle exactitude
    #            mais serait inutile. Ce poids équilibre les deux classes.
    # WHY scale_pos_weight: late orders are a minority (~10%); uncorrected, the model would
    #      predict "never late" with great accuracy but be useless. This weight balances
    #      the two classes.
    ratio = float((y_tr == 0).sum()) / max(float((y_tr == 1).sum()), 1.0)

    modele = xgb.XGBClassifier(
        n_estimators=300,        # nombre d'arbres / number of trees
        max_depth=5,             # profondeur des arbres / tree depth
        learning_rate=0.05,      # pas d'apprentissage / learning rate
        subsample=0.8,           # échantillonnage des lignes / row sampling
        colsample_bytree=0.8,    # échantillonnage des colonnes / column sampling
        scale_pos_weight=ratio,  # équilibre des classes / class balance
        random_state=SEED,
        n_jobs=4,                # 4 threads max / max 4 threads
        eval_metric="auc",
    )
    modele.fit(X_tr, y_tr)

    # Prédictions : proba pour l'AUC, classe 0.5 pour les méta-métriques.
    # Predictions: probabilities for AUC, class at 0.5 for meta metrics.
    proba_te = modele.predict_proba(X_te)[:, 1]
    pred_te = (proba_te >= 0.5).astype(int)

    metriques = {
        "auc": round(float(roc_auc_score(y_te, proba_te)), 4),
        "exactitude": round(float(accuracy_score(y_te, pred_te)), 4),
        "f1": round(float(f1_score(y_te, pred_te)), 4),
        "precision": round(float(precision_score(y_te, pred_te)), 4),
        "rappel": round(float(recall_score(y_te, pred_te)), 4),
        "n_train": int(len(X_tr)),
        "n_test": int(len(X_te)),
        "taux_retard_pct": round(float(y.mean() * 100), 2),
        "scale_pos_weight": round(ratio, 2),
    }

    # POURQUOI l'importance (gain) : elle répond à la question métier « qu'est-ce qui
    #            fait dériver les livraisons ? » — exigence « Monitoring » du contrat.
    # WHY feature importance (gain): it answers the business question "what drives
    #      delivery delays?" — a contractual 'Monitoring' requirement.
    imp = (pd.DataFrame({
        "variable": FEATURES,
        "gain": modele.feature_importances_,
    })
        .sort_values("gain", ascending=False)
        .reset_index(drop=True))

    return modele, metriques, imp, X_te, y_te, proba_te


def persister(spark, modele, metriques, imp, X_te, y_te, proba_te, pdf):
    """Écrit les résultats en Gold (Hive + PostgreSQL) pour Power BI.
    Writes results to Gold (Hive + PostgreSQL) for Power BI.
    """
    from datetime import datetime
    ts = datetime.now().isoformat(timespec="seconds")

    # 1) Métriques (une ligne par exécution) / Metrics (one row per run)
    m_df = spark.createDataFrame([{
        "run_ts": ts, "modele": "xgboost_pred_retard", "version": "1.0",
        "auc": metriques["auc"], "exactitude": metriques["exactitude"],
        "f1": metriques["f1"], "precision": metriques["precision"],
        "rappel": metriques["rappel"], "n_train": metriques["n_train"],
        "n_test": metriques["n_test"], "taux_retard_pct": metriques["taux_retard_pct"],
    }])
    # Signature exacte de write_table : (df, database, table, hdfs_path) — 4 arguments.
    # Exact signature of write_table: (df, database, table, hdfs_path) — 4 arguments.
    write_table(m_df, DST, "ml_pred_retard_metriques",
                f"{CONFIG['hdfs_gold']}/ml_pred_retard_metriques")
    write_postgres(m_df, "ml_pred_retard_metriques")

    # 2) Importance des variables / Feature importance
    imp_rows = [{"run_ts": ts, "rang": int(i + 1), "variable": r["variable"],
                 "gain": round(float(r["gain"]), 5)} for i, r in imp.iterrows()]
    imp_schema = StructType([
        StructField("run_ts", StringType()), StructField("rang", IntegerType()),
        StructField("variable", StringType()), StructField("gain", DoubleType())])
    imp_df = spark.createDataFrame(imp_rows, schema=imp_schema)
    write_table(imp_df, DST, "ml_pred_retard_importance",
                f"{CONFIG['hdfs_gold']}/ml_pred_retard_importance")
    write_postgres(imp_df, "ml_pred_retard_importance")

    # 3) Prédictions sur le jeu de test (pour analyse Power BI) / Test predictions
    pred_rows = [{
        "run_ts": ts,
        "order_id": str(oid), "y_reel": int(yt), "y_predit": int(yp >= 0.5),
        "probabilite_retard": round(float(yp), 4),
    } for oid, yt, yp in zip(pdf.loc[X_te.index, "order_id"], y_te, proba_te)]
    pred_schema = StructType([
        StructField("run_ts", StringType()), StructField("order_id", StringType()),
        StructField("y_reel", IntegerType()), StructField("y_predit", IntegerType()),
        StructField("probabilite_retard", DoubleType())])
    pred_df = spark.createDataFrame(pred_rows, schema=pred_schema)
    write_table(pred_df, DST, "ml_pred_retard_predictions",
                f"{CONFIG['hdfs_gold']}/ml_pred_retard_predictions")
    write_postgres(pred_df, "ml_pred_retard_predictions")

    # 4) Le modèle lui-même est sauvegardé sur disque (rejeu sans ré-entraînement).
    # The model itself is saved to disk (replay without retraining).
    dossier = os.environ.get("ML_MODEL_DIR", "/opt/project/ml/modeles")
    os.makedirs(dossier, exist_ok=True)
    modele.save_model(f"{dossier}/pred_retard_xgboost.json")
    print(f"[ml] modele enregistre / model saved: {dossier}/pred_retard_xgboost.json")


def main():
    spark = get_spark("train_pred_retard")

    print("[ml] === Modèle 1 : prédiction des retards / Model 1: delay prediction ===")
    pdf = charger_donnees(spark)
    print(f"[ml] donnees chargees / data loaded: {len(pdf)} commandes livrees, "
          f"taux de retard = {100 * pdf[LABEL].mean():.1f} %")

    modele, metriques, imp, X_te, y_te, proba_te = entrainer_modele(pdf)
    print("[ml] metriques / metrics:", metriques)
    print("[ml] top 5 variables / top 5 features:")
    print(imp.head(5).to_string(index=False))

    persister(spark, modele, metriques, imp, X_te, y_te, proba_te, pdf)

    # --- VÉRIFICATION / VERIFICATION (lue à la fin de l'exécution) -----------
    derniere = (spark.table(f"{DST}.ml_pred_retard_metriques")
                .orderBy(F.col("run_ts").desc()).limit(1))
    for r in derniere.collect():
        print(f"[ml] VERIF modele={r['modele']} auc={r['auc']} f1={r['f1']} "
              f"n_test={r['n_test']}")
    spark.stop()
    print("[ml] TERMINE / DONE")


if __name__ == "__main__":
    main()
