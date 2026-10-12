# 04 — Guide d'installation pas à pas (Phase 0)

**Objectif** : faire démarrer la plateforme complète sur une machine neuve, en comprenant
**pourquoi** chaque action existe et **comment** la vérifier. Chaque étape suit le même
format : *Pourquoi → Comment → Vérification*.

**Prérequis** (à vérifier avant tout) :
| Élément | Minimum | Commande de vérification |
|---|---|---|
| Docker Desktop | installé **et démarré** | `docker ps` répond sans erreur |
| RAM allouée à Docker | 8 Go | Docker Desktop → Settings → Resources |
| Disque libre | 25 Go | `df -h` (ou Explorateur → C:) |
| Git | installé | `git --version` |

---

## Étape 1 — Récupérer et préparer le code

**Pourquoi** : le code est versionné sur GitHub ; la première chose qu'un nouvel arrivant
fait est un clone. Les données ne sont PAS dans Git (trop volumineuses, et ce sont des
données tierces) — elles sont posées à côté.

**Comment** :
```bash
git clone <url-du-depot> CIDSEC_CONST
cd CIDSEC_CONST
# Les données locales sont fournies hors Git : les archives doivent être DÉCOMPRESSÉES
cd Données && python -c "import zipfile,os,shutil
for z,d in [('E-Commerce/archive.zip','E-Commerce'),
            ('Investissment_Cymball/archive (1).zip','Investissment_Cymball'),
            ('UCI_power/archive.zip','UCI_power')]:
    with zipfile.ZipFile(z) as zf:
        for i in zf.infolist():
            t=os.path.join(d,os.path.basename(i.filename))
            if not os.path.exists(t):
                with zf.open(i) as s, open(t,'wb') as o: shutil.copyfileobj(s,o)
                print('extrait:',t)"
```

**Vérification** : `ls Données/E-Commerce` affiche **9 fichiers .csv** ;
`ls Données/UCI_power` affiche `household_power_consumption.txt` (≈ 133 Mo) ;
`ls Données/Investissment_Cymball` affiche le CSV Cymbal (≈ 335 Mo).

## Étape 2 — Créer le fichier .env

**Pourquoi** : `.env` centralise les réglages ajustables ; `.env.example` documente
les valeurs par défaut. Le fichier `.env` réel n'est pas versionné (il peut contenir
des secrets).

**Comment** : `cp docker/.env.example docker/.env` (puis l'éditer si besoin).

**Vérification** : `cat docker/.env` affiche `COMPOSE_PROJECT_NAME=cidsec`.

## Étape 3 — Démarrer la plateforme

**Pourquoi** : une seule commande lance les 11 services (PostgreSQL, HDFS ×2 + init,
Hive ×3, Spark ×2, Airflow ×3, Adminer) dans le bon ordre (dépendances + healthchecks).

**Comment** :
```bash
cd docker
docker compose up -d
# Premier démarrage : téléchargement des images (~15 min selon le réseau)
```

**Vérification** :
```bash
docker compose ps          # tous les services : running / exited(0) pour les "init"
```
Les services `hdfs-init`, `hive-init`, `airflow-init` terminent (`exited (0)`) : c'est
normal, ce sont des initialisations "une fois". Les autres doivent être `running`.

Puis tester les UIs dans le navigateur :
| URL | Attendu |
|---|---|
| http://localhost:8180 | Connexion Airflow (admin / admin) — *port 8180 : le 8080 est occupé par un logiciel local EDB* |
| http://localhost:8081 | Spark Master : "Workers: 1 Alive" |
| http://localhost:9870 | NameNode HDFS : "Live datanodes: 1" |
| http://localhost:8083 | Adminer (choisir PostgreSQL, serveur `postgres`) |

## Étape 4 — Vérifier le Data Lake HDFS

**Pourquoi** : l'arborescence `/data/{landing,bronze,silver,gold,...}` doit exister et
les sources doivent y être archivées (copie immuable de traçabilité).

**Comment** :
```bash
docker exec cidsec-namenode hdfs dfs -ls -R /data
docker exec cidsec-namenode hdfs dfs -du -h /data/landing
```

**Vérification** : `/data/landing/olist` contient 9 fichiers, `/data/landing/cymbal`
1 fichier, `/data/landing/iot` 2 fichiers. Si un fichier manque : relancer
`docker compose run --rm hdfs-init` (le script est idempotent, il ne recopie pas
ce qui existe déjà).

## Étape 5 — Vérifier PostgreSQL (3 bases)

**Pourquoi** : les bases `metastore` (Hive), `airflow_db` (Airflow) et `gold_db`
(service Power BI) doivent exister avec leurs utilisateurs.

**Comment** :
```bash
docker exec cidsec-postgres psql -U postgres -c "\l"
docker exec cidsec-postgres psql -U postgres -c "\du"
```

**Vérification** : la liste contient `metastore`, `airflow_db`, `gold_db` ; les rôles
`hive`, `airflow`, `spark`, `powerbi` existent. Si ce n'est pas le cas, le volume
PostgreSQL était déjà initialisé : `docker compose down -v` **puis** `docker compose up -d`
(⚠️ `down -v` supprime les données des volumes — uniquement pour une réinstallation).

## Étape 6 — Vérifier Hive (catalogue)

**Pourquoi** : Hive donne des noms de tables SQL aux fichiers Parquet de HDFS. Sans
lui, aucun `SELECT` n'est possible.

**Comment** :
```bash
docker exec cidsec-hive-server beeline -u jdbc:hive2://localhost:10000 -e "SHOW DATABASES;"
```

**Vérification** : `bronze_db`, `silver_db`, `gold_db` apparaissent (créés par le
premier job Spark) ou `default` seulement (plateforme neuve, normal avant le premier job).
Créer les vues métier (une fois, puis à chaque évolution) :
```bash
docker exec -i cidsec-hive-server beeline -u jdbc:hive2://localhost:10000 \
    -f /opt/project/sql/hive/01_views_silver.hql
```

## Étape 7 — Premier job Spark à la main (comprendre avant d'automatiser)

**Pourquoi** : Airflow n'est qu'un automate ; **comprendre la commande manuelle** est
indispensable pour diagnostiquer. On teste un fichier simple (clients Olist).

**Comment** :
```bash
docker exec cidsec-airflow-sched spark-submit --master spark://spark-master:7077 \
    /opt/project/spark_jobs/bronze_ingest.py \
    --source olist --table olist_customers_dataset \
    --input /landing/E-Commerce/olist_customers_dataset.csv --sep ,
```

**Vérification** :
```bash
docker exec cidsec-hive-server beeline -u jdbc:hive2://localhost:10000 \
    -e "SELECT count(*) FROM bronze_db.olist_customers_dataset;"
# Attendu : 99441
```

Puis enchaîner manuellement la chaîne complète (recommandé une fois pour comprendre) :
```bash
docker exec cidsec-airflow-sched spark-submit --master spark://spark-master:7077 /opt/project/spark_jobs/silver_olist.py
docker exec cidsec-airflow-sched spark-submit --master spark://spark-master:7077 /opt/project/spark_jobs/gold_build.py
docker exec cidsec-postgres psql -U spark -d gold_db -c "SELECT * FROM gold.kpi_ca_quotidien ORDER BY jour DESC LIMIT 5;"
```

## Étape 8 — Automatiser avec Airflow

**Pourquoi** : une fois la commande manuelle comprise, Airflow l'ordonnance, la
surveille, la rejoue et alerte — ce qu'aucun script manuel ne fait.

**Comment** : dans http://localhost:8180 → activer les DAGs `dag_01_ingest_bronze`,
`dag_04_silver_transform`, `dag_05_gold_build` (bouton à gauche) → cliquer
"Trigger DAG" sur `dag_01_ingest_bronze` pour un premier essai immédiat.

**Vérification** : chaque DAG passe au vert ; dans PostgreSQL :
```bash
docker exec cidsec-postgres psql -U spark -d gold_db -c "\dt gold.*"
# Attendu : dim_*, fact_*, kpi_ca_quotidien
```

## Étape 9 — Brancher Power BI

**Pourquoi** : Power BI est la porte d'entrée des utilisateurs métier ; il lit
**uniquement** PostgreSQL (jamais HDFS/Hive directement).

**Comment** : Power BI Desktop → Obtenir des données → PostgreSQL →
serveur `localhost`, base `gold_db` → utilisateur `powerbi` / `powerbi` →
modèle de données : `fact_commande` + `dim_date`, `dim_client`, `dim_produit`…

**Vérification** : `SELECT count(*) FROM gold.fact_commande` dans Power BI (nouvelle
mesure) doit donner **99 441**.

---

## Dépannage courant / common troubleshooting

| Symptôme | Cause probable | Solution |
|---|---|---|
| `docker ps` : "cannot connect to the Docker daemon" | Docker Desktop non démarré | Lancer Docker Desktop, attendre l'icône verte |
| `hive-init` en échec | schéma déjà créé | normal au 2e démarrage (`|| true`) ; vérifier avec `beeline ... SHOW DATABASES` |
| Job Spark : "Connection refused" sur 9000 | NameNode pas encore prêt | attendre 30 s, relancer le job |
| Job Spark : `OutOfMemoryError` | RAM Docker insuffisante | réduire `spark.executor.memory` dans `docker/spark/spark-defaults.conf` |
| Airflow : DAG absent | erreur de syntaxe dans le fichier | `docker logs cidsec-airflow-sched` affiche l'erreur exacte |
| Power BI : "tables absentes" | export Gold pas encore lancé | lancer `dag_05_gold_build` (étape 8) |
| Chemin `Données` non monté (accent) | Docker Desktop mal paramétré | renommer le dossier en `Donnees` **et** adapter `docker-compose.yml` (2 montages) |

## Arrêt et remise à zéro

```bash
cd docker
docker compose stop          # arrêter sans rien perdre (reprendre plus tard)
docker compose down          # arrêter + supprimer les conteneurs (VOLUMES CONSERVES)
docker compose down -v       # TOUT supprimer (volumes = donnees) — reinstallation
```
