# dags_init/ — dossier VOLONTAIREMENT vide au démarrage
#              Intentionally EMPTY directory at startup

**Pourquoi ce dossier existe-t-il ? / Why does this folder exist?**

`docker-compose.yml` monte `./dags_init` (lecture seule) dans `airflow-init`, la tâche
qui initialise la base Airflow **avant** que les DAGs réels ne soient chargés :

- Au tout premier démarrage, la base Airflow est vide : laisser `airflow db migrate`
  analyser les DAGs du projet ralentirait l'init et masquerait les vraies erreurs de
  parsing derrière des erreurs de base.
- Une fois l'init terminée, `airflow-webserver` et `airflow-scheduler` montent le vrai
  dossier `../dags` : les DAGs apparaissent alors dans l'interface.

*At the very first startup the Airflow DB is empty: letting `airflow db migrate` parse
the project DAGs would slow init down and hide real parsing errors behind DB errors.
Once init is done, `airflow-webserver` and `airflow-scheduler` mount the real `../dags`
folder: DAGs then appear in the UI.*

**Que mettre dedans ? / What to put in it?**
Rien. Ne supprimez pas le dossier : Docker Desktop sur Windows crée alors un dossier
anonyme et la configuration perd sa traçabilité.
*Nothing. Do not delete the folder: Docker Desktop on Windows then creates an anonymous
folder and the configuration loses its traceability.*
