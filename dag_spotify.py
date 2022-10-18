from multiprocessing import context
from airflow import DAG
from datetime import date, datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd

# Variables globales
# A MODIFIER
SPOTIFY_CLIENT_ID=''
SPOTIFY_CLIENT_SECRET=''
path_home_user=''
# A MODIFIER
# NE PAS MODIFIER
path_appli='/spotify_playlist'
path_home=path_home_user+path_appli
path_local_csv=path_home+'/CSV'
path_hdfs_archive_csv=path_appli+'/CSV'
path_hdfs_db=path_appli+'/DB'
path_hdfs_db_tmp=path_hdfs_db+'/tmp'
date_trt=date.today().strftime("%Y-%m-%d")
# NE PAS MODIFIER

# Créer un DAG se lançant tous les jours démarrant le 14/10 à 1H avec une relance possible
# en cas d'erreur au bout de 30 minutes
args = {
  'owner': 'Simplon Team',
  'depends_on_past': False,
  'start_date': datetime(2022, 10, 14).replace(hour=1),
  'email': ['toto.tata@gmail.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'max_active_runs': 1,
  'retry_delay': timedelta(minutes=30)
}

dag_id = 'dag_spotify'
my_dag = DAG(
  dag_id,
  default_args=args,
  schedule_interval=timedelta(days=1)
)

# Ajouter un dummy operator au DAG et le nommer avec le paramètre name
def dummyOperator(name):
  return EmptyOperator(
  task_id=f'{name}',
  dag=my_dag)

start = dummyOperator("start")
split1 = dummyOperator("split1")
split2 = dummyOperator("split2")
split3 = dummyOperator("split3")
split4 = dummyOperator("split4")
join2 = dummyOperator("join2")
join3 = dummyOperator("join3")
join4 = dummyOperator("join4")

# Ajouter une tâche utilisant le bash operator pour les fonctions get
def getBash(name):
  return BashOperator(
  task_id=f'{name}_exec',
  bash_command=f"""
  export SPOTIFY_CLIENT_ID={SPOTIFY_CLIENT_ID};
  export SPOTIFY_CLIENT_SECRET={SPOTIFY_CLIENT_SECRET};
  cd {path_home};
  python3 ./get_{name}.py
  """,
  dag=my_dag)

tracks_exec = getBash("tracks")
artists_exec = getBash("artists")

# Ajouter une tâche utilisant le bash operator pour archiver les *.csv
def archiveBash(name):
  return BashOperator(
  task_id=f'{name}_archive',
  bash_command=f"""
  hdfs dfs -mkdir -p {path_hdfs_archive_csv}/{name}/{date_trt};
  test -e {path_local_csv}/{name}_{date_trt}.csv;
  if (( $? == 0 )); then \
    hdfs dfs -rm -f {path_hdfs_archive_csv}/{name}/{date_trt}/{name}.csv; \
    hdfs dfs -put {path_local_csv}/{name}_{date_trt}.csv \
      {path_hdfs_archive_csv}/{name}/{date_trt}/{name}.csv; \
  fi;
  """,
  dag=my_dag)

tracks_archive = archiveBash("tracks")
artists_archive = archiveBash("artists")
inout_archive = archiveBash("inout")

# Ajouter une tâche utilisant le python opérateur pour gérer les IN OUT
def in_out(previous_tracks, current_tracks):
  previous_tracks = previous_tracks.drop('track_id', axis=1).drop_duplicates()
  current_tracks = current_tracks.drop('track_id', axis=1).drop_duplicates()
  outer = previous_tracks.merge(current_tracks, how='outer', indicator=True)
  removed_artist = outer[(outer._merge=='left_only')].drop('_merge', axis=1)
  removed_artist["status"] = "out"
  new_artist = outer[(outer._merge=='right_only')].drop('_merge', axis=1)
  new_artist["status"] = "in"
  return pd.concat([removed_artist, new_artist])

def gestion_inout(**context):
  pd_prev=pd.read_csv(path_local_csv+"/tracks_" \
    + str(context["yesterday_ds"]) + ".csv")
  pd_curr=pd.read_csv(path_local_csv+"/tracks_" \
    + str(context["execution_date"])[:10] + ".csv")
  pdres=in_out(pd_prev,pd_curr)
  pdres.to_csv(path_local_csv+"/inout_" \
    + str(context["execution_date"])[:10] + ".csv", index=False)

inout_exec = PythonOperator(
  task_id='inout_exec',
  python_callable=gestion_inout,
  dag=my_dag
)

# Ajouter une tâche utilisant le bash operator pour gérer la partition de la table hive
def hiveBash(name):
  return BashOperator(
  task_id=f'{name}_hive',
  bash_command=f"""
  hdfs dfs -mkdir -p {path_hdfs_db_tmp}/{name};
  hdfs dfs -mkdir -p {path_hdfs_db}/{name};
  hdfs dfs -test -e {path_hdfs_db}/{name}/date_jour={date_trt};
  if (( $? != 0 )); then \
    hive -e "alter table {name} add partition(date_jour='{date_trt}')"; \
  fi;
  test -e {path_local_csv}/{name}_{date_trt}.csv;
  if (( $? == 0 )); then \
    hdfs dfs -rm -f {path_hdfs_db}/{name}/date_jour={date_trt}/000000_0; \
    hive -e "LOAD DATA LOCAL INPATH '{path_local_csv}/{name}_{date_trt}.csv' \
      overwrite INTO TABLE {name}_tmp; \
      insert into table {name} partition(date_jour='{date_trt}') select * from {name}_tmp; \
      truncate table {name}_tmp;"; \
  fi;
  """,
  dag=my_dag)

tracks_hive = hiveBash("tracks")
artists_hive = hiveBash("artists")
inout_hive = hiveBash("inout")

# Ajouter une tâche utilisant le bash operator pour supprimer les fichiers de plus de 2 jours
# On garde les 2 derniers
def cleanBash(name):
  return BashOperator(
  task_id=f'clean_local_{name}',
  bash_command=f"find {path_local_csv} -name {name}*.csv -ctime +1 -exec rm " + "{}  \;",
  dag=my_dag)

clean_local_tracks = cleanBash("tracks")
clean_local_artists = cleanBash("artists")
clean_local_inout = cleanBash("inout")

# Workflow
start >> tracks_exec >> split1 >> [split4,artists_exec,inout_exec]

split4 >> [tracks_archive,tracks_hive] >> join4 >> clean_local_tracks

artists_exec >> split3 >> [artists_archive,artists_hive] >> join3 >> clean_local_artists

inout_exec >> split2 >> [inout_archive,inout_hive] >> join2 >> clean_local_inout
