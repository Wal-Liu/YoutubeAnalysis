from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import subprocess

default_arg = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 26),
    'retries': 1,
}

with DAG(
    'youtube_etl_pipeline',
    default_args=default_arg,
    description='Pipeline ETL YouTube: ingest -> clean -> checkcleaned -> transform',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    task_ingest = BashOperator(
        task_id='ingest',
        bash_command='docker exec spark-submit bash /opt/spark-apps/youtube-script/ingest.sh'
    )

    task_clean = BashOperator(
        task_id='clean',
        bash_command='docker exec spark-submit bash /opt/spark-apps/youtube-script/clean.sh'
    )

    task_checkcleaned = BashOperator(
        task_id='checkcleaned',
        bash_command='docker exec spark-submit bash /opt/spark-apps/youtube-script/checkcleaned.sh'
    )

    task_transform = BashOperator(
        task_id='transform',
        bash_command='docker exec spark-submit bash /opt/spark-apps/youtube-script/transform.sh'
    )

    task_ingest >> task_clean >> task_checkcleaned >> task_transform
