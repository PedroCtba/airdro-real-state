from pathlib import Path
from airflow import DAG
from datetime import datetime, timedelta
from realStateClassesAndMethods import KedroOperator

# Kedro settings required to run your pipeline
env = "airflow"
pipeline_name = "__default__"
project_path = Path.cwd()
package_name = "realstatekedro"

# Default settings applied to all tasks
default_args = {
    'owner': 'Pedro Miyasaki',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG(
    "kedroAll",
    start_date=datetime(2022, 5, 28),
    max_active_runs=1,
    schedule_interval=None,
    default_args=default_args,
    catchup=False
) as dag:

    tasks = {}

    tasks["cleanquintoandar"] = KedroOperator(
        task_id="cleanquintoandar",
        package_name=package_name,
        pipeline_name=pipeline_name,
        node_name="cleanQuintoAndar",
        project_path=project_path,
        env=env,
    )



    tasks["cleanquintoandar"]