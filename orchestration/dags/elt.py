from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

# dbt config
DBT_DIR = '/transform/dbt_cryto'
DBT_TARGET_PROFILE = "dev"

default_args = {
	"profiles_dir": "profile",
	"dir": DBT_DIR,
	"target": DBT_TARGET_PROFILE,
	"start_date": datetime(2024, 3, 20)
	}

with DAG(
	dag_id="elt_crypto",
	default_args=default_args,
	schedule_interval='* * * * 1',
	catchup=False
	) as dag:

	@task(task_id="start_task")
	def start_task():
		return "start DAG"

	@task(task_id="extract_and_load")
	def extract_and_load(day=7, start_date="12-03-2024"):
		from extract import get_records
		from load import load_data
		records = get_records(day, start_date)
		load_data(records)

	transform = BashOperator(
		task_id='transform',
		bash_command=f'''
			cd {DBT_DIR} &&
			dbt build --threads 2
		'''
		)

	@task(task_id="end_task")
	def end_task():
		return "end DAG"

	start_task >> extract_and_load >> transform >> end_task

