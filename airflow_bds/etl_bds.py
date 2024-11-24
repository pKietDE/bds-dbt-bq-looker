from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from ConfigReader import *

config_reader = ConfigReader()

# Task 1 
FOLDER_SELENIUM = str(config_reader.get_config("PATH", "FOLDER_PROJECT_SELENIUM"))
EXEC_VENV = str(config_reader.get_config("EXEC", "VENV"))
FILE_SET_VENV_PRJ = str(config_reader.get_config("PATH", "FILE_SET_VENV_PRJ"))

# Task 2
FOLDER_DBT = str(config_reader.get_config("PATH", "FOLDER_DBT"))
print(
            f'source $SET_VENV_PRJ_BDS_ARF && '
            f'cd {FOLDER_DBT} && '
            f'source {EXEC_VENV} && '
            'cd bds_dbt && '
            'dbt run '
        )

print(f'source $SET_VENV_PRJ_BDS_ARF && '
            f'cd {FOLDER_SELENIUM} && '
            f'source {FILE_SET_VENV_PRJ} && '
            f'source {EXEC_VENV} && '
            'pip install -r requirements.txt && '
            'python3 main.py ')


dag_args = {
    'owner': 'admin',
    'email': 'phanmaiquockiet2004@gmail.com', 
    'email_on_failure': True,
    'email_on_retry': True,
    'start_date': datetime(2024, 11, 17),
    'retries': 2,
}

with DAG(
    'etl_bds',
    default_args=dag_args,
    description='ETL pipeline for real estate data',
    schedule='@weekly',
    catchup=False,
) as dag:

     
    # Task 1: Crawl data 
    bash_task2 = BashOperator(
        task_id='crawl_data',
        bash_command=(
            f'source /home/kiet/set_venv/prj_bds_arf.sh && '
            f'cd {FOLDER_SELENIUM} && '
            f'source {FILE_SET_VENV_PRJ} && '
            f'source {EXEC_VENV} && '
            'pip install -r requirements.txt && '
            'python3 main.py '
        )
    )

    # Task 2: Transform Data
    bash_task3 = BashOperator(
        task_id='transform_data',  
        bash_command=(
            f'source $SET_VENV_PRJ_BDS_ARF && '
            f'cd {FOLDER_DBT} && '
            f'source {EXEC_VENV} && '
            'cd bds_dbt && '
            'dbt run '
        )
    )

    # Task dependencies
    bash_task2 >> bash_task3