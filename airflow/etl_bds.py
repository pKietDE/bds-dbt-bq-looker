from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator

dag_args = {
    'owner': 'admin',
    'email': 'kiet@gmail.com',
    'start_date': datetime(2024, 11, 24),  # Ngày bắt đầu công việc yyyy/mm/dd
    'retries': 2  # Thử lại 2 lần
}

with DAG(
    'etl_bds',
    default_args=dag_args,
    description='Một DAG thực thi hàng tuần',
    schedule_interval='@weekly',
    catchup=False
) as dag:

     
    # Task 2: Crawl data
    bash_task1 = BashOperator(
        task_id='crawl_data',
        bash_command=(
            'cd /home/kiet/Documents/Project/Selenium/ &&'
            'source /usr/local/set_venv/prj_bds.sh && '
            'source .venv/bin/activate && '
            'pip install -r requirements.txt && '
            'python3 main.py'
        )
    )

    # Task 3: Transform Data
    bash_task2 = BashOperator(
        task_id='transform_data',  
        bash_command=(
            'cd /home/kiet/Documents/Project/DBT/ &&'
            'source .venv/bin/activate && '
            'cd bds_dbt && '
            'dbt run'
        )
    )

    # Task phụ thuộc vào task trước
    bash_task1 >> bash_task2  # bash_task2 chỉ chạy khi bash_task1 thành công
