from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'spark_submit_dag',
    default_args=default_args,
    description='DAG to run spark-submit every 5 minutes',
    schedule_interval=timedelta(minutes=5),
)

# Replace the path with the actual path to your spark-submit script (s1.py)
spark_submit_command = "/usr/local/spark/bin/spark-submit --conf spark.executorEnv.PYTHONPATH=/Users/user_name/Documents/GitHub/code_base/venv/lib/python3.9/site-packages:./venv \
                         --conf spark.yarn.appMasterEnv.PYTHONPATH=/Users/user_name/Documents/GitHub/code_base/venv/lib/python3.9/site-packages:./venv \
                         --conf spark.pyspark.driver.python=/Users/user/Documents/GitHub/code_base/venv/bin/python3 \
                         --conf spark.pyspark.python=/Users/user/Documents/GitHub/code_base/venv/bin/python3 \
                         --jars /Users/user/Documents/GitHub/code_base/postgresql-42.7.1.jar \
                         /Users/user/Documents/GitHub/code_base/s1.py"

run_spark_submit = BashOperator(
    task_id='run_spark_submit',
    bash_command=spark_submit_command,
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()
