from datetime import timedelta

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

args = {'owner': 'airflow', 'retries': 3, 'start_date': days_ago(2)}

dag = DAG(
    dag_id='test_example_bash_operator',
    default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
)

cmd = 'ls -l'
run_this_last = DummyOperator(task_id='run_this_last', dag=dag)

run_this = BashOperator(task_id='run_after_loop', bash_command='echo 1', dag=dag)
run_this.set_downstream(run_this_last)

for i in range(3):
    task = BashOperator(
        task_id='runme_' + str(i), bash_command='echo "{{ task_instance_key_str }}" && sleep 1', dag=dag
    )
    task.set_downstream(run_this)

task = BashOperator(
    task_id='also_run_this', bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"', dag=dag
)
task.set_downstream(run_this_last)

if __name__ == "__main__":
    dag.cli()