from airflow.operators.python import PythonOperator
from airflow.models import DAG
from datetime import datetime, timedelta

import time
import requests
from pprint import pprint

seven_days_ago = datetime.combine(
        datetime.today() - timedelta(7), datetime.min.time())

args = {
    'owner': 'airflow',
    'start_date': seven_days_ago,
}

dag = DAG(
    dag_id='example_python_operator', default_args=args,
    schedule_interval=None)



def print_context(ds, **kwargs):
    allPokemons = []
    limit = 20
    offset = 0
    total = 1025
    while offset <=total:
        api_url = ("https://pokeapi.co/api/v2/pokemon-species/?limit=%s&offset=%s" % (limit, 20 * offset))
        response_str = requests.get(api_url)
        response_json = requests.get(api_url).json()
        pokemons = []
        [pokemons.append(pokemon['name']) for pokemon in response_json['results']]
        print([pokemon for pokemon in pokemons])
        allPokemons.extend(pokemons)            
        offset = offset + 1
    return allPokemons

run_this = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=print_context,
    dag=dag)