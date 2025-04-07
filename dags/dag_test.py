from datetime import datetime
from time import sleep

from airflow.models import DAG
from airflow.operators.python import PythonOperator

import json
import requests

DEFAULT_DATE = datetime(2025, 1, 1)

args = {
    'owner': 'airflow',
    'start_date': DEFAULT_DATE,
}

def pokemon():
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
    
    
def printPokemons(**pokemons):
    print([pokemon for pokemon in pokemons])
    
dag = DAG(dag_id='test_pokemon', default_args=args)
task = PythonOperator(task_id='task1', python_callable=pokemon(), dag=dag)