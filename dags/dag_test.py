from airflow.decorators import dag, task
import pendulum
import json
import requests

@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["pokemon"],

)

def dag_test():

    @task(task_id="download pokemon list")
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
    
    pokemons = pokemon()
    
    @task(task_id="print pokemon list")
    def printPokemons(**pokemons):
        print([pokemon for pokemon in pokemons])
        
    printPokemons = printPokemons
    
    printPokemons >> printPokemons