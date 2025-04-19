from datetime import timedelta

from airflow.decorators import dag, task
from airflow.models import Param
from airflow.models.param import ParamsDict
from pendulum import today
from typing import List

DEFAULT_MINIO_CONNECTION_ID = "minio-connection"
DEFAULT_MINIO_BUCKET_NAME = "imdb-raw"
DATASETS = {
    "title.basics.tsv.gz": "Títulos",
    "title.akas.tsv.gz": "Títulos Alternativos",
    "title.episode.tsv.gz": "Episódios",
    "title.ratings.tsv.gz": "Avaliações",
    "title.crew.tsv.gz": "Diretores/Escritores",
    "title.principals.tsv.gz": "Elenco",
    "name.basics.tsv.gz": "Dados Pessoais",
}


@task
def select_datasets(**kwargs) -> List[str]:
    params: ParamsDict = kwargs["params"]
    return params["datasets"]


@dag(
    dag_id="imdb",
    dag_display_name="IMDb",
    max_active_runs=1,
    start_date=today(),
    schedule="@daily",
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    params={
        "url": Param(
            default="https://datasets.imdbws.com",
            type="string",
            title="URL",
            description="URL base para download dos datasets",
        ),
        "datasets": Param(
            default=["title.basics.tsv.gz", "title.episode.tsv.gz", "title.ratings.tsv.gz"],
            type="array",
            title="IMDb Datasets",
            description="Selecione um ou mais arquivos da IMDb Non-Commercial Datasets.",
            examples=DATASETS.keys(),
            values_display=DATASETS,
        ), },
    tags=['imdb'],
)
def dag():
    select_datasets()


dag()
