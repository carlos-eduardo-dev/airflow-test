import os
import tempfile
from datetime import timedelta
from typing import List, Dict
from urllib.parse import urljoin

import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Param
from airflow.models.param import ParamsDict
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pendulum import today

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


@task(task_id="generate_urls", task_display_name="Generate URLs to download")
def datasets_info(**kwargs) -> List[Dict[str, str]]:
    params: ParamsDict = kwargs["params"]
    url = params["url"]
    datasets = params["datasets"]
    return [{"name": dataset, "url": urljoin(url.rstrip("/") + "/", dataset)} for dataset in datasets]


@task(task_id="download_dataset", task_display_name="Download dataset", retries=3, retry_delay=timedelta(minutes=2))
def download_dataset(dataset: str) -> str:
    dataset_name = dataset["name"]
    dataset_url = dataset["url"]
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=dataset_name) as temp_file:
            print(f"Baixando {dataset_name} para {temp_file.name}")
            request = requests.get(dataset_url, stream=True, timeout=120)
            request.raise_for_status()
            for chunk in request.iter_content(chunk_size=1024 * 1024):
                temp_file.write(chunk)

        print(f"Download de {dataset_name} completo: {temp_file.name}")
        return temp_file.name
    except requests.exceptions.RequestException as e:
        print(f"Erro no download de {dataset_name}: {e}")
        raise AirflowFailException(f"Falha ao baixar {dataset_name} de {dataset_url}")

@task(task_id="create_bucket", task_display_name="Create bucket if not exists")
def create_bucket():
    s3_hook = S3Hook(aws_conn_id=DEFAULT_MINIO_CONNECTION_ID)
    s3_bucket = DEFAULT_MINIO_BUCKET_NAME
    if not s3_hook.check_for_bucket(bucket_name=s3_bucket):
        s3_hook.create_bucket(bucket_name=s3_bucket)


@task(task_id="delete_temp_file", task_display_name="Delete temporary file")
def delete_temp_file(path):
    if os.path.exists(path):
        os.remove(path)
        print(f"Arquivo {path} removido")
    else:
        print(f"Arquivo {path} não encontrado")

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
            examples=list(DATASETS.keys()),
            values_display=DATASETS,
        ), },
    tags=['imdb'],
)
def dag():
    datasets = datasets_info()
    downloaded_datasets = download_dataset.expand(dataset=datasets)
    datasets_info() >> [downloaded_datasets]
dag()
