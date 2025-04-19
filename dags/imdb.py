from datetime import timedelta

from airflow.decorators import dag
from airflow.models import Param
from pendulum import datetime, today

DEFAULT_MINIO_CONNECTION_ID = "minio-connection"
DEFAULT_MINIO_BUCKET_NAME = "imdb-raw"


@dag(
    max_active_runs=1,
    start_date=today(),
    schedule="@daily",
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    params={
        "arquivos_imdb": Param(
            default=["title.basics.tsv.gz"],
            type="array",
            title="Arquivos",
            description="Selecione um ou mais arquivos da IMDb Non-Commercial Datasets.",
            examples=[
                "name.basics.tsv.gz",
                "title.akas.tsv.gz",
                "title.basics.tsv.gz",
                "title.crew.tsv.gz",
                "title.episode.tsv.gz",
                "title.principals.tsv.gz",
                "title.ratings.tsv.gz",
            ],
            values_display={
                "name.basics.tsv.gz": "Name basics",
                "title.akas.tsv.gz": "Títulos Alternativos",
                "title.basics.tsv.gz": "Título Principal",
                "title.principals.tsv.gz": "Elenco",
                "title.crew.tsv.gz": "Diretores/Escritores",
                "title.episode.tsv.gz": "Episódios",
                "title.ratings.tsv.gz": "Avaliações do Título",
            },
        ),
    },
    tags=['imdb'],
)
def dag():
    return []


dag()
