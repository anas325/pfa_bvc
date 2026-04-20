import os
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

with DAG(
    dag_id="stock_prices",
    schedule="0 17 * * 1-5",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["stocks", "postgres"],
) as dag:
    DockerOperator(
        task_id="scrape_lematin",
        image="pfa_bvc_pipelines:latest",
        command=[
            "uv", "run", "scrapy", "crawl", "lematin",
            "--settings", "scrapers.stock_settings",
        ],
        working_dir="/pipelines",
        environment={
            "PG_HOST": "postgres",
            "PG_PORT": "5432",
            "PG_DB": "pfa_bvc",
            "PG_USER": "postgres",
            "PG_PASSWORD": os.environ.get("PG_PASSWORD", "postgres"),
        },
        network_mode="bvc_net",
        auto_remove="force",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
    )
