import os
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Host-side path to the Services/logs directory. Must be absolute on the Docker
# host (Linux: /.../Services/logs; Docker Desktop / Windows: C:/.../Services/logs).
# Set PIPELINE_LOG_HOST_PATH in Pipelines/.env.
LOG_HOST_PATH = os.environ.get("PIPELINE_LOG_HOST_PATH")

with DAG(
    dag_id="stock_prices",
    schedule="0 17 * * 1-5",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["stocks", "postgres"],
) as dag:
    mounts = []
    if LOG_HOST_PATH:
        mounts.append(
            Mount(source=LOG_HOST_PATH, target="/var/log/pfa_bvc", type="bind")
        )

    DockerOperator(
        task_id="scrape_lematin",
        image="pfa_bvc_pipelines:latest",
        command=["uv", "run", "scrapy", "crawl", "lematin"],
        working_dir="/pipelines",
        environment={
            "SCRAPY_SETTINGS_MODULE": "scrapers.stock_settings",
            "PYTHONPATH": "/pipelines",
            "PG_HOST": "postgres",
            "PG_PORT": "5432",
            "PG_DB": "pfa_bvc",
            "PG_USER": "postgres",
            "PG_PASSWORD": os.environ.get("PG_PASSWORD", "postgres"),
            "PIPELINE_LOG_DB": "/var/log/pfa_bvc/pipeline_logs.db",
        },
        mounts=mounts,
        network_mode="bvc_net",
        auto_remove="force",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
    )
