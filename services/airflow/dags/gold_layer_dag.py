import os
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

LOG_HOST_PATH = os.environ.get("PIPELINE_LOG_HOST_PATH")

with DAG(
    dag_id="gold_layer",
    schedule="0 2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["gold", "postgres", "analytics"],
) as dag:
    mounts = []
    if LOG_HOST_PATH:
        mounts.append(
            Mount(source=LOG_HOST_PATH, target="/var/log/pfa_bvc", type="bind")
        )

    DockerOperator(
        task_id="run_gold_pipeline",
        image="pfa_bvc_pipelines:latest",
        command="uv run python -m gold.pipeline",
        working_dir="/pipelines",
        environment={
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
