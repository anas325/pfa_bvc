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
    dag_id="rss_pipeline",
    schedule="0 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["rss", "neo4j"],
) as dag:
    mounts = []
    if LOG_HOST_PATH:
        mounts.append(
            Mount(source=LOG_HOST_PATH, target="/var/log/pfa_bvc", type="bind")
        )

    DockerOperator(
        task_id="run_rss_pipeline",
        image="pfa_bvc_pipelines:latest",
        command="uv run python -m rss.pipeline",
        working_dir="/pipelines",
        environment={
            "PYTHONPATH": "/pipelines",
            "NEO4J_URI": "bolt://neo4j:7687",
            "NEO4J_PASSWORD": os.environ.get("NEO4J_PASSWORD", ""),
            "OPENROUTER_API_KEY": os.environ.get("OPENROUTER_API_KEY", ""),
            "PIPELINE_LOG_DB": "/var/log/pfa_bvc/pipeline_logs.db",
        },
        mounts=mounts,
        network_mode="bvc_net",
        auto_remove="force",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
    )
