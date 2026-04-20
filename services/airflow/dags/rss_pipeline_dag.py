import os
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

with DAG(
    dag_id="rss_pipeline",
    schedule="0 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["rss", "neo4j"],
) as dag:
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
        },
        network_mode="bvc_net",
        auto_remove="force",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
    )
