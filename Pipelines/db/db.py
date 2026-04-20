from neo4j import GraphDatabase
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
AUTH = ("neo4j", os.getenv("NEO4J_PASSWORD"))

PG_DSN = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", 5432)),
    "dbname": os.getenv("PG_DB", "pfa_bvc"),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", "postgres"),
}


def get_pg_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(**PG_DSN)


def get_driver():
    return GraphDatabase.driver(URI, auth=AUTH)


def verify_connection():
    with get_driver() as driver:
        driver.verify_connectivity()
        print("Connected to Neo4j successfully.")


if __name__ == "__main__":
    verify_connection()
