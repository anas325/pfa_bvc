from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()

URI = "bolt://localhost:7687"
AUTH = ("neo4j", os.getenv("NEO4J_PASSWORD"))


def get_driver():
    return GraphDatabase.driver(URI, auth=AUTH)


def verify_connection():
    with get_driver() as driver:
        driver.verify_connectivity()
        print("Connected to Neo4j successfully.")


if __name__ == "__main__":
    verify_connection()
