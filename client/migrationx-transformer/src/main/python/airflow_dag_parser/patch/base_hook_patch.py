import os
from typing import Iterable

from airflow.models import Connection

from airflow_dag_parser.connections import CsvConnections


@classmethod
def get_connections(cls, conn_id):  # type: (str) -> Iterable[Connection]
    csv = os.environ.get("connections_csv")

    if not csv and not cls.connections:
        return None

    if not cls.connections:
        cls.connections = CsvConnections(csv)
        cls.connections.load()

    return cls.connections.get_connection(conn_id)
