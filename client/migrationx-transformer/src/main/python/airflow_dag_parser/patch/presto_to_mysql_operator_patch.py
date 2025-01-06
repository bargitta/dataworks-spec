from airflow.providers.mysql.hooks.mysql.MySqlHook import MySqlHook
from airflow.providers.mysql.hooks.mysql.MySqlHook import PrestoHook


def execute(self, context):
    self.presto = PrestoHook(presto_conn_id=self.presto_conn_id)
    self.log.info("Extracting data from Presto: %s", self.sql)
    # results = presto.get_records(self.sql)

    self.mysql = MySqlHook(mysql_conn_id=self.mysql_conn_id)

    # self.log.info("Inserting rows into MySQL")
    # mysql.insert_rows(table=self.mysql_table, rows=results)
