import logging

from airflow.utils.log.logging_mixin import LoggingMixin

logger = logging.getLogger(__name__)
OPERATORS = {}
try:
    from airflow.operators.bash import BashOperator

    OPERATORS[BashOperator] = BashOperator
except ImportError as e:
    logger.warning(e)

try:
    from airflow.operators.empty import EmptyOperator

    OPERATORS[EmptyOperator] = EmptyOperator
except ImportError as e:
    logger.warning(e)

try:
    from airflow.operators.dummy import DummyOperator

    OPERATORS[DummyOperator] = DummyOperator
except ImportError as e:
    logger.warning(e)

try:
    from airflow.providers.apache.hive.operators.hive import HiveOperator

    OPERATORS[HiveOperator] = HiveOperator
except ImportError as e:
    logger.warning(e)

try:
    from airflow.operators.http_operator import SimpleHttpOperator

    OPERATORS[SimpleHttpOperator] = SimpleHttpOperator
except ImportError as e:
    logger.warning(e)

try:
    from airflow.operators.presto_to_mysql import PrestoToMySqlTransfer

    OPERATORS[PrestoToMySqlTransfer] = PrestoToMySqlTransfer
except ImportError as e:
    logger.warning(e)

try:
    from airflow.operators.hive_to_mysql import HiveToMySqlTransfer

    OPERATORS[HiveToMySqlTransfer] = HiveToMySqlTransfer
except ImportError as e:
    logger.warning(e)

try:
    from airflow.contrib.operators.spark_sql_operator import SparkSqlOperator

    OPERATORS[SparkSqlOperator] = SparkSqlOperator
except ImportError as e:
    logger.warning(e)

try:
    from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

    OPERATORS[SparkSubmitOperator] = SparkSubmitOperator
except ImportError as e:
    logger.warning(e)

try:
    from airflow.contrib.operators.sqoop_operator import SqoopOperator

    OPERATORS[SqoopOperator] = SqoopOperator
except ImportError as e:
    logger.warning(e)

try:
    from airflow.operators.python import PythonOperator

    OPERATORS[PythonOperator] = PythonOperator
except ImportError as e:
    logger.warning(e)

try:
    from airflow.operators.python import BranchPythonOperator

    OPERATORS[BranchPythonOperator] = BranchPythonOperator
except ImportError as e:
    logger.warning(e)

try:
    from airflow.sensors.external_task_sensor import ExternalTaskSensor

    OPERATORS[ExternalTaskSensor] = ExternalTaskSensor
except ImportError as e:
    logger.warning(e)

try:
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

    OPERATORS[SQLExecuteQueryOperator] = SQLExecuteQueryOperator
except ImportError as e:
    logger.warning(e)

try:
    from airflow.providers.mysql.operators.mysql import MySqlOperator

    OPERATORS[MySqlOperator] = MySqlOperator
except ImportError as e:
    logger.warning(e)

try:
    from airflow.providers.postgres.operators.postgres import PostgresOperator

    OPERATORS[PostgresOperator] = PostgresOperator
except ImportError as e:
    logger.warning(e)

try:
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

    OPERATORS[TriggerDagRunOperator] = TriggerDagRunOperator
except ImportError as e:
    logger.warning(e)

str_content = list()
for clz in OPERATORS:
    str_content.append(f"\t{clz.__name__}\n")
logger.info(f"Supported operators are as follows\n{''.join(str_content)}")


class Supports(LoggingMixin):
    @classmethod
    def is_operator_available(cls, operator_clz):
        res = [clz for clz in OPERATORS if clz.__name__ == operator_clz]
        if len(res) > 0:
            return res[0]
        return None

    @classmethod
    def is_operator_supported(cls, task):
        return cls.find_operator_class(task) != None

    @classmethod
    def find_operator_class(cls, task):
        res = [clz for clz in OPERATORS if clz.__name__ == task.__class__.__name__]
        if len(res) > 0:
            return res[0]
        return None
