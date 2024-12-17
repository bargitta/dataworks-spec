import json
import logging
import os
import re
import inspect

from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.log.logging_mixin import LoggingMixin

from airflow_dag_parser.common.configs import Config
from airflow_dag_parser.common.datax import datax_json_to_di_code
from airflow_dag_parser.common.supports import Supports
from airflow_dag_parser.converter.task_converter import BaseTaskConverter
from airflow_dag_parser.models.dw_spec import SpecScript, SpecScriptRuntime, SpecNodeIO, SpecBranch, SpecVariable
from airflow_dag_parser.patch.base_hook_patch import get_connections

BaseHook._get_connections_from_db = get_connections
BaseHook.connections = None

logger = logging.getLogger(__name__)


def get_converter(task):
    converter = PythonOperatorTaskConverter(task)
    clz = Supports.find_operator_class(task)
    if clz:
        for sub_clz in BaseTaskConverter.__subclasses__():
            logger.debug("clz: %s, sub_clz: %s", clz.__name__, sub_clz.__name__)
            sub_clzes = {sub_clz}
            sub_clzes.update(sub_clz.__subclasses__())
            for single_sc in sub_clzes:
                op_clz = single_sc.get_operator_class()
                if op_clz and clz.__name__ == op_clz.__name__:
                    converter = single_sc(task)
                    return converter

    logger.warning(f"\tNo converter implementation found for operator type: {task.__class__.__name__}, "
                   f"taking default implementation: {converter.__class__.__name__}, "
                   f"task name: {task.task_id}, with file path: {task.dag.full_filepath}")
    return converter


class BashOperatorTaskConverter(BaseTaskConverter, LoggingMixin):
    parameters = None

    def __init__(self, task: BashOperator):
        super(BashOperatorTaskConverter, self).__init__(task)
        self.task = task
        self.node_type = Config.get_node_type_or_default(self.task.__class__.__name__)

    @classmethod
    def get_operator_class(cls):
        if Supports.is_operator_available("BashOperator"):
            return BashOperator
        return None

    def get_node_code(self):
        code = self.task.bash_command
        self.adaptive_conversion = bool(Config.settings.get('bashoperator.content.adaptive.conversion', False))
        if self.adaptive_conversion and (self._is_datax_command(code) or self._is_sqoop_import_or_export_command(code)):
            # Not converting code lines to DI node, just create this node as a placeholder
            # code = self._process_datax_command(code)
            return self._convert_as_di_offline_sync_node(code)
        # for hive -f / beeline -f to get file content
        if self._is_hive_run_sql_file_command(code):
            code = self._process_hive_run_sql_file_command(code)
        runtime = SpecScriptRuntime()
        runtime.engine = 'General'
        runtime.command = Config.get_node_type_or_default(self.task.__class__.__name__)
        script = SpecScript()
        script.language = 'shell-script'
        script.path = self.node.name
        script.content = code
        script.runtime = runtime
        script.parameters = self.get_node_parameters()
        return script

    def get_node_parameters(self):
        if self.parameters:
            return self.parameters
        return super(BashOperatorTaskConverter, self).get_node_parameters()

    def _process_hive_run_sql_file_command(self, code):
        tokens = code.split(" ")
        for i in range(len(tokens)):
            if tokens[i] == '-f':
                if os.path.exists(tokens[i + 1]) and len(tokens) > i + 1:
                    tokens[i + 1] = '"' + self._read_file(tokens[i + 1]) + '"'
                    tokens[i] = '-e'
        return ' '.join(tokens)

    def _process_datax_command(self, code):
        tokens = code.split(' ')
        json_file = tokens[-1]
        if os.path.exists(json_file):
            self.node_type = 'DI'
            params = []
            for i in range(len(tokens)):
                if tokens[i].startswith('-D'):
                    params.append(tokens[i][2:])
            if params:
                self.parameters = ' '.join(params)
            json_str = self._read_file(json_file)
            json_str = re.sub(':\\s+(\\$\\{\\w+\\})', r': "\1"', json_str)
            js = None
            try:
                js = json.loads(json_str)
            except:
                js = eval(json_str)
            finally:
                pass
            di_js = datax_json_to_di_code(js)
            return json.dumps(di_js)

        return code

    def _is_hive_run_sql_file_command(self, code):
        if not code:
            return False

        return (code.startswith("hive ") or code.startswith("beeline ")) and " -f " in code

    def _is_datax_command(self, code):
        if not code:
            return False

        tokens = code.split(' ')
        if len(tokens) == 0:
            return False

        if code.startswith('python '):
            if len(tokens) > 1 and tokens[1].endswith("datax.py"):
                return True
        else:
            if tokens[0].endswith('datax.py'):
                return True
        return False

    def get_node_type(self):
        return self.node_type

    def _read_file(self, file_path):
        if not os.path.exists(file_path):
            return file_path

        return open(file_path).read()

    def _is_sqoop_import_or_export_command(self, code):
        if not code:
            return False
        # Ref: https://sqoop.apache.org/docs/1.4.7/SqoopUserGuide.html#_sqoop_tools
        return 'sqoop ' in code and (' import' in code or ' export' in code)

    def _convert_as_di_offline_sync_node(self, code):
        self.node_type = None
        script = SpecScript()
        script.path = self.node.name
        script.runtime = SpecScriptRuntime(command='DI')
        parameters = self.get_node_parameters()
        has_bizdate_param = False
        for sv in parameters:
            if sv.name == 'bizdate':
                has_bizdate_param = True
                break
        if not has_bizdate_param:
            parameters.append(SpecVariable(name='bizdate', scope='NodeParameter', type='System', value='$bizdate'))
        script.parameters = parameters
        return script


class PythonOperatorTaskConverter(BaseTaskConverter, LoggingMixin):
    @classmethod
    def get_operator_class(cls):
        if Supports.is_operator_available("PythonOperator"):
            from airflow.operators.python import PythonOperator
            return PythonOperator
        return None

    def __init__(self, task):
        super(PythonOperatorTaskConverter, self).__init__(task)
        pass

    def get_node_content(self):
        if "PythonOperator" == self.task.__class__.__name__:
            try:
                return inspect.getsource(self.task.python_callable)
            except Exception as e:
                self.log.error(f"Failed getting source for task {self.task.task_id}, exception is: {e}")
                return "pass"


class BranchPythonOperatorTaskConverter(PythonOperatorTaskConverter, LoggingMixin):
    @classmethod
    def get_operator_class(cls):
        if Supports.is_operator_available("BranchPythonOperator"):
            from airflow.operators.python import BranchPythonOperator
            return BranchPythonOperator
        return None

    def __init__(self, task):
        super(BranchPythonOperatorTaskConverter, self).__init__(task)
        pass

    def get_branch_list(self):
        branches = list()
        for downstream_id in self.task.downstream_task_ids:
            output_data_id = '.'.join([self.task.dag.dag_id, self.task.task_id, downstream_id])
            node_io = SpecNodeIO()
            node_io.data = output_data_id
            node_io.artifactType = 'NodeOutput'
            branches.append(SpecBranch(when=downstream_id, desc=downstream_id, output=node_io))
        return branches

    def get_node_content(self):
        contents = list()
        for downstream_id in self.task.downstream_task_ids:
            contents.append({
                "condition": downstream_id,
                "nodeoutput": '.'.join([self.task.dag.dag_id, self.task.task_id, downstream_id])
            })
        return json.dumps(contents, ensure_ascii=False)


class HiveOperatorTaskConverter(BaseTaskConverter, LoggingMixin):
    @classmethod
    def get_operator_class(cls):
        if Supports.is_operator_available("HiveOperator"):
            from airflow.providers.apache.hive.operators.hive import HiveOperator
            return HiveOperator
        return None

    def __init__(self, task):
        super(HiveOperatorTaskConverter, self).__init__(task)

    def get_node_content(self):
        return self.task.hql


class PrestoToMySqlTransferTaskConverter(BaseTaskConverter, LoggingMixin):
    @classmethod
    def get_operator_class(cls):
        if Supports.is_operator_available("PrestoToMySqlTransfer"):
            from airflow.operators.presto_to_mysql import PrestoToMySqlTransfer
            return PrestoToMySqlTransfer
        return None

    def __init__(self, task):
        super(PrestoToMySqlTransferTaskConverter, self).__init__(task)
        pass

    def get_node_code(self):
        self.log.info(self.task.__dict__)
        from ..patch.presto_to_mysql_operator_patch import execute
        from ..patch.mysql_hook_patch import get_conn
        from ..patch.presto_hook_patch import get_conn as presto_get_conn
        from airflow.hooks.mysql_hook import MySqlHook
        from airflow.hooks.presto_hook import PrestoHook
        MySqlHook.get_conn = get_conn
        PrestoHook.get_conn = presto_get_conn
        PrestoToMySqlTransfer.execute = execute
        self.task.execute(self.task_instance.get_template_context())
        code = {
            "type": 'PrestoToMySqlTransfer',
            "mysql": {
                "connection": self.task.mysql.get_conn(),
                "mysql_table": self.task.mysql_table
            },
            "sql": self.task.sql,
            "presto": self.task.presto.get_conn()
        }
        return json.dumps(code, intent=4)


class HiveToMySqlTransferTaskConverter(BaseTaskConverter, LoggingMixin):
    @classmethod
    def get_operator_class(cls):
        if Supports.is_operator_available("HiveToMySqlTransfer"):
            from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
            return HiveToMySqlTransfer
        return None

    def __init__(self, task):
        super(HiveToMySqlTransferTaskConverter, self).__init__(task)
        pass

    def get_node_content(self):
        return self.task.sql


class SparkSqlOperatorTaskConverter(BaseTaskConverter, LoggingMixin):
    @classmethod
    def get_operator_class(cls):
        if Supports.is_operator_available("SparkSqlOperator"):
            from airflow.contrib.operators.spark_sql_operator import SparkSqlOperator
            return SparkSqlOperator
        return None

    def __init__(self, task):
        super(SparkSqlOperatorTaskConverter, self).__init__(task)
        pass

    def get_node_content(self):
        return self.task._sql


class SparkSubmitOperatorTaskConverter(BaseTaskConverter, LoggingMixin):
    @classmethod
    def get_operator_class(cls):
        if Supports.is_operator_available("SparkSubmitOperator"):
            from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
            return SparkSubmitOperator
        return None

    def __init__(self, task):
        super(SparkSubmitOperatorTaskConverter, self).__init__(task)
        pass

    def get_node_content(self):
        from airflow_dag_parser.patch.spark_submit_hook_patch import submit
        from airflow.contrib.hooks.spark_submit_hook import SparkSubmitHook
        SparkSubmitHook.submit = submit
        self.task.execute(self.task_instance.get_template_context())
        code = " ".join(self.task._hook.spark_submit_cmd)
        return code


class SqoopOperatorTaskConverter(BaseTaskConverter, LoggingMixin):
    @classmethod
    def get_operator_class(cls):
        if Supports.is_operator_available("SqoopOperator"):
            from airflow.contrib.operators.sqoop_operator import SqoopOperator
            return SqoopOperator
        return None

    def __init__(self, task):
        super(SqoopOperatorTaskConverter, self).__init__(task)
        pass

    def get_node_content(self):
        from airflow_dag_parser.patch.sqoop_hook_patch import import_table, export_table, import_query
        from airflow.contrib.hooks.sqoop_hook import SqoopHook
        SqoopHook.import_table = import_table
        SqoopHook.export_table = export_table
        SqoopHook.import_query = import_query
        self.task.execute(self.task_instance.get_template_context())
        self.log.info("sqoop cmd: %s" % self.task.hook.cmd)
        return " ".join(self.task.hook.cmd)


class ExternalTaskSensorConverter(BaseTaskConverter, LoggingMixin):
    @classmethod
    def get_operator_class(cls):
        if Supports.is_operator_available("ExternalTaskSensor"):
            from airflow.sensors.external_task import ExternalTaskSensor
            return ExternalTaskSensor
        return None

    def __init__(self, task):
        super(ExternalTaskSensorConverter, self).__init__(task)
        pass

    def get_dependencies(self):
        ios = super().get_dependencies()
        if not ios:
            ios = list()
        if self.task.external_dag_id:
            upstream_dag_id = self.task.external_dag_id
        else:
            upstream_dag_id = ''
        if self.task.external_task_id:
            upstream_task_id = self.task.external_task_id
        else:
            upstream_task_id = ''

        if upstream_dag_id != '' and upstream_task_id != '':
            # Adding cross DAG level task dependencies. (Not supported right now, take it as DAG dependency as below did).
            # io = SpecNodeIO()
            # io.output = self.generator_id(upstream_dag_id, upstream_task_id)
            # io.type = 'Normal'
            # io.refTableName = upstream_dag_id
            # ios.append(io)

            # registering external dependencies
            self._register_external_dependency(self.task.dag.dag_id, upstream_dag_id, upstream_task_id)
        if len(ios) > 0:
            return {"nodeId": self.node.id, "depends": ios}
        return None


class TriggerDagRunOperatorTaskConverter(BaseTaskConverter, LoggingMixin):
    @classmethod
    def get_operator_class(cls):
        if Supports.is_operator_available("TriggerDagRunOperator"):
            return TriggerDagRunOperator
        return None

    def __init__(self, task):
        super(TriggerDagRunOperatorTaskConverter, self).__init__(task)
        self.task = task

    def get_node_outputs(self):
        ios = []
        io = SpecNodeIO()
        io.artifactType = 'NodeOutput'
        io.refTableName = self.task.task_id
        io.data = self.generator_id(self.task.dag.dag_id, self.task.task_id)
        ios.append(io)
        if not self.task.trigger_dag_id:
            return ios

        downstream_dag_id = self.task.trigger_dag_id
        io = SpecNodeIO()
        io.artifactType = 'PassThrough'
        io.refTableName = downstream_dag_id
        io.data = self.generator_id(downstream_dag_id, '')
        ios.append(io)

        # registering external dependencies
        self._register_external_dependency(downstream_dag_id, self.task.dag.dag_id, self.task.task_id)
        return ios


try:
    from airflow.operators.empty import EmptyOperator


    class EmptyOperatorTaskConverter(BaseTaskConverter, LoggingMixin):
        @classmethod
        def get_operator_class(cls):
            if Supports.is_operator_available("EmptyOperator"):
                return EmptyOperator
            return None

        def __init__(self, task):
            super(EmptyOperatorTaskConverter, self).__init__(task)
            self.task = task

        def get_node_inputs(self):
            inputs = super(EmptyOperatorTaskConverter, self).get_node_inputs()
            if not inputs:
                inputs = []
            return inputs
except ImportError as e:
    logger.warning("Empty operator not supported.")

try:
    from airflow.operators.dummy import DummyOperator


    class DummyOperatorTaskConverter(BaseTaskConverter, LoggingMixin):
        @classmethod
        def get_operator_class(cls):
            if Supports.is_operator_available("DummyOperator"):
                return DummyOperator
            return None

        def __init__(self, task):
            super(DummyOperatorTaskConverter, self).__init__(task)
            self.task = task

        def get_node_inputs(self):
            inputs = super(DummyOperatorTaskConverter, self).get_node_inputs()
            if not inputs:
                inputs = []
            return inputs
except ImportError as e:
    logger.warning("Dummy operator not supported.")

try:
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


    class SQLExecuteQueryOperatorTaskConverter(BaseTaskConverter, LoggingMixin):
        @classmethod
        def get_operator_class(cls):
            if Supports.is_operator_available("SQLExecuteQueryOperator"):
                return SQLExecuteQueryOperator
            return None

        def __init__(self, task):
            super(SQLExecuteQueryOperatorTaskConverter, self).__init__(task)
            self.task = task

        def get_node_inputs(self):
            inputs = super(SQLExecuteQueryOperatorTaskConverter, self).get_node_inputs()
            if not inputs:
                inputs = []
            return inputs

        def get_node_content(self):
            return self.get_sql_content(self.task.sql)


except ImportError as e:
    logger.warning("SQLExecuteQuery operator not supported.")

try:
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
    from airflow.providers.postgres.operators.postgres import PostgresOperator


    class PostgresOperatorTaskConverter(SQLExecuteQueryOperatorTaskConverter, LoggingMixin):
        @classmethod
        def get_operator_class(cls):
            if Supports.is_operator_available("PostgresOperator"):
                return PostgresOperator
            return None
except ImportError as e:
    try:
        from airflow.models import BaseOperator
        from airflow.providers.postgres.operators.postgres import PostgresOperator


        class PostgresOperatorTaskConverter(BaseTaskConverter, LoggingMixin):
            def __init__(self, task):
                super(BaseTaskConverter, self).__init__(task)
                self.task = task

            @classmethod
            def get_operator_class(cls):
                if Supports.is_operator_available("PostgresOperator"):
                    return PostgresOperator
                return None

            def get_node_content(self):
                return self.get_sql_content(self.task.sql)

            def get_node_parameters(self):
                if not self.task.parameters:
                    return super().get_node_parameters()
                dw_variables = list()
                for param_key in self.task.parameters:
                    dw_variables.append(self._get_param_item_as_dw_variable(self.task.parameters, param_key))
                return dw_variables
    except ImportError as e2:
        logger.warning("Postgres operator not supported.")

try:
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
    from airflow.providers.mysql.operators.mysql import MySqlOperator


    class MySqlOperatorTaskConverter(SQLExecuteQueryOperatorTaskConverter, LoggingMixin):
        @classmethod
        def get_operator_class(cls):
            if Supports.is_operator_available("MySqlOperator"):
                return MySqlOperator
            return None
except ImportError as e:
    try:
        from airflow.models import BaseOperator
        from airflow.providers.mysql.operators.mysql import MySqlOperator


        class MySqlOperatorTaskConverter(BaseTaskConverter, LoggingMixin):
            def __init__(self, task):
                super(BaseTaskConverter, self).__init__(task)
                self.task = task

            @classmethod
            def get_operator_class(cls):
                if Supports.is_operator_available("MySqlOperator"):
                    return MySqlOperator
                return None

            def get_node_content(self):
                return self.get_sql_content(self.task.sql)

    except ImportError as e2:
        logger.warning("MySQL operator not supported.")
