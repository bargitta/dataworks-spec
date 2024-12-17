import os
import re
import hashlib
import math
from airflow.utils.log.logging_mixin import LoggingMixin
from pendulum.datetime import DateTime

from airflow_dag_parser.common.configs import Config
from airflow_dag_parser.common.supports import Supports
from airflow_dag_parser.miscs.helpers import resolve_param_type_title
from airflow_dag_parser.models.dw_spec import (SpecNode, SpecTrigger, \
                                               SpecNodeIO, RuntimeResource, SpecVariable, SpecScriptRuntime, SpecScript)

'''
basic implementation for airflow task converter
'''


class BaseTaskConverter(LoggingMixin):
    try:
        from airflow.models.operator import Operator
        def __init__(self, task: Operator):
            super(BaseTaskConverter, self).__init__()
            self.task = task
            self.task_instance = None
            self.node = None
    except ImportError as ie:
        from airflow.models.baseoperator import BaseOperator
        def __init__(self, task: BaseOperator):
            super(BaseTaskConverter, self).__init__()
            self.task = task
            self.task_instance = None
            self.node = None

    def convert(self) -> SpecNode:
        self.log.debug("converting task: %s.%s, dag file: %s",
                       self.task.dag.dag_id, self.task.task_id, self.task.dag.full_filepath)
        node = SpecNode()
        self.node = node
        node.id = self.generator_id(self.task.dag.dag_id, self.task.task_id)
        # node.name = self.task.task_id
        node.name = self.normalize_identifier(self.task.task_id)
        if hasattr(self.task, 'description'):
            node.description = self.task.description
        if self.task.execution_timeout:
            # parse execution timeout to node timeout in HOUR
            node.timeout = math.ceil(self.task.execution_timeout.total_seconds() / 3600)

        self._set_strategy(node)
        node.trigger = self._get_trigger()
        node.runtimeResource = self._get_runtime_resources()
        node.script = self.get_node_code()

        # node.script =
        # TODO check context
        # self.task_instance = TaskInstance(
        #     task=self.task, execution_date=datetime.now())
        # patch self.task_instance.get_template_context
        # self.task_instance.get_template_context = get_template_context.__get__(
        #     self.task_instance, TaskInstance)
        # node.parameter = self.get_node_parameters()

        # self.task_instance.render_templates()
        # node.airflowTask = self.task
        #
        # node.cronExpress = self.get_cron_express()

        # node.inputs = self.get_node_inputs()
        branch_list = self.get_branch_list()
        if branch_list:
            node.branch = {'branches': branch_list}
        outputs = self.get_node_outputs()
        if outputs:
            node.outputs = {'nodeOutputs': outputs}
        node.type = self.get_node_type()
        return node

    def _set_strategy(self, node: SpecNode) -> None:
        node.instanceMode = 'T+1'
        node.rerunMode = 'FailureAllowed'

        node.rerunTimes = self.task.retries
        if self.task.retry_delay:
            node.rerunInterval = int(self.task.retry_delay.total_seconds() * 1000)

    def _get_trigger(self):
        trigger = SpecTrigger()
        trigger.type = 'Scheduler'
        if self.task.start_date and isinstance(self.task.start_date, DateTime):
            trigger.startTime = self.task.start_date.format('YYYY-MM-DD HH:mm:ss')
            if self.task.start_date.timezone and trigger.timezone is None:
                trigger.timezone = self.task.start_date.timezone.name
        if self.task.end_date and isinstance(self.task.end_date, DateTime):
            trigger.endTime = self.task.end_date.format('YYYY-MM-DD HH:mm:ss')
            if self.task.end_date.timezone and trigger.timezone is None:
                trigger.timezone = self.task.end_date.timezone.name
        return trigger

    def _get_runtime_resources(self):
        rr = RuntimeResource()
        if Config.settings is not None and "workflow.converter.target.schedule.resGroupIdentifier" in Config.settings:
            rr.resourceGroup = Config.settings["workflow.converter.target.schedule.resGroupIdentifier"]
        return rr

    def get_node_parameters(self):
        dw_variables = list()
        task_params = self.task.params
        if not task_params:
            return dw_variables
        dag_params = self.task.dag.params
        if not dag_params:
            dag_params = {}
        for param_key in task_params:
            if param_key in dag_params:
                self.log.debug(f"Skipping DAG level param {param_key}")
                continue
            if hasattr(task_params, 'get_param'):
                sv = self._get_param_as_dw_variable(task_params, param_key)
            else:
                sv = self._get_param_item_as_dw_variable(task_params, param_key)
            dw_variables.append(sv)
        return dw_variables

    def _get_param_item_as_dw_variable(self, task_params, param_key):
        param_value = task_params.__getitem__(param_key)
        self.log.debug(f"Migrating Task param {param_key} with value: {param_value}")
        return SpecVariable(
            name=param_key,
            scope='NodeParameter',
            type='System',
            value=param_value
        )

    def _get_param_as_dw_variable(self, task_params, param_key):
        af_param = task_params.get_param(param_key)
        (param_type, param_title) = resolve_param_type_title(af_param)
        param_value = str(af_param.value)
        self.log.debug(f"Migrating Task param {param_key} with type {param_type} value: {param_value}")
        return SpecVariable(
            name=param_key,
            scope='NodeParameter',
            type='System',
            value=param_value,
            description=param_title
        )

    def get_node_code(self):
        runtime = SpecScriptRuntime()
        runtime.command = Config.get_node_type_or_default(self.task.__class__.__name__)
        script = SpecScript()
        script.path = self.node.name
        script.content = self.get_node_content()
        script.runtime = runtime
        script.parameters = self.get_node_parameters()
        return script

    def get_node_content(self):
        return None

    def get_branch_list(self):
        return None

    def get_node_outputs(self):
        ios = []
        io = SpecNodeIO()
        io.data = self.generator_id(self.task.dag.dag_id, self.task.task_id)
        ios.append(io)

        for downstream_task_id in self.task.downstream_task_ids:
            io = SpecNodeIO()
            io.data = '.'.join([self.task.dag.dag_id, self.task.task_id, downstream_task_id])
            io.artifactType = 'NodeOutput'
            io.refTableName = self.task.task_id
            ios.append(io)
        return ios

    def get_node_inputs(self):
        pass

    def get_dependencies(self):
        ios = []
        for upstream in self.task.upstream_list:
            upstream_task_id = upstream.task_id
            if (Supports.is_operator_supported(upstream) and
                    upstream.__class__.__name__ in Config.typeMapping and
                    Config.typeMapping[upstream.__class__.__name__] == 'CONTROLLER_BRANCH'):
                # branch style dependency
                node_output = '.'.join([self.task.dag.dag_id, upstream_task_id, self.task.task_id])
            else:
                # legacy style dependency
                node_output = self.generator_id(self.task.dag.dag_id, upstream_task_id)
            io = SpecNodeIO()
            io.type = 'Normal'
            io.output = node_output
            io.refTableName = upstream_task_id
            ios.append(io)
        if len(ios) > 0:
            return {"nodeId": self.node.id, "depends": ios}
        return None

    def _register_external_dependency(self, been_triggered_dag_id, triggering_dag_id, triggering_task_id):
        """
        {
            "been_triggered_dag_id": [
                {
                    "dag_id": "mock_triggering_dag_id",
                    "workflow_id":1234567890,
                    "task_id": "mock_triggering_task_id",
                    "node_id":123456789
                }
            ]
        }
        """
        downstream_dag_dependencies = Config.runtimeDagDependencies.get(been_triggered_dag_id, [])
        downstream_dag_dependencies.append({
            "dag_id": triggering_dag_id,
            "workflow_id": self.generator_id(triggering_dag_id, ''),
            "task_id": triggering_task_id,
            "node_id": self.generator_id(triggering_dag_id, triggering_task_id)
        })
        Config.runtimeDagDependencies[been_triggered_dag_id] = downstream_dag_dependencies

    @staticmethod
    def generator_id(dag_id: str = None, task_id: str = None, include_path_prefix=True) -> str:
        if dag_id:
            code = dag_id + task_id
        else:
            code = task_id
        if include_path_prefix:
            code = Config.workflowPathPrefix + code
        h = hashlib.sha256(code.encode('utf-8'))
        return str(int(h.hexdigest(), 16) % (10 ** 16))

    @staticmethod
    def normalize_identifier(identifier: str):
        REG = re.compile(r'^[a-zA-Z0-9._\u4e00-\u9fa5]*$')
        id = list()
        for ch in identifier:
            if REG.match(ch):
                id.append(ch)
            else:
                id.append('_')
        return ''.join(id)

    @staticmethod
    def get_sql_content(task_sql):
        if task_sql.startswith('/') or task_sql.startswith('.'):
            # take task_sql as the sql file path name
            file_paths = {
                task_sql,
                os.path.join(Config.resourceDir, task_sql),
                os.path.join(os.getcwd(), task_sql)
            }
            for file in file_paths:
                if os.path.exists(file):
                    return open(file, 'r').read()
        return task_sql

    def get_node_type(self):
        return Config.get_node_type_or_default(self.task.__class__.__name__)
