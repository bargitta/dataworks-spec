import random
import traceback
from datetime import timedelta

from airflow.models.dag import DAG
from airflow.utils.log.logging_mixin import LoggingMixin
from pendulum.datetime import DateTime

from airflow_dag_parser.common.configs import Config
from airflow_dag_parser.converter.converters import get_converter, logger
from airflow_dag_parser.converter.task_converter import BaseTaskConverter
from airflow_dag_parser.miscs.helpers import resolve_param_type_title
from airflow_dag_parser.models.dw_spec import SpecNode, SpecTrigger, SpecScheduleStrategy, SpecWorkflow, SpecVariable, \
    SpecScriptRuntime


class DagConverter(LoggingMixin):
    def __init__(self, dag: DAG):
        super().__init__()
        self.dag = dag

    def convert(self):
        workflow = SpecWorkflow()
        workflow.id = BaseTaskConverter.generator_id(dag_id=self.dag.dag_id, task_id='')
        # workflow.name = self.dag.dag_id
        workflow.name = BaseTaskConverter.normalize_identifier(self.dag.dag_id)
        self.log.info(f"Converting DAG {self.dag.dag_id} to DataWorks workflow {workflow.name}")
        workflow.trigger = self.get_dag_trigger()
        if not workflow.trigger:
            self.log.warning(
                f"Converting DAG {self.dag.dag_id} to Manual Workflow since its `schedule_interval` either is marked as once or not exist..")
            workflow.type = 'ManualWorkflow'
            workflow.script.runtime = SpecScriptRuntime(command="MANUAL_WORKFLOW")
        else:
            workflow.type = 'CycleWorkflow'
        workflow.strategy = self.get_dag_strategy()
        workflow_path_prefix = Config.workflowPathPrefix
        workflow.script.path = workflow_path_prefix + workflow.name
        workflow.script.parameters = self._get_dag_params()

        for task in self.dag.tasks:
            task_converter = get_converter(task)
            try:
                node: SpecNode = task_converter.convert()
            except Exception as e:
                self.log.error(traceback.format_exc())
                exception = traceback.format_exc()
                raise e
            workflow.nodes.append(node)
            dependency = task_converter.get_dependencies()
            if dependency:
                workflow.dependencies.append(dependency)
        node_outputs = []
        if 'nodeOutputs' in workflow.outputs:
            node_outputs = workflow.outputs['nodeOutputs']
        node_outputs.append({
            "artifactType": "NodeOutput",
            "data": workflow.id,
            "refTableName": workflow.name
        })
        workflow.outputs['nodeOutputs'] = node_outputs
        self.log.info(f"Finished converting DAG {self.dag.dag_id}\n")
        return workflow

    def get_dag_trigger(self) -> SpecTrigger:
        cron_expr = self.get_cron_express()
        if not cron_expr:
            return None
        trigger = SpecTrigger()
        trigger.delaySeconds = 0
        trigger.type = 'Scheduler'
        if self.dag.start_date and isinstance(self.dag.start_date, DateTime):
            trigger.startTime = self.dag.start_date.format('YYYY-MM-DD HH:mm:ss')
        if self.dag.end_date and isinstance(self.dag.end_date, DateTime):
            trigger.endTime = self.dag.end_date.format('YYYY-MM-DD HH:mm:ss')
        # TODO timezone
        # trigger.timezone = "Asia/Shanghai"
        trigger.timezone = self.dag.timezone.name
        trigger.cron = cron_expr
        return trigger

    def get_dag_strategy(self) -> SpecScheduleStrategy:
        strategy = SpecScheduleStrategy()
        strategy.timeout = 0
        strategy.instanceMode = 'T+1'
        strategy.rerunMode = 'Allowed'
        strategy.rerunTimes = 0
        strategy.rerunInterval = 0
        strategy.failureStrategy = 'Continue'
        return strategy

    def _get_dag_params(self):
        dag_params = self.dag.params
        if not dag_params:
            return ()
        dw_variables = list()
        for param_key in dag_params:
            if hasattr(dag_params, 'get_param'):
                sv = self._get_param_as_dw_variable(dag_params, param_key)
            else:
                sv = self._get_param_item_as_dw_variable(dag_params, param_key)
            dw_variables.append(sv)
        return dw_variables

    def _get_param_item_as_dw_variable(self, dag_params, param_key):
        param_value = dag_params.__getitem__(param_key)
        self.log.debug(f"Migrating DAG param {param_key} with value: {param_value}")
        return SpecVariable(
            name=param_key,
            scope='Workflow',
            type='System',
            value=param_value
        )

    def _get_param_as_dw_variable(self, dag_params, param_key):
        af_param = dag_params.get_param(param_key)
        (param_type, param_title) = resolve_param_type_title(af_param)
        param_value = str(af_param.value)
        self.log.debug(f"Migrating DAG param {param_key} with type {param_type} value: {param_value}")
        return SpecVariable(
            name=param_key,
            scope='Workflow',
            type='System',
            value=param_value,
            description=param_title
        )

    def get_cron_express(self):
        dag = self.dag
        if not dag.schedule_interval:
            return None

        self.log.debug("dag: %s, schedule_interval: %s, %s" % (
            dag.dag_id, dag.schedule_interval, type(dag.schedule_interval)))
        if type(dag.schedule_interval) == type(""):
            if dag.schedule_interval in ["@once", "@continuous"]:
                return None

            if dag.schedule_interval in ["@hourly", "@daily", "@weekly", "@monthly", "@yearly"]:
                return self._get_cron_for_predefined_schedule_interval(dag.schedule_interval)

            tokens = dag.schedule_interval.split(" ")
            if len(tokens) == 5:
                tokens.insert(0, "0")
            if len(tokens) == 6 and tokens[-1] == '*':
                tokens[-1] = "?"
            return " ".join(tokens)
        elif type(dag.schedule_interval) == type(timedelta(days=1)):
            return self._timedelta_to_cron(dag.schedule_interval)
        return dag.schedule_interval

    def _random_under_cap(self, cap_num: int, format_number: bool = False):
        str_rn = str(random.randint(0, cap_num))
        return str_rn if not format_number else str_rn.zfill(2)

    def _parse_literal_hourly_schedule(self):
        """
        :return: cron expression triggered at before 30 minutes
        """
        second = self._random_under_cap(59, format_number=True)
        minute = self._random_under_cap(30, format_number=True)
        return " ".join([second, minute, "00-23/1 * * ?"])

    def _parse_literal_daily_schedule(self):
        """
        :return: cron expression triggered before 2 o'clock
        """
        second = self._random_under_cap(59, format_number=True)
        minute = self._random_under_cap(59, format_number=True)
        hour = self._random_under_cap(1, format_number=True)
        return " ".join([second, minute, hour, "* * ?"])

    def _parse_literal_weekly_schedule(self):
        """
        :return: cron expression triggered always on Monday
        """
        second = self._random_under_cap(59, format_number=True)
        minute = self._random_under_cap(59, format_number=True)
        hour = self._random_under_cap(1, format_number=True)
        return " ".join([second, minute, hour, "? * 1"])

    def _parse_literal_monthly_schedule(self):
        """
        :return: cron expression triggered always on the first day of each month
        """
        second = self._random_under_cap(59, format_number=True)
        minute = self._random_under_cap(59, format_number=True)
        hour = self._random_under_cap(1, format_number=True)
        return " ".join([second, minute, hour, "1 * ?"])

    def _parse_literal_yearly_schedule(self):
        """
        :return: cron expression triggered always on the first day of the year
        """
        second = self._random_under_cap(59, format_number=True)
        minute = self._random_under_cap(59, format_number=True)
        hour = self._random_under_cap(1, format_number=True)
        return " ".join([second, minute, hour, "1 1 ?"])

    def _get_cron_for_predefined_schedule_interval(self, schedule_interval):
        simple_interval = schedule_interval.replace('@', '')
        switcher = {
            "hourly": self._parse_literal_hourly_schedule,
            "daily": self._parse_literal_daily_schedule,
            "weekly": self._parse_literal_weekly_schedule,
            "monthly": self._parse_literal_monthly_schedule,
            "yearly": self._parse_literal_yearly_schedule
        }
        return switcher.get(simple_interval, self._parse_literal_daily_schedule)()


def _timedelta_to_cron(self, delta):
    """
    Convert a timedelta to a cron expression.
    Seconds will ALWAYS be set to 00, which means seconds level scheduler is not supported
    """
    cron = [self._random_under_cap(59, format_number=True), self._random_under_cap(59, format_number=True), "*", "*", "*", "?"]
    minutes = int(delta.total_seconds()) / 60
    steps = [1, 60, 1440, 43200]
    indices = list(reversed(range(len(steps))))
    self.log.debug("indices: %s", indices)
    for i in indices:
        self.log.debug("minutes: %s, steps: %s" % (minutes, steps[i]))
        mini_step = int(minutes / steps[i])
        if mini_step > 0:
            cron[i + 1] = "*/" + str(mini_step)
            break
    return " ".join(cron)
