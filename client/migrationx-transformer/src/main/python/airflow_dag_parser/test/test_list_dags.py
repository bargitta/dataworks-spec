import json
import os

from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG

from airflow_dag_parser.common.configs import Config
from airflow_dag_parser.converter.dag_converter import DagConverter
from airflow_dag_parser.models.dw_spec import Specification, DataWorksWorkflowSpec, SpecEntityEncoder
from airflow_dag_parser.models.products import ProductType

from .code import example_twitter_dag


def list_dags():
    dags = []
    tasks = []
    # for v in example_bash_operator.__dict__.values():
    for v in example_twitter_dag.__dict__.values():
        # for v in example_mysql.__dict__.values():
        # for v in example_postgres.__dict__.values():
        # print(f'dict value : {v}')
        if isinstance(v, DAG):
            print(f'is dag {v}')
            print(f'###dag id : {v.dag_id}')
            dags.append(v)

        elif isinstance(v, BaseOperator):
            print(f'is bash {v}')
            tasks.append(v)
    Config.typeMapping.update({
        "HiveOperator": "ODPS_SQL",
    })
    Config.targetDir = './temp_dags'
    if not os.path.exists(Config.targetDir):
        os.mkdir(Config.targetDir)

    for dag in dags:
        # for task in tasks:
        #     if task.dag_id == dag.dag_id:
        #         dag.add_task(task)

        dag_converter = DagConverter(dag)
        workflow = dag_converter.convert()
        spec = DataWorksWorkflowSpec()
        spec.workflows.append(workflow)
        dw_spec = Specification()
        dw_spec.version = '1.1.0'
        dw_spec.kind = 'CycleWorkflow'
        dw_spec.spec = spec

        # write json
        path = os.path.join(Config.targetDir, ProductType.DATA_STUDIO.value)
        if not os.path.exists(path):
            os.mkdir(path)

        spec_content = json.dumps(
            dw_spec,
            indent=2,
            ensure_ascii=False,
            cls=SpecEntityEncoder,
        )
        print(spec_content)
        with open(os.path.join(path, f'{dag.dag_id}.json'), 'w') as f:
            f.write(spec_content)


def test_list_dags():
    list_dags()
