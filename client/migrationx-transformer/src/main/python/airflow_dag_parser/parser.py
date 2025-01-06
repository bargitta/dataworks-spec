#!/usr/bin/python
import logging
import optparse
import os
import sys
import traceback

from airflow.configuration import AIRFLOW_HOME
from airflow.logging_config import configure_logging
from airflow.utils.log.logging_mixin import LoggingMixin

from airflow_dag_parser.common.configs import Config
from airflow_dag_parser.converter.dag_converter import DagConverter
from airflow_dag_parser.converter.task_converter import BaseTaskConverter
from airflow_dag_parser.dag_parser import DagParser
from airflow_dag_parser.models.dw_spec import DataWorksWorkflowSpec, Specification, SpecEntityEncoder, SpecNodeIO, Spec

try:
    import json
except Exception as e:
    import simplejson as json

try:
    reload(sys)
except Exception as e:
    from imp import reload

logger = logging.getLogger(__name__)
try:
    sys.setdefaultencoding('utf-8')
except Exception as e:
    logger.warning(e)


class Main(LoggingMixin):
    def __init__(self, dag_folder, workflow_folder):
        self.dag_folder = os.path.abspath(dag_folder)
        self.workflow_folder = os.path.abspath(workflow_folder)
        if not os.path.exists(self.workflow_folder):
            os.mkdir(self.workflow_folder)
        self.dag_parser = None
        self.reports = []

    def run(self):
        reload(sys)
        sys.path.insert(0, self.dag_folder)

        self.log.info("conf: " + AIRFLOW_HOME)
        self.dag_parser = DagParser(self.dag_folder)
        self.dag_parser.parse()

        dags = self.dag_parser.get_dags()
        specifications = []
        for dag_id in dags:
            dag = dags[dag_id]
            report = {
                "dag_id": dag_id,
                "file": dag.fileloc,
                "specification": None,
                "workflow": None,
                "message": "",
                "exception": "",
                "success": False
            }
            try:
                workflow_name, specification = self._parse_single_dag(dags[dag_id])
                specifications.append((dag_id, specification))
                report['specification'] = os.path.join(self.workflow_folder, f'{workflow_name}.json')
                report['success'] = True
            except Exception as e:
                self.log.error(traceback.format_exc())
                report['success'] = False
                report['message'] = str(e)
                report['exception'] = traceback.format_exc()
            self.reports.append(report)

        # dags needed to be re-converted
        for dag_id, specification in specifications:
            workflow = specification.spec.workflows[0]
            self._populate_external_dependencies(dag_id, specification.spec)
            workflow_name = workflow.name
            spec_content = json.dumps(specification, indent=2, ensure_ascii=False, cls=SpecEntityEncoder)
            spec_file_path = os.path.join(self.workflow_folder, f'{workflow_name}.json')
            with open(spec_file_path, 'w') as f:
                f.write(spec_content)
                f.flush()

    @staticmethod
    def _parse_single_dag(dag):
        dag_converter = DagConverter(dag)
        workflow = dag_converter.convert()
        spec = DataWorksWorkflowSpec()
        spec.workflows.append(workflow)
        return workflow.name, Specification(spec, kind=workflow.type)

    @staticmethod
    def _populate_external_dependencies(a_dag_id:str, spec:DataWorksWorkflowSpec):
        flow = spec.flow
        deps = Config.runtimeDagDependencies.get(a_dag_id, [])
        ios = []
        for dep in deps:
            dep_dag_id = dep.get('dag_id')
            io = SpecNodeIO()
            io.type = "Normal"
            io.artifactType = 'PassThrough'
            io.refTableName = dep_dag_id
            io.output = BaseTaskConverter.generator_id(dep_dag_id, '')
            ios.append(io)
        flow.append({
            "nodeId": BaseTaskConverter.generator_id(a_dag_id, ''),
            "depends": ios
        })


def dump_python_path(doted_prefix):
    for path in sys.path:
        cc_path = os.path.join(path, doted_prefix.replace('.', os.sep))
        found = os.path.isdir(cc_path) or os.path.exists(cc_path + '.py')
        print(f'\tPath or file [{cc_path}(.py)] exists? [{found}]')


def parse_args():
    global dag_folder, output
    parser = optparse.OptionParser(
        "airflow-workflow", description="converting Airflow dag to DataWorks workflow", version="1.0")
    parser.add_option("-d", "--dag_folder", action="store", dest="dag_folder", help="airflow dag folder")
    parser.add_option("-o", "--output", action="store", dest="output", help="workflow storing folder")
    parser.add_option("-c", "--connections", action="store", dest="connections", help="connection csv file")
    parser.add_option("-p", "--prefix", action="store", dest="prefix", help="workflow location prefix in IDE")
    parser.add_option("-m", "--mapping", action="store", dest="mapping",
                      help='type mapping json file, e.g. /path/to/conf/FUTU_flowspec-airflowV2-transformer-config.json')
    parser.add_option("-x", "--modules", action="store", dest="modules", help="modules to dump out")
    opts, args = parser.parse_args(sys.argv)
    if not opts.dag_folder:
        logger.error("dag_folder not set")
        sys.exit(-1)
    dag_folder = opts.dag_folder
    if not opts.output:
        logger.error("output folder not set")
        sys.exit(-2)
    if not os.path.exists(opts.output):
        os.mkdir(opts.output)
    output = opts.output
    # if opts.connections:
    #     os.environ['connections_csv'] = connections
    customer_config = None
    if opts.mapping:
        logger.info(f"Overwriting type mapping with file: {opts.mapping}")
        with open(opts.mapping) as fd:
            customer_config = json.load(fd)
        if customer_config is not None and 'typeMapping' in customer_config:
            customized_mapping = customer_config['typeMapping']
            Config.typeMapping.update(customized_mapping)
        str_content = list()
        for k, v in Config.typeMapping.items():
            str_content.append(f"\t{k}: {v}\n")
        logger.info(f"Effective type mappings are: \n{''.join(str_content)}")
    if customer_config is not None and 'workflowPathPrefix' in customer_config:
        cc_prefix = customer_config['workflowPathPrefix']
        logger.info(f"Also setting workflow path prefix to {cc_prefix}")
        Config.workflowPathPrefix = cc_prefix
    if customer_config is not None and 'settings' in customer_config:
        cc_settings = customer_config['settings']
        logger.info(f"Also overwriting settings with {cc_settings}")
        Config.settings.update(cc_settings)
    if opts.prefix:
        logger.info(f"Setting workflow path prefix to {opts.prefix}")
        Config.workflowPathPrefix = opts.prefix
    logger.info(f"Effective workflow path prefix is: {Config.workflowPathPrefix}")
    try:
        dump_modules = []
        if opts.modules:
            dump_modules = opts.modules.split(',')
        for mod in dump_modules:
            dump_python_path(mod)
            print('\n')
    except Exception as e:
        logger.error(e)
    return dag_folder, output


if __name__ == "__main__":
    configure_logging()
    dag_folder, output = parse_args()
    main = Main(dag_folder, output)
    main.run()
    logger.info("Finished parsing Airflow DAGs.")

    sys.exit(0)
