class Config(object):
    targetDir: str
    resourceDir = './'
    workflowPathPrefix = 'Airflow导入/'
    # default operator type to DataWorks node type mapping
    typeMapping: dict = {
        "EmptyOperator": "VIRTUAL",
        "DummyOperator": "VIRTUAL",
        "ExternalTaskSensor": "VIRTUAL",
        "BashOperator": "DIDE_SHELL",
        "HiveToMySqlTransfer": "DI",
        "PrestoToMySqlTransfer": "DI",
        "PythonOperator": "PYTHON",
        "BranchPythonOperator": "CONTROLLER_BRANCH",
        "HiveOperator": "EMR_HIVE",
        "SqoopOperator": "EMR_SQOOP",
        "SparkSqlOperator": "EMR_SPARK_SQL",
        "SparkSubmitOperator": "EMR_SPARK",
        "SQLExecuteQueryOperator": "MySQL",
        "PostgresOperator": "Postgresql",
        "MySqlOperator": "MySQL",
        "default": "PYTHON"
    }
    settings: dict = {
        "workflow.converter.target.schedule.resGroupIdentifier": "",
        "bashoperator.content.adaptive.conversion": False
    }

    runtimeDagDependencies: dict = {}

    @staticmethod
    def get_node_type_or_default(operator_name):
        if operator_name in Config.typeMapping:
            return Config.typeMapping.get(operator_name)
        if 'default' in Config.typeMapping:
            return Config.typeMapping.get('default')
        return 'PYTHON'
