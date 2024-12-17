# 使用方法

1 解压airflow-workflow.tgz

```shell
tar zxvf airflow-workflow.tgz
```

2 设置PTYHONPATH
指定PYTHONPATH到airflow的python lib目录，例如

```shell
export PYTHONPATH=/usr/local/lib/python3.6/site-packages
# 路径只是举例子，实际export以现场环境为准
export AIRFLOW_HOME=/var/lib/airflow
export AIRFLOW_CONFIG=/var/run/cloudera-scm-agent/process/2531-airflow-AIRFLOW_SCHEDULER/airflow.cfg
```

3 执行airflow DAG导出Workflow操作

```shell
cd airflow-workflow
python3.6 ./parser \
  -p 'airflow_imported/my_dags/' \   # optional
  -m /path/to/conf/flowspec-airflowV2-transformer-config.json \ # optional
  -d /path/to/airflow/dag/floder/ \
  -o /path/to/workflow/saving/folder/
```
