from airflow.models import DagBag
from airflow.utils.log.logging_mixin import LoggingMixin


class DagParser(LoggingMixin):
    def __init__(self, dag_folder):
        self.dag_folder = dag_folder
        self.dag_bag = None

    def parse(self):
        try:
            self.dag_bag = DagBag(self.dag_folder, include_examples=False)
            self.log.info("Init DagBag success\n\n")
        except Exception as e:
            self.log.error(e)

    def get_dags(self):
        if self.dag_bag:
            return self.dag_bag.dags
        return None
