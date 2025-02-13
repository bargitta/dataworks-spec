from __future__ import annotations

import datetime
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.models.dagrun import DagRun
from airflow.models.param import Param
from airflow.models.taskinstance import TaskInstance
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id=Path(__file__).stem,
    doc_md=__doc__,
    schedule=None,
    start_date=datetime.datetime(2022, 3, 4),
    catchup=False,
    tags=["example_ui"],
    params={
        "names": Param(
            ["Linda", "Martha", "Thomas"],
            type="array",
            description="Define the list of names for which greetings should be generated in the logs."
            " Please have one name per line.",
            title="Names to greet",
        ),
        "english": Param(True, type="boolean", title="English"),
        "german": Param(True, type="boolean", title="German (Formal)"),
        "french": Param(True, type="boolean", title="French"),
    },
) as dag:

    @task(task_id="get_names")
    def get_names(**kwargs) -> list[str]:
        ti: TaskInstance = kwargs["ti"]
        dag_run: DagRun = ti.dag_run
        if "names" not in dag_run.conf:
            print("Uuups, no names given, was no UI used to trigger?")
            return []
        return dag_run.conf["names"]

    @task.branch(task_id="select_languages")
    def select_languages(**kwargs) -> list[str]:
        ti: TaskInstance = kwargs["ti"]
        dag_run: DagRun = ti.dag_run
        selected_languages = []
        for lang in ["english", "german", "french"]:
            if lang in dag_run.conf and dag_run.conf[lang]:
                selected_languages.append(f"generate_{lang}_greeting")
        return selected_languages

    @task(task_id="generate_english_greeting")
    def generate_english_greeting(name: str) -> str:
        return f"Hello {name}!"

    @task(
        task_id="generate_german_greeting",
        params={
            "formal": Param(True, type="boolean", title="Formal")
        }
    )
    def generate_german_greeting(name: str) -> str:
        return f"Sehr geehrter Herr/Frau {name}."

    @task(task_id="generate_french_greeting")
    def generate_french_greeting(name: str) -> str:
        return f"Bonjour {name}!"

    @task(task_id="print_greetings", trigger_rule=TriggerRule.ALL_DONE)
    def print_greetings(greetings1, greetings2, greetings3) -> None:
        for g in greetings1 if greetings1 else []:
            print(g)
        for g in greetings2 if greetings2 else []:
            print(g)
        for g in greetings3 if greetings3 else []:
            print(g)
        if not greetings1 and not greetings2 and not greetings3:
            print("sad, nobody to greet :-(")

    lang_select = select_languages()
    names = get_names()
    english_greetings = generate_english_greeting.expand(name=names)
    german_greetings = generate_german_greeting.expand(name=names)
    french_greetings = generate_french_greeting.expand(name=names)
    lang_select >> [english_greetings, german_greetings, french_greetings]
    results_print = print_greetings(english_greetings, german_greetings, french_greetings)
