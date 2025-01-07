import logging
import pytest
import sys

from datetime import datetime, timezone
from functools import cache
from airflow.exceptions import AirflowSkipException
from airflow.models.dag import DAG
from airflow.models.dagbag import DagBag
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.db import create_default_connections
from airflow.utils.session import provide_session, NEW_SESSION
from airflow.utils.state import DagRunState, State
from airflow.utils.types import DagRunType
from sqlalchemy.orm import Session

log = logging.getLogger(__name__)

@provide_session
def get_session(session: Session=NEW_SESSION):
    create_default_connections(session)
    return session

@cache
def get_dag_bag() -> DagBag:
    return DagBag(dag_folder="great_expectations_provider/example_dags")

@pytest.fixture()
def session():
    return get_session()

class TestExampleDag:
    def test_example_dag(self, session: Session):
        dag_id = "gx_provider_example_dag"
        dag_bag = get_dag_bag()
        dag = dag_bag.get_dag(dag_id)
        print(dag_bag.dag_ids)
        assert dag

        dag.clear()
        dag_run = dag.test()

        assert dag_run.get_state() == DagRunState.SUCCESS
        assert all([ti.state == State.SUCCESS for ti in dag_run.get_task_instances()])
