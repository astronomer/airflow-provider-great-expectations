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
        execution_time = datetime.now(tz=timezone.utc)
        run_id = DagRun.generate_run_id(DagRunType.MANUAL, execution_time)

        dag_run = dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=execution_time,
            run_id=run_id,
            start_date=execution_time,
            session=session,
            conf=None,
        )
        while dag_run.state == State.RUNNING:
            schedulable_tis, _ = dag_run.update_state(session=session)
            for ti in schedulable_tis:
                add_logger_if_needed(dag, ti)
                tasks = dag.task_dict
                ti.task = tasks[ti.task_id]
                ti.run()



def add_logger_if_needed(dag: DAG, ti: TaskInstance):
    """
    Add a formatted logger to the taskinstance so all logs are surfaced to the command line instead
    of into a task file. Since this is a local test run, it is much better for the user to see logs
    in the command line, rather than needing to search for a log file.
    Args:
        ti: The taskinstance that will receive a logger

    """
    logging_format = logging.Formatter("[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s")
    handler = logging.StreamHandler(sys.stdout)
    handler.level = logging.INFO
    handler.setFormatter(logging_format)
    # only add log handler once
    if not any(isinstance(h, logging.StreamHandler) for h in ti.log.handlers):
        dag.log.debug("Adding Streamhandler to taskinstance %s", ti.task_id)
        ti.log.addHandler(handler)

def _run_task(ti: TaskInstance, session):
    """
    Run a single task instance, and push result to Xcom for downstream tasks. Bypasses a lot of
    extra steps used in `task.run` to keep our local running as fast as possible
    This function is only meant for the `dag.test` function as a helper function.

    Args:
        ti: TaskInstance to run
    """
    log.info("*****************************************************")
    if hasattr(ti, "map_index") and ti.map_index > 0:
        log.info("Running task %s index %d", ti.task_id, ti.map_index)
    else:
        log.info("Running task %s", ti.task_id)
    try:
        ti._run_raw_task(session=session)
        session.flush()
        log.info("%s ran successfully!", ti.task_id)
    except AirflowSkipException:
        log.info("Task Skipped, continuing")
    log.info("*****************************************************")