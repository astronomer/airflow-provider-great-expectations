"""
Unit test module to test Great Expectations Operators.

Requirements can be installed in, for instance, a virtual environment, with::

    pip install -r requirements.txt`

and tests can be run with, for instance::

    pytest -p no:warnings
"""

from contextlib import contextmanager
import logging
import os
from pathlib import Path
import unittest.mock as mock

from airflow.exceptions import AirflowException
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)
from great_expectations.exceptions.exceptions import GreatExpectationsError

import pytest


logger = logging.getLogger(__name__)

# Set relative paths for Great Expectations directory and sample data
base_path = Path(__file__).parents[2]
good_data_file = os.path.join(
    base_path, "include", "data/yellow_tripdata_sample_2019-01.csv"
)
bad_data_file = os.path.join(
    base_path, "include", "data/yellow_tripdata_sample_2019-02.csv"
)
ge_root_dir = os.path.join(base_path, "include", "great_expectations")


def test_great_expectations_operator__valid_expectation_suite():
    operator = GreatExpectationsOperator(
        task_id="task_id",
        expectation_suite_name="taxi.demo",
        batch_kwargs={"path": good_data_file, "datasource": "data__dir"},
        data_context_root_dir=ge_root_dir,
    )
    result = operator.execute(context={})
    logger.info(result)
    assert result["success"] == True


def test_great_expectations_operator__invalid_expectation_suite():
    operator = GreatExpectationsOperator(
        task_id="task_id",
        expectation_suite_name="invalid-expectation-suite.name",
        batch_kwargs={"path": good_data_file, "datasource": "data__dir"},
        data_context_root_dir=ge_root_dir,
    )
    with pytest.raises(GreatExpectationsError):
        operator.execute(context={})


def test_great_expectations_operator__valid_checkpoint():
    @contextmanager
    def temp_open(path, mode):
        file = open(path, mode)
        try:
            yield file
        finally:
            file.close()
            os.remove(path)

    temp_yaml = """validation_operator_name: action_list_operator
batches:
  - batch_kwargs:
      path: {base_path}/include/data/yellow_tripdata_sample_2019-01.csv
      datasource: data__dir
      data_asset_name: yellow_tripdata_sample_2019-01
    expectation_suite_names: # one or more suites may validate against a single batch
      - taxi.demo
""".format(
        base_path=base_path
    )
    with temp_open(
        path=os.path.join(ge_root_dir, "checkpoints", "taxi.temp.chk.yml"),
        mode="w+",
    ) as temp_yaml_file:
        temp_yaml_file.write(temp_yaml)
        temp_yaml_file.close()
        operator = GreatExpectationsOperator(
            task_id="task_id",
            checkpoint_name="taxi.temp.chk",
            data_context_root_dir=ge_root_dir,
        )
        result = operator.execute(context={})
        logger.info(result)
        assert result["success"] == True


def test_great_expectations_operator__invalid_checkpoint():
    operator = GreatExpectationsOperator(
        task_id="task_id",
        checkpoint_name="invalid-checkpoint.name",
        data_context_root_dir=ge_root_dir,
    )
    with pytest.raises(GreatExpectationsError):
        operator.execute(context={})


def test_great_expectations_operator__validation_failure_raises_exc():
    operator = GreatExpectationsOperator(
        task_id="task_id",
        expectation_suite_name="taxi.demo",
        batch_kwargs={"path": bad_data_file, "datasource": "data__dir"},
        data_context_root_dir=ge_root_dir,
    )
    with pytest.raises(AirflowException):
        operator.execute(context={})


def test_great_expectations_operator__validation_failure_logs_warning(caplog):
    operator = GreatExpectationsOperator(
        task_id="task_id",
        expectation_suite_name="taxi.demo",
        batch_kwargs={"path": bad_data_file, "datasource": "data__dir"},
        data_context_root_dir=ge_root_dir,
        fail_task_on_validation_failure=False,
    )
    operator._log = logging.getLogger("my_test_logger")
    caplog.set_level(level="WARNING", logger="my_test_logger")
    caplog.clear()
    result = operator.execute(context={})
    assert result["success"] == False
    assert ("my_test_logger", logging.WARNING) in (
        (r.name, r.levelno) for r in caplog.records
    )


def test_great_expectations_operator__validation_failure_callback():
    my_callback = mock.MagicMock()
    operator = GreatExpectationsOperator(
        task_id="task_id",
        expectation_suite_name="taxi.demo",
        batch_kwargs={"path": bad_data_file, "datasource": "data__dir"},
        data_context_root_dir=ge_root_dir,
        fail_task_on_validation_failure=False,
        validation_failure_callback=my_callback,
    )
    result = operator.execute(context={})
    assert result["success"] == False
    my_callback.assert_called_once_with(result)
