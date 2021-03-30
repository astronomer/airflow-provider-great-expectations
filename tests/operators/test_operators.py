"""
Unittest module to test Great Expectations Operators.

Requires the unittest, pytest, and requests-mock Python libraries.
pip install google-cloud-storage

Run test:

    python3 -m unittest tests.operators.test_operators.TestGreatExpectationsOperator

"""

from pathlib import Path
import logging
import os
import pytest
import requests_mock
import unittest
from unittest import mock

# Import Operator as module
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator


log = logging.getLogger(__name__)

# Set relative paths for Great Expectations directory and sample data
base_path = Path(__file__).parents[2]
data_file = os.path.join(base_path,
                         'include',
                         'data/yellow_tripdata_sample_2019-01.csv')
ge_root_dir = os.path.join(base_path, 'include', 'great_expectations')

# @mock.patch.dict('os.environ', AIRFLOW_CONN_MY_BIGQUERY_CONN_ID='http://API_KEY:API_SECRET@?extra__google_cloud_platform__key_path=extra__google_cloud_platform__key_path')
class TestGreatExpectationsOperator(unittest.TestCase):
    """ 
    Test functions for GreatExpectationsOperator Operator. 
    """

    def test_great_expectations_operator_batch_kwargs_success(self):

        operator = GreatExpectationsOperator(
            task_id='ge_batch_kwargs_pass',
            expectation_suite_name='taxi.demo',
            batch_kwargs={
                'path': data_file,
                'datasource': 'data__dir'
            },
            data_context_root_dir=ge_root_dir
        )

        result = operator.execute({})
        log.info(result)

        self.assertTrue(result['success'])

if __name__ == '__main__':
    unittest.main()
