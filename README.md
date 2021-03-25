# Apache Airflow Provider for Great Expectations

**This is an experimental library as of March 2021! The Great Expectations core team maintains this provider in an experimental state and does not guarantee ongoing support yet.**

An Airflow operator for [Great Expectations](greatexpectations.io), a Python library for testing and validating data.


###Notes on compatibility 

* This operator has been updated to use Great Expectations Checkpoints instead of the former ValidationOperators. Therefore, it requires Great Expectations >=v0.13.9, which is pinned in the requirements.txt starting with release 0.0.5.
* Great Expectations version 0.13.8 unfortunately contained a bug that would make this operator not work.
* Great Expectations version 0.13.7 and below will work with version 0.0.4 of this operator and below.

## Installation

Pre-requisites: An environment running `great-expectations` and `apache-airflow`- these are requirements of this package that will be installed as dependencies.

```
pip install airflow-provider-great-expectations
```

In order to run the `BigQueryOperator`, you will also need to install the relevant dependencies: `pybigquery` and `apache-airflow-providers-google`

Depending on your use-case, you might need to add `ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=true` to your Dockerfile to enable XCOM to pass data between tasks.


## Modules

[Great Expectations Operator](https://github.com/great-expectations/airflow-provider-great-expectations/blob/main/great_expectations_provider/operators/great_expectations.py): A base operator for Great Expectations. Import into your DAG via: 

```
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
```


[Great Expectations BigQuery Operator](https://github.com/great-expectations/airflow-provider-great-expectations/blob/main/great_expectations_provider/operators/great_expectations_bigquery.py): An operator for Great Expectations that provides some pre-set parameters for a BigQuery Datasource and Expectation, Validation, and Data Docs stores in Google Cloud Storage. The operator can also be configured to send email on validation failure. See the docstrings in the class for more configuration options. Import into your DAG via: 

```
from great_expectations_provider.operators.great_expectations_bigquery import GreatExpectationsBigQueryOperator
```

## Examples

See the [**example_dags**](https://github.com/great-expectations/airflow-provider-great-expectations/tree/main/great_expectations_provider/example_dags) directory for an example DAG with some sample tasks that demonstrate operator functionality. The example DAG file contains a comment with instructions on how to run the examples.

To run these examples:
1. Initialize a project with the [Astro CLI](https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart)
2. Copy the example DAG into the `dags/` folder of your astro project
3. Copy the directories in the `include` folder of this repository into the `include` directory of your Astro project
4. Add `airflow-provider-great-expectations` to your `requirements.txt`
5. Run `astro dev start` to view the DAG on a local Airflow instance (you will need Docker running)

**This operator is in very early stages of development! Feel free to submit issues, PRs, or ping the current author (Sam Bail) in the [Great Expectations Slack](http://greatexpectations.io/slack) for feedback. Thanks to [Pete DeJoy](https://github.com/petedejoy) and the [Astronomer.io](https://www.astronomer.io/) team for the support.

## Constraints

This package has been most recently tested with Airflow 2.0 and Great Expectations v0.13.7. 
