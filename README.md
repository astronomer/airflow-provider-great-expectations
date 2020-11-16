# Apache Airflow Provider for Great Expectations

**Early development as of 11/05/2020**

An Airflow operator for [Great Expectations](greatexpectations.io), a Python library for testing and validating data.

## Installation

Pre-requisites: An environment running `great_expectations` and `apache-airflow`, of course.

```
pip install airflow-provider-great-expectations
```

## Modules

[Great Expectations Operator](./great_expectations_provider/operators/great_expectations.py): A base operator for Great Expectations. Import into your DAG via: 

```
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
```

## Examples

See the [**examples**](./great_expectations_provider/examples) directory for an example DAG with some sample tasks that demonstrate operator functionality. The example DAG file contains a comment with instructions on how to run the examples.

**This operator is in very early stages of development! Feel free to submit issues, PRs, or ping the current author (Sam Bail) in the Great Expectations Slack for feedback.