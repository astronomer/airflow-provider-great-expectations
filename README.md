# great_expectations_airflow 

**Early development as of 11/05/2020**

An Airflow operator for Great Expectations (greatexpectations.io), a Python library for testing and validating data.

Install with `pip install great_expectations_airflow` and then import `GreatExpectationsOperator` in your DAG file.

Pre-requisites: Install `great_expectations` and `apache-airflow`, of course.

See the **examples** directory for an example DAG with some sample tasks that demonstrate operator functionality. The example DAG file contains a comment with instructions on how to run the examples.

**This operator is in very early stages of development! Feel free to submit issues, PRs, or ping the current author (Sam Bail) in the Great Expectations Slack for feedback.
