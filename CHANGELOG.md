# Apache Airflow Provider for Great Expectations

## Upcoming
* (please add here)

## 0.2.6
* [FEATURE] Use Snowflake provider to build connection string by @ivanstillfront in https://github.com/astronomer/airflow-provider-great-expectations/pull/98
* [BUGFIX] Snowflake Region Should be Optional by @mpgreg in https://github.com/astronomer/airflow-provider-great-expectations/pull/101
* [MAINTENANCE] Update deprecated map_metric import in custom  by @cdkini in https://github.com/astronomer/airflow-provider-great-expectations/pull/99
* [FEATURE] Add Athena Connection Support by @denimalpaca in https://github.com/astronomer/airflow-provider-great-expectations/pull/91

## 0.2.5
* [BUGFIX] Schema variable update fixes Issues #94 and #87 by @mpgreg in https://github.com/astronomer/airflow-provider-great-expectations/pull/95
* [FEATURE] Snowflake key auth by @ivanstillfront in https://github.com/astronomer/airflow-provider-great-expectations/pull/93

## 0.2.4
* [MAINTENANCE] Simplify example path manipulation with `pathlib` (#77)
* [BUGFIX] Change kwargs in Snowflake URI generation (#84)
* [FEATURE] Add data asset name guard (#85)

## 0.2.3
* [FEATURE] Add schema parameter and data_asset_name parsing (#75)
* [MAINTENANCE] Fixes `README.md` errors when building the package (#80)
* [BUGFIX] Remove unnecessary ValueError (#82)

## 0.2.2
* [BUGFIX] Fix template extension bug (#64)

## 0.2.1
* [BUGFIX] Add back in removed template fields

## 0.2.0
* [FEATURE] Enable the use of a default Checkpoint when a Checkpoint is not supplied, with the option to use the OpenLineage Validation Action
* [FEATURE] Add support to use Airflow connection information instead of separate Great Expectation Datasources.
* [FEATURE] For cloud-store Data Docs, provide a link to Data Docs from Airflow
* [MAINTENANCE] Improvements to failure logging and testing

## 0.1.5
* [MAINTENANCE] Remove upper pin for SqlAlchemy to enable use with later versions of apache-airflow

## 0.1.4
* [BUGFIX] Fix bug around the instantiation of Checkpoints from CheckpointConfig
* [DOCS] Add test and example demonstrating the use of custom expectations in an Airflow pipeline

## 0.1.3
* [FEATURE] Improve performance by moving DataContext and Checkpoint initialization to Operator.execute() - thanks @denimalpaca

## 0.1.2
* [BUGFIX] Fix error with the instantiation of a Checkpoint from a CheckpointConfig

## 0.1.1
* [BUGFIX] Resolve dependency resolution conflict with Astronomer Docker image

## 0.1.0
* [FEATURE] Update Operator to work with Great Expectations V3 API - thanks @denimalpaca @josh-fell @jeffkpayne!
* [FEATURE] Combine BigQuery Operator with general Great Expectations Operator
* [MAINTENANCE] Update required Airflow version to Airflow 2.1

## 0.0.8
* [FEATURE] Add explicit Airflow version handling - thanks, @jeffkpayne!
* [FEATURE] Add example DAG setup conveniences - thanks, @jeffkpayne!
* [FEATURE] Defer initialization of DataContext and other resources - thanks, @jeffkpayne!
* [DOCS] Update README with virtual env and testing process - thanks, @jeffkpayne!
* [MAINTENANCE] Mark additional vars templated in BigQuery operator - thanks, @jeffkpayne!
* [MAINTENANCE] Add type hints and update docstrings - thanks, @jeffkpayne!
* [MAINTENANCE] Add unit and integration test coverage - thanks, @jeffkpayne!

## 0.0.7
* [BUGFIX] Addressed a bug whereby the fail_task_on_validation_failure in the BigQuery operator was being shadowed by the parent class.
* [ENHANCEMENT] Add support for validation operators when running LegacyCheckpoints with the GreatExpectationsOperator
* [DOCS] Fixed typos in documentation - thanks @armandduijn and @sunkickr!
* [MAINTENANCE] Make some improvements to the package by updating setup.py dependencies, exposing the example_dags within that, and adding an __init__.py to the example DAG directory - thanks @petedejoy and @pgzmnk!

## 0.0.6
* [BUGFIX] Update setup.py with appropriate versions

## 0.0.5
* [BREAKING] Updated GreatExpectations operator to work with class-based Checkpoints (Great Expectations >= 0.13.9)
* [ENHANCEMENT] Restructured project, provider metadata, examples, added "how to run" steps to README, and added tests

## 0.0.4
* [ENHANCEMENT] Adding BigQuery operator for easier use with BigQuery and Google Cloud Storage

## 0.0.3
* [ENHANCEMENT] Adding template fields
* [ENHANCEMENT] Adding fail callback function parameter

## 0.0.2
* [ENHANCEMENT] Updated Great Expectations examples

## 0.0.1
* Initial release of the provider
