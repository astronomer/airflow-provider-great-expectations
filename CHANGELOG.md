# Apache Airflow Provider for Great Expectations

## Upcoming 
* (please add here)

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
