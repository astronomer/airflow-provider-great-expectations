# Apache Airflow Provider for Great Expectations
A set of Airflow operators for [Great Expectations](https://greatexpectations.io/), a Python library for testing and validating data.

### Version Warning:
Due to apply_default decorator removal, this version of the provider requires Airflow 2.1.0+. If your Airflow version is < 2.1.0, and you want to install this provider version, first upgrade Airflow to at least version 2.1.0. Otherwise, your Airflow package version will be upgraded automatically, and you will have to manually run airflow upgrade db to complete the migration.

### Notes on compatibility
* This operator currently works with the Great Expectations V3 Batch Request API only. If you would like to use the operator in conjunction with the V2 Batch Kwargs API, you must use a version below 0.1.0
* This operator uses Great Expectations Checkpoints instead of the former ValidationOperators.
* Because of the above, this operator requires Great Expectations >=v0.13.9, which is pinned in the requirements.txt starting with release 0.0.5.
* Great Expectations version 0.13.8 contained a bug that would make this operator not work.
* Great Expectations version 0.13.7 and below will work with version 0.0.4 of this operator and below.

This package has been most recently unit tested with `apache-airflow=2.4.3` and `great-expectation=0.15.34`.

[comment]: <> (The example DAG has been most recently tested in the `quay.io/astronomer/astro-runtime:6.0.4` Docker image using the [Astro CLI]&#40;https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart&#41;, with `great-expectation=0.15.34` and `SQLAlchemy=1.4.44`)

**Formerly, there was a separate operator for BigQuery, to facilitate the use of GCP stores. This functionality is now baked into the core Great Expectations library, so the generic Operator will work with any back-end and SQL dialect for which you have a working Data Context and Datasources.**


## Installation

Pre-requisites: An environment running `great-expectations` and `apache-airflow`- these are requirements of this package that will be installed as dependencies.

```
pip install airflow-provider-great-expectations
```

Depending on your use-case, you might need to add `ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=true` to your Dockerfile to enable XCOM to pass data between tasks.

## Usage

The operator requires a DataContext to run which can be specified either as:
   1. A path to a directory in which a yaml-based DataContext configuration is located
   2. A Great Expectations DataContextConfig object

Additonally, a Checkpoint may be supplied, which can be specified either as:
   1. The name of a Checkpoint already located in the Checkpoint Store of the specified DataContext
   2. A Great Expectations CheckpointConfig object

Although if no Checkpoint is supplied, a default one will be built.

The operator also enables you to pass in a Python dictionary containing kwargs which will be added/substituted to the Checkpoint at runtime.

## Modules

[Great Expectations Base Operator](https://github.com/great-expectations/airflow-provider-great-expectations/blob/main/great_expectations_provider/operators/great_expectations.py): A base operator for Great Expectations. Import into your DAG via:

```
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
```

### Previously Available Email Alert Functionality

The email alert functionality available in version `0.0.7` has been removed, in order to keep the purpose of the operator more narrow and related to running the Great Expectations validations, etc.  There is now a `validation_failure_callback` parameter to the base operator's constructor, which can be used for any kind of notification upon failure, given that the notification mechanisms provided by the Great Expectations framework itself doesn't suffice.

## Examples

See the [**example_dags**](https://github.com/great-expectations/airflow-provider-great-expectations/tree/main/great_expectations_provider/example_dags) directory for an example DAG with some sample tasks that demonstrate operator functionality.

The example DAG can be exercised in one of two ways:

**With the open-source Astro CLI (recommended):**
1. Initialize a project with the [Astro CLI](https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart)
2. Copy the example DAG into the `dags/` folder of your astro project
3. Copy the directories in the `include` folder of this repository into the `include` directory of your Astro project
4. Copy your GCP `credentials.json` file into the base directory of your Astro project
5. Add the following to your `Dockerfile` to install the `airflow-provider-great-expectations` package, enable xcom pickling, and add the required Airflow variables and connection to run the example DAG:

   ```
   RUN pip install --user airflow_provider_great_expectations
   ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
   ENV GOOGLE_APPLICATION_CREDENTIALS=/usr/local/airflow/credentials.json
   ENV AIRFLOW_VAR_MY_PROJECT=<YOUR_GCP_PROJECT_ID>
   ENV AIRFLOW_VAR_MY_BUCKET=<YOUR_GCS_BUCKET>
   ENV AIRFLOW_VAR_MY_DATASET=<YOUR_BQ_DATASET>
   ENV AIRFLOW_VAR_MY_TABLE=<YOUR_BQ_TABLE>
   ENV AIRFLOW_CONN_MY_BIGQUERY_CONN_ID='google-cloud-platform://?extra__google_cloud_platform__scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fbigquery&extra__google_cloud_platform__project=bombora-dev&extra__google_cloud_platform__key_path=%2Fusr%2Flocal%2Fairflow%2Fairflow-gcp.bombora-dev.iam.gserviceaccount.com.json'
   ```

6. Run `astro dev start` to view the DAG on a local Airflow instance (you will need Docker running)

**With a vanilla Airflow installation**:
1. Add the example DAG to your `dags/` folder
2. Make the `great_expectations` and `data` directories in `include/` available in your environment.
3. Change the `data_file` and `ge_root_dir` paths in your DAG file to point to the appropriate places.
4. Change the paths in `great-expectations/checkpoints/*.yml` to point to the absolute path of your data files.
5. Change the value of [`enable_xcom_pickling`](https://github.com/apache/airflow/blob/master/airflow/config_templates/default_airflow.cfg#L181) to `true` in your airflow.cfg
6. Set the appropriate Airflow variables and connection as detailed in the above instructions for using the `astro` CLI

## Development

### Setting Up the Virtual Environment

Any virtual environment tool can be used, but the simplest approach is likely using the `venv` tool included
in the Python standard library.

For example, creating a virtual environment for development against this package can be done with the following
(assuming `bash`):

```
# Create the virtual environment using venv:
$ python -m venv --prompt my-af-ge-venv .venv

# Activate the virtual environment:
$ . .venv/bin/activate

# Install the package and testing dependencies:
(my-af-ge-venv) $ pip install -e '.[tests]'
```

### Running Unit, Integration, and Functional Tests

Once the above is done, running the unit and integration tests can be done with either of the following approaches.

#### Using `pytest`

The `pytest` library and CLI is preferred by this project, and many Python developers, because of its
rich API, and the additional control it gives you over things like test output, test markers, etc.
It is included as a dependency in `requirements.txt`.

The simple command `pytest -p no:warnings`, when run in the virtual environment created with the above
process, provides a concise output when all tests pass, filtering out deprecation warnings that may be
issued by Airflow, and a only as detailed as necessary output when they dont:

```
(my-af-ge-venv) $ pytest -p no:warnings
=========================================================================================== test session starts ============================================================================================
platform darwin -- Python 3.7.4, pytest-6.2.4, py-1.10.0, pluggy-0.13.1
rootdir: /Users/jpayne/repos-bombora/bombora-airflow-provider-great-expectations, configfile: pytest.ini, testpaths: tests
plugins: anyio-3.3.0
collected 7 items

tests/operators/test_great_expectations.py .......                                                                                                                                                   [100%]

============================================================================================ 7 passed in 11.99s ============================================================================================
```

#### Functional Testing

Functional testing entails simply running the example DAG using, for instance, one of the approaches outlined above, only with the adjustment that the local development package be installed in the target Airflow environment.

Again, the recommended approach is to use the [Astro CLI](https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart)

**This operator is in early stages of development! Feel free to submit issues, PRs, or join the #integration-airflow channel in the [Great Expectations Slack](http://greatexpectations.io/slack) for feedback. Thanks to [Pete DeJoy](https://github.com/petedejoy) and the [Astronomer.io](https://www.astronomer.io/) team for the support.
