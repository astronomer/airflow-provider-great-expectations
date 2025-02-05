# Apache Airflow Provider for Great Expectations
A set of Airflow operators for [Great Expectations](https://greatexpectations.io/), a Python library for testing and validating data.

### Version Warning:
Due to apply_default decorator removal, this version of the provider requires Airflow 2.1.0+. If your Airflow version is < 2.1.0, and you want to install this provider version, first upgrade Airflow to at least version 2.1.0. Otherwise, your Airflow package version will be upgraded automatically, and you will have to manually run airflow upgrade db to complete the migration.

### Notes on compatibility
* This operator uses Great Expectations v1.3.5 and above.

This package has been most recently unit tested with `apache-airflow=2.4.3` and `great-expectation=1.3.5`.

[comment]: <> (The example DAG has been most recently tested in the `quay.io/astronomer/astro-runtime:6.0.4` Docker image using the [Astro CLI]&#40;https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart&#41;, with `great-expectation=0.15.34` and `SQLAlchemy=1.4.44`)


## Installation

Pre-requisites: An environment running `great-expectations` and `apache-airflow`- these are requirements of this package that will be installed as dependencies.

```
pip install airflow-provider-great-expectations
```
The following backends are supported by Great Expectations as optional dependencies:
- `athena`
- `azure`
- `bigquery`
- `gcp`
- `mssql`
- `postgresql`
- `s3`
- `snowflake`
- `spark`

For example, to install the optional Snowflake dependency, use the following command:
```
pip install "airflow-provider-great-expectations[snowflake]"
```

## Usage

### GXValidateDataFrameOperator
```python
from great_expectations_provider.operators.validate_dataframe import (
    GXValidateDataFrameOperator,
)
```
This Operator has the simplest API. The user is responsible for loading data into a DataFrame, and
GX validates it against the provided Expectations. It has two required parameters:
- `configure_dataframe` is a function that returns a DataFrame. This is how you pass your data to the Operator.
- `expect` is either a single Expectation or an ExpectationSuite

Optionally, you can also pass a `result_format` parameter to control the verbosity of the output.
Param `context_type` allows you to specify the type of DataContext used by the Operator.
The default value `ephemeral` uses an EphemeralDataContext, which does not persist configuration between runs.
You can also use `cloud` to store operator configuration and validation results in GX Cloud.
Visit `https://app.greatexpectations.io/` to create a free account and get your cloud credentials.
To use the Operator in cloud mode, you must set the following Airflow variables:
- `GX_CLOUD_ACCESS_TOKEN`
- `GX_CLOUD_ORGANIZATION_ID`

The GXValidateDataFrameOperator will return a serialized ExpectationSuiteValidationResult.

### GXValidateBatchOperator
```python
from great_expectations_provider.operators.validate_batch import GXValidateBatchOperator
```
This Operator is similar to the GXValidateDataFrameOperator, except that GX is responsible
for loading the data. The Operator can load and validate data from any data source
supported by GX.
Its required parameters are:
- `configure_batch_definition` is a function that takes a single argument, a DataContext, and returns a BatchDefinition. This is how you configure GX to read your data.
- `expect` is either a single Expectation or an ExpectationSuite

Optionally, you can also pass a `result_format` parameter to control the verbosity of the output, and `batch_parameters` to specify a batch of data at runtime.
Param `context_type` allows you to specify the type of DataContext used by the Operator.
The default value `ephemeral` uses an EphemeralDataContext, which does not persist configuration between runs.
You can also use `cloud` to store operator configuration and validation results in GX Cloud.
Visit `https://app.greatexpectations.io/` to create a free account and get your cloud credentials.
To use the Operator in cloud mode, you must set the following Airflow variables:
- `GX_CLOUD_ACCESS_TOKEN`
- `GX_CLOUD_ORGANIZATION_ID`

The GXValidateBatchOperator will return a serialized ExpectationSuiteValidationResult.

### GXValidateCheckpointOperator
```python
from great_expectations_provider.operators.validate_checkpoint import (
    GXValidateCheckpointOperator,
)
```
This Operator can take advantage of all the features of GX. The user configures a `Checkpoint`,
which orchestrates a `BatchDefinition`, `ValidationDefinition`, and `ExpectationSuite`.
Actions can also be triggered after a Checkpoint run, which can send Slack messages,
MicrosoftTeam messages, email alerts, and more.
It has a single required parameter:
- `configure_checkpoint` is a function that takes a single argument, a DataContext, and returns a Checkpoint.

Optionally, you can pass in `batch_parameters` to specify a batch of data at runtime.
Param `context_type` allows you to specify the type of DataContext used by the Operator.
The default value `ephemeral` uses an EphemeralDataContext, which does not persist configuration between runs.
You can also use `cloud` to store operator configuration and validation results in GX Cloud.
Visit `https://app.greatexpectations.io/` to create a free account and get your cloud credentials.
To use the Operator in cloud mode, you must set the following Airflow variables:
- `GX_CLOUD_ACCESS_TOKEN`
- `GX_CLOUD_ORGANIZATION_ID`

Additionally, the GXValidateCheckpointOperator can be used with a FileDataContext.
To use a FileDataContext, pass the `configure_file_data_context` parameter, which is a function that returns a FileDataContext.
GX will automatically write results back to the configuration directory. If you are retrieving your FileDataContext
from a remote location, you can `yield` the FileDataContext in the `configure_file_data_context` function, and write
the directory back to the remote after control is returned to the generator.
The GXValidateCheckpointOperator will return a serialized CheckpointResult.


## Examples

See the [**example_dags**](https://github.com/great-expectations/airflow-provider-great-expectations/tree/main/great_expectations_provider/example_dags) directory for an example DAG with some sample tasks that demonstrate operator functionality.

The example DAG can be exercised in one of two ways:

**With the open-source Astro CLI (recommended):**
1. Initialize a project with the [Astro CLI](https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart)
2. Copy the example DAG into the `dags/` folder of your astro project
3. Copy the directories in the `include` folder of this repository into the `include` directory of your Astro project
4. Add the following to your `Dockerfile` to install the `airflow-provider-great-expectations` package, and add the required Airflow variables and connection to run the example DAG:

   ```
   RUN pip install --user airflow_provider_great_expectations
   ```

6. Run `astro dev start` to view the DAG on a local Airflow instance (you will need Docker running)

**With a vanilla Airflow installation**:
1. Add the example DAG to your `dags/` folder
2. Copy the `include/data` directory into your environment
3. Set the appropriate Airflow variables and connection as detailed in the above instructions for using the `astro` CLI

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
It is included as a dependency when installing the package with `pip install "airflow-provider-great-expectations[tests]"`.

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
