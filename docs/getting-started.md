# Getting started

[Great Expectations](https://greatexpectations.io/) (GX) is a framework for describing data using expressive tests and then validating that the data meets test criteria. [Astronomer](https://www.astronomer.io/) maintains the Great Expectations Airflow Provider to give users a convenient method for running validations directly from their DAGs. The Great Expectations Airflow Provider has three Operators to choose from, which vary in the amount of configuration they require and the flexibility they provide.

- `GXValidateDataFrameOperator`
- `GXValidateBatchOperator`
- `GXValidateCheckpointOperator`


## Operator use cases

When deciding which Operator best fits your use case, consider the location of the data you are validating, whether or not you need external alerts or actions to be triggered by the Operator, and what Data Context you will use. When picking a Data Context, consider whether or not you need to view how results change over time.

- If your data is in memory as a Spark or Pandas DataFrame, we recommend using the `GXValidateDataFrameOperator`. This option requires only a DataFrame and your Expectations to create a validation result.
- If your data is not in memory, we recommend configuring GX to connect to it by defining a BatchDefinition with the `GXValidateBatchOperator`. This option requires a BatchDefinition and your Expectations to create a validation result.
- If you want to [trigger actions](https://docs.greatexpectations.io/docs/core/trigger_actions_based_on_results/create_a_checkpoint_with_actions) based on validation results, use the `GXValidateCheckpointOperator`. This option supports all features of GX Core, so it requires the most configuration - you have to define a  Checkpoint, BatchDefinition, ExpectationSuite, and ValidationDefinition to get validation results.

The Operators vary in which [Data Contexts](https://docs.greatexpectations.io/docs/core/set_up_a_gx_environment/create_a_data_context) they support. All 3 Operators support Ephemeral and GX Cloud Data Contexts. Only the `GXValidateCheckpointOperator` supports the File Data Context.

- If the results are used only within the Airflow DAG by other tasks, we recommend using an Ephemeral Data Context. The serialized Validation Result will be available within the DAG as the task result, but will not persist externally for viewing the results across multiple runs. All 3 Operators support the Ephemeral Data Context.
- To persist and view results outside of Airflow, we recommend using a Cloud Data Context. Validation Results are automatically visible in the GX Cloud UI when using a Cloud Data Context, and the task result contains a link to the stored validation result. All 3 Operators support the Cloud Data Context.
- If you want to manage Validation Results yourself, use a File Data Context. With this option, Validation Results can be viewed in [Data Docs](https://docs.greatexpectations.io/docs/core/configure_project_settings/configure_data_docs/). Only the `GXValidateCheckpointOperator` supports the File Data Context.

## Prerequisites

- [Python](https://www.python.org/) version 3.9 to 3.12
- [Great Expectations](https://docs.greatexpectations.io/docs/core/set_up_a_gx_environment/install_gx) version 1.3.11+
- [Apache Airflow®](https://airflow.apache.org/) version 2.1.0+

## Assumed knowledge

To get the most out of this getting started guide, make sure you have an understanding of:

- The basics of Great Expectations. See [Try GX Core](https://docs.greatexpectations.io/docs/core/introduction/try_gx/).
- Airflow fundamentals, such as writing DAGs and defining tasks. See [Get started with Apache Airflow](https://www.astronomer.io/docs/learn/get-started-with-airflow/).
- Airflow Operators. See [Operators 101](https://www.astronomer.io/docs/learn/what-is-an-operator/).
- Airflow connections. See [Managing your Connections in Apache Airflow](https://www.astronomer.io/docs/learn/connections/).

## Install the provider and dependencies

1. Install the provider.

   ```bash
   pip install airflow-provider-great-expectations
   ```
2. (Optional) Install additional dependencies for the data sources you’ll use. For example, to install the optional Snowflake dependency, use the following command.

   ```bash
   pip install "airflow-provider-great-expectations[snowflake]"
   ```
   The following backends are supported as optional dependencies:
    - `athena`
    - `azure`
    - `bigquery`
    - `gcp`
    - `mssql`
    - `postgresql`
    - `s3`
    - `snowflake`
    - `spark`

## Configure an Operator

After deciding [which Operator best fits your use case](#operator-use-cases), follow the Operator-specific instructions below to configure it.

### Data Frame Operator

1. Import the Operator.

    ```python
    from great_expectations_provider.operators.validate_dataframe import (
        GXValidateDataFrameOperator,
    )
    ```

2. Instantiate the Operator with required and optional parameters.

    ```python
    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from pandas import DataFrame

    def my_data_frame_configuration(): DataFrame:
        import pandas as pd  # airflow best practice is to not import heavy dependencies in the top level
        return pd.read_csv(my_data_file)

    my_data_frame_operator = GXValidateDataFrameOperator(
        task_id="my_data_frame_operator",
        configure_dataframe=my_data_frame_configuration,
        expect=my_expectation_suite,
    )
    ```

    - **`task_id`**: alphanumeric name used in the Airflow UI and GX Cloud.
    - **`configure_dataframe`**: function that returns a DataFrame to pass data to the Operator.
    - **`expect`**: either a [single Expectation](https://docs.greatexpectations.io/docs/core/define_expectations/create_an_expectation) or an [Expectation Suite](https://docs.greatexpectations.io/docs/core/define_expectations/organize_expectation_suites) to validate against your data.
    - **`result_format` (optional)**: accepts `BOOLEAN_ONLY`, `BASIC`, `SUMMARY`, or `COMPLETE` to set the [verbosity of returned Validation Results](https://docs.greatexpectations.io/docs/core/trigger_actions_based_on_results/choose_a_result_format/). Defaults to `SUMMARY`.
    - **`context_type` (optional)**: accepts `ephemeral` or `cloud` to set the [Data Context](https://docs.greatexpectations.io/docs/core/set_up_a_gx_environment/create_a_data_context) used by the Operator. Defaults to `ephemeral`, which does not persist results between runs. To save and view Validation Results in GX Cloud, use `cloud` and complete the additional Cloud Data Context configuration below.

   For more details, explore this [end-to-end code sample](https://github.com/astronomer/airflow-provider-great-expectations/tree/docs/great_expectations_provider/example_dags/example_great_expectations_dag.py#L134-L138).

3. If you use a Cloud Data Context, create a [free GX Cloud account](https://app.greatexpectations.io/) to get your [Cloud credentials](https://docs.greatexpectations.io/docs/cloud/connect/connect_python#get-your-user-access-token-and-organization-id) and then set the following Airflow variables.

    - `GX_CLOUD_ACCESS_TOKEN`
    - `GX_CLOUD_ORGANIZATION_ID`


### Batch Operator

1. Import the Operator.

    ```python
    from great_expectations_provider.operators.validate_batch import (
        GXValidateBatchOperator,
    )
    ```

2. Instantiate the Operator with required and optional parameters.

    ```python
    my_batch_operator = GXValidateBatchOperator(
        task_id="my_batch_operator",
        configure_batch_definition=my_batch_definition_function,
        expect=my_expectation_suite,
    )
    ```

    - **`task_id`**: alphanumeric name used in the Airflow UI and GX Cloud.
    - **`configure_batch_definition`**: function that returns a [BatchDefinition](https://docs.greatexpectations.io/docs/core/connect_to_data/filesystem_data/#create-a-batch-definition) to configure GX to read your data.
    - **`expect`**: either a [single Expectation](https://docs.greatexpectations.io/docs/core/define_expectations/create_an_expectation) or an [Expectation Suite](https://docs.greatexpectations.io/docs/core/define_expectations/organize_expectation_suites) to validate against your data.
    - **`batch_parameters` (optional)**: dictionary that specifies a [time-based Batch of data](https://docs.greatexpectations.io/docs/core/define_expectations/retrieve_a_batch_of_test_data) to validate your Expectations against. Defaults to the first valid Batch found, which is the most recent Batch (with default sort ascending) or the oldest Batch if the Batch Definition has been configured to sort descending.
    - **`result_format` (optional)**: accepts `BOOLEAN_ONLY`, `BASIC`, `SUMMARY`, or `COMPLETE` to set the [verbosity of returned Validation Results](https://docs.greatexpectations.io/docs/core/trigger_actions_based_on_results/choose_a_result_format/). Defaults to `SUMMARY`.
    - **`context_type` (optional)**: accepts `ephemeral` or `cloud` to set the [Data Context](https://docs.greatexpectations.io/docs/core/set_up_a_gx_environment/create_a_data_context) used by the Operator. Defaults to `ephemeral`, which does not persist results between runs. To save and view Validation Results in GX Cloud, use `cloud` and complete the additional Cloud Data Context configuration below.

   For more details, explore this [end-to-end code sample](https://github.com/astronomer/airflow-provider-great-expectations/tree/docs/great_expectations_provider/example_dags/example_dag_with_batch_parameters.py#L123-L127).

3. If you use a Cloud Data Context, create a [free GX Cloud account](https://app.greatexpectations.io/) to get your [Cloud credentials](https://docs.greatexpectations.io/docs/cloud/connect/connect_python#get-your-user-access-token-and-organization-id) and then set the following Airflow variables.

    - `GX_CLOUD_ACCESS_TOKEN`
    - `GX_CLOUD_ORGANIZATION_ID`

### Checkpoint Operator

1. Import the Operator.

    ```python
    from great_expectations_provider.operators.validate_checkpoint import (
        GXValidateCheckpointOperator,
    )
    ```

2. Instantiate the Operator with required and optional parameters.

    ```python
    my_checkpoint_operator = GXValidateCheckpointOperator(
        task_id="my_checkpoint_operator",
        configure_checkpoint=my_checkpoint_function,
    )
    ```

    - **`task_id`**: alphanumeric name used in the Airflow UI and GX Cloud.
    - **`configure_checkpoint`**: function that returns a [Checkpoint](https://docs.greatexpectations.io/docs/core/trigger_actions_based_on_results/create_a_checkpoint_with_actions), which orchestrates a ValidationDefinition, BatchDefinition, and ExpectationSuite. The Checkpoint can also specify a Result Format and trigger actions based on Validation Results.
    - **`batch_parameters` (optional)**: dictionary that specifies a [time-based Batch of data](https://docs.greatexpectations.io/docs/core/define_expectations/retrieve_a_batch_of_test_data) to validate your Expectations against. Defaults to the first valid Batch found, which is the most recent Batch (with default sort ascending) or the oldest Batch if the Batch Definition has been configured to sort descending.
    - **`context_type` (optional)**: accepts `ephemeral`, `cloud`, or `file` to set the [Data Context](https://docs.greatexpectations.io/docs/core/set_up_a_gx_environment/create_a_data_context) used by the Operator. Defaults to `ephemeral`, which does not persist results between runs. To save and view Validation Results in GX Cloud, use `cloud` and complete the additional Cloud Data Context configuration below. To manage Validation Results yourself, use `file` and complete the additional File Data Context configuration below.
    - **`configure_file_data_context` (optional)**: function that returns a [FileDataContext](https://docs.greatexpectations.io/docs/core/set_up_a_gx_environment/create_a_data_context?context_type=file). Applicable only when using a File Data Context. See the additional File Data Context configuration below for more information.

   For more details, explore this [end-to-end code sample](https://github.com/astronomer/airflow-provider-great-expectations/tree/docs/great_expectations_provider/example_dags/example_dag_with_batch_parameters.py#L134-L137).

3. If you use a Cloud Data Context, create a [free GX Cloud account](https://app.greatexpectations.io/) to get your [Cloud credentials](https://docs.greatexpectations.io/docs/cloud/connect/connect_python#get-your-user-access-token-and-organization-id) and then set the following Airflow variables.

    - `GX_CLOUD_ACCESS_TOKEN`
    - `GX_CLOUD_ORGANIZATION_ID`

4. If you use a File Data Context, pass the `configure_file_data_context` parameter. This takes a function that returns a [FileDataContext](https://docs.greatexpectations.io/docs/core/set_up_a_gx_environment/create_a_data_context?context_type=file). By default, GX will write results in the configuration directory. If you are retrieving your FileDataContext from a remote location, you can yield the FileDataContext in the `configure_file_data_context` function and write the directory back to the remote after control is returned to the generator.

### Manage Data Source credentials with Airflow Connections

The Great Expectations Airflow Provider includes hooks to retrieve connection credentials from third party Airflow Connections.
The following external Connections are supported:
- Redshift
- MySQL
- MSSQL
- PostgreSQL
- Snowflake (Connection String and Key Pair Authentication)
- BigQuery
- Sqlite
- AWS Athena

To use these hooks, first install the Airflow Provider that maintains the connection you need, 
and use the Airflow UI to configure the Connection with your credentials.
Then, these hooks can then be called within your `configure_batch_definition` or `configure_checkpoint` functions.


```python
from __future__ import annotations

from typing import TYPE_CHECKING

from great_expectations_provider.hooks.external_connections import (
    build_postgres_connection_string,
)

if TYPE_CHECKING:
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.core.batch_definition import BatchDefinition


def configure_postgres_batch_definition(
    context: AbstractDataContext,
) -> BatchDefinition:
    task_id = "example_task"
    table_name = "example_table"
    postgres_conn_id = "example_conn_id"
    return (
        context.data_sources.add_postgres(
            name=task_id,
            connection_string=build_postgres_connection_string(
                conn_id=postgres_conn_id
            ),
        )
        .add_table_asset(
            name=task_id,
            table_name=table_name,
        )
        .add_batch_definition_whole_table(task_id)
    )

```


## Add the configured Operator to a DAG

After configuring an Operator, add it to a DAG. Explore our [example DAGs](https://github.com/astronomer/airflow-provider-great-expectations/tree/docs/great_expectations_provider/example_dags), which have sample tasks that demonstrate Operator functionality.

Note that the shape of the Validation Results depends on both the Operator type and whether or not you set the optional `result_format` parameter.
- `GXValidateDataFrameOperator` and `GXValidateBatchOperator` return a serialized [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/reference/api/core/expectationsuitevalidationresult_class/)
- `GXValidateCheckpointOperator` returns a [CheckpointResult](https://docs.greatexpectations.io/docs/reference/api/checkpoint/CheckpointResult_class).
- The included fields depend on the [Result Format verbosity](https://docs.greatexpectations.io/docs/core/trigger_actions_based_on_results/choose_a_result_format/?results=verbosity#validation-results-reference-tables).

## Run the DAG

[Trigger the DAG manually](https://www.astronomer.io/docs/learn/get-started-with-airflow#step-7-run-the-new-dag) or [run it on a schedule](https://www.astronomer.io/docs/learn/scheduling-in-airflow/) to start validating your expectations of your data.
