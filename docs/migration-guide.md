# Migration guide

This guide will help you migrate from V0 to V1 of the Great Expectations Airflow Provider.

Here is an overview of key differences between versions:

| Provider version | V0 | V1 |
|---|---|---|
| Operators | `GreatExpectationsOperator` | `GXValidateDataFrameOperator`<br>`GXValidateBatchOperator`<br>`GXValidateCheckpointOperator` |
| GX version | 0.18 and earlier | 1.3.11 and later |
| Data Contexts | File | Ephemeral<br>Cloud<br>File (`GXValidateCheckpointOperator` only) |
| Response handling | By default, any Expectation failure raises an `AirflowException`. To override this behavior and continue running the pipeline even if tests fail, you can set the `fail_task_on_validation_failure` flag to `False`. | Regardless of Expectation failure or success, a Validation Result is made available to subsequent tasks, which can decide what to do with the result. |

For guidance on which Operator and Data Context best fit your needs, see [Operator use cases](getting-started.md/#operator-use-cases). Note that while File Data Contexts are still supported with `GXValidateCheckpointOperator`, they require extra configuration and can be challenging to use when Airflow is running in a distributed environment. Most uses of the legacy `GreatExpectationsOperator` can now be satisfied with an Ephemeral or Cloud Data Context with either the `GXValidateDataFrameOperator` or the `GXValidateBatchOperator` to minimize configuration.

## Switch to the new Data Frame or Batch Operator (recommended)

The configuration options for the new `GXValidateDataFrameOperator` and `GXValidateBatchOperator` are streamlined compared to the old `GreatExpectationsOperator`. Switching to one of these doesn’t involve translating existing configuration into new syntax line for line but rather paring back to a more minimal configuration.

- See [getting started](getting-started.md) for an overview of required configuration.
- Explore [examples](https://github.com/astronomer/airflow-provider-great-expectations/tree/docs/great_expectations_provider/example_dags) of end-to-end configuration and usage.

## Migrate to the new Checkpoint Operator

If you want to update your existing `GreatExpectationsOperator` configuration to use the new `GXValidateCheckpointOperator` with a File Data Context, follow these steps:

1. Update your GX project to be compatible with GX V1 by following the [GX V0 to V1 Migration Guide](https://docs.greatexpectations.io/docs/reference/learn/migration_guide).

2. Write a function that instantiates and returns your File Data Context.

   Here is a basic example for running Airflow in a stable file system where you can rely on your GX project being discoverable at the same place over multiple runs. This is important because GX will write Validation Results back to the project directory.

    ```
    import great_expectatations as gx
    from great_expectations.data_context import FileDataContext

    def configure_file_data_context() -> FileDataContext:
        return gx.get_context(
            mode="file",
            project_root_dir="./path_to_your_existing_project"
        )
    ```

    Here's a more advanced example for running Airflow in an environment where the underlying file system is not stable. The steps here are as follows:
    - fetch your GX project
    - load the context
    - yield the context to the Operator
    - after the Operator has finished, write your project configuration back to the remote

    Be aware that you are responsible for managing concurrency, in the case that multiple tasks are reading and writing back to the remote simultaneously.

    ```
    import great_expectatations as gx
    from great_expectations.data_context import FileDataContext

    def configure_file_data_context() -> FileDataContext:
        # load your GX project from its remote source
        yield gx.get_context(
            mode="file",
            project_root_dir="./path_to_the_local_copy_of_your_existing_project"
        )
        # write your GX project back to its remote source
    ```

3. Write a function that returns your Checkpoint.

    ```
    from great_expectations import Checkpoint
    from great_expectations.data_context import AbstractDataContext

    def configure_checkpoint(context: AbstractDataContext) -> Checkpoint:
        return context.checkpoints.get(name="<YOUR CHECKPOINT NAME>")
    ```

- See [getting started](getting-started.md) for more information about required and optional configuration.
- Explore [examples](https://github.com/astronomer/airflow-provider-great-expectations/tree/docs/great_expectations_provider/example_dags) of end-to-end configuration and usage.


## Migrate Connections

The legacy Great Expectations Airflow Provider accepted a `conn_id` argument, which
would attempt to retrieve credentials from other Airflow provider Connections. That logic
is now available as provider-specific functions in `great_expectations_provider.common.external_connections`,
and can be used when configuring your Great Expectations data source within the `configure_batch_definition` or
`configure_checkpoint` function.

Here is an example that uses the `build_snowflake_key_connection` function to connect to Snowflake using a private key.

```python
from great_expectations_provider.operators.validate_batch import GXValidateBatchOperator
from great_expectations_provider.common.external_connections import (
   build_snowflake_key_connection
)

import great_expectations as gx
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.data_context import AbstractDataContext


def my_batch_definition_function(context: AbstractDataContext) -> BatchDefinition:
   snowflake_config = build_snowflake_key_connection(conn_id="snowflake_conn_id")
   return (
      context.data_sources.add_snowflake(
         name="snowflake sandbox",
         account=snowflake_config.account,
         user=snowflake_config.user,
         role=snowflake_config.role,
         password="<PLACEHOLDER PASSWORD>",  # must be provided to pass validation but will be ignored
         warehouse=snowflake_config.warehouse,
         database=snowflake_config.database,
         schema=snowflake_config.schema,
         kwargs={"private_key": snowflake_config.private_key},
      )
      .add_table_asset(name="<SCHEMA.TABLE>", table_name="<TABLE_NAME>")
      .add_batch_definition_whole_table(  # you can also batch by year, month, or day here
         name="Whole Table Batch Definition"
      )
   )


my_expectations = gx.ExpectationSuite(
   name="<SUITE_NAME>",
   expectations=[
      # define expectations
   ],
)

my_batch_operator = GXValidateBatchOperator(
   task_id="my_batch_operator",
   configure_batch_definition=my_batch_definition_function,
   expect=my_expectations,
   context_type="ephemeral",
)
```
