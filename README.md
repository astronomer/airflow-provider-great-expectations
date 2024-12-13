# gx-airflow-provider
The GX Airflow Provider has three Operators to validate Expectations against your data.

## ValidateDataFrameOperator
This Operator has the simplest API. The user is responsible for loading data into a DataFrame, and
GX validates it against the provided Expectations. It has two required parameters:
- `configure_dataframe` is a function that returns a DataFrame. This is how you pass your data to the Operator.
- `expect` is either a single Expectation or an ExpectationSuite
Optionally, you can also pass a `result_format` parameter to control the verbosity of the output.
The ValidateDataFrameOperator will return a serialized ExpectationValidationResult, or ExpectationSuiteValidationResult.

## ValidateBatchOperator
This Operator is similar to the ValidateDataFrameOperator, except that GX is responsible
for loading the data. The Operator can load and validate data from any data source
supported by GX. 
Its required parameters are:
- `configure_batch_definition` is a function that takes a single argument, a DataContext, and returns a BatchDefinition. This is how you configure GX to read your data.
- `expect` is either a single Expectation or an ExpectationSuite
Optionally, you can also pass a `result_format` parameter to control the verbosity of the output, and
`batch_parameters` to specify a specific Batch of data at runtime. 
The ValidateBatchOperator will return a serialized ExpectationValidationResult, or ExpectationSuiteValidationResult.

## ValidateCheckpointOperator
This Operator can take advantage of all the features of GX. The user configures a `Checkpoint`,
which orchestrates a `BatchDefinition`, `ValidationDefinition`, and `ExpectationSuite`.
Actions can also be triggered after a Checkpoint run, which can send Slack messages, 
MicrosoftTeam messages, email alerts, and more.
It has a single required parameter:
- `configure_checkpoint` is a function that takes a single argument, a DataContext, and returns a Checkpoint. 
Optionally, you can pass in `batch_parameters` to specify a specific Batch of data at runtime. 
The ValidateCheckpointOperator will return a serialized CheckpointResult.
