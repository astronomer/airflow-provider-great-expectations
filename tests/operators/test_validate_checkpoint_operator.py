import pytest
from great_expectations.data_context import AbstractDataContext, CloudDataContext
from great_expectations import Checkpoint

from great_expectations_provider.operators.validate_checkpoint import (
    ValidateCheckpointOperator,
)


class TestCloud:
    @pytest.fixture
    def context(self) -> CloudDataContext: ...

    @pytest.fixture
    def setup_checkpoint(self) -> str: ...

    def test_validate_checkpoint_success(self):
        # for now, just plugging in my cloud creds and running against a preconfigured checkpoint
        def configure_cloud_checkpoint(context: AbstractDataContext) -> Checkpoint:
            checkpoint_name = "top_running_performances LI787A - Default Checkpoint"
            return context.checkpoints.get(name=checkpoint_name)

        validate_cloud_checkpoint = ValidateCheckpointOperator(
            task_id="validate_cloud_checkpoint",
            configure_checkpoint=configure_cloud_checkpoint,
            context_type="cloud",
        )
        result = validate_cloud_checkpoint.execute(context={})
        assert result["success"]
