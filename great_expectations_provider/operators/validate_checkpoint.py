from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Callable, Generator, Literal, cast

from airflow.models import BaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context
    from great_expectations import Checkpoint
    from great_expectations.checkpoint.checkpoint import CheckpointDescriptionDict
    from great_expectations.core.batch import BatchParameters
    from great_expectations.data_context import AbstractDataContext, FileDataContext


class GXValidateCheckpointOperator(BaseOperator):
    def __init__(
        self,
        configure_checkpoint: Callable[[AbstractDataContext], Checkpoint],
        batch_parameters: BatchParameters | None = None,
        context_type: Literal["ephemeral", "cloud", "file"] = "ephemeral",
        configure_file_data_context: (
            Callable[[], FileDataContext]
            | Callable[[], Generator[FileDataContext, None, None]]
            | None
        ) = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        if batch_parameters is None:
            self.batch_parameters = {}
        else:
            self.batch_parameters = batch_parameters
        if context_type == "file" and not configure_file_data_context:
            raise ValueError(
                "Parameter `configure_file_data_context` must be specified if `context_type` is `file`"
            )
        self.context_type = context_type
        self.configure_file_data_context = configure_file_data_context
        self.configure_checkpoint = configure_checkpoint

    def execute(self, context: Context) -> CheckpointDescriptionDict:
        import great_expectations as gx
        from great_expectations.data_context import AbstractDataContext, FileDataContext

        gx_context: AbstractDataContext
        file_context_generator: Generator[FileDataContext, None, None] | None = None

        if self.context_type == "file":
            if not self.configure_file_data_context:
                raise ValueError(
                    "Parameter `configure_file_data_context` must be specified if `context_type` is `file`"
                )
            elif inspect.isgeneratorfunction(self.configure_file_data_context):
                file_context_generator = self.configure_file_data_context()
                gx_context = self._get_value_from_generator(file_context_generator)
            else:
                file_context_fn = cast(
                    Callable[[], FileDataContext], self.configure_file_data_context
                )
                gx_context = file_context_fn()
        else:
            gx_context = gx.get_context(mode=self.context_type)
        checkpoint = self.configure_checkpoint(gx_context)

        runtime_batch_params = context.get("params", {}).get("gx_batch_parameters")  # type: ignore[call-overload]
        if runtime_batch_params:
            batch_parameters = runtime_batch_params
        else:
            batch_parameters = self.batch_parameters
        result = checkpoint.run(batch_parameters=batch_parameters)

        if file_context_generator:
            self._allow_generator_teardown(file_context_generator)

        return result.describe_dict()

    def _get_value_from_generator(
        self, generator: Generator[FileDataContext, None, None]
    ) -> FileDataContext:
        try:
            return next(generator)
        except StopIteration:
            raise RuntimeError("Generator must yield exactly once; did not yield")

    def _allow_generator_teardown(self, generator: Generator) -> None:
        """Run the generator to completion to allow for any cleanup/teardown.

        Also does some error handling to ensure the generator doesn't yield more than once.
        """
        try:
            # Check if we have another yield (this is an error case)
            next(generator)
        except StopIteration:
            pass
        else:
            raise RuntimeError(
                "Generator must yield exactly once; yielded more than once"
            )
