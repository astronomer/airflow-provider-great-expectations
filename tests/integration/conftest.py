from typing import Callable, Generator
import great_expectations as gx
from great_expectations.data_context import AbstractDataContext

import pytest


@pytest.fixture
def cloud_context() -> AbstractDataContext:
    return gx.get_context(mode="cloud")


@pytest.fixture
def ensure_checkpoint_cleanup(
    ensure_validation_definition_cleanup,
    cloud_context: AbstractDataContext,
) -> Generator[Callable[[str], None], None, None]:
    to_cleanup: set[str] = set()

    def ensure_cleanup(name: str) -> None:
        to_cleanup.add(name)

    yield ensure_cleanup

    for name in to_cleanup:
        cloud_context.checkpoints.delete(name)


@pytest.fixture
def ensure_validation_definition_cleanup(
    ensure_suite_cleanup,
    ensure_data_source_cleanup,
    cloud_context: AbstractDataContext,
) -> Generator[Callable[[str], None], None, None]:
    to_cleanup: set[str] = set()

    def ensure_cleanup(name: str) -> None:
        to_cleanup.add(name)

    yield ensure_cleanup

    for name in to_cleanup:
        cloud_context.validation_definitions.delete(name)


@pytest.fixture
def ensure_suite_cleanup(
    cloud_context: AbstractDataContext,
) -> Generator[Callable[[str], None], None, None]:
    to_cleanup: set[str] = set()

    def ensure_cleanup(name: str) -> None:
        to_cleanup.add(name)

    yield ensure_cleanup

    for name in to_cleanup:
        cloud_context.validation_definitions.delete(name)


@pytest.fixture
def ensure_data_source_cleanup(
    cloud_context: AbstractDataContext,
) -> Generator[Callable[[str], None], None, None]:
    to_cleanup: set[str] = set()

    def ensure_cleanup(name: str) -> None:
        to_cleanup.add(name)

    yield ensure_cleanup

    for name in to_cleanup:
        cloud_context.suites.delete(name)
