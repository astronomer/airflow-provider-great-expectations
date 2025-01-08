from typing import Callable, Generator
import great_expectations as gx

import pytest


@pytest.fixture
def ensure_checkpoint_cleanup(
    ensure_validation_definition_cleanup,
) -> Generator[Callable[[str], None], None, None]:
    to_cleanup: set[str] = set()

    def ensure_cleanup(name: str) -> None:
        to_cleanup.add(name)

    yield ensure_cleanup

    context = gx.get_context(mode="cloud")
    for name in to_cleanup:
        context.checkpoints.delete(name)


@pytest.fixture
def ensure_validation_definition_cleanup(
    ensure_suite_cleanup,
    ensure_data_source_cleanup,
) -> Generator[Callable[[str], None], None, None]:
    to_cleanup: set[str] = set()

    def ensure_cleanup(name: str) -> None:
        to_cleanup.add(name)

    yield ensure_cleanup

    context = gx.get_context(mode="cloud")
    for name in to_cleanup:
        context.validation_definitions.delete(name)


@pytest.fixture
def ensure_suite_cleanup() -> Generator[Callable[[str], None], None, None]:
    to_cleanup: set[str] = set()

    def ensure_cleanup(name: str) -> None:
        to_cleanup.add(name)

    yield ensure_cleanup

    context = gx.get_context(mode="cloud")
    for name in to_cleanup:
        context.suites.delete(name)


@pytest.fixture
def ensure_data_source_cleanup() -> Generator[Callable[[str], None], None, None]:
    to_cleanup: set[str] = set()

    def ensure_cleanup(name: str) -> None:
        to_cleanup.add(name)

    yield ensure_cleanup

    context = gx.get_context(mode="cloud")
    for name in to_cleanup:
        context.data_sources.delete(name)
