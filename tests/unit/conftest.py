from typing import Generator
from unittest.mock import Mock

import pytest
from great_expectations.expectations import Expectation
from pytest_mock import MockerFixture


@pytest.fixture
def mock_gx(mocker: MockerFixture) -> Generator[Mock, None, None]:
    mock_gx = Mock()
    mock_gx.expectations.Expectation = Expectation  # required for isinstance check
    mocker.patch.dict("sys.modules", {"great_expectations": mock_gx})
    yield mock_gx
