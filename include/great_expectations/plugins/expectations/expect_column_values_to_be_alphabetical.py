import operator

import pandas

# !!! This giant block of imports should be something simpler, such as:
# from great_exepectations.helpers.expectation_creation import *
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics.map_metric import (
    ColumnMapMetricProvider,
    column_condition_partial,
)


# This class defines a Metric to support your Expectation
# For most Expectations, the main business logic for calculation will live here.
# To learn about the relationship between Metrics and Expectations, please visit {some doc}.
class ColumnValuesAreAlphabetical(ColumnMapMetricProvider):
    # This is the id string that will be used to reference your metric.
    # Please see {some doc} for information on how to choose an id string for your Metric.
    condition_metric_name = "column_values.are_alphabetical"
    condition_value_keys = ("reverse",)

    # This method defines the business logic for evaluating your metric when using a PandasExecutionEngine

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, reverse=False, **kwargs):
        # lowercase the whole column to avoid issues with capitalization
        # (since every capital letter is "before" the lowercase letters)
        column_lower = column.map(str.lower)

        column_length = column.size

        # choose the operator to use for comparison of consecutive items
        # could be easily adapted for other comparisons, perhaps of custom objects
        if reverse:
            compare_function = operator.ge
        else:
            compare_function = operator.le

        output = [True]  # first value is automatically in order
        for i in range(1, column_length):
            if column_lower[i] and column_lower[i - 1]:  # make sure we aren't comparing Nones
                output.append(compare_function(column_lower[i - 1], column_lower[i]))
            else:
                output.append(None)

        return pandas.Series(output)


# This class defines the Expectation itself
# The main business logic for calculation lives here.
class ExpectColumnValuesToBeAlphabetical(ColumnMapExpectation):
    """
    Given a list of string values, check if the list is alphabetical.

    Given a list of string values, check if the list is alphabetical, either forwards or backwards (specified with the
    `reverse` parameter). Comparison is case-insensitive. Using `mostly` will give you how many items are alphabetical
    relative to the immediately previous item in the list.

    conditions:
        reverse: Checks for Z to A alphabetical if True, otherwise checks A to Z
    """

    # These examples will be shown in the public gallery, and also executed as unit tests for your Expectation
    examples = [
        {
            "data": {
                "is_alphabetical_lowercase": [
                    "apple",
                    "banana",
                    "coconut",
                    "donut",
                    "eggplant",
                    "flour",
                    "grapes",
                    "jellybean",
                    None,
                    None,
                    None,
                ],
                "is_alphabetical_lowercase_reversed": [
                    "moon",
                    "monster",
                    "messy",
                    "mellow",
                    "marble",
                    "maple",
                    "malted",
                    "machine",
                    None,
                    None,
                    None,
                ],
                "is_alphabetical_mixedcase": [
                    "Atlanta",
                    "bonnet",
                    "Delaware",
                    "gymnasium",
                    "igloo",
                    "Montreal",
                    "Tennessee",
                    "toast",
                    "Washington",
                    "xylophone",
                    "zebra",
                ],
                "out_of_order": [
                    "Right",
                    "wrong",
                    "up",
                    "down",
                    "Opposite",
                    "Same",
                    "west",
                    "east",
                    None,
                    None,
                    None,
                ],
            },
            "tests": [
                {
                    "title": "positive_test_with_all_values_alphabetical_lowercase",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "is_alphabetical_lowercase",
                        "reverse": False,
                        "mostly": 1.0,
                    },
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                        "unexpected_list": [],
                    },
                },
                {
                    "title": "negative_test_with_all_values_alphabetical_lowercase_reversed",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "is_alphabetical_lowercase_reversed",
                        "reverse": False,
                        "mostly": 1.0,
                    },
                    "out": {
                        "success": False,
                        "unexpected_index_list": [1, 2, 3, 4, 5, 6, 7],
                        "unexpected_list": [
                            "monster",
                            "messy",
                            "mellow",
                            "marble",
                            "maple",
                            "malted",
                            "machine",
                        ],
                    },
                },
                {
                    "title": "positive_test_with_all_values_alphabetical_lowercase_reversed",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "is_alphabetical_lowercase_reversed",
                        "reverse": True,
                        "mostly": 1.0,
                    },
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                        "unexpected_list": [],
                    },
                },
                {
                    "title": "positive_test_with_all_values_alphabetical_mixedcase",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "is_alphabetical_mixedcase", "mostly": 1.0},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                        "unexpected_list": [],
                    },
                },
                {
                    "title": "negative_test_with_out_of_order_mixedcase",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "out_of_order", "mostly": 1.0},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [2, 3, 7],
                        "unexpected_list": ["up", "down", "east"],
                    },
                },
            ],
        }
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",  # "experimental", "beta", or "production"
        "tags": ["experimental"],  # Tags for this Expectation in the gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@sethdmay",
            "@maximetokman",
            "@Harriee02",  # Don't forget to add your github handle here!
        ],
        "package": "experimental_expectations",
    }

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.are_alphabetical"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    # Please see {some doc} for more information about domain and success keys, and other arguments to Expectations
    success_keys = ("mostly", "reverse")

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}


if __name__ == "__main__":
    diagnostics_report = ExpectColumnValuesToBeAlphabetical().print_diagnostic_checklist()
    # print(json.dumps(diagnostics_report, indent=2))
