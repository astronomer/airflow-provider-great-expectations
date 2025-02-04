# Contributing guide

All contributions, bug reports, bug fixes, documentation improvements, and enhancements are welcome.

All contributors and maintainers to this project should abide by the [Contributor Code of Conduct](docs/contributing/code-of-conduct.md).

Learn more about the contributors' roles in the [Roles](docs/contributing/contributor-roles.md) page.

This document describes how to contribute to the Great Expectations Airflow Provider, covering:

- Overview of how to contribute
- How to set up the local development environment
- Running tests
- Authoring the documentation

## Overview of how to contribute

To contribute to the Great Expectations Airflow Provider project:

1. Create a [GitHub Issue](https://github.com/astronomer/airflow-provider-great-expectations/issues) describing a bug, enhancement, or feature request.
2. Open a branch off of the `main` branch and create a Pull Request into the `main` branch from your feature branch.
3. Link your issue to the pull request.
4. After you complete development on your feature branch, request a review. A maintainer will merge your PR after all reviewers approve it.


## Set up a local development environment

Setting up a local development environment involves fulfilling requirements, cloning the repo, and setting up a virtual environment.

### Requirements

- [Git](https://git-scm.com/)
- [Python](https://www.python.org/) version 3.9 to 3.12
- [Great Expectations](https://docs.greatexpectations.io/docs/core/set_up_a_gx_environment/install_gx) v1.3.3+
- [Apache Airflow®](https://airflow.apache.org/) 2.1.0+ 

### Clone the repo

```bash
git clone https://github.com/astronomer/airflow-provider-great-expectations.git
```

### Set up a virtual environment

You can use any virtual environment tool. The following example uses the `venv` tool included in the Python standard library.

1. Create the virtual environment.
   ```bash
   $ python -m venv --prompt my-af-gx-venv .venv
   ```

2. Activate the virtual environment.
   ```bash
   $ . .venv/bin/activate
   ```

3. Install the package and testing dependencies.
   ```bash
   (my-af-gx-venv) $ pip install -e '.[tests]'
   ```

## Run tests

You can test with `pytest` or functional testing. 

To test with `pytest`:

1. Install `pytest` as a dependency
   ```bash
   pip install "airflow-provider-great-expectations[tests]"
   ```
2. Run `pytest -p no:warnings`, which will provide a concise output when all tests pass and minimum necessary details when they dont. The `no:warnings` flag filters out deprecation messages that may be issued by Airflow.

   ```
   (my-af-ge-venv) $ pytest -p no:warnings
   ====================================== test session starts ========================================
   platform darwin -- Python 3.12.8, pytest-6.2.4, py-1.10.0, pluggy-0.13.1
   rootdir: /Users/my-user/my-repos/airflow-provider-great-expectations, configfile: pytest.ini, testpaths: tests
   plugins: anyio-3.3.0
   collected 7 items

   tests/operators/test_great_expectations.py .......                                                [100%]
   ====================================== 7 passed in 11.99s =========================================
   ```

To run functional tests:

1. Install your local development package in an Airflow environment for testing.
2. [Exercise the example DAGs](/docs/examples.md). 



## Write docs

We use Markdown to author Great Expectations Airflow Provider documentation. We use hatch to build and release the docs.

1. Update Markdown files in the [`docs/` folder](https://github.com/klavavej/airflow-provider-great-expectations/tree/docs/docs).
2. Build and serve the documentation locally to preview your changes.
   ```bash
   hatch run docs:dev
   ```
3. [Open an issue and PR](#overview-of-how-to-contribute) for your changes.
4. Once approved, release the documentation with the current project version and set it to the latest.
   ```
   hatch run docs:gh-release
   ```