# Contributing guide

All contributions, bug reports, bug fixes, documentation improvements, and enhancements are welcome.

All contributors and maintainers to this project should abide by the [Contributor Code of Conduct](./code-of-conduct.md).

Learn more about the contributors' roles in the [Roles](./contributor-roles.md) page.

This document describes how to contribute to the Great Expectations Airflow Provider, covering:

- Overview of how to contribute
- How to set up the local development environment
- Running tests
- Authoring the documentation

## Overview of how to contribute

To contribute to the Great Expectations Airflow Provider project:

1. Create a [GitHub Issue](https://github.com/astronomer/airflow-provider-great-expectations/issues) describing a bug, enhancement, or feature request.
2. Fork the repository.
3. In your fork, open a branch off of the `main` branch.
4. Create a Pull Request into the `main` branch of the Provider repo from your forked feature branch.
5. Link your issue to the Pull Request.
6. After you complete development on your feature branch, request a review. A maintainer will merge your PR after all reviewers approve it.


## Set up a local development environment

Setting up a local development environment involves fulfilling requirements, getting a copy of the repository, and setting up a virtual environment.

### Requirements

- [Git](https://git-scm.com/)
- [Python](https://www.python.org/) version 3.9 to 3.12
- [Great Expectations](https://docs.greatexpectations.io/docs/core/set_up_a_gx_environment/install_gx) version 1.3.11+
- [Apache Airflow®](https://airflow.apache.org/) version 2.1.0+

### Get a copy of the repository

1. Fork the [Provider repository](https://github.com/astronomer/airflow-provider-great-expectations).

2. Clone your fork.
   ```bash
   git clone https://github.com/my-user/airflow-provider-great-expectations.git
   ```

### Set up a virtual environment

You can use any virtual environment tool. The following example uses the `venv` tool included in the Python standard library.

1. Create the virtual environment.
   ```bash
   $ python -m venv .venv
   ```

2. Activate the virtual environment.
   ```bash
   $ . .venv/bin/activate
   ```

3. Install the package and testing dependencies.
   ```bash
   (.venv) $ pip install -e '.[tests]'
   ```

## Run tests

Test with `pytest`:

1. Install `pytest` as a dependency.
   ```bash
   pip install "airflow-provider-great-expectations[tests]"
   ```
2. Run the following command, which will provide a concise output when all tests pass and minimum necessary details when they don't.
   ```bash
   pytest -p no:warnings
   ```
   The `no:warnings` flag filters out deprecation messages that may be issued by Airflow.


## Write docs

We use Markdown to author Great Expectations Airflow Provider documentation. We use hatch to build and release the docs.

1. Update Markdown files in the [`docs/` folder](https://github.com/astronomer/airflow-provider-great-expectations/tree/docs/docs).
2. Build and serve the documentation locally to preview your changes.
   ```bash
   hatch run docs:dev
   ```
3. [Open an issue and PR](#overview-of-how-to-contribute) for your changes.
4. Once approved, release the documentation with the current project version and set it to the latest.
   ```
   hatch run docs:gh-release
   ```
