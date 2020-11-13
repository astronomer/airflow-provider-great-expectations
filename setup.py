import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="airflow-provider-great-expectations",
    version="0.0.1",
    description="An Airflow provider for Great Expectations",
    url="https://github.com/great-expectations/great_expectations_airflow",
    packages=setuptools.find_packages(),
    python_requires='>=3.6',
)
