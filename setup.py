import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="airflow-provider-great-expectations",
    version="0.0.4",
    author="Great Expectations",
    description="An Apache Airflow provider for Great Expectations",
    long_description=long_description,
    long_description_content_type='text/markdown',
    url="https://github.com/great-expectations/airflow-provider-great-expectations",
    packages=setuptools.find_packages(),
    python_requires='>=3.6',
    package_data={'great_expectations_provider': ['examples/data/*.csv']},
    include_package_data=True
)
