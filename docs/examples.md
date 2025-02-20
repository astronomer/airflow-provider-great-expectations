# Examples

Visit the Great Expectations Airflow Provider repository to explore [examples](https://github.com/astronomer/airflow-provider-great-expectations/tree/docs/great_expectations_provider/example_dags) of end-to-end configuration and usage.

## Exercise an example DAG

The example DAGs can be exercised with the open-source Astro CLI or with the Airflow web UI.

To exercise an example DAG with the open-source Astro CLI:

1. Initialize a project with the [Astro CLI](https://www.astronomer.io/docs/astro/cli/get-started-cli/).
2. Copy the example DAG into the `dags/` folder of your Astro project.
3. Copy the contents of the `include/` folder of this repository into the `include/` directory of your Astro project.
4. Add the following to your `Dockerfile` to install the `airflow-provider-great-expectations` package:
   ```
   RUN pip install --user airflow-provider-great-expectations
   ```
5. Start Docker.
6. Run `astro dev start` to view the DAG on a local Airflow instance.

To exercise an example DAG with the Airflow web UI:

1. Add the example DAG to your `dags/` folder.
2. Make the `include/` directory available in your environment.
3. Go to the [DAGs View](https://airflow.apache.org/docs/apache-airflow/stable/ui.html#dags-view) in the Airflow UI.
4. Find the example DAG in the list and click the play button next to it. 
