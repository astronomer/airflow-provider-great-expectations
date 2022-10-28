from airflow.models import BaseOperatorLink, XCom
from airflow.plugins_manager import AirflowPlugin
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

class GreatExpectationsDataDocsLink(BaseOperatorLink):
    """Constructs a link to Great Expectations data docs site."""

    operators = [GreatExpectationsOperator]

    @property
    def name(self):
        return "Great Expectations Data Docs"

    def get_link(self, operator, *, ti_key) -> str:
        if ti_key is not None:
            return XCom.get_value(key="data_docs_url", ti_key=ti_key)
        return XCom.get_one(
                dag_id=ti_key.dag_id,
                task_id=ti_key.task_id,
                run_id=ti_key.run_id,
                key="data_docs_url"
            ) or ""


class AirflowExtraLinkPlugin(AirflowPlugin):
    name = "extra_link_plugin"
    operator_extra_links = [GreatExpectationsDataDocsLink(),]
