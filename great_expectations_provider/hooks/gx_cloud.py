from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from airflow.hooks.base import BaseHook

from great_expectations_provider.common.gx_context_actions import load_data_context


@dataclass(frozen=True)
class GXCloudConfig:
    cloud_access_token: str
    cloud_organization_id: str


class GXCloudHook(BaseHook):
    """
    Connect to the GX Cloud managed backend.

    :gx_cloud_conn_id str: name of the GX Cloud connection
    """

    conn_name_attr = "gx_cloud_conn_id"
    default_conn_name = "gx_cloud_default"
    conn_type = "gx_cloud"
    hook_name = "Great Expectations Cloud"

    def __init__(self, gx_cloud_conn_id: str = default_conn_name):
        super().__init__()
        self.gx_cloud_conn_id = gx_cloud_conn_id

    def get_conn(self) -> GXCloudConfig:  # type: ignore[override]
        config = self.get_connection(self.gx_cloud_conn_id)
        return GXCloudConfig(
            cloud_access_token=config.password, cloud_organization_id=config.login
        )

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port", "extra", "host"],
            "relabeling": {
                "login": "GX Cloud Organization ID",
                "password": "GX Cloud Access Token",
            },
            "placeholders": {},
        }

    def test_connection(self) -> tuple[bool, str]:
        try:
            config = self.get_conn()
            load_data_context(context_type="cloud", gx_cloud_config=config)
            return True, "Connection successful."
        except Exception as error:
            return False, str(error)
