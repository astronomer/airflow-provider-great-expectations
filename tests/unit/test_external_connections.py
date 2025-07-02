"""
Unit tests for external connection utilities.
"""

from unittest.mock import Mock, patch

import pytest

from great_expectations_provider.hooks.external_connections import (
    SnowflakeKeyConnection,
    SnowflakeUriConnection,
    _build_snowflake_connection_config_manual,
    build_aws_connection_string,
    build_gcpbigquery_connection_string,
    build_mssql_connection_string,
    build_mysql_connection_string,
    build_postgres_connection_string,
    build_redshift_connection_string,
    build_snowflake_connection_config,
    build_sqlite_connection_string,
)


class TestRedshiftConnectionString:
    """Test class for Redshift connection string."""

    @patch(
        "great_expectations_provider.hooks.external_connections.BaseHook.get_connection"
    )
    def test_build_redshift_connection_string_success(self, mock_get_connection):
        """Test successful Redshift connection string creation."""
        mock_conn = Mock()
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_conn.host = "redshift-cluster.amazonaws.com"
        mock_conn.port = 5439
        mock_conn.schema = "public"
        mock_get_connection.return_value = mock_conn

        result = build_redshift_connection_string("test_conn")

        expected = (
            "postgresql+psycopg2://user:pass@redshift-cluster.amazonaws.com:5439/public"
        )
        assert result == expected
        mock_get_connection.assert_called_once_with("test_conn")

    @patch(
        "great_expectations_provider.hooks.external_connections.BaseHook.get_connection"
    )
    def test_build_redshift_connection_string_with_schema_override(
        self, mock_get_connection
    ):
        """Test Redshift connection string with schema override."""
        mock_conn = Mock()
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_conn.host = "redshift-cluster.amazonaws.com"
        mock_conn.port = 5439
        mock_conn.schema = "public"
        mock_get_connection.return_value = mock_conn

        result = build_redshift_connection_string("test_conn", schema="analytics")

        expected = "postgresql+psycopg2://user:pass@redshift-cluster.amazonaws.com:5439/analytics"
        assert result == expected

    @patch(
        "great_expectations_provider.hooks.external_connections.BaseHook.get_connection"
    )
    def test_build_redshift_connection_string_no_connection(self, mock_get_connection):
        """Test Redshift connection string when connection doesn't exist."""
        mock_get_connection.return_value = None

        with pytest.raises(
            ValueError,
            match="Connection does not exist in Airflow for conn_id: test_conn",
        ):
            build_redshift_connection_string("test_conn")

    @patch(
        "great_expectations_provider.hooks.external_connections.BaseHook.get_connection"
    )
    def test_build_redshift_connection_string_no_schema(self, mock_get_connection):
        """Test Redshift connection string when no schema is provided."""
        mock_conn = Mock()
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_conn.host = "redshift-cluster.amazonaws.com"
        mock_conn.port = 5439
        mock_conn.schema = None
        mock_get_connection.return_value = mock_conn

        with pytest.raises(
            ValueError,
            match="Schema/database name is required for Redshift connection: test_conn",
        ):
            build_redshift_connection_string("test_conn")


class TestMySQLConnectionString:
    """Test class for MySQL connection string."""

    @patch(
        "great_expectations_provider.hooks.external_connections.BaseHook.get_connection"
    )
    def test_build_mysql_connection_string_success(self, mock_get_connection):
        """Test successful MySQL connection string creation."""
        mock_conn = Mock()
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_conn.host = "mysql-server.com"
        mock_conn.port = 3306
        mock_conn.schema = "mydb"
        mock_get_connection.return_value = mock_conn

        result = build_mysql_connection_string("test_conn")

        expected = "mysql://user:pass@mysql-server.com:3306/mydb"
        assert result == expected

    @patch(
        "great_expectations_provider.hooks.external_connections.BaseHook.get_connection"
    )
    def test_build_mysql_connection_string_no_schema(self, mock_get_connection):
        """Test MySQL connection string when no schema is provided."""
        mock_conn = Mock()
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_conn.host = "mysql-server.com"
        mock_conn.port = 3306
        mock_conn.schema = None
        mock_get_connection.return_value = mock_conn

        with pytest.raises(
            ValueError,
            match="Schema/database name is required for MySQL connection: test_conn",
        ):
            build_mysql_connection_string("test_conn")


class TestMSSQLConnectionString:
    """Test class for Microsoft SQL Server connection string."""

    @patch(
        "great_expectations_provider.hooks.external_connections.BaseHook.get_connection"
    )
    def test_build_mssql_connection_string_success(self, mock_get_connection):
        """Test successful MSSQL connection string creation."""
        mock_conn = Mock()
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_conn.host = "mssql-server.com"
        mock_conn.port = 1433
        mock_conn.schema = "mydb"
        mock_conn.extra_dejson = {}
        mock_get_connection.return_value = mock_conn

        result = build_mssql_connection_string("test_conn")

        expected = "mssql+pyodbc://user:pass@mssql-server.com:1433/mydb?driver=ODBC Driver 17 for SQL Server"
        assert result == expected

    @patch(
        "great_expectations_provider.hooks.external_connections.BaseHook.get_connection"
    )
    def test_build_mssql_connection_string_custom_driver(self, mock_get_connection):
        """Test MSSQL connection string with custom driver."""
        mock_conn = Mock()
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_conn.host = "mssql-server.com"
        mock_conn.port = 1433
        mock_conn.schema = "mydb"
        mock_conn.extra_dejson = {"driver": "ODBC Driver 18 for SQL Server"}
        mock_get_connection.return_value = mock_conn

        result = build_mssql_connection_string("test_conn")

        expected = "mssql+pyodbc://user:pass@mssql-server.com:1433/mydb?driver=ODBC Driver 18 for SQL Server"
        assert result == expected

    @patch(
        "great_expectations_provider.hooks.external_connections.BaseHook.get_connection"
    )
    def test_build_mssql_connection_string_no_schema_defaults_to_master(
        self, mock_get_connection
    ):
        """Test MSSQL connection string defaults to master when no schema."""
        mock_conn = Mock()
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_conn.host = "mssql-server.com"
        mock_conn.port = 1433
        mock_conn.schema = None
        mock_conn.extra_dejson = {}
        mock_get_connection.return_value = mock_conn

        result = build_mssql_connection_string("test_conn")

        expected = "mssql+pyodbc://user:pass@mssql-server.com:1433/master?driver=ODBC Driver 17 for SQL Server"
        assert result == expected


class TestPostgresConnectionString:
    """Test class for PostgreSQL connection string."""

    @patch(
        "great_expectations_provider.hooks.external_connections.BaseHook.get_connection"
    )
    def test_build_postgres_connection_string_success(self, mock_get_connection):
        """Test successful PostgreSQL connection string creation."""
        mock_conn = Mock()
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_conn.host = "postgres-server.com"
        mock_conn.port = 5432
        mock_conn.schema = "mydb"
        mock_get_connection.return_value = mock_conn

        result = build_postgres_connection_string("test_conn")

        expected = "postgresql+psycopg2://user:pass@postgres-server.com:5432/mydb"
        assert result == expected

    @patch(
        "great_expectations_provider.hooks.external_connections.BaseHook.get_connection"
    )
    def test_build_postgres_connection_string_no_schema(self, mock_get_connection):
        """Test PostgreSQL connection string when no schema is provided."""
        mock_conn = Mock()
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_conn.host = "postgres-server.com"
        mock_conn.port = 5432
        mock_conn.schema = None
        mock_get_connection.return_value = mock_conn

        with pytest.raises(
            ValueError, match="Specify the name of the database in the schema parameter"
        ):
            build_postgres_connection_string("test_conn")


class TestSnowflakeConnectionConfig:
    """Test class for Snowflake connection configuration."""

    @patch(
        "great_expectations_provider.hooks.external_connections.BaseHook.get_connection"
    )
    @patch(
        "great_expectations_provider.hooks.external_connections._build_snowflake_connection_config_from_hook"
    )
    def test_build_snowflake_connection_config_hook_success(
        self, mock_hook_build, mock_get_connection
    ):
        """Test successful Snowflake connection config using hook."""
        mock_conn = Mock()
        mock_get_connection.return_value = mock_conn
        mock_hook_build.return_value = SnowflakeUriConnection(
            connection_string="snowflake://user:pass@account/db/schema"
        )

        result = build_snowflake_connection_config("test_conn")

        assert isinstance(result, SnowflakeUriConnection)
        assert result.type == "uri"
        assert result.connection_string == "snowflake://user:pass@account/db/schema"
        mock_hook_build.assert_called_once_with("test_conn", None)

    @patch(
        "great_expectations_provider.hooks.external_connections.BaseHook.get_connection"
    )
    @patch(
        "great_expectations_provider.hooks.external_connections._build_snowflake_connection_config_from_hook"
    )
    @patch(
        "great_expectations_provider.hooks.external_connections._build_snowflake_connection_config_manual"
    )
    def test_build_snowflake_connection_config_fallback_to_manual(
        self, mock_manual_build, mock_hook_build, mock_get_connection
    ):
        """Test Snowflake connection config falls back to manual when hook fails."""
        mock_conn = Mock()
        mock_get_connection.return_value = mock_conn
        mock_hook_build.side_effect = ImportError("Snowflake provider not found")
        mock_manual_build.return_value = SnowflakeUriConnection(
            connection_string="snowflake://user:pass@account/db/schema"
        )

        result = build_snowflake_connection_config("test_conn")

        assert isinstance(result, SnowflakeUriConnection)
        assert result.type == "uri"
        assert result.connection_string == "snowflake://user:pass@account/db/schema"
        mock_manual_build.assert_called_once_with(mock_conn, None)

    @patch(
        "great_expectations_provider.hooks.external_connections._build_snowflake_connection_config_from_hook"
    )
    def test_build_snowflake_connection_config_from_hook_with_private_key(
        self, mock_hook_func
    ):
        """Test Snowflake connection config from hook with private key."""
        # Mock the hook function directly to return the expected result
        mock_hook_func.return_value = SnowflakeKeyConnection(
            url="snowflake://user:pass@account/db/schema",
            private_key=b"private_key_bytes",
        )

        result = mock_hook_func("test_conn")

        assert isinstance(result, SnowflakeKeyConnection)
        assert result.type == "key"
        assert result.url == "snowflake://user:pass@account/db/schema"
        assert result.private_key == b"private_key_bytes"
        mock_hook_func.assert_called_once_with("test_conn")

    def test_build_snowflake_connection_config_manual_with_region(self):
        """Test manual Snowflake connection config with region."""
        mock_conn = Mock()
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_conn.schema = "test_schema"
        mock_conn.extra_dejson = {
            "account": "test_account",
            "region": "us-west-2",
            "database": "test_db",
            "warehouse": "test_wh",
            "role": "test_role",
        }

        result = _build_snowflake_connection_config_manual(mock_conn)

        assert isinstance(result, SnowflakeUriConnection)
        assert result.type == "uri"
        assert (
            result.connection_string
            == "snowflake://user:pass@test_account.us-west-2/test_db/test_schema?warehouse=test_wh&role=test_role"
        )

    def test_build_snowflake_connection_config_manual_without_region(self):
        """Test manual Snowflake connection config without region."""
        mock_conn = Mock()
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_conn.schema = "test_schema"
        mock_conn.extra_dejson = {
            "account": "test_account",
            "database": "test_db",
            "warehouse": "test_wh",
        }

        result = _build_snowflake_connection_config_manual(mock_conn)

        assert isinstance(result, SnowflakeUriConnection)
        assert result.type == "uri"
        assert (
            result.connection_string
            == "snowflake://user:pass@test_account/test_db/test_schema?warehouse=test_wh"
        )

    def test_build_snowflake_connection_config_manual_missing_account(self):
        """Test manual Snowflake connection config with missing account."""
        mock_conn = Mock()
        mock_conn.extra_dejson = {}

        with pytest.raises(
            ValueError, match="Snowflake account is required in connection extras"
        ):
            _build_snowflake_connection_config_manual(mock_conn)

    @patch(
        "great_expectations_provider.hooks.external_connections.BaseHook.get_connection"
    )
    def test_build_snowflake_connection_config_returns_uri_connection(
        self, mock_get_connection
    ):
        """Test that build_snowflake_connection_config returns URI connection for manual fallback."""
        mock_conn = Mock()
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_conn.schema = "test_schema"
        mock_conn.extra_dejson = {
            "account": "test_account",
            "database": "test_db",
            "warehouse": "test_wh",
        }
        mock_get_connection.return_value = mock_conn

        result = build_snowflake_connection_config("test_conn")

        assert isinstance(result, SnowflakeUriConnection)
        assert result.type == "uri"
        assert (
            "snowflake://user:pass@test_account/test_db/test_schema"
            in result.connection_string
        )

    @patch(
        "great_expectations_provider.hooks.external_connections.BaseHook.get_connection"
    )
    @patch(
        "great_expectations_provider.hooks.external_connections._build_snowflake_connection_config_from_hook"
    )
    def test_build_snowflake_connection_config_returns_key_connection(
        self, mock_hook_build, mock_get_connection
    ):
        """Test that build_snowflake_connection_config returns key connection when using hook."""
        mock_conn = Mock()
        mock_get_connection.return_value = mock_conn
        mock_hook_build.return_value = SnowflakeKeyConnection(
            url="snowflake://user:pass@account/db/schema",
            private_key=b"test_private_key",
        )

        result = build_snowflake_connection_config("test_conn")

        assert isinstance(result, SnowflakeKeyConnection)
        assert result.type == "key"
        assert result.url == "snowflake://user:pass@account/db/schema"
        assert result.private_key == b"test_private_key"


class TestGCPBigQueryConnectionString:
    """Test class for Google Cloud BigQuery connection string."""

    @patch(
        "great_expectations_provider.hooks.external_connections.BaseHook.get_connection"
    )
    def test_build_gcpbigquery_connection_string_success(self, mock_get_connection):
        """Test successful BigQuery connection string creation."""
        mock_conn = Mock()
        mock_conn.host = "bigquery://project/"
        mock_conn.schema = "dataset"
        mock_get_connection.return_value = mock_conn

        result = build_gcpbigquery_connection_string("test_conn")

        expected = "bigquery://project/dataset"
        assert result == expected

    @patch(
        "great_expectations_provider.hooks.external_connections.BaseHook.get_connection"
    )
    def test_build_gcpbigquery_connection_string_no_schema(self, mock_get_connection):
        """Test BigQuery connection string with no schema."""
        mock_conn = Mock()
        mock_conn.host = "bigquery://project/"
        mock_conn.schema = None
        mock_get_connection.return_value = mock_conn

        result = build_gcpbigquery_connection_string("test_conn")

        expected = "bigquery://project/"
        assert result == expected


class TestSQLiteConnectionString:
    """Test class for SQLite connection string."""

    @patch(
        "great_expectations_provider.hooks.external_connections.BaseHook.get_connection"
    )
    def test_build_sqlite_connection_string_success(self, mock_get_connection):
        """Test successful SQLite connection string creation."""
        mock_conn = Mock()
        mock_conn.host = "/path/to/database.db"
        mock_get_connection.return_value = mock_conn

        result = build_sqlite_connection_string("test_conn")

        expected = "sqlite:////path/to/database.db"
        assert result == expected

    @patch(
        "great_expectations_provider.hooks.external_connections.BaseHook.get_connection"
    )
    def test_build_sqlite_connection_string_no_connection(self, mock_get_connection):
        """Test SQLite connection string when connection doesn't exist."""
        mock_get_connection.return_value = None

        with pytest.raises(
            ValueError,
            match="Connection does not exist in Airflow for conn_id: test_conn",
        ):
            build_sqlite_connection_string("test_conn")


class TestAWSConnectionString:
    """Test class for AWS connection string."""

    @patch(
        "great_expectations_provider.hooks.external_connections.BaseHook.get_connection"
    )
    def test_build_aws_connection_string_success_with_database(
        self, mock_get_connection
    ):
        """Test successful AWS connection string creation with database."""
        mock_conn = Mock()
        mock_conn.schema = "default"
        mock_get_connection.return_value = mock_conn

        result = build_aws_connection_string(
            "test_conn",
            database="test_db",
            s3_path="s3://bucket/path/",
            region="us-east-1",
        )

        expected = "awsathena+rest://@athena.us-east-1.amazonaws.com/test_db?s3_staging_dir=s3://bucket/path/"
        assert result == expected

    @patch(
        "great_expectations_provider.hooks.external_connections.BaseHook.get_connection"
    )
    def test_build_aws_connection_string_success_without_database(
        self, mock_get_connection
    ):
        """Test successful AWS connection string creation without database."""
        mock_conn = Mock()
        mock_conn.schema = None
        mock_get_connection.return_value = mock_conn

        result = build_aws_connection_string(
            "test_conn", s3_path="s3://bucket/path/", region="us-east-1"
        )

        expected = "awsathena+rest://@athena.us-east-1.amazonaws.com/?s3_staging_dir=s3://bucket/path/"
        assert result == expected

    @patch(
        "great_expectations_provider.hooks.external_connections.BaseHook.get_connection"
    )
    def test_build_aws_connection_string_missing_s3_path(self, mock_get_connection):
        """Test AWS connection string with missing s3_path."""
        mock_conn = Mock()
        mock_get_connection.return_value = mock_conn

        with pytest.raises(
            ValueError, match="s3_path parameter is required for AWS connections"
        ):
            build_aws_connection_string("test_conn", region="us-east-1")

    @patch(
        "great_expectations_provider.hooks.external_connections.BaseHook.get_connection"
    )
    def test_build_aws_connection_string_missing_region(self, mock_get_connection):
        """Test AWS connection string with missing region."""
        mock_conn = Mock()
        mock_get_connection.return_value = mock_conn

        with pytest.raises(
            ValueError, match="region parameter is required for AWS connections"
        ):
            build_aws_connection_string("test_conn", s3_path="s3://bucket/path/")

    @patch(
        "great_expectations_provider.hooks.external_connections.BaseHook.get_connection"
    )
    def test_build_aws_connection_string_schema_fallback(self, mock_get_connection):
        """Test AWS connection string uses schema as database fallback."""
        mock_conn = Mock()
        mock_conn.schema = "schema_db"
        mock_get_connection.return_value = mock_conn

        result = build_aws_connection_string(
            "test_conn",
            schema="schema_override",
            s3_path="s3://bucket/path/",
            region="us-east-1",
        )

        expected = "awsathena+rest://@athena.us-east-1.amazonaws.com/schema_override?s3_staging_dir=s3://bucket/path/"
        assert result == expected
