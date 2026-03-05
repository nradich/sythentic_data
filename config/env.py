"""
Environment configuration for synthetic data pipeline
Supports both Databricks secret scope and local development
"""

import os
import sys
from typing import Dict, List, Optional

# Databricks secret scope configuration
SCOPE_NAME = "adls-scope"
SECRET_KEY = "adls-sas-token"

def get_secret_from_databricks(scope: str, key: str) -> Optional[str]:
    """
    Retrieve secret from Databricks secret scope
    Returns None if not running in Databricks environment
    """
    try:
        # Import dbutils (only available in Databricks environment)
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        
        spark = SparkSession.getActiveSession()
        if spark is None:
            return None
            
        dbutils = DBUtils(spark)
        return dbutils.secrets.get(scope=scope, key=key)
    except ImportError:
        # Not running in Databricks environment
        return None
    except Exception as e:
        print(f"Warning: Could not retrieve secret {key} from scope {scope}: {e}")
        return None

def get_secret_from_env(key: str, default: str = None) -> str:
    """
    Get secret from environment variables with fallback to default
    """
    value = os.getenv(key, default)
    if value is None:
        raise ValueError(f"Please set {key} environment variable or configure Databricks secret scope")
    
    # For development, allow placeholder values but warn
    if value in ["your_nvidia_api_key_here", "your-container-name"]:
        print(f"Warning: Using placeholder value for {key}. Set environment variable or configure Databricks secret scope for production.")
    
    return value


def get_optional_env(key: str, default: Optional[str] = None) -> Optional[str]:
    """
    Get optional environment variable without raising if not set
    """
    return os.getenv(key, default)


def get_config_value(secret_key: str, env_key: str, default: Optional[str] = None) -> Optional[str]:
    """
    Resolve configuration with priority:
      1) Databricks secret scope key
      2) Environment variable
      3) Provided default
    """
    secret_value = get_secret_from_databricks(SCOPE_NAME, secret_key)
    if secret_value:
        return secret_value
    return get_optional_env(env_key, default)


def get_first_config_value(
    secret_keys: List[str],
    env_keys: List[str],
    default: Optional[str] = None,
) -> Optional[str]:
    """
    Resolve configuration by trying multiple Databricks secret keys first,
    then multiple environment variables.
    """
    for secret_key in secret_keys:
        secret_value = get_secret_from_databricks(SCOPE_NAME, secret_key)
        if secret_value:
            return secret_value

    for env_key in env_keys:
        env_value = get_optional_env(env_key)
        if env_value:
            return env_value

    return default


def get_sql_server_config() -> Dict[str, Optional[str]]:
    """
    Resolve SQL Server configuration.

    Priority:
      1) Databricks secret scope (Key Vault-backed) using provided keys
      2) Legacy Databricks secret keys
      3) Environment variables
    """
    return {
        "host": get_first_config_value(
            secret_keys=["sqlservername", "sql-server-host"],
            env_keys=["SQL_SERVER_HOST"],
        ),
        "database": get_first_config_value(
            secret_keys=["sqldbname", "sql-server-database"],
            env_keys=["SQL_SERVER_DATABASE"],
        ),
        "username": get_first_config_value(
            secret_keys=["sqldbuser", "sql-server-username"],
            env_keys=["SQL_SERVER_USERNAME"],
        ),
        "password": get_first_config_value(
            secret_keys=["sqldbpassword", "sql-server-password"],
            env_keys=["SQL_SERVER_PASSWORD"],
        ),
        "port": get_first_config_value(
            secret_keys=["sqlserverport"],
            env_keys=["SQL_SERVER_PORT"],
            default="1433",
        ),
    }

# Get secrets directly from Databricks secret scope only
NVIDIA_API_KEY = get_secret_from_databricks(SCOPE_NAME, "nvidiaapi")

AZURE_STORAGE_ACCOUNT = get_secret_from_databricks(SCOPE_NAME, "synthenticstorage")

AZURE_SAS_TOKEN = get_secret_from_databricks(SCOPE_NAME, "adls-sas-token")

AZURE_CONTAINER_NAME = "datadesign"

# Auto Loader ingestion configuration
AUTOLOADER_FILE_FORMAT = get_optional_env("AUTOLOADER_FILE_FORMAT", "json")
AUTOLOADER_SCHEMA_EVOLUTION_MODE = get_optional_env("AUTOLOADER_SCHEMA_EVOLUTION_MODE", "addNewColumns")
AUTOLOADER_MAX_FILES_PER_TRIGGER = get_optional_env("AUTOLOADER_MAX_FILES_PER_TRIGGER", "500")
AUTOLOADER_MAX_BYTES_PER_TRIGGER = get_optional_env("AUTOLOADER_MAX_BYTES_PER_TRIGGER", None)
AUTOLOADER_USE_MANAGED_FILE_EVENTS = get_optional_env("AUTOLOADER_USE_MANAGED_FILE_EVENTS", "false")

# SQL Server publish configuration
_sql_server_config = get_sql_server_config()
SQL_SERVER_HOST = _sql_server_config["host"]
SQL_SERVER_DATABASE = _sql_server_config["database"]
SQL_SERVER_USERNAME = _sql_server_config["username"]
SQL_SERVER_PASSWORD = _sql_server_config["password"]
SQL_SERVER_PORT = _sql_server_config["port"]

# SQL Server destination tables
SQL_TABLE_CUSTOMERS = get_optional_env("SQL_TABLE_CUSTOMERS", "syn_data.customers")
SQL_TABLE_PRODUCTS = get_optional_env("SQL_TABLE_PRODUCTS", "syn_data.products")
SQL_TABLE_ORDERS = get_optional_env("SQL_TABLE_ORDERS", "syn_data.orders")
SQL_WATERMARK_TABLE = get_optional_env("SQL_WATERMARK_TABLE", "syn_data.ingestion_watermark")

# Legacy support - keeping original variable names available for backward compatibility
key = NVIDIA_API_KEY
container = AZURE_CONTAINER_NAME