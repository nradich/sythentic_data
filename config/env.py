"""
Environment configuration for synthetic data pipeline
Supports both Databricks secret scope and local development
"""

import os
import sys
from typing import Optional

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

# Get secrets directly from Databricks secret scope only
NVIDIA_API_KEY = get_secret_from_databricks(SCOPE_NAME, "nvidiaapi")

AZURE_STORAGE_ACCOUNT = get_secret_from_databricks(SCOPE_NAME, "synthenticstorage")

AZURE_SAS_TOKEN = get_secret_from_databricks(SCOPE_NAME, "adls-sas-token")

AZURE_CONTAINER_NAME = "datadesign"

# Legacy support - keeping original variable names available for backward compatibility
key = NVIDIA_API_KEY
container = AZURE_CONTAINER_NAME