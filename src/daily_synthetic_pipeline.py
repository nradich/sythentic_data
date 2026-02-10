"""
Daily Synthetic Data Pipeline for ADLS Upload
Generates synthetic data in JSON format and uploads to Azure Data Lake Storage
with date-partitioned folder structure: dataset/YYYY/MM/DD/
"""

import os
import json
from datetime import datetime
from pathlib import Path
from config.env import container, AZURE_SAS_TOKEN, AZURE_STORAGE_ACCOUNT
from generate_realistic_data import main as generate_synthetic_data


def configure_spark_adls_access(spark_session=None):
    """
    Configure Spark session for ADLS Gen2 access using SAS token authentication
    Implements the ADLS authentication pattern for Databricks/Spark environments
    """
    try:
        # Get or create Spark session
        if spark_session is None:
            from pyspark.sql import SparkSession
            spark_session = SparkSession.getActiveSession()
            if spark_session is None:
                print("No active Spark session found - Spark ADLS configuration skipped")
                return None
        
        if AZURE_SAS_TOKEN and AZURE_STORAGE_ACCOUNT:
            # Configure Spark for ADLS Gen2 access with SAS token
            storage_account = AZURE_STORAGE_ACCOUNT
            sas_token = AZURE_SAS_TOKEN
            
            # Set authentication type to SAS
            spark_session.conf.set(
                f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", 
                "SAS"
            )
            
            # Set SAS token provider
            spark_session.conf.set(
                f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", 
                "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider"
            )
            
            # Set the actual SAS token
            spark_session.conf.set(
                f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", 
                sas_token
            )
            
            # Define ABFSS base path for easy access
            base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
            
            print(f"✅ Spark ADLS configuration complete: {base_path}")
            return base_path
        else:
            print("⚠️ AZURE_SAS_TOKEN or AZURE_STORAGE_ACCOUNT not configured - Spark ADLS setup skipped")
            return None
            
    except ImportError:
        print("⚠️ PySpark not available - Spark ADLS configuration skipped")
        return None
    except Exception as e:
        print(f"❌ Error configuring Spark ADLS access: {e}")
        return None


def run_daily_pipeline():
    """
    Main pipeline function: Generate synthetic data and write to ADLS using Spark
    """
    print("🚀 Starting daily synthetic data pipeline...")
    
    # Configure Spark for ADLS Gen2 access
    try:
        from pyspark.sql import SparkSession
        spark_session = SparkSession.getActiveSession()
        if spark_session is None:
            spark_session = SparkSession.builder.appName("SyntheticDataPipeline").getOrCreate()
        
        abfss_base_path = configure_spark_adls_access(spark_session)
        if not abfss_base_path:
            raise RuntimeError("Failed to configure Spark ADLS access")
        
        print(f"✅ Spark ADLS configuration complete: {abfss_base_path}")
    except Exception as e:
        print(f"❌ Failed to configure Spark ADLS access: {e}")
        return False
    
    # Generate synthetic data and write directly to ADLS using Spark
    success = generate_synthetic_data(spark_session, abfss_base_path)
    
    if success:
        print("✅ Daily pipeline completed successfully!")
        return True
    else:
        print("❌ Pipeline completed with errors")
        return False


if __name__ == "__main__":
    # Run the daily pipeline
    run_daily_pipeline()