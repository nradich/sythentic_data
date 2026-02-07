"""
Daily Synthetic Data Pipeline for ADLS Upload
Generates synthetic data in JSON format and uploads to Azure Data Lake Storage
with date-partitioned folder structure: dataset/YYYY/MM/DD/
"""

import os
import json
from datetime import datetime
from pathlib import Path
from azure.storage.blob import BlobServiceClient
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


def get_blob_service_client():
    """
    Initialize Azure Blob Service Client using SAS token from Databricks secret scope
    Enhanced with error handling and fallback authentication methods
    """
    if not AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_ACCOUNT == "your-storage-account":
        raise ValueError("AZURE_STORAGE_ACCOUNT must be configured in Databricks secret scope or environment")
    
    account_url = f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net"
    
    try:
        if AZURE_SAS_TOKEN and AZURE_SAS_TOKEN != "None":
            # Primary: SAS token authentication (Databricks preferred)
            print(f"✅ Using SAS token authentication for {AZURE_STORAGE_ACCOUNT}")
            return BlobServiceClient(account_url=account_url, credential=AZURE_SAS_TOKEN)
        else:
            # Fallback: Managed identity authentication for local development
            print(f"⚠️ No SAS token found, attempting DefaultAzureCredential for {AZURE_STORAGE_ACCOUNT}")
            from azure.identity import DefaultAzureCredential
            credential = DefaultAzureCredential()
            return BlobServiceClient(account_url=account_url, credential=credential)
            
    except Exception as auth_error:
        error_msg = f"Azure authentication failed for {AZURE_STORAGE_ACCOUNT}: {auth_error}"
        print(f"❌ {error_msg}")
        raise RuntimeError(error_msg) from auth_error


def upload_json_to_adls(file_path, dataset_name, blob_service_client):
    """
    Upload JSON file to ADLS with date-partitioned folder structure
    Format: dataset/YYYY/MM/DD/dataset_YYYYMMDD_HHMM.json
    """
    now = datetime.now()
    date_path = f"{dataset_name}/{now.year:04d}/{now.month:02d}/{now.day:02d}"
    timestamp = now.strftime("%Y%m%d_%H%M")
    blob_name = f"{date_path}/{dataset_name}_{timestamp}.json"
    
    try:
        blob_client = blob_service_client.get_blob_client(
            container=container, 
            blob=blob_name
        )
        
        with open(file_path, 'rb') as data:
            blob_client.upload_blob(data, overwrite=True)
        
        print(f"✅ Uploaded {dataset_name} to ADLS: {blob_name}")
        return True
        
    except Exception as e:
        print(f"❌ Error uploading {dataset_name}: {e}")
        return False


def run_daily_pipeline():
    """
    Main pipeline function: Generate synthetic data and upload directly to ADLS
    """
    print("🚀 Starting daily synthetic data pipeline...")
    
    # Initialize Azure client first
    try:
        blob_service_client = get_blob_service_client()
        print(f"✅ Azure client initialized for storage account: {AZURE_STORAGE_ACCOUNT}")
    except Exception as e:
        print(f"❌ Failed to initialize Azure client: {e}")
        return False
    
    # Generate synthetic data and upload directly to ADLS
    success = generate_synthetic_data(blob_service_client, container)
    
    if success:
        print("✅ Daily pipeline completed successfully!")
        return True
    else:
        print("❌ Pipeline completed with errors")
        return False


if __name__ == "__main__":
    # Run the daily pipeline
    run_daily_pipeline()