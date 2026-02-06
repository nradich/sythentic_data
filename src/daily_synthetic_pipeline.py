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
from env import container
from generate_realistic_data import generate_synthetic_data


def get_blob_service_client():
    """
    Initialize Azure Blob Service Client using Databricks authentication
    Assumes Databricks is already authenticated to ADLS
    """
    try:
        # In Databricks, use the default credential provider
        from azure.identity import DefaultAzureCredential
        credential = DefaultAzureCredential()
        
        # You'll need to replace this with your storage account name
        account_url = f"https://your-storage-account.blob.core.windows.net"
        
        return BlobServiceClient(account_url=account_url, credential=credential)
    except Exception as e:
        print(f"❌ Error initializing Azure client: {e}")
        raise


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
    Main pipeline function: Generate synthetic data and upload to ADLS
    """
    print("🚀 Starting daily synthetic data pipeline...")
    
    # Generate synthetic data (creates JSON files in data/ directory)
    output_dir = "data"
    success = generate_synthetic_data(output_dir=output_dir)
    
    if not success:
        print("❌ Data generation failed, aborting pipeline")
        return False
    
    # Initialize Azure client
    try:
        blob_service_client = get_blob_service_client()
    except Exception as e:
        print(f"❌ Failed to initialize Azure client: {e}")
        return False
    
    # Upload each dataset to ADLS
    datasets = ["customers", "products", "orders"]
    upload_results = []
    
    for dataset in datasets:
        json_file = Path(output_dir) / f"{dataset}.json"
        
        if json_file.exists():
            result = upload_json_to_adls(json_file, dataset, blob_service_client)
            upload_results.append(result)
        else:
            print(f"❌ JSON file not found: {json_file}")
            upload_results.append(False)
    
    # Summary
    successful_uploads = sum(upload_results)
    total_datasets = len(datasets)
    
    print(f"📊 Pipeline Summary: {successful_uploads}/{total_datasets} datasets uploaded successfully")
    
    if successful_uploads == total_datasets:
        print("✅ Daily pipeline completed successfully!")
        return True
    else:
        print("⚠️ Pipeline completed with errors")
        return False


if __name__ == "__main__":
    # Run the daily pipeline
    run_daily_pipeline()