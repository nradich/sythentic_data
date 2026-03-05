"""
Auto Loader Bronze Ingestion Pipeline
Ingests JSON files from ADLS into Bronze Delta paths using Databricks Auto Loader.
"""

from datetime import datetime
from typing import Dict

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit

from config.env import (
    AZURE_CONTAINER_NAME,
    AZURE_STORAGE_ACCOUNT,
    AUTOLOADER_FILE_FORMAT,
    AUTOLOADER_MAX_BYTES_PER_TRIGGER,
    AUTOLOADER_MAX_FILES_PER_TRIGGER,
    AUTOLOADER_SCHEMA_EVOLUTION_MODE,
    AUTOLOADER_USE_MANAGED_FILE_EVENTS,
)
from daily_synthetic_pipeline import configure_spark_adls_access


DATASETS = ["customers", "products", "orders"]


def build_paths(dataset_name: str) -> Dict[str, str]:
    """Build source, schema, checkpoint, and bronze target paths for a dataset."""
    base_path = f"abfss://{AZURE_CONTAINER_NAME}@{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net"
    return {
        "source_path": f"{base_path}/{dataset_name}",
        "schema_path": f"{base_path}/_autoloader/schema/{dataset_name}",
        "checkpoint_path": f"{base_path}/_autoloader/checkpoints/{dataset_name}",
        "bronze_path": f"{base_path}/bronze/{dataset_name}",
    }


def run_dataset_ingestion(spark: SparkSession, dataset_name: str) -> None:
    """Run Auto Loader ingestion for one dataset with availableNow trigger."""
    paths = build_paths(dataset_name)

    reader = (
        spark.readStream.format("cloudFiles")
        # Input file format for Auto Loader discovery and parsing.
        .option("cloudFiles.format", AUTOLOADER_FILE_FORMAT)
        # Durable schema tracking path for inference/evolution state.
        .option("cloudFiles.schemaLocation", paths["schema_path"])
        # Infer typed columns from JSON values when schema is not predefined.
        .option("cloudFiles.inferColumnTypes", "true")
        # Allow additive schema drift in Bronze (new columns are appended).
        .option("cloudFiles.schemaEvolutionMode", AUTOLOADER_SCHEMA_EVOLUTION_MODE)
        # Managed file events when available; otherwise Auto Loader falls back to listing.
        .option("cloudFiles.useManagedFileEvents", AUTOLOADER_USE_MANAGED_FILE_EVENTS)
        # Throttle files per micro-batch for controlled SQL downstream pressure.
        .option("maxFilesPerTrigger", AUTOLOADER_MAX_FILES_PER_TRIGGER)
        # Read full historical files on first startup for initial backfill.
        .option("cloudFiles.includeExistingFiles", "true")
    )

    if AUTOLOADER_MAX_BYTES_PER_TRIGGER:
        reader = reader.option("maxBytesPerTrigger", AUTOLOADER_MAX_BYTES_PER_TRIGGER)

    source_df = (
        reader.load(paths["source_path"])
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingest_timestamp", current_timestamp())
        .withColumn("_dataset", lit(dataset_name))
        .withColumn("_ingest_run_date", lit(datetime.utcnow().strftime("%Y-%m-%d")))
    )

    query = (
        source_df.writeStream.format("delta")
        # Durable checkpoint for exactly-once progression into Bronze Delta.
        .option("checkpointLocation", paths["checkpoint_path"])
        .outputMode("append")
        # Scheduled batch-style trigger: process available files then stop.
        .trigger(availableNow=True)
        .start(paths["bronze_path"])
    )

    query.awaitTermination()


def run_bronze_ingestion() -> None:
    """Entry point for running Bronze ingestion across all datasets."""
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.appName("AutoLoaderBronzeIngestion").getOrCreate()

    configure_spark_adls_access(spark)

    if not AZURE_STORAGE_ACCOUNT:
        raise RuntimeError("AZURE_STORAGE_ACCOUNT is not configured.")

    for dataset_name in DATASETS:
        print(f"Starting Auto Loader Bronze ingestion for {dataset_name}...")
        run_dataset_ingestion(spark, dataset_name)
        print(f"Completed Auto Loader Bronze ingestion for {dataset_name}.")


if __name__ == "__main__":
    run_bronze_ingestion()
