"""
Clear Auto Loader checkpoint for a specific dataset.

Usage (Databricks Python script / job task):
    python clear_checkpoint.py                   # clears all datasets
    python clear_checkpoint.py --dataset customers
"""

import argparse
import sys

from pyspark.sql import SparkSession

from config.env import AZURE_CONTAINER_NAME, AZURE_STORAGE_ACCOUNT, AZURE_SAS_TOKEN

DATASETS = ["customers", "products", "orders"]


def get_checkpoint_path(dataset_name: str) -> str:
    base = f"abfss://{AZURE_CONTAINER_NAME}@{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net"
    return f"{base}/_autoloader/checkpoints/{dataset_name}"


def configure_adls(spark: SparkSession) -> None:
    """Set SAS token auth on the active Spark session so dbutils.fs can reach ADLS."""
    spark.conf.set(
        f"fs.azure.account.auth.type.{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net", "SAS"
    )
    spark.conf.set(
        f"fs.azure.sas.token.provider.type.{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider",
    )
    spark.conf.set(
        f"fs.azure.sas.fixed.token.{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net",
        AZURE_SAS_TOKEN,
    )


def clear_checkpoint(dataset_name: str) -> None:
    path = get_checkpoint_path(dataset_name)
    print(f"Clearing checkpoint: {path}")
    dbutils.fs.rm(path, recurse=True)  # noqa: F821
    print(f"Done: {dataset_name}")


def main(dataset: str) -> None:
    targets = DATASETS if dataset == "all" else [dataset]

    if dataset != "all" and dataset not in DATASETS:
        print(f"Unknown dataset '{dataset}'. Valid options: {DATASETS + ['all']}")
        sys.exit(1)

    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.appName("ClearCheckpoint").getOrCreate()
    configure_adls(spark)

    for name in targets:
        clear_checkpoint(name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clear Auto Loader checkpoint for a dataset.")
    parser.add_argument(
        "--dataset",
        default="all",
        help=f"Dataset name or 'all' (default). Options: {DATASETS}",
    )
    args, _ = parser.parse_known_args()  # ignore kernel-injected args
    main(args.dataset)
