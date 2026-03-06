"""
Clear Auto Loader checkpoint for a specific dataset.

Usage (Databricks Python script / job task):
    python clear_checkpoint.py                   # clears all datasets
    python clear_checkpoint.py --dataset customers
"""

import argparse
import sys

from config.env import AZURE_CONTAINER_NAME, AZURE_STORAGE_ACCOUNT

DATASETS = ["customers", "products", "orders"]


def get_checkpoint_path(dataset_name: str) -> str:
    base = f"abfss://{AZURE_CONTAINER_NAME}@{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net"
    return f"{base}/_autoloader/checkpoints/{dataset_name}"


def clear_checkpoint(dataset_name: str) -> None:
    try:
        dbutils  # noqa: F821 — available in Databricks runtime
    except NameError:
        raise RuntimeError("dbutils is not available. Run this script inside a Databricks environment.")

    path = get_checkpoint_path(dataset_name)
    print(f"Clearing checkpoint: {path}")
    dbutils.fs.rm(path, recurse=True)  # noqa: F821
    print(f"Done: {dataset_name}")


def main(dataset: str) -> None:
    targets = DATASETS if dataset == "all" else [dataset]

    if dataset != "all" and dataset not in DATASETS:
        print(f"Unknown dataset '{dataset}'. Valid options: {DATASETS + ['all']}")
        sys.exit(1)

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
