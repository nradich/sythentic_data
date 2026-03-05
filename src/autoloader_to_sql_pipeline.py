"""
Orchestrates Auto Loader Bronze ingestion followed by SQL Server publish.
"""

from autoloader_bronze import run_bronze_ingestion
from sqlserver_publish import run_sqlserver_publish


def run_pipeline() -> None:
    """Run ingestion pipeline end-to-end."""
    print("Starting Bronze Auto Loader ingestion...")
    run_bronze_ingestion()
    print("Bronze ingestion finished. Starting SQL Server publish...")
    run_sqlserver_publish()
    print("Pipeline completed successfully.")


if __name__ == "__main__":
    run_pipeline()
