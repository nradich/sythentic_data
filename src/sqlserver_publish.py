"""
Publish Bronze Delta data to SQL Server using ODBC with idempotent upserts.
"""

from datetime import datetime
from typing import Dict, List, Tuple

import pyodbc
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.env import (
    AZURE_CONTAINER_NAME,
    AZURE_STORAGE_ACCOUNT,
    SQL_TABLE_CUSTOMERS,
    SQL_TABLE_ORDERS,
    SQL_TABLE_PRODUCTS,
    SQL_WATERMARK_TABLE,
    get_sql_server_config,
)
from daily_synthetic_pipeline import configure_spark_adls_access


DATASET_SQL_CONFIG: Dict[str, Dict] = {
    "customers": {
        "target_table": SQL_TABLE_CUSTOMERS,
        "key_columns": ["customer_id"],
        "columns": [
            ("customer_id", "INT"),
            ("first_name", "NVARCHAR(100)"),
            ("last_name", "NVARCHAR(100)"),
            ("email", "NVARCHAR(255)"),
            ("phone", "NVARCHAR(40)"),
            ("address", "NVARCHAR(500)"),
            ("registration_date", "DATE"),
            ("customer_tier", "NVARCHAR(30)"),
            ("_ingest_timestamp", "DATETIME2"),
        ],
    },
    "products": {
        "target_table": SQL_TABLE_PRODUCTS,
        "key_columns": ["product_id"],
        "columns": [
            ("product_id", "INT"),
            ("product_name", "NVARCHAR(255)"),
            ("category", "NVARCHAR(100)"),
            ("price", "NVARCHAR(32)"),
            ("description", "NVARCHAR(1000)"),
            ("stock_quantity", "INT"),
            ("brand", "NVARCHAR(100)"),
            ("rating", "DECIMAL(3,1)"),
            ("_ingest_timestamp", "DATETIME2"),
        ],
    },
    "orders": {
        "target_table": SQL_TABLE_ORDERS,
        "key_columns": ["order_id"],
        "columns": [
            ("order_id", "INT"),
            ("customer_id", "INT"),
            ("product_id", "INT"),
            ("order_date", "DATE"),
            ("quantity", "INT"),
            ("total_amount", "NVARCHAR(32)"),
            ("status", "NVARCHAR(40)"),
            ("shipping_address", "NVARCHAR(500)"),
            ("_ingest_timestamp", "DATETIME2"),
        ],
    },
}


def _validate_sql_config(sql_config: Dict[str, str]) -> None:
    required = {
        "SQL_SERVER_HOST": sql_config.get("host"),
        "SQL_SERVER_DATABASE": sql_config.get("database"),
        "SQL_SERVER_USERNAME": sql_config.get("username"),
        "SQL_SERVER_PASSWORD": sql_config.get("password"),
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise RuntimeError(f"Missing SQL configuration values: {', '.join(missing)}")


def _safe_name(name: str) -> str:
    return f"[{name.replace(']', ']]')}]"


def _parse_table_name(table_name: str) -> Tuple[str, str]:
    if "." in table_name:
        schema_name, object_name = table_name.split(".", 1)
    else:
        schema_name, object_name = "dbo", table_name
    return schema_name.strip("[]"), object_name.strip("[]")


def _fq_table_name(table_name: str) -> str:
    schema_name, object_name = _parse_table_name(table_name)
    return f"{_safe_name(schema_name)}.{_safe_name(object_name)}"


def _table_object_id_literal(table_name: str) -> str:
    schema_name, object_name = _parse_table_name(table_name)
    return f"{schema_name}.{object_name}"


def _connection(sql_config: Dict[str, str]) -> pyodbc.Connection:
    connection_string = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_config['host']},{sql_config.get('port', '1433')};"
        f"DATABASE={sql_config['database']};"
        f"UID={sql_config['username']};"
        f"PWD={sql_config['password']};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    return pyodbc.connect(connection_string)


def _ensure_watermark_table(cursor: pyodbc.Cursor) -> None:
    watermark_fqn = _fq_table_name(SQL_WATERMARK_TABLE)
    watermark_object = _table_object_id_literal(SQL_WATERMARK_TABLE)
    cursor.execute(
        f"""
IF OBJECT_ID(N'{watermark_object}', N'U') IS NULL
BEGIN
    CREATE TABLE {watermark_fqn} (
        dataset_name NVARCHAR(100) NOT NULL PRIMARY KEY,
        last_ingest_timestamp DATETIME2 NULL,
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
"""
    )


def _ensure_target_table(
    cursor: pyodbc.Cursor, table_name: str, columns: List[Tuple[str, str]], key_columns: List[str]
) -> None:
    table_fqn = _fq_table_name(table_name)
    table_object = _table_object_id_literal(table_name)

    column_ddl = []
    for col_name, col_type in columns:
        nullability = "NOT NULL" if col_name in key_columns else "NULL"
        column_ddl.append(f"{_safe_name(col_name)} {col_type} {nullability}")

    pk_name = f"PK_{_parse_table_name(table_name)[1]}"
    pk_cols = ", ".join(_safe_name(col) for col in key_columns)

    cursor.execute(
        f"""
IF OBJECT_ID(N'{table_object}', N'U') IS NULL
BEGIN
    CREATE TABLE {table_fqn} (
        {", ".join(column_ddl)},
        CONSTRAINT {_safe_name(pk_name)} PRIMARY KEY ({pk_cols})
    );
END
"""
    )


def _get_last_watermark(cursor: pyodbc.Cursor, dataset_name: str):
    watermark_fqn = _fq_table_name(SQL_WATERMARK_TABLE)
    row = cursor.execute(
        f"SELECT last_ingest_timestamp FROM {watermark_fqn} WHERE dataset_name = ?",
        dataset_name,
    ).fetchone()
    return row[0] if row else None


def _update_watermark(cursor: pyodbc.Cursor, dataset_name: str, watermark_value: datetime) -> None:
    watermark_fqn = _fq_table_name(SQL_WATERMARK_TABLE)
    cursor.execute(
        f"""
MERGE {watermark_fqn} AS target
USING (SELECT ? AS dataset_name, ? AS last_ingest_timestamp) AS source
ON target.dataset_name = source.dataset_name
WHEN MATCHED THEN
    UPDATE SET
        target.last_ingest_timestamp = source.last_ingest_timestamp,
        target.updated_at = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (dataset_name, last_ingest_timestamp, updated_at)
    VALUES (source.dataset_name, source.last_ingest_timestamp, SYSUTCDATETIME());
""",
        dataset_name,
        watermark_value,
    )


def _build_merge_sql(table_name: str, columns: List[str], key_columns: List[str]) -> str:
    table_fqn = _fq_table_name(table_name)
    source_cols = ", ".join(_safe_name(col) for col in columns)
    placeholders = ", ".join("?" for _ in columns)

    on_clause = " AND ".join([f"target.{_safe_name(col)} = source.{_safe_name(col)}" for col in key_columns])
    update_columns = [col for col in columns if col not in key_columns]
    update_set = ", ".join([f"target.{_safe_name(col)} = source.{_safe_name(col)}" for col in update_columns])

    insert_cols = ", ".join(_safe_name(col) for col in columns)
    insert_vals = ", ".join([f"source.{_safe_name(col)}" for col in columns])

    return f"""
MERGE {table_fqn} AS target
USING (SELECT {placeholders}) AS source ({source_cols})
ON {on_clause}
WHEN MATCHED THEN
    UPDATE SET {update_set}
WHEN NOT MATCHED THEN
    INSERT ({insert_cols}) VALUES ({insert_vals});
"""


def _bronze_path(dataset_name: str) -> str:
    return f"abfss://{AZURE_CONTAINER_NAME}@{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/{dataset_name}"


def _get_incremental_df(spark: SparkSession, dataset_name: str, last_watermark):
    dataset_conf = DATASET_SQL_CONFIG[dataset_name]
    columns = [name for name, _ in dataset_conf["columns"]]
    key_columns = dataset_conf["key_columns"]

    source_df = spark.read.format("delta").load(_bronze_path(dataset_name))

    incremental_df = source_df
    if last_watermark:
        incremental_df = incremental_df.filter(F.col("_ingest_timestamp") > F.lit(last_watermark))

    dedupe_window = Window.partitionBy(*key_columns).orderBy(F.col("_ingest_timestamp").desc())
    incremental_df = (
        incremental_df.withColumn("_row_rank", F.row_number().over(dedupe_window))
        .filter(F.col("_row_rank") == 1)
        .drop("_row_rank")
    )

    return incremental_df.select(*columns)


def _publish_dataset(cursor: pyodbc.Cursor, spark: SparkSession, dataset_name: str) -> int:
    dataset_conf = DATASET_SQL_CONFIG[dataset_name]
    table_name = dataset_conf["target_table"]
    key_columns = dataset_conf["key_columns"]
    col_defs = dataset_conf["columns"]
    col_names = [name for name, _ in col_defs]

    _ensure_target_table(cursor, table_name, col_defs, key_columns)
    last_watermark = _get_last_watermark(cursor, dataset_name)

    incremental_df = _get_incremental_df(spark, dataset_name, last_watermark)
    if incremental_df.rdd.isEmpty():
        print(f"No new records to publish for {dataset_name}.")
        return 0

    max_watermark = incremental_df.agg(F.max("_ingest_timestamp").alias("max_ts")).collect()[0]["max_ts"]
    rows = [tuple(row[c] for c in col_names) for row in incremental_df.collect()]

    merge_sql = _build_merge_sql(table_name, col_names, key_columns)
    cursor.fast_executemany = True
    cursor.executemany(merge_sql, rows)
    _update_watermark(cursor, dataset_name, max_watermark)

    print(f"Published {len(rows)} rows to {table_name}.")
    return len(rows)


def run_sqlserver_publish() -> None:
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.appName("BronzeToSqlServerPublish").getOrCreate()

    sql_config = get_sql_server_config()
    _validate_sql_config(sql_config)

    configure_spark_adls_access(spark)

    with _connection(sql_config) as conn:
        cursor = conn.cursor()
        _ensure_watermark_table(cursor)

        total_rows = 0
        for dataset_name in DATASET_SQL_CONFIG.keys():
            total_rows += _publish_dataset(cursor, spark, dataset_name)

        conn.commit()
        print(f"SQL publish complete. Total rows written: {total_rows}")


if __name__ == "__main__":
    run_sqlserver_publish()
