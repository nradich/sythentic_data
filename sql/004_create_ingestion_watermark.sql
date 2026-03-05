IF OBJECT_ID(N'syn_data.ingestion_watermark', N'U') IS NULL
BEGIN
    CREATE TABLE [syn_data].[ingestion_watermark] (
        [dataset_name] NVARCHAR(100) NULL,
        [last_ingest_timestamp] DATETIME2 NULL,
        [updated_at] DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
