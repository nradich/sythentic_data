IF OBJECT_ID(N'syn_data.customers', N'U') IS NULL
BEGIN
    CREATE TABLE [syn_data].[customers] (
        [customer_id] INT NULL,
        [first_name] NVARCHAR(100) NULL,
        [last_name] NVARCHAR(100) NULL,
        [email] NVARCHAR(255) NULL,
        [phone] NVARCHAR(40) NULL,
        [address] NVARCHAR(500) NULL,
        [registration_date] DATE NULL,
        [customer_tier] NVARCHAR(30) NULL,
        [_ingest_timestamp] DATETIME2 NULL
    );
END
