IF OBJECT_ID(N'syn_data.products', N'U') IS NULL
BEGIN
    CREATE TABLE [syn_data].[products] (
        [product_id] INT NULL,
        [product_name] NVARCHAR(255) NULL,
        [category] NVARCHAR(100) NULL,
        [price] NVARCHAR(32) NULL,
        [description] NVARCHAR(1000) NULL,
        [stock_quantity] INT NULL,
        [brand] NVARCHAR(100) NULL,
        [rating] DECIMAL(3,1) NULL,
        [_ingest_timestamp] DATETIME2 NULL
    );
END
