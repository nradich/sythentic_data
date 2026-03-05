IF OBJECT_ID(N'syn_data.orders', N'U') IS NULL
BEGIN
    CREATE TABLE [syn_data].[orders] (
        [order_id] INT NULL,
        [customer_id] INT NULL,
        [product_id] INT NULL,
        [order_date] DATE NULL,
        [quantity] INT NULL,
        [total_amount] NVARCHAR(32) NULL,
        [status] NVARCHAR(40) NULL,
        [shipping_address] NVARCHAR(500) NULL,
        [_ingest_timestamp] DATETIME2 NULL
    );
END
