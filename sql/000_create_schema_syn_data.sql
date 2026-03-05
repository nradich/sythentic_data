IF SCHEMA_ID(N'syn_data') IS NULL
BEGIN
    EXEC('CREATE SCHEMA [syn_data]');
END
