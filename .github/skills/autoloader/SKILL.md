---
name: autoloader
# Copilot Instructions — Azure Databricks Auto Loader Ingestion

## Goal
When asked to implement ingestion from cloud object storage, generate production-quality Azure Databricks Auto Loader code.

## Auto Loader Fundamentals (must follow)
- Use Structured Streaming source `cloudFiles` (Auto Loader) for incremental file ingestion. 
- Always configure a durable streaming checkpoint (`checkpointLocation`) for exactly-once semantics to Delta.
- Prefer writing to Delta sinks (tables or paths) using streaming writes.

## Required readStream pattern
- Use: spark.readStream.format("cloudFiles")
- Always set:
  - .option("cloudFiles.format", "<format>")
  - .option("cloudFiles.schemaLocation", "<schema_location>") when using schema inference/evolution
- Include schema inference options only when needed:
  - .option("cloudFiles.inferColumnTypes", "true") for JSON/CSV when you want typed inference.
  - .option("cloudFiles.schemaEvolutionMode", "<mode>") to handle drift (addNewColumns/rescue/failOnNewColumns/none).

## File discovery guidance
- If the source is a Unity Catalog external location with file events enabled, prefer:
  - .option("cloudFiles.useManagedFileEvents", "true")
- Otherwise, default to directory listing with appropriate throttles (maxFilesPerTrigger/maxBytesPerTrigger).

## Output write pattern (must follow)
- Use .writeStream.format("delta")
- Always set:
  - .option("checkpointLocation", "<checkpoint_location>")
- Prefer .trigger(availableNow=True) for batch-style backfills or .trigger(processingTime="...") for continuous pipelines.
- Use outputMode("append") for Bronze ingestion unless explicitly asked for upserts.

## Observability & safety
- Add comments explaining each Auto Loader option.
- Do not hardcode secrets. Assume storage access is handled via Unity Catalog external locations or secret scopes.
- Provide a short "How to run" section (Job/Notebook) when asked.

## Deliverables format
- Provide: (1) concise explanation, (2) runnable PySpark code, and optionally (3) SQL streaming table equivalent if requested.