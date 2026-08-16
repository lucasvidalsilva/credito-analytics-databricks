# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — pagamentos incrementais com Auto Loader
# MAGIC Usa schema explícito, checkpoint governado, metadados e coluna de resgate.

# COMMAND ----------

import re

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

dbutils.widgets.text("catalog", "credlake", "Catálogo")
CATALOG = dbutils.widgets.get("catalog").strip()

if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", CATALOG):
    raise ValueError(f"Nome de catálogo inválido: {CATALOG!r}")

source_path = f"/Volumes/{CATALOG}/landing/raw/payments/"
schema_location = f"/Volumes/{CATALOG}/ops/pipeline_state/schemas/payments/"
checkpoint_location = (
    f"/Volumes/{CATALOG}/ops/pipeline_state/checkpoints/payments_bronze/"
)
target_table = f"{CATALOG}.bronze.payments_raw"

# COMMAND ----------

payment_schema = StructType(
    [
        StructField("payment_id", StringType(), True),
        StructField("installment_id", StringType(), True),
        StructField("contract_id", LongType(), True),
        StructField("installment_number", IntegerType(), True),
        StructField("payment_date", DateType(), True),
        StructField("amount_paid", DecimalType(18, 2), True),
        StructField("event_type", StringType(), True),
        StructField("event_timestamp", TimestampType(), True),
        StructField("source_system", StringType(), True),
        StructField("source_batch_id", StringType(), True),
        StructField("_rescued_data", StringType(), True),
    ]
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS `{CATALOG}`.bronze.payments_raw (
        payment_id STRING,
        installment_id STRING,
        contract_id BIGINT,
        installment_number INT,
        payment_date DATE,
        amount_paid DECIMAL(18,2),
        event_type STRING,
        event_timestamp TIMESTAMP,
        source_system STRING,
        source_batch_id STRING,
        _rescued_data STRING,
        event_date DATE,
        source_file_path STRING,
        source_file_name STRING,
        source_file_size BIGINT,
        source_file_modification_time TIMESTAMP,
        ingested_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Eventos de pagamento ingeridos incrementalmente com Auto Loader.'
    TBLPROPERTIES (
        'data_layer' = 'bronze',
        'data_domain' = 'payments',
        'ingestion_pattern' = 'auto_loader_available_now',
        'contains_synthetic_data' = 'true'
    )
    """
)

# COMMAND ----------

payments_stream = (
    spark.readStream
    .format("cloudFiles")
    .schema(payment_schema)
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("rescuedDataColumn", "_rescued_data")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("cloudFiles.partitionColumns", "event_date")
    .load(source_path)
    .select(
        "payment_id",
        "installment_id",
        "contract_id",
        "installment_number",
        "payment_date",
        "amount_paid",
        "event_type",
        "event_timestamp",
        "source_system",
        "source_batch_id",
        "_rescued_data",
        F.to_date("event_date").alias("event_date"),
        F.col("_metadata.file_path").alias("source_file_path"),
        F.col("_metadata.file_name").alias("source_file_name"),
        F.col("_metadata.file_size").alias("source_file_size"),
        F.col("_metadata.file_modification_time").alias(
            "source_file_modification_time"
        ),
        F.current_timestamp().alias("ingested_at"),
    )
)

query = (
    payments_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_location)
    .queryName("credlake_payments_bronze_autoloader")
    .trigger(availableNow=True)
    .toTable(target_table)
)
query.awaitTermination()

# COMMAND ----------

metrics = spark.table(target_table).agg(
    F.count("*").alias("rows"),
    F.countDistinct("payment_id").alias("ids"),
    F.count(F.when(F.col("payment_id").isNull(), 1)).alias("null_ids"),
    F.count(F.when(F.col("amount_paid").isNull(), 1)).alias("null_amounts"),
    F.count(F.when(F.col("_rescued_data").isNotNull(), 1)).alias("rescued"),
).first()

assert metrics["rows"] > 0, metrics
assert metrics["rows"] - metrics["ids"] == 1, metrics
assert metrics["null_ids"] == 0, metrics
assert metrics["null_amounts"] == 1, metrics
assert metrics["rescued"] == 0, metrics

display(spark.table(target_table).limit(20))
print("Pagamentos Bronze processados incrementalmente.")

