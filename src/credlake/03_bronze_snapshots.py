# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — snapshots de clientes e parcelas
# MAGIC Publica snapshots de forma atômica e idempotente com `replaceWhere`.

# COMMAND ----------

import re
from datetime import date

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

dbutils.widgets.text("catalog", "credlake", "Catálogo")
dbutils.widgets.text("reference_date", "2026-08-01", "Data de referência")
CATALOG = dbutils.widgets.get("catalog").strip()
REFERENCE_DATE = dbutils.widgets.get("reference_date").strip()

if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", CATALOG):
    raise ValueError(f"Nome de catálogo inválido: {CATALOG!r}")
date.fromisoformat(REFERENCE_DATE)

customers_path = (
    f"/Volumes/{CATALOG}/landing/raw/customers/"
    f"snapshot_date={REFERENCE_DATE}/"
)
installments_path = (
    f"/Volumes/{CATALOG}/landing/raw/installments/"
    f"snapshot_date={REFERENCE_DATE}/"
)

# COMMAND ----------

customer_schema = StructType(
    [
        StructField("customer_id", LongType(), True),
        StructField("customer_type", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("document_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("state", StringType(), True),
        StructField("risk_rating", StringType(), True),
        StructField("monthly_income", DecimalType(18, 2), True),
        StructField("created_at", DateType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("source_system", StringType(), True),
        StructField("source_batch_id", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)

customers_df = (
    spark.read
    .schema(customer_schema)
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .json(customers_path)
    .select(
        "customer_id",
        "customer_type",
        "customer_name",
        "document_id",
        "email",
        "state",
        "risk_rating",
        "monthly_income",
        "created_at",
        "updated_at",
        "is_active",
        "source_system",
        "source_batch_id",
        F.to_date(F.lit(REFERENCE_DATE)).alias("snapshot_date"),
        F.col("_metadata.file_path").alias("source_file_path"),
        F.col("_metadata.file_name").alias("source_file_name"),
        F.col("_metadata.file_modification_time").alias(
            "source_file_modification_time"
        ),
        F.col("_corrupt_record").alias("corrupt_record"),
        F.current_timestamp().alias("ingested_at"),
    )
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS `{CATALOG}`.bronze.customers_raw (
        customer_id BIGINT,
        customer_type STRING,
        customer_name STRING,
        document_id STRING,
        email STRING,
        state STRING,
        risk_rating STRING,
        monthly_income DECIMAL(18,2),
        created_at DATE,
        updated_at TIMESTAMP,
        is_active BOOLEAN,
        source_system STRING,
        source_batch_id STRING,
        snapshot_date DATE,
        source_file_path STRING,
        source_file_name STRING,
        source_file_modification_time TIMESTAMP,
        corrupt_record STRING,
        ingested_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Snapshots de clientes recebidos em JSON.'
    TBLPROPERTIES (
        'data_layer' = 'bronze',
        'ingestion_pattern' = 'snapshot_replace_where',
        'contains_synthetic_data' = 'true'
    )
    """
)

customer_metrics = customers_df.agg(
    F.count("*").alias("rows"),
    F.countDistinct("customer_id").alias("ids"),
    F.count(F.when(F.col("corrupt_record").isNotNull(), 1)).alias("corrupt"),
).first()

assert customer_metrics["rows"] == 2_000, customer_metrics
assert customer_metrics["ids"] == 2_000, customer_metrics
assert customer_metrics["corrupt"] == 0, customer_metrics

(
    customers_df.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"snapshot_date = DATE '{REFERENCE_DATE}'")
    .option("userMetadata", f"customers snapshot_date={REFERENCE_DATE}")
    .saveAsTable(f"{CATALOG}.bronze.customers_raw")
)

# COMMAND ----------

installments_df = (
    spark.read.parquet(installments_path)
    .select(
        F.col("installment_id").cast("string").alias("installment_id"),
        F.col("contract_id").cast("long").alias("contract_id"),
        F.col("installment_number").cast("int").alias("installment_number"),
        F.col("due_date").cast("date").alias("due_date"),
        F.col("scheduled_amount").cast("decimal(18,2)").alias("scheduled_amount"),
        F.col("source_system").cast("string").alias("source_system"),
        F.col("source_batch_id").cast("string").alias("source_batch_id"),
        F.to_date(F.lit(REFERENCE_DATE)).alias("snapshot_date"),
        F.col("_metadata.file_path").alias("source_file_path"),
        F.col("_metadata.file_name").alias("source_file_name"),
        F.col("_metadata.file_size").alias("source_file_size"),
        F.col("_metadata.file_modification_time").alias(
            "source_file_modification_time"
        ),
        F.current_timestamp().alias("ingested_at"),
    )
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS `{CATALOG}`.bronze.installments_raw (
        installment_id STRING,
        contract_id BIGINT,
        installment_number INT,
        due_date DATE,
        scheduled_amount DECIMAL(18,2),
        source_system STRING,
        source_batch_id STRING,
        snapshot_date DATE,
        source_file_path STRING,
        source_file_name STRING,
        source_file_size BIGINT,
        source_file_modification_time TIMESTAMP,
        ingested_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Snapshots das parcelas contratuais recebidas em Parquet.'
    TBLPROPERTIES (
        'data_layer' = 'bronze',
        'ingestion_pattern' = 'snapshot_replace_where',
        'source_format' = 'parquet',
        'contains_synthetic_data' = 'true'
    )
    """
)

installment_metrics = installments_df.agg(
    F.count("*").alias("rows"),
    F.countDistinct("installment_id").alias("ids"),
    F.count(F.when(F.col("scheduled_amount") <= 0, 1)).alias("invalid_amounts"),
).first()

assert installment_metrics["rows"] > 0, installment_metrics
assert installment_metrics["rows"] == installment_metrics["ids"], installment_metrics
assert installment_metrics["invalid_amounts"] == 0, installment_metrics

(
    installments_df.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"snapshot_date = DATE '{REFERENCE_DATE}'")
    .option("userMetadata", f"installments snapshot_date={REFERENCE_DATE}")
    .saveAsTable(f"{CATALOG}.bronze.installments_raw")
)

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT 'customers_raw' AS dataset, COUNT(*) AS rows
        FROM `{CATALOG}`.bronze.customers_raw
        WHERE snapshot_date = DATE '{REFERENCE_DATE}'
        UNION ALL
        SELECT 'installments_raw', COUNT(*)
        FROM `{CATALOG}`.bronze.installments_raw
        WHERE snapshot_date = DATE '{REFERENCE_DATE}'
        """
    )
)

print("Snapshots Bronze publicados com sucesso.")

