# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — produtos e contratos com COPY INTO
# MAGIC Carga de arquivos CSV com tipagem, metadados e idempotência por arquivo.

# COMMAND ----------

import re

dbutils.widgets.text("catalog", "credlake", "Catálogo")
CATALOG = dbutils.widgets.get("catalog").strip()

if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", CATALOG):
    raise ValueError(f"Nome de catálogo inválido: {CATALOG!r}")

products_path = f"/Volumes/{CATALOG}/landing/raw/products/full/"
contracts_path = f"/Volumes/{CATALOG}/landing/raw/contracts/"

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS `{CATALOG}`.bronze.products_raw (
        product_id INT,
        product_name STRING,
        allowed_customer_type STRING,
        base_annual_interest_rate DECIMAL(10,4),
        max_term_months INT,
        source_batch_id STRING,
        source_file_path STRING,
        source_file_name STRING,
        source_file_modification_time TIMESTAMP,
        ingested_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Produtos recebidos em CSV, preservados na camada Bronze.'
    TBLPROPERTIES (
        'data_layer' = 'bronze',
        'data_domain' = 'credit',
        'ingestion_pattern' = 'copy_into',
        'contains_synthetic_data' = 'true'
    )
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS `{CATALOG}`.bronze.contracts_raw (
        contract_id BIGINT,
        customer_id BIGINT,
        product_id INT,
        contract_date DATE,
        principal_amount DECIMAL(18,2),
        annual_interest_rate DECIMAL(10,4),
        term_months INT,
        contract_status STRING,
        source_updated_at TIMESTAMP,
        source_system STRING,
        source_batch_id STRING,
        source_file_path STRING,
        source_file_name STRING,
        source_file_modification_time TIMESTAMP,
        ingested_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Contratos recebidos em CSV, incluindo problemas para tratamento posterior.'
    TBLPROPERTIES (
        'data_layer' = 'bronze',
        'data_domain' = 'credit',
        'ingestion_pattern' = 'copy_into',
        'contains_synthetic_data' = 'true'
    )
    """
)

# COMMAND ----------

products_result = spark.sql(
    f"""
    COPY INTO `{CATALOG}`.bronze.products_raw
    FROM (
        SELECT
            CAST(product_id AS INT) AS product_id,
            CAST(product_name AS STRING) AS product_name,
            CAST(allowed_customer_type AS STRING) AS allowed_customer_type,
            CAST(base_annual_interest_rate AS DECIMAL(10,4)) AS base_annual_interest_rate,
            CAST(max_term_months AS INT) AS max_term_months,
            CAST(source_batch_id AS STRING) AS source_batch_id,
            _metadata.file_path AS source_file_path,
            _metadata.file_name AS source_file_name,
            _metadata.file_modification_time AS source_file_modification_time,
            current_timestamp() AS ingested_at
        FROM '{products_path}'
    )
    FILEFORMAT = CSV
    FORMAT_OPTIONS ('header' = 'true')
    COPY_OPTIONS ('mergeSchema' = 'false')
    """
)
display(products_result)

# COMMAND ----------

contracts_result = spark.sql(
    f"""
    COPY INTO `{CATALOG}`.bronze.contracts_raw
    FROM (
        SELECT
            CAST(contract_id AS BIGINT) AS contract_id,
            CAST(customer_id AS BIGINT) AS customer_id,
            CAST(product_id AS INT) AS product_id,
            CAST(contract_date AS DATE) AS contract_date,
            CAST(principal_amount AS DECIMAL(18,2)) AS principal_amount,
            CAST(annual_interest_rate AS DECIMAL(10,4)) AS annual_interest_rate,
            CAST(term_months AS INT) AS term_months,
            CAST(contract_status AS STRING) AS contract_status,
            CAST(source_updated_at AS TIMESTAMP) AS source_updated_at,
            CAST(source_system AS STRING) AS source_system,
            CAST(source_batch_id AS STRING) AS source_batch_id,
            _metadata.file_path AS source_file_path,
            _metadata.file_name AS source_file_name,
            _metadata.file_modification_time AS source_file_modification_time,
            current_timestamp() AS ingested_at
        FROM '{contracts_path}'
    )
    FILEFORMAT = CSV
    FORMAT_OPTIONS ('header' = 'true')
    COPY_OPTIONS ('mergeSchema' = 'false')
    """
)
display(contracts_result)

# COMMAND ----------

metrics = spark.sql(
    f"""
    SELECT
        (SELECT COUNT(*) FROM `{CATALOG}`.bronze.products_raw) AS products,
        (SELECT COUNT(*) FROM `{CATALOG}`.bronze.contracts_raw) AS contracts,
        (
            SELECT COUNT(*)
            FROM (
                SELECT contract_id
                FROM `{CATALOG}`.bronze.contracts_raw
                GROUP BY contract_id
                HAVING COUNT(*) > 1
            )
        ) AS duplicated_contract_ids,
        (
            SELECT COUNT(*)
            FROM `{CATALOG}`.bronze.contracts_raw
            WHERE principal_amount <= 0
        ) AS invalid_principal_rows
    """
).first()

assert metrics["products"] == 5, metrics
assert metrics["contracts"] == 5_003, metrics
assert metrics["duplicated_contract_ids"] == 1, metrics
assert metrics["invalid_principal_rows"] == 1, metrics

print("Bronze batch carregada e validada.")

