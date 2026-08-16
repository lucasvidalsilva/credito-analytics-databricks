# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — clientes e produtos
# MAGIC Seleciona a versão mais recente, aplica regras básicas e publica dimensões confiáveis.

# COMMAND ----------

import re

dbutils.widgets.text("catalog", "credlake", "Catálogo")
CATALOG = dbutils.widgets.get("catalog").strip()

if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", CATALOG):
    raise ValueError(f"Nome de catálogo inválido: {CATALOG!r}")

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE `{CATALOG}`.silver.customers
    USING DELTA
    COMMENT 'Cadastro atual de clientes validado e deduplicado.'
    TBLPROPERTIES (
        'data_layer' = 'silver',
        'data_domain' = 'customer',
        'contains_pii' = 'true',
        'contains_synthetic_data' = 'true'
    )
    AS
    SELECT
        customer_id,
        customer_type,
        customer_name,
        document_id,
        email,
        state,
        risk_rating,
        monthly_income,
        created_at,
        updated_at,
        is_active,
        snapshot_date,
        source_system,
        source_batch_id,
        CURRENT_TIMESTAMP() AS silver_processed_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY customer_id
                ORDER BY snapshot_date DESC, updated_at DESC, ingested_at DESC
            ) AS row_priority
        FROM `{CATALOG}`.bronze.customers_raw
        WHERE corrupt_record IS NULL
          AND customer_id IS NOT NULL
          AND customer_type IN ('PF', 'PJ')
          AND risk_rating IN ('A', 'B', 'C', 'D', 'E')
          AND monthly_income > 0
    ) ranked
    WHERE row_priority = 1
    """
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE `{CATALOG}`.silver.products
    USING DELTA
    COMMENT 'Produtos de crédito vigentes, tipados e deduplicados.'
    TBLPROPERTIES (
        'data_layer' = 'silver',
        'data_domain' = 'credit',
        'contains_synthetic_data' = 'true'
    )
    AS
    SELECT
        product_id,
        product_name,
        allowed_customer_type,
        base_annual_interest_rate,
        max_term_months,
        source_batch_id,
        CURRENT_TIMESTAMP() AS silver_processed_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY product_id
                ORDER BY source_file_modification_time DESC, ingested_at DESC
            ) AS row_priority
        FROM `{CATALOG}`.bronze.products_raw
        WHERE product_id IS NOT NULL
          AND base_annual_interest_rate > 0
          AND base_annual_interest_rate <= 1
          AND max_term_months > 0
          AND allowed_customer_type IN ('PF', 'PJ', 'PF/PJ')
    ) ranked
    WHERE row_priority = 1
    """
)

# COMMAND ----------

metrics = spark.sql(
    f"""
    SELECT
        (SELECT COUNT(*) FROM `{CATALOG}`.silver.customers) AS customers,
        (SELECT COUNT(*) FROM `{CATALOG}`.silver.products) AS products
    """
).first()

assert metrics["customers"] == 2_000, metrics
assert metrics["products"] == 5, metrics

display(spark.table(f"{CATALOG}.silver.products"))
print("Dimensões Silver publicadas.")

