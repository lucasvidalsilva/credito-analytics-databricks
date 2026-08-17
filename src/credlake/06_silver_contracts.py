# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — contratos e quarentena
# MAGIC Deduplica contratos e aplica regras estruturais, financeiras e referenciais.

# COMMAND ----------

import re

dbutils.widgets.text("catalog", "credlake", "Catálogo")
CATALOG = dbutils.widgets.get("catalog").strip()

if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", CATALOG):
    raise ValueError(f"Nome de catálogo inválido: {CATALOG!r}")

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS `{CATALOG}`.silver.contracts (
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
        customer_snapshot_date DATE,
        source_file_path STRING,
        silver_processed_at TIMESTAMP,
        record_hash STRING
    )
    USING DELTA
    COMMENT 'Contratos deduplicados e aprovados pelas regras de qualidade.'
    TBLPROPERTIES (
        'data_layer' = 'silver',
        'data_domain' = 'credit',
        'contains_synthetic_data' = 'true'
    )
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS `{CATALOG}`.silver.contracts_quarantine (
        quarantine_id STRING,
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
        duplicate_rank INT,
        error_codes ARRAY<STRING>,
        error_reasons ARRAY<STRING>,
        quarantined_at TIMESTAMP,
        record_hash STRING
    )
    USING DELTA
    COMMENT 'Contratos rejeitados com motivos auditáveis.'
    TBLPROPERTIES (
        'data_layer' = 'silver_quarantine',
        'data_domain' = 'credit',
        'contains_synthetic_data' = 'true'
    )
    """
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW credlake_contracts_classified AS
    WITH ranked AS (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY b.contract_id
                ORDER BY
                    b.source_updated_at DESC NULLS LAST,
                    b.source_file_modification_time DESC NULLS LAST,
                    b.source_file_path DESC,
                    SHA2(TO_JSON(NAMED_STRUCT(
                        'customer_id', b.customer_id,
                        'product_id', b.product_id,
                        'principal_amount', b.principal_amount,
                        'term_months', b.term_months
                    )), 256) DESC
            ) AS duplicate_rank
        FROM `{CATALOG}`.bronze.contracts_raw b
    ),
    enriched AS (
        SELECT
            r.*,
            c.customer_id AS matched_customer_id,
            c.customer_type,
            c.snapshot_date AS customer_snapshot_date,
            p.product_id AS matched_product_id,
            p.allowed_customer_type,
            p.max_term_months
        FROM ranked r
        LEFT JOIN `{CATALOG}`.silver.customers c
            ON r.customer_id = c.customer_id
        LEFT JOIN `{CATALOG}`.silver.products p
            ON r.product_id = p.product_id
    )
    SELECT
        *,
        FILTER(ARRAY(
            CASE WHEN duplicate_rank > 1 THEN 'DUPLICATE_CONTRACT_ID' END,
            CASE WHEN contract_id IS NULL OR contract_id <= 0 THEN 'INVALID_CONTRACT_ID' END,
            CASE WHEN customer_id IS NULL THEN 'NULL_CUSTOMER_ID' END,
            CASE WHEN customer_id IS NOT NULL AND matched_customer_id IS NULL THEN 'CUSTOMER_NOT_FOUND' END,
            CASE WHEN product_id IS NULL THEN 'NULL_PRODUCT_ID' END,
            CASE WHEN product_id IS NOT NULL AND matched_product_id IS NULL THEN 'PRODUCT_NOT_FOUND' END,
            CASE WHEN principal_amount IS NULL OR principal_amount <= 0 THEN 'INVALID_PRINCIPAL_AMOUNT' END,
            CASE WHEN annual_interest_rate IS NULL OR annual_interest_rate <= 0 OR annual_interest_rate > 1 THEN 'INVALID_INTEREST_RATE' END,
            CASE WHEN term_months IS NULL OR term_months <= 0 THEN 'INVALID_TERM' END,
            CASE WHEN max_term_months IS NOT NULL AND term_months > max_term_months THEN 'TERM_EXCEEDS_PRODUCT_LIMIT' END,
            CASE
                WHEN matched_customer_id IS NOT NULL
                 AND matched_product_id IS NOT NULL
                 AND allowed_customer_type <> 'PF/PJ'
                 AND allowed_customer_type <> customer_type
                THEN 'CUSTOMER_TYPE_NOT_ALLOWED'
            END,
            CASE WHEN contract_status NOT IN ('ACTIVE', 'CLOSED') THEN 'INVALID_CONTRACT_STATUS' END,
            CASE WHEN contract_date IS NULL OR contract_date > CURRENT_DATE() THEN 'INVALID_CONTRACT_DATE' END
        ), code -> code IS NOT NULL) AS error_codes,
        FILTER(ARRAY(
            CASE WHEN duplicate_rank > 1 THEN 'Outro registro foi priorizado para o mesmo contrato' END,
            CASE WHEN contract_id IS NULL OR contract_id <= 0 THEN 'Identificador de contrato inválido' END,
            CASE WHEN customer_id IS NULL THEN 'Cliente obrigatório' END,
            CASE WHEN customer_id IS NOT NULL AND matched_customer_id IS NULL THEN 'Cliente não encontrado' END,
            CASE WHEN product_id IS NULL THEN 'Produto obrigatório' END,
            CASE WHEN product_id IS NOT NULL AND matched_product_id IS NULL THEN 'Produto não encontrado' END,
            CASE WHEN principal_amount IS NULL OR principal_amount <= 0 THEN 'Valor principal deve ser positivo' END,
            CASE WHEN annual_interest_rate IS NULL OR annual_interest_rate <= 0 OR annual_interest_rate > 1 THEN 'Taxa anual fora do intervalo decimal válido' END,
            CASE WHEN term_months IS NULL OR term_months <= 0 THEN 'Prazo inválido' END,
            CASE WHEN max_term_months IS NOT NULL AND term_months > max_term_months THEN 'Prazo supera o limite do produto' END,
            CASE
                WHEN matched_customer_id IS NOT NULL
                 AND matched_product_id IS NOT NULL
                 AND allowed_customer_type <> 'PF/PJ'
                 AND allowed_customer_type <> customer_type
                THEN 'Tipo de cliente incompatível com o produto'
            END,
            CASE WHEN contract_status NOT IN ('ACTIVE', 'CLOSED') THEN 'Status contratual desconhecido' END,
            CASE WHEN contract_date IS NULL OR contract_date > CURRENT_DATE() THEN 'Data contratual inválida' END
        ), reason -> reason IS NOT NULL) AS error_reasons
    FROM enriched
    """
)

# COMMAND ----------

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW credlake_valid_contracts AS
    SELECT
        contract_id,
        customer_id,
        product_id,
        contract_date,
        principal_amount,
        annual_interest_rate,
        term_months,
        contract_status,
        source_updated_at,
        source_system,
        source_batch_id,
        customer_snapshot_date,
        source_file_path,
        CURRENT_TIMESTAMP() AS silver_processed_at,
        SHA2(TO_JSON(NAMED_STRUCT(
            'contract_id', contract_id,
            'customer_id', customer_id,
            'product_id', product_id,
            'contract_date', contract_date,
            'principal_amount', principal_amount,
            'annual_interest_rate', annual_interest_rate,
            'term_months', term_months,
            'contract_status', contract_status,
            'source_updated_at', source_updated_at
        )), 256) AS record_hash
    FROM credlake_contracts_classified
    WHERE SIZE(error_codes) = 0
    """
)

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW credlake_quarantined_contracts AS
    SELECT
        SHA2(CONCAT_WS(
            '||',
            COALESCE(CAST(contract_id AS STRING), 'NULL'),
            COALESCE(source_file_path, 'NULL'),
            CAST(duplicate_rank AS STRING)
        ), 256) AS quarantine_id,
        contract_id,
        customer_id,
        product_id,
        contract_date,
        principal_amount,
        annual_interest_rate,
        term_months,
        contract_status,
        source_updated_at,
        source_system,
        source_batch_id,
        source_file_path,
        duplicate_rank,
        error_codes,
        error_reasons,
        CURRENT_TIMESTAMP() AS quarantined_at,
        SHA2(TO_JSON(NAMED_STRUCT(
            'contract_id', contract_id,
            'customer_id', customer_id,
            'product_id', product_id,
            'principal_amount', principal_amount,
            'duplicate_rank', duplicate_rank,
            'error_codes', error_codes
        )), 256) AS record_hash
    FROM credlake_contracts_classified
    WHERE SIZE(error_codes) > 0
    """
)

# COMMAND ----------

spark.sql(
    f"""
    MERGE INTO `{CATALOG}`.silver.contracts target
    USING credlake_valid_contracts source
    ON target.contract_id = source.contract_id
    WHEN MATCHED AND target.record_hash <> source.record_hash THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE THEN DELETE
    """
)

spark.sql(
    f"""
    MERGE INTO `{CATALOG}`.silver.contracts_quarantine target
    USING credlake_quarantined_contracts source
    ON target.quarantine_id = source.quarantine_id
    WHEN MATCHED AND target.record_hash <> source.record_hash THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE THEN DELETE
    """
)

# COMMAND ----------

metrics = spark.sql(
    f"""
    SELECT
        (SELECT COUNT(*) FROM `{CATALOG}`.silver.contracts) AS valid_rows,
        (SELECT COUNT(*) FROM `{CATALOG}`.silver.contracts_quarantine) AS quarantine_rows,
        (SELECT COUNT(DISTINCT contract_id) FROM `{CATALOG}`.silver.contracts) AS distinct_ids
    """
).first()

assert metrics["valid_rows"] == 5_000, metrics
assert metrics["distinct_ids"] == 5_000, metrics
assert metrics["quarantine_rows"] == 3, metrics

display(spark.table(f"{CATALOG}.silver.contracts_quarantine"))
print("Contratos Silver e quarentena publicados.")

