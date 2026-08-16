# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — pagamentos e quarentena
# MAGIC Deduplica eventos e valida valores, chaves e coerência com as parcelas.

# COMMAND ----------

import re

dbutils.widgets.text("catalog", "credlake", "Catálogo")
CATALOG = dbutils.widgets.get("catalog").strip()

if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", CATALOG):
    raise ValueError(f"Nome de catálogo inválido: {CATALOG!r}")

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS `{CATALOG}`.silver.payments (
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
        event_date DATE,
        source_file_path STRING,
        silver_processed_at TIMESTAMP,
        record_hash STRING
    )
    USING DELTA
    COMMENT 'Pagamentos deduplicados e aprovados pelas regras de qualidade.'
    TBLPROPERTIES (
        'data_layer' = 'silver',
        'data_domain' = 'payments',
        'contains_synthetic_data' = 'true'
    )
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS `{CATALOG}`.silver.payments_quarantine (
        quarantine_id STRING,
        payment_id STRING,
        installment_id STRING,
        contract_id BIGINT,
        installment_number INT,
        payment_date DATE,
        amount_paid DECIMAL(18,2),
        event_type STRING,
        event_timestamp TIMESTAMP,
        source_batch_id STRING,
        event_date DATE,
        source_file_path STRING,
        duplicate_rank INT,
        error_codes ARRAY<STRING>,
        error_reasons ARRAY<STRING>,
        quarantined_at TIMESTAMP,
        record_hash STRING
    )
    USING DELTA
    COMMENT 'Pagamentos rejeitados com motivos auditáveis.'
    TBLPROPERTIES (
        'data_layer' = 'silver_quarantine',
        'data_domain' = 'payments',
        'contains_synthetic_data' = 'true'
    )
    """
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW credlake_payments_classified AS
    WITH latest_installment_snapshot AS (
        SELECT MAX(snapshot_date) AS snapshot_date
        FROM `{CATALOG}`.bronze.installments_raw
    ),
    installments AS (
        SELECT installment_id, contract_id, installment_number
        FROM `{CATALOG}`.bronze.installments_raw
        WHERE snapshot_date = (SELECT snapshot_date FROM latest_installment_snapshot)
    ),
    ranked AS (
        SELECT
            p.*,
            ROW_NUMBER() OVER (
                PARTITION BY payment_id
                ORDER BY
                    event_timestamp DESC NULLS LAST,
                    source_file_modification_time DESC NULLS LAST,
                    source_file_path DESC
            ) AS duplicate_rank
        FROM `{CATALOG}`.bronze.payments_raw p
    ),
    enriched AS (
        SELECT
            p.*,
            i.installment_id AS matched_installment_id,
            i.contract_id AS expected_contract_id,
            i.installment_number AS expected_installment_number
        FROM ranked p
        LEFT JOIN installments i
            ON p.installment_id = i.installment_id
    )
    SELECT
        *,
        FILTER(ARRAY(
            CASE WHEN duplicate_rank > 1 THEN 'DUPLICATE_PAYMENT_ID' END,
            CASE WHEN payment_id IS NULL OR TRIM(payment_id) = '' THEN 'INVALID_PAYMENT_ID' END,
            CASE WHEN installment_id IS NULL THEN 'NULL_INSTALLMENT_ID' END,
            CASE WHEN installment_id IS NOT NULL AND matched_installment_id IS NULL THEN 'INSTALLMENT_NOT_FOUND' END,
            CASE WHEN expected_contract_id IS NOT NULL AND contract_id <> expected_contract_id THEN 'CONTRACT_MISMATCH' END,
            CASE WHEN expected_installment_number IS NOT NULL AND installment_number <> expected_installment_number THEN 'INSTALLMENT_NUMBER_MISMATCH' END,
            CASE WHEN amount_paid IS NULL OR amount_paid <= 0 THEN 'INVALID_PAYMENT_AMOUNT' END,
            CASE WHEN payment_date IS NULL THEN 'INVALID_PAYMENT_DATE' END,
            CASE WHEN event_timestamp IS NULL THEN 'INVALID_EVENT_TIMESTAMP' END,
            CASE WHEN event_type <> 'PAYMENT' THEN 'INVALID_EVENT_TYPE' END,
            CASE WHEN _rescued_data IS NOT NULL THEN 'RESCUED_SOURCE_DATA' END
        ), code -> code IS NOT NULL) AS error_codes,
        FILTER(ARRAY(
            CASE WHEN duplicate_rank > 1 THEN 'Outro evento foi priorizado para o mesmo pagamento' END,
            CASE WHEN payment_id IS NULL OR TRIM(payment_id) = '' THEN 'Identificador de pagamento inválido' END,
            CASE WHEN installment_id IS NULL THEN 'Parcela obrigatória' END,
            CASE WHEN installment_id IS NOT NULL AND matched_installment_id IS NULL THEN 'Parcela não encontrada' END,
            CASE WHEN expected_contract_id IS NOT NULL AND contract_id <> expected_contract_id THEN 'Contrato diverge da parcela' END,
            CASE WHEN expected_installment_number IS NOT NULL AND installment_number <> expected_installment_number THEN 'Número da parcela divergente' END,
            CASE WHEN amount_paid IS NULL OR amount_paid <= 0 THEN 'Valor pago deve ser positivo' END,
            CASE WHEN payment_date IS NULL THEN 'Data de pagamento obrigatória' END,
            CASE WHEN event_timestamp IS NULL THEN 'Timestamp do evento obrigatório' END,
            CASE WHEN event_type <> 'PAYMENT' THEN 'Tipo de evento inválido' END,
            CASE WHEN _rescued_data IS NOT NULL THEN 'Existem campos não compatíveis com o contrato de schema' END
        ), reason -> reason IS NOT NULL) AS error_reasons
    FROM enriched
    """
)

# COMMAND ----------

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW credlake_valid_payments AS
    SELECT
        payment_id,
        installment_id,
        contract_id,
        installment_number,
        payment_date,
        amount_paid,
        event_type,
        event_timestamp,
        source_system,
        source_batch_id,
        event_date,
        source_file_path,
        CURRENT_TIMESTAMP() AS silver_processed_at,
        SHA2(TO_JSON(NAMED_STRUCT(
            'payment_id', payment_id,
            'installment_id', installment_id,
            'contract_id', contract_id,
            'payment_date', payment_date,
            'amount_paid', amount_paid,
            'event_timestamp', event_timestamp
        )), 256) AS record_hash
    FROM credlake_payments_classified
    WHERE SIZE(error_codes) = 0
    """
)

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW credlake_quarantined_payments AS
    SELECT
        SHA2(CONCAT_WS(
            '||',
            COALESCE(payment_id, 'NULL'),
            COALESCE(source_file_path, 'NULL'),
            CAST(duplicate_rank AS STRING)
        ), 256) AS quarantine_id,
        payment_id,
        installment_id,
        contract_id,
        installment_number,
        payment_date,
        amount_paid,
        event_type,
        event_timestamp,
        source_batch_id,
        event_date,
        source_file_path,
        duplicate_rank,
        error_codes,
        error_reasons,
        CURRENT_TIMESTAMP() AS quarantined_at,
        SHA2(TO_JSON(NAMED_STRUCT(
            'payment_id', payment_id,
            'installment_id', installment_id,
            'amount_paid', amount_paid,
            'duplicate_rank', duplicate_rank,
            'error_codes', error_codes
        )), 256) AS record_hash
    FROM credlake_payments_classified
    WHERE SIZE(error_codes) > 0
    """
)

# COMMAND ----------

spark.sql(
    f"""
    MERGE INTO `{CATALOG}`.silver.payments target
    USING credlake_valid_payments source
    ON target.payment_id = source.payment_id
    WHEN MATCHED AND target.record_hash <> source.record_hash THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE THEN DELETE
    """
)

spark.sql(
    f"""
    MERGE INTO `{CATALOG}`.silver.payments_quarantine target
    USING credlake_quarantined_payments source
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
        (SELECT COUNT(*) FROM `{CATALOG}`.silver.payments) AS valid_rows,
        (SELECT COUNT(*) FROM `{CATALOG}`.silver.payments_quarantine) AS quarantine_rows,
        (SELECT COUNT(*) - COUNT(DISTINCT payment_id) FROM `{CATALOG}`.silver.payments) AS duplicate_rows
    """
).first()

assert metrics["valid_rows"] > 0, metrics
assert metrics["quarantine_rows"] == 2, metrics
assert metrics["duplicate_rows"] == 0, metrics

display(spark.table(f"{CATALOG}.silver.payments_quarantine"))
print("Pagamentos Silver e quarentena publicados.")

