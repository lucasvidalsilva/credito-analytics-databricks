# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — reconciliação de parcelas e pagamentos
# MAGIC Calcula valor pago, saldo pendente, atraso e situação financeira por parcela.

# COMMAND ----------

import re

dbutils.widgets.text("catalog", "credlake", "Catálogo")
CATALOG = dbutils.widgets.get("catalog").strip()

if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", CATALOG):
    raise ValueError(f"Nome de catálogo inválido: {CATALOG!r}")

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS `{CATALOG}`.silver.installments (
        installment_id STRING,
        contract_id BIGINT,
        installment_number INT,
        due_date DATE,
        scheduled_amount DECIMAL(18,2),
        total_paid DECIMAL(18,2),
        outstanding_amount DECIMAL(18,2),
        overpaid_amount DECIMAL(18,2),
        payment_count BIGINT,
        first_payment_date DATE,
        last_payment_date DATE,
        days_past_due INT,
        installment_status STRING,
        snapshot_date DATE,
        silver_processed_at TIMESTAMP,
        record_hash STRING
    )
    USING DELTA
    COMMENT 'Parcelas reconciliadas com pagamentos válidos e status financeiro calculado.'
    TBLPROPERTIES (
        'data_layer' = 'silver',
        'data_domain' = 'credit',
        'contains_synthetic_data' = 'true'
    )
    """
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW credlake_reconciled_installments AS
    WITH parameters AS (
        SELECT MAX(snapshot_date) AS as_of_date
        FROM `{CATALOG}`.bronze.installments_raw
    ),
    latest_installments AS (
        SELECT * EXCEPT (row_priority)
        FROM (
            SELECT
                i.*,
                ROW_NUMBER() OVER (
                    PARTITION BY installment_id
                    ORDER BY snapshot_date DESC, ingested_at DESC
                ) AS row_priority
            FROM `{CATALOG}`.bronze.installments_raw i
        )
        WHERE row_priority = 1
    ),
    payment_totals AS (
        SELECT
            p.installment_id,
            CAST(SUM(p.amount_paid) AS DECIMAL(18,2)) AS total_paid,
            COUNT(*) AS payment_count,
            MIN(p.payment_date) AS first_payment_date,
            MAX(p.payment_date) AS last_payment_date
        FROM `{CATALOG}`.silver.payments p
        CROSS JOIN parameters params
        WHERE p.payment_date <= params.as_of_date
        GROUP BY p.installment_id
    ),
    calculated AS (
        SELECT
            i.installment_id,
            i.contract_id,
            i.installment_number,
            i.due_date,
            CAST(i.scheduled_amount AS DECIMAL(18,2)) AS scheduled_amount,
            CAST(COALESCE(p.total_paid, 0) AS DECIMAL(18,2)) AS total_paid,
            CAST(
                GREATEST(
                    i.scheduled_amount - COALESCE(p.total_paid, 0),
                    0
                ) AS DECIMAL(18,2)
            ) AS outstanding_amount,
            CAST(
                GREATEST(
                    COALESCE(p.total_paid, 0) - i.scheduled_amount,
                    0
                ) AS DECIMAL(18,2)
            ) AS overpaid_amount,
            COALESCE(p.payment_count, 0) AS payment_count,
            p.first_payment_date,
            p.last_payment_date,
            CASE
                WHEN i.due_date < params.as_of_date
                 AND i.scheduled_amount - COALESCE(p.total_paid, 0) > 0
                THEN DATEDIFF(params.as_of_date, i.due_date)
                ELSE 0
            END AS days_past_due,
            CASE
                WHEN i.scheduled_amount - COALESCE(p.total_paid, 0) <= 0.01
                    THEN 'PAID'
                WHEN i.due_date < params.as_of_date
                    THEN 'OVERDUE'
                WHEN COALESCE(p.total_paid, 0) > 0
                    THEN 'PARTIALLY_PAID'
                ELSE 'OPEN'
            END AS installment_status,
            params.as_of_date AS snapshot_date
        FROM latest_installments i
        INNER JOIN `{CATALOG}`.silver.contracts c
            ON i.contract_id = c.contract_id
        LEFT JOIN payment_totals p
            ON i.installment_id = p.installment_id
        CROSS JOIN parameters params
        WHERE i.scheduled_amount > 0
          AND i.due_date IS NOT NULL
    )
    SELECT
        *,
        CURRENT_TIMESTAMP() AS silver_processed_at,
        SHA2(TO_JSON(NAMED_STRUCT(
            'installment_id', installment_id,
            'scheduled_amount', scheduled_amount,
            'total_paid', total_paid,
            'outstanding_amount', outstanding_amount,
            'days_past_due', days_past_due,
            'installment_status', installment_status,
            'snapshot_date', snapshot_date
        )), 256) AS record_hash
    FROM calculated
    """
)

# COMMAND ----------

spark.sql(
    f"""
    MERGE INTO `{CATALOG}`.silver.installments target
    USING credlake_reconciled_installments source
    ON target.installment_id = source.installment_id
    WHEN MATCHED AND target.record_hash <> source.record_hash THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE THEN DELETE
    """
)

# COMMAND ----------

metrics = spark.sql(
    f"""
    SELECT
        COUNT(*) AS rows,
        COUNT(DISTINCT installment_id) AS distinct_ids,
        SUM(CASE WHEN outstanding_amount < 0 THEN 1 ELSE 0 END) AS negative_balances,
        SUM(CASE WHEN installment_status = 'OVERDUE' THEN 1 ELSE 0 END) AS overdue_installments,
        SUM(scheduled_amount) AS scheduled_amount,
        SUM(total_paid) AS total_paid,
        SUM(outstanding_amount) AS outstanding_amount
    FROM `{CATALOG}`.silver.installments
    """
).first()

assert metrics["rows"] > 0, metrics
assert metrics["rows"] == metrics["distinct_ids"], metrics
assert metrics["negative_balances"] == 0, metrics

display(
    spark.sql(
        f"""
        SELECT installment_status, COUNT(*) AS installments,
               SUM(outstanding_amount) AS outstanding_amount
        FROM `{CATALOG}`.silver.installments
        GROUP BY installment_status
        ORDER BY installment_status
        """
    )
)

print("Parcelas Silver reconciliadas.")

