# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — carteira de crédito e indicadores executivos
# MAGIC Publica dimensões sem PII, fato por contrato, KPIs segmentados e visão executiva.

# COMMAND ----------

import re

dbutils.widgets.text("catalog", "credlake", "Catálogo")
CATALOG = dbutils.widgets.get("catalog").strip()

if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", CATALOG):
    raise ValueError(f"Nome de catálogo inválido: {CATALOG!r}")

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE `{CATALOG}`.gold.dim_customer
    USING DELTA
    COMMENT 'Dimensão analítica de clientes sem documento, nome ou e-mail.'
    TBLPROPERTIES (
        'data_layer' = 'gold',
        'contains_pii' = 'false',
        'contains_synthetic_data' = 'true'
    )
    AS
    SELECT
        customer_id,
        customer_type,
        state,
        risk_rating,
        monthly_income,
        is_active,
        snapshot_date
    FROM `{CATALOG}`.silver.customers
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE `{CATALOG}`.gold.dim_product
    USING DELTA
    COMMENT 'Dimensão analítica dos produtos de crédito.'
    TBLPROPERTIES (
        'data_layer' = 'gold',
        'contains_synthetic_data' = 'true'
    )
    AS
    SELECT
        product_id,
        product_name,
        allowed_customer_type,
        base_annual_interest_rate,
        max_term_months
    FROM `{CATALOG}`.silver.products
    """
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE `{CATALOG}`.gold.fact_credit_portfolio
    USING DELTA
    COMMENT 'Posição financeira da carteira de crédito por contrato.'
    TBLPROPERTIES (
        'data_layer' = 'gold',
        'data_domain' = 'credit-risk',
        'grain' = 'one_row_per_contract_per_snapshot',
        'contains_pii' = 'false',
        'contains_synthetic_data' = 'true'
    )
    AS
    WITH installment_summary AS (
        SELECT
            contract_id,
            snapshot_date,
            COUNT(*) AS installment_count,
            SUM(CASE WHEN installment_status = 'PAID' THEN 1 ELSE 0 END) AS paid_installment_count,
            SUM(CASE WHEN installment_status = 'OVERDUE' THEN 1 ELSE 0 END) AS overdue_installment_count,
            CAST(SUM(scheduled_amount) AS DECIMAL(20,2)) AS contractual_amount,
            CAST(SUM(total_paid) AS DECIMAL(20,2)) AS total_paid,
            CAST(SUM(outstanding_amount) AS DECIMAL(20,2)) AS outstanding_amount,
            CAST(SUM(CASE WHEN installment_status = 'OVERDUE' THEN outstanding_amount ELSE 0 END) AS DECIMAL(20,2)) AS overdue_amount,
            MAX(days_past_due) AS max_days_past_due,
            MIN(CASE WHEN installment_status IN ('OPEN', 'PARTIALLY_PAID') THEN due_date END) AS next_due_date
        FROM `{CATALOG}`.silver.installments
        GROUP BY contract_id, snapshot_date
    )
    SELECT
        c.contract_id,
        c.customer_id,
        c.product_id,
        s.snapshot_date,
        c.contract_date,
        c.contract_status,
        c.principal_amount,
        c.annual_interest_rate,
        c.term_months,
        s.installment_count,
        s.paid_installment_count,
        s.overdue_installment_count,
        s.contractual_amount,
        s.total_paid,
        s.outstanding_amount,
        s.overdue_amount,
        s.max_days_past_due,
        s.next_due_date,
        CASE
            WHEN s.max_days_past_due = 0 THEN 'CURRENT'
            WHEN s.max_days_past_due <= 30 THEN 'DPD_01_30'
            WHEN s.max_days_past_due <= 60 THEN 'DPD_31_60'
            WHEN s.max_days_past_due <= 90 THEN 'DPD_61_90'
            ELSE 'DPD_90_PLUS'
        END AS delinquency_bucket,
        CASE WHEN s.max_days_past_due > 90 THEN TRUE ELSE FALSE END AS is_npl_90,
        CASE
            WHEN s.contractual_amount = 0 THEN CAST(0 AS DECIMAL(10,6))
            ELSE CAST(s.outstanding_amount / s.contractual_amount AS DECIMAL(10,6))
        END AS outstanding_ratio,
        CURRENT_TIMESTAMP() AS gold_processed_at
    FROM `{CATALOG}`.silver.contracts c
    INNER JOIN installment_summary s
        ON c.contract_id = s.contract_id
    """
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE `{CATALOG}`.gold.portfolio_kpis
    USING DELTA
    COMMENT 'Indicadores segmentados da carteira para consumo analítico.'
    TBLPROPERTIES (
        'data_layer' = 'gold',
        'grain' = 'snapshot_product_state_risk',
        'contains_pii' = 'false',
        'contains_synthetic_data' = 'true'
    )
    AS
    SELECT
        f.snapshot_date,
        f.product_id,
        p.product_name,
        c.state,
        c.risk_rating,
        COUNT(*) AS contract_count,
        SUM(CASE WHEN f.contract_status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_contract_count,
        CAST(SUM(f.principal_amount) AS DECIMAL(22,2)) AS principal_amount,
        CAST(SUM(f.contractual_amount) AS DECIMAL(22,2)) AS contractual_amount,
        CAST(SUM(f.total_paid) AS DECIMAL(22,2)) AS total_paid,
        CAST(SUM(f.outstanding_amount) AS DECIMAL(22,2)) AS outstanding_amount,
        CAST(SUM(f.overdue_amount) AS DECIMAL(22,2)) AS overdue_amount,
        CAST(SUM(CASE WHEN f.is_npl_90 THEN f.outstanding_amount ELSE 0 END) AS DECIMAL(22,2)) AS npl_90_amount,
        CAST(
            CASE WHEN SUM(f.outstanding_amount) = 0 THEN 0
                 ELSE SUM(f.overdue_amount) / SUM(f.outstanding_amount)
            END AS DECIMAL(12,6)
        ) AS delinquency_ratio,
        CAST(
            CASE WHEN SUM(f.principal_amount) = 0 THEN 0
                 ELSE SUM(f.principal_amount * f.annual_interest_rate) / SUM(f.principal_amount)
            END AS DECIMAL(12,6)
        ) AS weighted_average_interest_rate,
        MAX(f.max_days_past_due) AS max_days_past_due,
        CURRENT_TIMESTAMP() AS gold_processed_at
    FROM `{CATALOG}`.gold.fact_credit_portfolio f
    INNER JOIN `{CATALOG}`.gold.dim_customer c
        ON f.customer_id = c.customer_id
    INNER JOIN `{CATALOG}`.gold.dim_product p
        ON f.product_id = p.product_id
    GROUP BY f.snapshot_date, f.product_id, p.product_name, c.state, c.risk_rating
    """
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE VIEW `{CATALOG}`.gold.vw_executive_portfolio AS
    SELECT
        snapshot_date,
        COUNT(*) AS contract_count,
        CAST(SUM(principal_amount) AS DECIMAL(22,2)) AS principal_amount,
        CAST(SUM(total_paid) AS DECIMAL(22,2)) AS total_paid,
        CAST(SUM(outstanding_amount) AS DECIMAL(22,2)) AS outstanding_amount,
        CAST(SUM(overdue_amount) AS DECIMAL(22,2)) AS overdue_amount,
        CAST(SUM(CASE WHEN is_npl_90 THEN outstanding_amount ELSE 0 END) AS DECIMAL(22,2)) AS npl_90_amount,
        CAST(
            CASE WHEN SUM(outstanding_amount) = 0 THEN 0
                 ELSE SUM(overdue_amount) / SUM(outstanding_amount)
            END AS DECIMAL(12,6)
        ) AS delinquency_ratio,
        CAST(
            CASE WHEN SUM(outstanding_amount) = 0 THEN 0
                 ELSE SUM(CASE WHEN is_npl_90 THEN outstanding_amount ELSE 0 END) / SUM(outstanding_amount)
            END AS DECIMAL(12,6)
        ) AS npl_90_ratio
    FROM `{CATALOG}`.gold.fact_credit_portfolio
    GROUP BY snapshot_date
    """
)

# COMMAND ----------

metrics = spark.sql(
    f"""
    SELECT COUNT(*) AS rows, COUNT(DISTINCT contract_id) AS contracts
    FROM `{CATALOG}`.gold.fact_credit_portfolio
    """
).first()

assert metrics["rows"] == 5_000, metrics
assert metrics["contracts"] == 5_000, metrics

display(spark.table(f"{CATALOG}.gold.vw_executive_portfolio"))
print("Modelo Gold publicado.")

