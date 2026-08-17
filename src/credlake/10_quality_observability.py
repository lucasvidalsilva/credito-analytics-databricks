# Databricks notebook source
# MAGIC %md
# MAGIC # Operações — qualidade e observabilidade
# MAGIC Registra controles executáveis e interrompe o job quando uma regra crítica falha.

# COMMAND ----------

import re
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

dbutils.widgets.text("catalog", "credlake", "Catálogo")
CATALOG = dbutils.widgets.get("catalog").strip()

if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", CATALOG):
    raise ValueError(f"Nome de catálogo inválido: {CATALOG!r}")

RUN_ID = str(uuid.uuid4())
RUN_TS = datetime.now(timezone.utc).replace(tzinfo=None)

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS `{CATALOG}`.ops.data_quality_results (
        run_id STRING,
        run_timestamp TIMESTAMP,
        layer STRING,
        dataset STRING,
        check_name STRING,
        passed BOOLEAN,
        actual_value STRING,
        expected_value STRING,
        severity STRING,
        details STRING
    )
    USING DELTA
    COMMENT 'Histórico append-only dos controles de qualidade do CredLake.'
    TBLPROPERTIES (
        'data_domain' = 'operations',
        'contains_synthetic_data' = 'true'
    )
    """
)

# COMMAND ----------


def scalar(sql: str):
    return spark.sql(sql).first()[0]


checks = []


def add_check(layer, dataset, name, actual, expected, severity="ERROR", details=""):
    checks.append(
        (
            RUN_ID,
            RUN_TS,
            layer,
            dataset,
            name,
            bool(actual == expected),
            str(actual),
            str(expected),
            severity,
            details,
        )
    )


add_check(
    "bronze",
    "products_raw",
    "expected_row_count",
    scalar(f"SELECT COUNT(*) FROM `{CATALOG}`.bronze.products_raw"),
    5,
)
add_check(
    "bronze",
    "customers_raw",
    "expected_distinct_customers",
    scalar(f"SELECT COUNT(DISTINCT customer_id) FROM `{CATALOG}`.bronze.customers_raw"),
    2_000,
)
add_check(
    "bronze",
    "contracts_raw",
    "expected_row_count_with_known_errors",
    scalar(f"SELECT COUNT(*) FROM `{CATALOG}`.bronze.contracts_raw"),
    5_003,
)
add_check(
    "silver",
    "contracts",
    "expected_valid_contracts",
    scalar(f"SELECT COUNT(*) FROM `{CATALOG}`.silver.contracts"),
    5_000,
)
add_check(
    "silver",
    "contracts_quarantine",
    "expected_known_rejections",
    scalar(f"SELECT COUNT(*) FROM `{CATALOG}`.silver.contracts_quarantine"),
    3,
)
add_check(
    "silver",
    "payments_quarantine",
    "expected_known_rejections",
    scalar(f"SELECT COUNT(*) FROM `{CATALOG}`.silver.payments_quarantine"),
    2,
)
add_check(
    "silver",
    "payments",
    "no_duplicate_payment_ids",
    scalar(
        f"SELECT COUNT(*) - COUNT(DISTINCT payment_id) "
        f"FROM `{CATALOG}`.silver.payments"
    ),
    0,
)
add_check(
    "silver",
    "installments",
    "no_negative_outstanding_balance",
    scalar(
        f"SELECT COUNT(*) FROM `{CATALOG}`.silver.installments "
        "WHERE outstanding_amount < 0"
    ),
    0,
)
add_check(
    "gold",
    "fact_credit_portfolio",
    "one_row_per_contract",
    scalar(
        f"SELECT COUNT(*) - COUNT(DISTINCT contract_id) "
        f"FROM `{CATALOG}`.gold.fact_credit_portfolio"
    ),
    0,
)

silver_total = scalar(
    f"SELECT CAST(SUM(scheduled_amount) AS DECIMAL(22,2)) "
    f"FROM `{CATALOG}`.silver.installments"
)
gold_total = scalar(
    f"SELECT CAST(SUM(contractual_amount) AS DECIMAL(22,2)) "
    f"FROM `{CATALOG}`.gold.fact_credit_portfolio"
)
reconciliation_delta = abs((silver_total or Decimal(0)) - (gold_total or Decimal(0)))
add_check(
    "gold",
    "fact_credit_portfolio",
    "scheduled_amount_reconciliation",
    reconciliation_delta,
    Decimal("0.00"),
    details="Silver installments versus Gold contract portfolio.",
)

gold_customer_columns = set(spark.table(f"{CATALOG}.gold.dim_customer").columns)
pii_columns = {"customer_name", "document_id", "email"}
add_check(
    "gold",
    "dim_customer",
    "pii_columns_not_exposed",
    len(gold_customer_columns.intersection(pii_columns)),
    0,
)

# COMMAND ----------

schema = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("run_timestamp", TimestampType(), False),
        StructField("layer", StringType(), False),
        StructField("dataset", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("passed", BooleanType(), False),
        StructField("actual_value", StringType(), True),
        StructField("expected_value", StringType(), True),
        StructField("severity", StringType(), False),
        StructField("details", StringType(), True),
    ]
)

results_df = spark.createDataFrame(checks, schema)
results_df.write.mode("append").saveAsTable(f"{CATALOG}.ops.data_quality_results")

display(results_df.orderBy("passed", "layer", "dataset", "check_name"))

critical_failures = results_df.filter("passed = false AND severity = 'ERROR'").count()

if critical_failures:
    raise AssertionError(f"{critical_failures} controles críticos falharam. Run ID: {RUN_ID}")

print(f"Todos os controles críticos passaram. Run ID: {RUN_ID}")

