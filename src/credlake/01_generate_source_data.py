# Databricks notebook source
# MAGIC %md
# MAGIC # CredLake — geração das fontes sintéticas
# MAGIC Gera uma carteira determinística e injeta problemas conhecidos para validar a camada de qualidade.

# COMMAND ----------

import re
from datetime import date

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "credlake", "Catálogo")
dbutils.widgets.text("reference_date", "2026-08-01", "Data de referência")
dbutils.widgets.dropdown(
    "force_regenerate",
    "false",
    ["false", "true"],
    "Regenerar fontes",
)

CATALOG = dbutils.widgets.get("catalog").strip()
REFERENCE_DATE = dbutils.widgets.get("reference_date").strip()
FORCE_REGENERATE = dbutils.widgets.get("force_regenerate") == "true"

if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", CATALOG):
    raise ValueError(f"Nome de catálogo inválido: {CATALOG!r}")

date.fromisoformat(REFERENCE_DATE)

N_CUSTOMERS = 2_000
N_CONTRACTS = 5_000
BASE_PATH = f"/Volumes/{CATALOG}/landing/raw"
SOURCE_BATCH_ID = f"initial_{REFERENCE_DATE}"

print(f"Data de referência: {REFERENCE_DATE}")
print(f"Diretório de destino: {BASE_PATH}")
print(f"Regeneração forçada: {FORCE_REGENERATE}")

# COMMAND ----------

product_rows = [
    (1, "Crédito Pessoal", "PF", 0.2890, 48),
    (2, "Consignado", "PF", 0.1890, 60),
    (3, "Financiamento", "PF", 0.1590, 60),
    (4, "Capital de Giro", "PJ", 0.2490, 36),
    (5, "Crédito Rural", "PF/PJ", 0.1290, 48),
]

products = (
    spark.createDataFrame(
        product_rows,
        [
            "product_id",
            "product_name",
            "allowed_customer_type",
            "base_annual_interest_rate",
            "max_term_months",
        ],
    )
    .withColumn("product_id", F.col("product_id").cast("int"))
    .withColumn("max_term_months", F.col("max_term_months").cast("int"))
    .withColumn("source_batch_id", F.lit(SOURCE_BATCH_ID))
)

# COMMAND ----------

states = [
    "RO", "AC", "AM", "RR", "PA", "AP", "TO", "MT", "MS", "GO",
    "DF", "SP", "RJ", "MG", "PR", "SC", "RS", "BA", "PE", "CE",
]
ratings = ["A", "B", "C", "D", "E"]

state_array = F.array(*[F.lit(value) for value in states])
rating_array = F.array(*[F.lit(value) for value in ratings])

customers = (
    spark.range(1, N_CUSTOMERS + 1)
    .withColumnRenamed("id", "customer_id")
    .withColumn(
        "customer_type",
        F.when(F.pmod("customer_id", F.lit(5)) == 0, F.lit("PJ"))
        .otherwise(F.lit("PF")),
    )
    .withColumn(
        "customer_name",
        F.concat(
            F.lit("Cliente Sintético "),
            F.lpad(F.col("customer_id").cast("string"), 6, "0"),
        ),
    )
    .withColumn(
        "document_id",
        F.concat(
            F.lit("SYN"),
            F.lpad(F.col("customer_id").cast("string"), 11, "0"),
        ),
    )
    .withColumn(
        "email",
        F.concat(
            F.lit("cliente"),
            F.lpad(F.col("customer_id").cast("string"), 6, "0"),
            F.lit("@example.invalid"),
        ),
    )
    .withColumn(
        "state",
        F.element_at(
            state_array,
            (F.pmod(F.xxhash64("customer_id"), F.lit(len(states))) + 1).cast("int"),
        ),
    )
    .withColumn(
        "risk_rating",
        F.element_at(
            rating_array,
            (
                F.pmod(
                    F.xxhash64("customer_id", F.lit("risk")),
                    F.lit(len(ratings)),
                )
                + 1
            ).cast("int"),
        ),
    )
    .withColumn(
        "monthly_income",
        F.when(
            F.col("customer_type") == "PJ",
            (
                F.lit(20_000)
                + F.pmod(
                    F.xxhash64("customer_id", F.lit("income")),
                    F.lit(480_000),
                )
            ).cast("double"),
        ).otherwise(
            (
                F.lit(1_500)
                + F.pmod(
                    F.xxhash64("customer_id", F.lit("income")),
                    F.lit(28_500),
                )
            ).cast("double")
        ),
    )
    .withColumn(
        "created_at",
        F.date_sub(
            F.to_date(F.lit(REFERENCE_DATE)),
            (
                F.pmod(
                    F.xxhash64("customer_id", F.lit("created")),
                    F.lit(730),
                )
                + 30
            ).cast("int"),
        ),
    )
    .withColumn("updated_at", F.to_timestamp(F.lit(f"{REFERENCE_DATE} 06:00:00")))
    .withColumn("is_active", F.lit(True))
    .withColumn("source_system", F.lit("CUSTOMER_CORE"))
    .withColumn("source_batch_id", F.lit(SOURCE_BATCH_ID))
)

# COMMAND ----------

pf_products = F.array(F.lit(1), F.lit(2), F.lit(3), F.lit(5))
pj_products = F.array(F.lit(4), F.lit(5))
term_options = F.array(
    F.lit(6), F.lit(12), F.lit(18), F.lit(24),
    F.lit(36), F.lit(48), F.lit(60),
)

contracts_seed = (
    spark.range(1, N_CONTRACTS + 1)
    .withColumnRenamed("id", "contract_id")
    .withColumn(
        "customer_id",
        (
            F.pmod(
                F.xxhash64("contract_id", F.lit("customer")),
                F.lit(N_CUSTOMERS),
            )
            + 1
        ).cast("long"),
    )
    .join(customers.select("customer_id", "customer_type", "risk_rating"), "customer_id")
    .withColumn(
        "product_id",
        F.when(
            F.col("customer_type") == "PJ",
            F.element_at(
                pj_products,
                (
                    F.pmod(F.xxhash64("contract_id", F.lit("product")), F.lit(2))
                    + 1
                ).cast("int"),
            ),
        ).otherwise(
            F.element_at(
                pf_products,
                (
                    F.pmod(F.xxhash64("contract_id", F.lit("product")), F.lit(4))
                    + 1
                ).cast("int"),
            )
        ),
    )
    .join(products, "product_id")
    .withColumn(
        "contract_date",
        F.date_sub(
            F.to_date(F.lit(REFERENCE_DATE)),
            (
                F.pmod(F.xxhash64("contract_id", F.lit("date")), F.lit(540))
                + 30
            ).cast("int"),
        ),
    )
    .withColumn(
        "principal_amount",
        (
            F.lit(1_000)
            + F.pmod(
                F.xxhash64("contract_id", F.lit("principal")),
                F.lit(199_000),
            )
        ).cast("double"),
    )
    .withColumn(
        "raw_term_months",
        F.element_at(
            term_options,
            (
                F.pmod(F.xxhash64("contract_id", F.lit("term")), F.lit(7))
                + 1
            ).cast("int"),
        ),
    )
    .withColumn("term_months", F.least("raw_term_months", "max_term_months"))
)

risk_premium = (
    F.when(F.col("risk_rating") == "A", F.lit(0.000))
    .when(F.col("risk_rating") == "B", F.lit(0.015))
    .when(F.col("risk_rating") == "C", F.lit(0.035))
    .when(F.col("risk_rating") == "D", F.lit(0.065))
    .otherwise(F.lit(0.100))
)

contracts = (
    contracts_seed
    .withColumn(
        "annual_interest_rate",
        F.round(F.col("base_annual_interest_rate") + risk_premium, 4),
    )
    .withColumn(
        "contract_status",
        F.when(
            F.pmod(F.xxhash64("contract_id", F.lit("status")), F.lit(100)) < 8,
            F.lit("CLOSED"),
        ).otherwise(F.lit("ACTIVE")),
    )
    .withColumn("source_updated_at", F.to_timestamp(F.lit(f"{REFERENCE_DATE} 07:00:00")))
    .withColumn("source_system", F.lit("CREDIT_CORE"))
    .withColumn("source_batch_id", F.lit(SOURCE_BATCH_ID))
    .select(
        "contract_id",
        "customer_id",
        "product_id",
        "contract_date",
        "principal_amount",
        "annual_interest_rate",
        "term_months",
        "contract_status",
        "source_updated_at",
        "source_system",
        "source_batch_id",
    )
)

duplicate_contract = contracts.filter(F.col("contract_id") == 10)
negative_contract = (
    contracts.filter(F.col("contract_id") == 11)
    .withColumn("contract_id", F.lit(9_000_001).cast("long"))
    .withColumn("principal_amount", F.lit(-500.00))
)
orphan_contract = (
    contracts.filter(F.col("contract_id") == 12)
    .withColumn("contract_id", F.lit(9_000_002).cast("long"))
    .withColumn("customer_id", F.lit(9_999_999).cast("long"))
)

contracts_source = (
    contracts
    .unionByName(duplicate_contract)
    .unionByName(negative_contract)
    .unionByName(orphan_contract)
)

# COMMAND ----------

installments = (
    contracts
    .withColumn("installment_number", F.explode(F.sequence(F.lit(1), "term_months")))
    .withColumn(
        "installment_id",
        F.concat_ws(
            "-",
            "contract_id",
            F.lpad(F.col("installment_number").cast("string"), 3, "0"),
        ),
    )
    .withColumn("due_date", F.expr("add_months(contract_date, installment_number)"))
    .withColumn(
        "scheduled_amount",
        F.round(
            (
                F.col("principal_amount")
                * (
                    F.lit(1)
                    + F.col("annual_interest_rate") * F.col("term_months") / F.lit(12)
                )
            )
            / F.col("term_months"),
            2,
        ),
    )
    .withColumn("source_system", F.lit("INSTALLMENT_CORE"))
    .withColumn("source_batch_id", F.lit(SOURCE_BATCH_ID))
    .select(
        "installment_id",
        "contract_id",
        "installment_number",
        "due_date",
        "scheduled_amount",
        "source_system",
        "source_batch_id",
    )
)

payments = (
    installments
    .filter(F.col("due_date") <= F.to_date(F.lit(REFERENCE_DATE)))
    .filter(
        F.pmod(F.xxhash64("installment_id", F.lit("paid")), F.lit(100)) < 82
    )
    .withColumn(
        "delay_days",
        (
            F.pmod(F.xxhash64("installment_id", F.lit("delay")), F.lit(55))
            - 5
        ).cast("int"),
    )
    .withColumn("payment_date", F.date_add("due_date", "delay_days"))
    .filter(F.col("payment_date") <= F.to_date(F.lit(REFERENCE_DATE)))
    .withColumn(
        "amount_paid",
        F.when(
            F.pmod(F.xxhash64("installment_id", F.lit("partial")), F.lit(100)) < 12,
            F.round(F.col("scheduled_amount") * F.lit(0.60), 2),
        ).otherwise(F.col("scheduled_amount")),
    )
    .withColumn(
        "payment_id",
        F.sha2(
            F.concat_ws("|", "installment_id", "payment_date", "amount_paid"),
            256,
        ),
    )
    .withColumn("event_type", F.lit("PAYMENT"))
    .withColumn(
        "event_timestamp",
        F.to_timestamp(
            F.concat(F.date_format("payment_date", "yyyy-MM-dd"), F.lit(" 10:00:00"))
        ),
    )
    .withColumn("source_system", F.lit("PAYMENT_GATEWAY"))
    .withColumn("source_batch_id", F.lit(SOURCE_BATCH_ID))
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
    )
)

duplicate_payment = payments.orderBy("payment_id").limit(1)
invalid_payment = (
    payments.orderBy("payment_id").limit(1)
    .withColumn("payment_id", F.lit("INVALID_NULL_AMOUNT"))
    .withColumn("amount_paid", F.lit(None).cast("double"))
)
payments_source = payments.unionByName(duplicate_payment).unionByName(invalid_payment)

# COMMAND ----------

paths = {
    "products": f"{BASE_PATH}/products/full",
    "customers": f"{BASE_PATH}/customers/snapshot_date={REFERENCE_DATE}",
    "contracts": f"{BASE_PATH}/contracts/batch_date={REFERENCE_DATE}",
    "installments": f"{BASE_PATH}/installments/snapshot_date={REFERENCE_DATE}",
    "payments": f"{BASE_PATH}/payments/event_date={REFERENCE_DATE}",
}


def path_has_data(path: str) -> bool:
    try:
        return any(not item.name.startswith("_") for item in dbutils.fs.ls(path))
    except Exception:
        return False


def should_write(name: str) -> bool:
    exists = path_has_data(paths[name])
    if exists and not FORCE_REGENERATE:
        print(f"SKIP {name}: fonte já inicializada em {paths[name]}")
        return False
    return True


if should_write("products"):
    products.coalesce(1).write.mode("overwrite").option("header", True).csv(paths["products"])

if should_write("customers"):
    customers.coalesce(2).write.mode("overwrite").json(paths["customers"])

if should_write("contracts"):
    contracts_source.coalesce(2).write.mode("overwrite").option("header", True).csv(paths["contracts"])

if should_write("installments"):
    installments.repartition(4).write.mode("overwrite").parquet(paths["installments"])

if should_write("payments"):
    payments_source.repartition(4).write.mode("overwrite").json(paths["payments"])

# COMMAND ----------

summary = spark.createDataFrame(
    [
        ("products", products.count(), paths["products"]),
        ("customers", customers.count(), paths["customers"]),
        ("contracts_source", contracts_source.count(), paths["contracts"]),
        ("installments", installments.count(), paths["installments"]),
        ("payments_source", payments_source.count(), paths["payments"]),
    ],
    ["dataset", "record_count", "path"],
)

display(summary)

assert products.count() == 5
assert customers.count() == 2_000
assert contracts.count() == 5_000
assert contracts_source.count() == 5_003
assert contracts_source.select("contract_id").distinct().count() == 5_002

print("Fontes sintéticas disponíveis e contratos de geração validados.")

