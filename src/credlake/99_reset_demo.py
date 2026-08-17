# Databricks notebook source
# MAGIC %md
# MAGIC # Utilitário manual — reset completo do ambiente de demonstração
# MAGIC **Não faz parte do job.** Remove exclusivamente o catálogo informado, incluindo tabelas, volumes, arquivos sintéticos e checkpoints.

# COMMAND ----------

import re

dbutils.widgets.text("catalog", "credlake", "Catálogo")
dbutils.widgets.text("confirmation", "NO", "Confirmação")

CATALOG = dbutils.widgets.get("catalog").strip()
CONFIRMATION = dbutils.widgets.get("confirmation").strip()

if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", CATALOG):
    raise ValueError(f"Nome de catálogo inválido: {CATALOG!r}")

expected = f"RESET {CATALOG.upper()}"

if CONFIRMATION != expected:
    raise ValueError(
        "Reset cancelado. Para confirmar conscientemente, informe "
        f"{expected!r} no widget confirmation."
    )

spark.sql(f"DROP CATALOG IF EXISTS `{CATALOG}` CASCADE")
print(f"Catálogo {CATALOG!r} removido. Execute o pipeline desde 00_setup.")

