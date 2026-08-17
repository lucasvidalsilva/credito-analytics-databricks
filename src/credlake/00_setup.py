# Databricks notebook source
# MAGIC %md
# MAGIC # CredLake — preparação do ambiente
# MAGIC Cria o catálogo, schemas e volumes governados usados pelo pipeline.

# COMMAND ----------

import re

dbutils.widgets.text("catalog", "credlake", "Catálogo")
CATALOG = dbutils.widgets.get("catalog").strip()

if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", CATALOG):
    raise ValueError(f"Nome de catálogo inválido: {CATALOG!r}")

print(f"Catálogo do pipeline: {CATALOG}")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")

for schema, comment in {
    "landing": "Arquivos sintéticos que simulam os sistemas de origem.",
    "bronze": "Dados ingeridos com metadados técnicos e mínima transformação.",
    "silver": "Entidades validadas, deduplicadas e reconciliadas.",
    "gold": "Modelo analítico e indicadores da carteira de crédito.",
    "ops": "Estado de pipelines, checkpoints e resultados de qualidade.",
}.items():
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{schema}` "
        f"COMMENT '{comment}'"
    )

# COMMAND ----------

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`landing`.`raw` "
    "COMMENT 'Arquivos de entrada do projeto CredLake.'"
)
spark.sql(
    f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`ops`.`pipeline_state` "
    "COMMENT 'Schemas e checkpoints dos pipelines incrementais.'"
)

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT catalog_name, schema_name
        FROM system.information_schema.schemata
        WHERE catalog_name = '{CATALOG}'
        ORDER BY schema_name
        """
    )
)

print("Ambiente CredLake preparado com sucesso.")

