-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Consultas para análise e dashboard

-- COMMAND ----------

-- Visão executiva
SELECT *
FROM credlake.gold.vw_executive_portfolio
ORDER BY snapshot_date;

-- COMMAND ----------

-- Distribuição da carteira por faixa de atraso
SELECT
    snapshot_date,
    delinquency_bucket,
    COUNT(*) AS contracts,
    SUM(outstanding_amount) AS outstanding_amount,
    SUM(overdue_amount) AS overdue_amount
FROM credlake.gold.fact_credit_portfolio
GROUP BY snapshot_date, delinquency_bucket
ORDER BY snapshot_date, delinquency_bucket;

-- COMMAND ----------

-- Produtos com maior saldo vencido
SELECT
    product_name,
    SUM(outstanding_amount) AS outstanding_amount,
    SUM(overdue_amount) AS overdue_amount,
    SUM(npl_90_amount) AS npl_90_amount,
    SUM(overdue_amount) / NULLIF(SUM(outstanding_amount), 0) AS delinquency_ratio
FROM credlake.gold.portfolio_kpis
GROUP BY product_name
ORDER BY overdue_amount DESC;

-- COMMAND ----------

-- Risco e inadimplência por UF
SELECT
    state,
    risk_rating,
    SUM(contract_count) AS contracts,
    SUM(outstanding_amount) AS outstanding_amount,
    SUM(overdue_amount) AS overdue_amount
FROM credlake.gold.portfolio_kpis
GROUP BY state, risk_rating
ORDER BY overdue_amount DESC;

-- COMMAND ----------

-- Último resultado de qualidade
SELECT *
FROM credlake.ops.data_quality_results
QUALIFY DENSE_RANK() OVER (ORDER BY run_timestamp DESC) = 1
ORDER BY passed, layer, dataset, check_name;

