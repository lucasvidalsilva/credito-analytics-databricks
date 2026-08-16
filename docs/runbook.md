# Runbook de execução

## Pré-requisitos

- workspace Databricks com Unity Catalog;
- permissão para criar catálogo, schemas, volumes e tabelas;
- compute compatível com Auto Loader e Structured Streaming;
- repositório adicionado como Databricks Git Folder.

## Primeira execução em ambiente limpo

Execute os notebooks de `src/credlake` nesta ordem:

1. `00_setup.py`;
2. `01_generate_source_data.py`;
3. `02_bronze_batch.py`, `03_bronze_snapshots.py` e `04_bronze_payments.py`;
4. `05_silver_dimensions.py`;
5. `06_silver_contracts.py` e `07_silver_payments.py`;
6. `08_silver_installments.py`;
7. `09_gold_portfolio.py`;
8. `10_quality_observability.py`.

O último notebook deve terminar sem exceção e todos os controles devem apresentar `passed = true`.

## Ambiente que executou os notebooks antigos

Os notebooks canônicos alteram contratos de schema e corrigem a geração de prazos e produtos. Para um teste realmente limpo:

1. abra `99_reset_demo.py`;
2. mantenha `catalog = credlake`;
3. escreva `RESET CREDLAKE` no widget `confirmation`;
4. execute somente depois de confirmar que o catálogo contém apenas dados deste projeto;
5. volte ao `00_setup.py`.

O reset remove tabelas, volumes, arquivos sintéticos e checkpoints do catálogo. Ele não faz parte do job e falha por padrão sem a confirmação exata.

## Execução pelo bundle

```bash
databricks auth login
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run credlake_end_to_end -t dev
```

Para outro catálogo:

```bash
databricks bundle deploy -t dev --var catalog=credlake_lucas
databricks bundle run credlake_end_to_end -t dev --var catalog=credlake_lucas
```

## Reexecução

- o gerador ignora fontes existentes por padrão;
- `COPY INTO` ignora arquivos já carregados;
- snapshots substituem apenas a data informada;
- Auto Loader retoma do checkpoint;
- Silver usa `MERGE` e hash funcional;
- Gold é reconstruída deterministicamente a partir da Silver.

## Diagnóstico

### Auto Loader não recarregou arquivos

O checkpoint provavelmente já registra esses arquivos. Não apague apenas a tabela ou apenas o checkpoint. Para reconstrução integral da demonstração, use o reset controlado.

### `COPY INTO` carregou zero linhas

Isso é esperado quando todos os arquivos já estão no histórico de ingestão. Confirme a contagem da tabela e o `DESCRIBE HISTORY`.

### Contratos válidos diferentes de 5.000

Consulte:

```sql
SELECT error_code, COUNT(*)
FROM credlake.silver.contracts_quarantine
LATERAL VIEW EXPLODE(error_codes) e AS error_code
GROUP BY error_code;
```

Se o catálogo recebeu dados da versão antiga, faça o reset controlado e execute o fluxo canônico.

### Pagamentos em `_rescued_data`

Uma coluna nova ou um tipo incompatível chegou à origem. Inspecione os valores antes de alterar o schema. Não descarte `_rescued_data` silenciosamente.

### Controle final falhou

Localize o `run_id` mais recente:

```sql
SELECT *
FROM credlake.ops.data_quality_results
QUALIFY DENSE_RANK() OVER (ORDER BY run_timestamp DESC) = 1
ORDER BY passed, layer, dataset;
```

## Consultas de aceite

```sql
SELECT * FROM credlake.gold.vw_executive_portfolio;

SELECT delinquency_bucket, COUNT(*), SUM(outstanding_amount)
FROM credlake.gold.fact_credit_portfolio
GROUP BY delinquency_bucket
ORDER BY delinquency_bucket;

SELECT *
FROM credlake.ops.data_quality_results
ORDER BY run_timestamp DESC, passed;
```

