# Arquitetura e decisões técnicas

## Fronteiras das camadas

### Landing

Simula sistemas independentes e preserva formatos heterogêneos. Os arquivos ficam no volume governado `credlake.landing.raw`.

### Bronze

Preserva o evento ou snapshot recebido e acrescenta metadados como arquivo, modificação e horário de ingestão. Problemas de negócio não são silenciosamente removidos.

### Silver

Define contratos confiáveis por entidade:

- chave e tipos obrigatórios;
- deduplicação determinística;
- integridade com clientes, produtos, contratos e parcelas;
- valores financeiros positivos;
- compatibilidade entre produto e tipo de cliente;
- quarentena com múltiplos motivos por registro.

### Gold

Expõe uma dimensão de clientes sem PII, uma dimensão de produtos, uma tabela fato por contrato e KPIs segmentados. A granularidade é declarada nas propriedades das tabelas.

### Ops

Separa estado operacional de dados de negócio. O volume `pipeline_state` guarda checkpoints e schema do Auto Loader; `data_quality_results` guarda o histórico de verificações.

## Idempotência em três níveis

| Nível | Mecanismo | O que resolve |
|---|---|---|
| Arquivo batch | `COPY INTO` | Não relê o mesmo arquivo registrado |
| Stream | checkpoint do Auto Loader | Mantém progresso e semântica incremental |
| Entidade | `MERGE` + `record_hash` | Evita duplicação e atualiza somente mudanças funcionais |

Idempotência de arquivo não substitui deduplicação de negócio. Um arquivo pode conter duas linhas com o mesmo `payment_id`; ambas pertencem à Bronze, mas somente uma chega à Silver.

## Snapshots

Clientes e parcelas representam uma fotografia completa. `replaceWhere` substitui atomicamente somente a data recebida. Isso permite manter outros snapshots sem sobrescrever a tabela inteira.

## Data de referência

Dias de atraso e pagamentos acumulados são calculados com `snapshot_date`. Usar `current_date()` tornaria o mesmo snapshot diferente a cada execução e impediria reconciliação histórica.

## Qualidade auditável

As tabelas de quarentena guardam:

- dados originais relevantes;
- posição do registro na deduplicação;
- lista de códigos de erro;
- explicações legíveis;
- hash funcional;
- instante do processamento.

O pipeline falha ao final quando um controle de severidade `ERROR` não passa. O resultado é gravado antes da falha, permitindo diagnóstico.

## Segurança e governança

- catálogo, schemas, tabelas e volumes são registrados no Unity Catalog;
- dados pessoais aparecem apenas em Bronze/Silver;
- `gold.dim_customer` remove nome, documento e e-mail;
- o repositório não contém credenciais;
- os dados usam e-mails `.invalid` e documentos sintéticos.

## Performance

As tabelas desta demonstração são pequenas e não são particionadas. Particionar poucos dados aumentaria a quantidade de arquivos e metadados. Em escala, o caminho é medir filtros recorrentes e avaliar liquid clustering por data, contrato ou produto.

## Evoluções naturais

1. histórico SCD Tipo 2 para clientes e contratos;
2. múltiplos pagamentos por parcela e estornos;
3. amortização SAC/Price;
4. Lakeflow Declarative Pipelines com expectations;
5. CDC de clientes e contratos;
6. alertas com base em `ops.data_quality_results`;
7. políticas de mascaramento e row filters;
8. testes de integração executados em workspace efêmero.

