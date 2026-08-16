# Guia para apresentar o projeto em entrevista

## Resumo de 60 segundos

> Construí um Lakehouse financeiro no Databricks para acompanhar uma carteira de crédito. Usei formatos e padrões de ingestão diferentes conforme o comportamento da fonte: COPY INTO para batch CSV, replaceWhere para snapshots e Auto Loader com checkpoint para pagamentos incrementais. A Bronze preserva os dados recebidos; a Silver deduplica, valida integridade e mantém quarentenas auditáveis; a Gold reconcilia parcelas e pagamentos para calcular saldo, atraso e NPL 90. O pipeline é orquestrado como um Lakeflow Job e grava testes de qualidade antes de liberar o consumo.

## Perguntas que o projeto responde

### Por que não limpar diretamente na Bronze?

Porque perderíamos evidência da origem e dificultaríamos auditoria e reprocessamento. A Bronze responde “o que chegou”; a Silver responde “o que pode ser usado”.

### Checkpoint elimina pagamento duplicado?

Não. O checkpoint evita reprocessar o mesmo arquivo. Uma duplicidade dentro do arquivo continua existindo e precisa ser resolvida pela chave de negócio na Silver.

### Por que usar a data do snapshot?

Para tornar os indicadores reproduzíveis. Se os dias de atraso usassem a data atual, uma reexecução do mesmo snapshot produziria outro resultado.

### Por que `DECIMAL`?

Valores monetários não devem depender da aproximação binária de `double`. A Bronze tipa a entrada e a Silver/Gold executam as contas em escala decimal definida.

### Por que não particionar?

O conjunto é pequeno. Partições criariam arquivos pequenos e metadados sem benefício. Em escala, eu mediria os filtros e avaliaria liquid clustering antes de particionamento manual.

### Como o pipeline se recupera?

Batch usa o histórico do `COPY INTO`; Auto Loader retoma do checkpoint; snapshots substituem uma data atomicamente; Silver usa `MERGE` com hash. Para reconstrução integral existe um reset manual com confirmação explícita.

## Trade-offs assumidos

- a carteira é sintética e tem data fixa;
- o cálculo de juros é simplificado;
- a Gold mostra posição atual, não SCD Tipo 2;
- o modo `availableNow` oferece incrementalidade com custo controlado, mas não latência contínua;
- quarentenas são sincronizadas com a fonte atual, e não um log imutável de todos os estados históricos.

## Evolução que demonstra maturidade

Ao explicar próximos passos, priorize necessidade de negócio:

1. SCD Tipo 2 se auditoria histórica for requisito;
2. CDC se contratos sofrerem atualizações frequentes;
3. file events se o volume e custo de listagem crescerem;
4. políticas de mascaramento se analistas precisarem acessar a Silver;
5. cálculo Price/SAC se o projeto for usado para contabilidade contratual;
6. alertas e SLA se o pipeline passar a sustentar decisões diárias.

