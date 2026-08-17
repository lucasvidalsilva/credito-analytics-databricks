# Dicionário de dados

## Bronze

| Tabela | Granularidade | Chave de origem | Observação |
|---|---|---|---|
| `bronze.products_raw` | produto por arquivo | `product_id` | CSV via `COPY INTO` |
| `bronze.customers_raw` | cliente por snapshot | `customer_id`, `snapshot_date` | JSON com schema explícito |
| `bronze.contracts_raw` | linha recebida de contrato | nenhuma garantida | contém erros intencionais |
| `bronze.installments_raw` | parcela por snapshot | `installment_id`, `snapshot_date` | Parquet tipado |
| `bronze.payments_raw` | evento recebido | nenhuma garantida | Auto Loader e `_rescued_data` |

## Silver

### `silver.contracts`

| Coluna | Tipo | Definição |
|---|---|---|
| `contract_id` | BIGINT | Identificador do contrato |
| `customer_id` | BIGINT | Cliente validado |
| `product_id` | INT | Produto validado |
| `principal_amount` | DECIMAL(18,2) | Principal concedido |
| `annual_interest_rate` | DECIMAL(10,4) | Taxa anual em representação decimal |
| `term_months` | INT | Prazo contratual em meses |
| `record_hash` | STRING | Hash dos atributos funcionais |

### `silver.payments`

| Coluna | Tipo | Definição |
|---|---|---|
| `payment_id` | STRING | Identificador único do evento válido |
| `installment_id` | STRING | Parcela relacionada |
| `payment_date` | DATE | Data efetiva do pagamento |
| `amount_paid` | DECIMAL(18,2) | Valor válido recebido |
| `event_timestamp` | TIMESTAMP | Horário do evento na origem |

### `silver.installments`

| Coluna | Tipo | Definição |
|---|---|---|
| `scheduled_amount` | DECIMAL(18,2) | Valor previsto da parcela |
| `total_paid` | DECIMAL(18,2) | Pagamentos válidos acumulados até o snapshot |
| `outstanding_amount` | DECIMAL(18,2) | Saldo ainda não pago, limitado a zero |
| `overpaid_amount` | DECIMAL(18,2) | Valor pago acima do previsto |
| `days_past_due` | INT | Dias de atraso na data do snapshot |
| `installment_status` | STRING | `PAID`, `OVERDUE`, `PARTIALLY_PAID` ou `OPEN` |

## Gold

### `gold.fact_credit_portfolio`

Granularidade: um contrato por snapshot.

| Coluna | Definição |
|---|---|
| `contractual_amount` | Soma do valor previsto das parcelas |
| `total_paid` | Total recebido até o snapshot |
| `outstanding_amount` | Saldo a receber |
| `overdue_amount` | Parcela do saldo que está vencida |
| `max_days_past_due` | Maior atraso entre as parcelas abertas |
| `delinquency_bucket` | Faixa de atraso do contrato |
| `is_npl_90` | Indica atraso superior a 90 dias |
| `outstanding_ratio` | Saldo pendente dividido pelo valor contratual |

### `gold.portfolio_kpis`

Granularidade: snapshot, produto, UF e classificação de risco.

Principais medidas:

- quantidade de contratos;
- principal concedido;
- valor contratual;
- total pago;
- saldo pendente;
- saldo vencido;
- saldo NPL 90;
- índice de inadimplência;
- taxa anual média ponderada pelo principal.

