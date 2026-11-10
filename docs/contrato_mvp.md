# Contrato entre a camada Ouro e o MVP

O MVP consome somente as tabelas `mvp_*`. Elas não têm nome, CPF, e-mail, telefone nem o identificador original da residência. A chave `residencia_hash` é um SHA-256 com sal.

O notebook `databricks/05_exportar_mvp.py` grava tudo em `seguwatts_mvp.json`. Esse arquivo é o que a tela `mvp/index.html` lê.

## Arquivo `seguwatts_mvp.json`

```json
{
  "gerado_em": "2026-11-09T22:00:00+00:00",
  "residencias": [
    {
      "residencia_hash": "…",
      "rotulo": "Casa ab12cd",
      "municipio": "Campinas",
      "distribuidora": "CPFL-PAULISTA",
      "moradores": 3,
      "painel": [],
      "perfil_semana": [],
      "aparelhos": [],
      "alertas": []
    }
  ]
}
```

## Tabelas

### `mvp_painel_mensal` (lista `painel`)

| Campo | Tipo | Significado |
|---|---|---|
| `residencia_hash` | string | Identificador protegido da casa |
| `ano_mes` | string | Mês no formato `AAAA-MM` |
| `kwh_total` | double | Consumo no mês |
| `custo_rs` | double | Custo sem impostos |
| `temperatura_media` | double | Temperatura média em °C |
| `bandeira` | string | Bandeira tarifária do mês |
| `dias_medidos` | long | Dias com leitura; divide o total em meses incompletos |
| `variacao_kwh_pct` | double | Variação sobre o mês anterior |

### `mvp_perfil_semana` (lista `perfil_semana`)

| Campo | Tipo | Significado |
|---|---|---|
| `dia_semana_num` | int | 1 = domingo, 7 = sábado |
| `dia_semana` | string | Nome do dia |
| `hora` | int | Hora local, de 0 a 23 |
| `kwh_medio` | double | Consumo médio da casa naquela hora |

### `mvp_aparelhos_ciclo` (lista `aparelhos`)

Último ciclo de conta fechado.

| Campo | Tipo | Significado |
|---|---|---|
| `rotulo`, `tipo` | string | Nome de exibição e código do aparelho |
| `origem_medicao` | string | `medido` (tomada) ou `estimado` (rateio) |
| `fim_ciclo` | date | Data de leitura da conta |
| `kwh`, `custo_rs` | double | Consumo e custo no ciclo |
| `participacao_pct` | double | Percentual sobre o total da casa |

### `mvp_alertas_aparelho` (lista `alertas`)

| Campo | Tipo | Significado |
|---|---|---|
| `rotulo`, `tipo` | string | Aparelho ou `demais_cargas` |
| `origem_medicao` | string | `medido` ou `residual` |
| `status` | string | `normal`, `atencao`, `critico` ou `sem_base` |
| `excesso_pct` | double | Excesso médio em 7 dias de uso; nulo sem alerta |
| `desde` | date | Primeira detecção; nulo sem alerta |
| `kwh_excesso_mes`, `rs_excesso_mes` | double | Desperdício projetado em 30 dias |
| `mensagem` | string | Texto simples para o morador |

## Regras para quem consome

- `sem_base` significa que o aparelho ainda não tem 30 dias de uso. Não trate como problema.
- O alerta sugere uma verificação. Não apresente como defeito confirmado.
- Informe que os valores em reais não incluem ICMS, PIS e COFINS.
