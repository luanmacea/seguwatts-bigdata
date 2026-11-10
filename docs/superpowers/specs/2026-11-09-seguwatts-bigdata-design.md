# SeguWatts: design do componente de Big Data

**Disciplina:** Big Data Architecture & Data Integration (GS 2026.2, FIAP 4ES)
**Entrega:** 19/11/2026, 23h55, PDF com o link do repositório no Teams
**Decisões:** tomadas de forma autônoma a pedido do autor, para revisão posterior.

## 1. Produto em uma frase

O SeguWatts mostra ao morador quando e onde a casa gasta energia, quanto isso custa e qual aparelho pode estar com defeito, antes que o problema apareça na conta.

**Cidadão:** famílias de renda baixa e média no estado de São Paulo, para quem a conta de luz pesa no orçamento.

**ODS:** 7 não consta na lista do briefing, então o projeto se liga aos ODS 11 (cidades sustentáveis), 13 (clima) e 10 (desigualdades).

### Perguntas que o MVP responde

| # | Pergunta do cidadão | Consulta |
|---|---|---|
| 1 | Em que dia da semana e horário minha casa gasta mais? | `q1` |
| 2 | Como meu gasto muda de mês para mês, com o calor e a bandeira? | `q2` |
| 3 | Qual aparelho pesa mais na minha conta? | `q3` |
| 4 | Algum aparelho está gastando mais do que deveria? | `q4` |
| 5 | Quanto eu economizo se resolver esses problemas? (impacto) | `q5` |

## 2. Decisão de sensores: híbrido

Um sensor único lendo por hora não separa o consumo por aparelho, porque registra só a soma. A casa tem:

- **1 sensor geral** no quadro de energia, com leitura por hora;
- **sensores de tomada** em 2 ou 3 aparelhos críticos: geladeira sempre e, quando existirem, freezer e ar-condicionado;
- **conta de luz e cadastro de aparelhos**, informados pelo morador.

| Tipo de aparelho | Como o consumo é obtido | O que o sistema detecta |
|---|---|---|
| Monitorado | Medido por hora na tomada | Defeito no próprio aparelho |
| Demais cargas | `geral - soma das tomadas`, por hora | "Consumo sem explicação" |
| Aparelho sem sensor | Rateio do residual por ciclo, usando potência x horas declaradas e a conta | Participação estimada, sem alerta |

## 3. Fontes

| Fonte | Tipo | Origem | Formato | Frequência |
|---|---|---|---|---|
| `clima` | Real | Open-Meteo Archive, 6 municípios | NDJSON | Diária |
| `tarifas` | Real | ANEEL CKAN, B1 residencial convencional | NDJSON | Diária (retrato) |
| `bandeiras` | Real | ANEEL CKAN, acionamento mensal | NDJSON | Diária (retrato) |
| `municipios` | Real | IBGE Localidades, SP | NDJSON | Diária (retrato) |
| `cadastro` | Simulada | Morador + aparelhos (JSON aninhado) | NDJSON | Diária (retrato) |
| `sensores` | Simulada | Leituras horárias | NDJSON | Diária |
| `contas` | Simulada | Contas com ciclo fechado no dia | NDJSON | Diária |

**Municípios e distribuidoras:** São Paulo e Osasco (`ELETROPAULO`), Campinas e Ribeirão Preto (`CPFL-PAULISTA`), Guarulhos e São José dos Campos (`EDP SP`).

### Por que parte dos dados é simulada

Não existe base aberta de consumo por residência, e dados pessoais reais não podem ir para o repositório. O briefing aceita dados simulados com geração justificada e documentada.

**Método do simulador** (`dags/seguwatts/simulador.py`):

- 100 residências, com semente fixa: repetir um dia gera os mesmos dados;
- perfis de uso por hora e dia da semana, moradores e aparelhos variados;
- o ar-condicionado e a geladeira reagem à **temperatura real** coletada do Open-Meteo;
- nomes, CPFs e e-mails claramente fictícios (`Morador Exemplo 0001`, `@exemplo.invalid`), usados só para demonstrar a proteção LGPD;
- sujeira proposital: `kwh` nulo, negativo, absurdo, com vírgula decimal, leituras duplicadas, residência ou ponto inexistente;
- **falhas injetadas** com data e intensidade gravadas em `gabarito/falhas.json`, para medir a detecção com precisão e cobertura reais.

Falhas: degradação de 25% a 45% em aparelho monitorado, com rampa de 14 dias, e carga oculta contínua de 80 W a 150 W nas demais cargas. Geladeira e freezer falham a partir de 15/07; ar-condicionado a partir de 15/10, para já haver histórico de uso.

## 4. Arquitetura

```
APIs + simulador -> Airflow (DAG diário) -> chegada/ -> upload manual em lotes -> Volume
  -> Bronze (Auto Loader) -> Prata (MERGE + qualidade) -> Ouro (fato + dimensões) -> JSON -> tela do MVP
```

O Databricks Free Edition não se conecta ao Airflow, então os arquivos sobem pela interface, como o enunciado determina.

### Airflow (`dags/seguwatts_coleta.py`)

`inicio >> [clima, tarifas, bandeiras, municipios, cadastro] >> sensores >> contas >> conferir_chegada >> notificar`

- `@daily`, `catchup=True`, `max_active_runs=1`, 2 tentativas por tarefa;
- configuração e URL de notificação em Airflow Variables;
- `notificar` roda mesmo com falha (`trigger_rule=all_done`);
- `conferir_chegada` falha se faltar arquivo ou se ele estiver vazio;
- `scripts/carga_historica.py` usa as mesmas funções para gerar o período de 01/05/2026 em diante. O DAG agenda a partir de 25/10/2026.

### Bronze (`databricks/01_bronze.py`)

Auto Loader com `availableNow`, uma tabela por fonte, colunas `arquivo_origem` e `carregado_em`. O checkpoint impede releitura e duplicação. Imprime contagens para o relatório.

### Prata (`databricks/02_prata.py`)

Tipos corretos, deduplicação e `MERGE` pela chave de cada tabela. Registros barrados vão para `prata_rejeitos` e o total por regra para `prata_qualidade`.

| Tabela | Chave do MERGE | Regras com contagem |
|---|---|---|
| `prata_leituras` | residência, ponto, instante | kWh nulo ou inválido; fora de 0 a 15 kWh/h; residência inexistente; ponto inexistente; duplicata |
| `prata_clima` | município, instante | temperatura fora de -10 a 50 °C |
| `prata_contas` | conta | kWh não positivo; datas invertidas |
| `prata_tarifas` | distribuidora, início da vigência | tarifa não positiva |
| `prata_bandeiras` | competência | adicional inválido |
| `prata_residencias`, `prata_aparelhos`, `prata_municipios` | identificador | registro mais recente |

### Ouro (`databricks/03_ouro.py`)

| Tabela | Grão | Papel |
|---|---|---|
| `dim_data` | dia | Calendário e bandeira vigente |
| `dim_localidade` | município | Município e distribuidora |
| `dim_residencia` | residência | Hash do titular e dados mascarados |
| `dim_aparelho` | aparelho da residência | Tipo, potência e forma de medição |
| `fato_consumo_hora` | residência, aparelho, hora | Fato aditivo: kWh, R$, temperatura |
| `fato_rateio_ciclo` | residência, aparelho, ciclo | Estimativa calibrada pela conta |
| `mvp_alertas_aparelho` | aparelho | Contrato: alertas |
| `mvp_painel_mensal` | residência, mês | Contrato: painel |
| `mvp_perfil_semana` | residência, dia da semana, hora | Contrato: gráfico |

**Aditividade:** o fato guarda tomadas e `demais_cargas`, nunca o sensor geral junto. A soma das linhas é igual ao consumo da casa.

**Custo:** `kWh x (TUSD + TE + bandeira) / 1000`, sem impostos estaduais e federais. O painel indica isso.

### Detecção de defeito

Cada aparelho é comparado com ele mesmo no início do histórico:

1. Calcula por dia a potência média nas horas em que o aparelho ficou ligado.
2. Usa os 30 primeiros dias de uso como referência e ajusta uma reta potência x temperatura com `regr_slope` e `regr_intercept`. Com menos de 30 dias de uso, o status é `sem_base`.
3. Calcula o excesso diário sobre o esperado.
4. Alerta quando a média de 7 dias de uso passa de 20% e pelo menos 5 desses dias passam do limite (25% em `demais_cargas`). Acima de 35%, a severidade é `critico`.

Os limites são medidos contra o gabarito na consulta `q6_validacao`. A referência é fixa, então não aprende o defeito como normal.

**IA responsável:** o alerta é regra explicável, sugere verificação e não afirma defeito. O técnico ou o morador decide.

## 5. LGPD

- Na Ouro não há nome, CPF, e-mail, telefone ou CEP completo.
- `titular_hash = sha2(sal + cpf, 256)`. O sal fica no Databricks Secrets ou em widget, fora do código.
- Máscaras: `Mo***`, `m***@***`, CEP com 5 dígitos.
- O repositório não leva chaves, senhas nem dados reais. Todas as identidades são fictícias.
- `.gitignore` exclui `chegada/`, `.env` e checkpoints.

## 6. Contrato com o MVP

O notebook `databricks/05_exportar_mvp.py` grava `mvp_*` como JSON e a tela `mvp/index.html` lê esses arquivos. Campos em `docs/contrato_mvp.md`. A página funciona sem servidor de dados e guarda o último arquivo, o que atende ao princípio de resiliência a conexões ruins.

## 7. Estratégia de validação

| O quê | Como | Limitação |
|---|---|---|
| Coleta e simulador | `pytest` e execução real nas APIs | Nenhuma |
| DAG | Execução em Airflow 3 no Docker | Depende de o Docker subir |
| Prata, Ouro e SQL | Spark + Delta local, com os mesmos notebooks | Nenhuma de lógica |
| Bronze (Auto Loader) | Só no Databricks | Fica para o autor rodar |
| Detecção | Precisão e cobertura contra o gabarito | Dados simulados |

## 8. Fora de escopo

MongoDB (opcional e sem ponto extra), Tarifa Social, impostos na tarifa, desagregação por modelo de aprendizado e as entregas das outras seis disciplinas.

## 9. Pendências do autor

Requerem conta ou decisão humana: nome e RM dos integrantes, execução no Databricks com os prints, publicação no GitHub e montagem do PDF final.
