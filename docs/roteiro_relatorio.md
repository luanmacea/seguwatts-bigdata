# Roteiro do relatório em PDF

Segue a ordem exigida no enunciado. Os resultados abaixo vêm da validação local com Spark e Delta. **Substitua pelos números do Databricks**, que são os que valem na entrega.

## 01. Integrantes

Nome completo e RM de todos. A falta de um integrante tira 0,5 ponto.

## 02. Produto e perguntas

O SeguWatts mostra ao morador quando e onde a casa gasta energia, quanto isso custa e qual aparelho pode estar com defeito, antes que o problema apareça na conta.

Perguntas: as cinco da seção 1 de `docs/superpowers/specs/2026-11-09-seguwatts-bigdata-design.md`.

## 03. Diagrama do fluxo

Use o diagrama da seção Arquitetura do `README.md`.

## 04. Tabela de fontes

| Fonte | Origem | Formato | Frequência | Tipo |
|---|---|---|---|---|
| clima | Open-Meteo Archive API | JSON | Diária | Real |
| tarifas | ANEEL, dados abertos | JSON | Diária | Real |
| bandeiras | ANEEL, dados abertos | JSON | Diária | Real |
| municipios | IBGE Localidades | JSON | Diária | Real |
| sensores | Simulador documentado | JSON | Diária | Simulada |
| contas | Simulador documentado | JSON | Diária | Simulada |
| cadastro | Simulador documentado | JSON aninhado | Diária | Simulada |

## 05. Requisitos 01 a 09

| Req. | O que mostrar | Onde está |
|---|---|---|
| 01 | Tabela de fontes | Seção 04 |
| 02 | DAG agendado e arquivos com data em `chegada/` | `dags/seguwatts_coleta.py` |
| 03 | 5 tarefas em paralelo, `conferir_chegada`, Variables | Graph do Airflow e `dags/seguwatts_coleta.py` |
| 04 | Dois lotes no Volume | Pastas `lotes/lote_1` e `lotes/lote_2` |
| 05 | Bronze incremental | `databricks/01_bronze.py` e contagens da seção 07 |
| 06 | MERGE e regras com contagem | `databricks/02_prata.py` e tabela `prata_qualidade` |
| 07 | Fato, dimensões e 5 consultas | `databricks/03_ouro.py` e `04_consultas.sql` |
| 08 | Hash, máscara, tabela usada e tela | `dim_residencia`, `docs/contrato_mvp.md`, `mvp/index.html` |
| 09 | README | `README.md` |

## 06. Prints do Airflow

Com `docker compose up -d`, abra `http://localhost:8080`, entre em `seguwatts_coleta` e capture as visões Graph e Grid. Legende cada print.

## 07. Contagens da Bronze

Preencha ao rodar `01_bronze`:

| Momento | `bronze_sensores` | `bronze_clima` | `bronze_contas` |
|---|---|---|---|
| Depois do lote 1 | | | |
| Depois do lote 2 | | | |
| 2ª execução sem lote novo (deve repetir a linha anterior) | | | |

## Regras de qualidade (validação local)

| Tabela | Regra | Total barrado |
|---|---|---|
| leituras | L1: kWh ausente ou não numérico | 4.863 |
| leituras | L2: kWh fora de 0 a 15 | 3.345 |
| leituras | L3: residência não cadastrada | 190 |
| leituras | L4: ponto não cadastrado | 190 |
| leituras | Leituras repetidas | 3.685 |
| contas | F1: kWh não positivo | 14 |
| contas | F2: datas invertidas | 16 |

Os valores podem variar um pouco, porque dependem de quantos dias estão carregados.

## 08. Consultas e interpretação

| Consulta | Resultado local | Interpretação |
|---|---|---|
| q1: dia e horário | Pico às 19h em todos os dias da semana; maior média na quarta, 0,84 kWh | O início da noite concentra banho, TV e iluminação; é o melhor horário para orientar mudanças |
| q2: mês a mês | De 7,55 kWh/dia em junho a 8,12 em novembro; bandeira amarela de maio a setembro | A variação mensal é pequena: o calor aumenta geladeira e ar-condicionado, e o frio aumenta o chuveiro. A bandeira de novembro ainda não foi publicada pela ANEEL |
| q3: aparelhos | Chuveiro 36,8% (estimado), geladeira 17,7% (medido) | O chuveiro é o maior peso, mas a geladeira é o maior consumo medido e um bom alvo de manutenção |
| q4: alertas | 20 geladeiras, 5 freezers e 5 casas em "demais aparelhos" com alerta; 16 aparelhos de ar sem base | Sem 30 dias de uso, o aparelho não é avaliado, evitando alerta sem fundamento |
| q5: **impacto** | 30 casas, 799 kWh e R$ 621,80 por mês; R$ 20,73 por casa; 10,5% de redução | Agir sobre os avisos reduz em cerca de 10% o consumo das casas afetadas |

Validação da detecção (q6): 31 falhas detectadas, nenhum alarme falso e nenhuma perdida, em média 9,9 dias após o início. Das 32 injetadas, 1 não pôde ser avaliada por falta de 30 dias de uso. **Declare no relatório que esse resultado vale para dados simulados**, em que os defeitos são nítidos. Em uso real, a taxa tende a ser menor.

## 09. Conclusão e link

Resuma as decisões: sensor híbrido, fato sem contagem dupla, detecção explicável e LGPD na Ouro. Cite as limitações do `README.md` e inclua o link público do repositório.
