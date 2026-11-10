# SeguWatts Big Data: plano de implementação

**Goal:** entregar o componente Dados do SeguWatts, da coleta no Airflow até a tela do MVP, atendendo aos requisitos 01 a 09 da disciplina.

**Architecture:** Airflow coleta 4 fontes reais e gera 3 simuladas em `chegada/`; lotes sobem ao Volume; notebooks fazem Bronze, Prata e Ouro; a Ouro exporta JSON para a tela.

**Tech Stack:** Python 3.12 (biblioteca padrão + requests), Airflow 3, Databricks Free Edition, PySpark 4 + Delta para validação local, HTML/CSS/JS sem dependências.

**Spec:** `docs/superpowers/specs/2026-11-09-seguwatts-bigdata-design.md`

## Global Constraints

- Semente fixa: repetir um dia gera arquivo idêntico.
- SQL compatível com modo ANSI (`try_cast`, `try_divide`).
- Sem `.cache()` nos notebooks, que rodam em serverless.
- Sem chaves, senhas ou dados pessoais reais no repositório.
- Arquivos no formato `chegada/<fonte>/<fonte>_<AAAA-MM-DD>.json`, um JSON por linha.
- O dia coletado é o dia anterior à execução, no fuso de São Paulo.

## Task 1: Configuração, fontes e simulador

**Files:** `dags/seguwatts/{__init__,config,arquivos,fontes,simulador}.py`, `tests/test_{fontes,simulador}.py`, `requirements*.txt`, `.gitignore`

**Interfaces produzidas:**
- `arquivos.caminho(chegada, fonte, dia) -> Path`, `arquivos.escrever_ndjson(path, registros)`, `arquivos.ler_ndjson(path) -> list[dict]`
- `fontes.coletar_{clima,tarifas,bandeiras,municipios}(dia, chegada, http=requests) -> Path`
- `simulador.gerar_residencias(n, seed) -> list[dict]`, `simulador.gerar_falhas(residencias, seed) -> list[dict]`
- `simulador.gerar_cadastro(dia, chegada) -> Path`, `gerar_sensores(dia, chegada) -> Path`, `gerar_contas(dia, chegada) -> Path`
- `fontes.conferir_chegada(dia, chegada) -> dict`

**Aceite:** testes cobrem determinismo, sujeira injetada, soma `geral` igual à soma dos aparelhos, conta só com ciclo completo e falha da conferência sem arquivo.

## Task 2: DAG e ambiente Airflow

**Files:** `dags/seguwatts_coleta.py`, `docker-compose.yml`, `scripts/carga_historica.py`, `scripts/montar_lotes.py`

**Aceite:** `airflow dags list-import-errors` vazio, execuções concluídas com tarefas em paralelo e arquivos em `chegada/`.

## Task 3: Lakehouse

**Files:** `databricks/01_bronze.py`, `02_prata.py`, `03_ouro.py`, `04_consultas.sql`, `05_exportar_mvp.py`, `scripts/validar_local.py`

**Aceite local:**
- Prata registra total barrado por regra e uma segunda execução não altera contagens;
- soma de `fato_consumo_hora` por casa bate com `geral` válido;
- Ouro não contém nome, CPF, e-mail nem telefone;
- as consultas q1 a q6 retornam linhas;
- precisão e cobertura medidas contra o gabarito.

## Task 4: Tela, contrato e documentação

**Files:** `mvp/index.html`, `mvp/dados/seguwatts_mvp.json`, `docs/contrato_mvp.md`, `docs/roteiro_relatorio.md`, `README.md`, `amostras/`

**Aceite:** a tela abre, troca de residência, mostra gráfico e alertas e funciona por teclado; README reproduz o pipeline; `amostras/` tem um pouco de cada fonte.

## Fora do alcance desta execução

Rodar no Databricks, tirar os prints dele, publicar no GitHub e preencher nome e RM dos integrantes.
