# Databricks notebook source
# MAGIC %md
# MAGIC # SeguWatts: Prata
# MAGIC Converte tipos, aplica regras de qualidade, conta o que foi barrado por cada
# MAGIC regra e faz MERGE pela chave. Rodar de novo não altera o resultado.

# COMMAND ----------

from datetime import datetime, timezone

dbutils.widgets.text("schema", "workspace.gs")
SCHEMA = dbutils.widgets.get("schema")


def t(nome):
    return f"{SCHEMA}.{nome}"


qualidade = []
spark.sql(
    f"""CREATE OR REPLACE TABLE {t('prata_rejeitos')}
        (tabela STRING, regra STRING, registro STRING) USING DELTA"""
)

# COMMAND ----------


def tratar(tabela, view_tipada, chaves, regras, colunas):
    """Classifica cada linha, registra os barrados, remove duplicatas e faz MERGE.

    regras: lista de (codigo, descricao, condicao_sql); vale a primeira que bater.
    A view tipada precisa da coluna _ordem: entre duplicatas, fica a maior.
    """
    caso = " ".join(f"WHEN {condicao} THEN '{codigo}'" for codigo, _, condicao in regras)
    expressao = f"CASE {caso} END" if regras else "CAST(NULL AS STRING)"
    estagio = t(f"stg_{tabela}")
    spark.sql(f"CREATE OR REPLACE TABLE {estagio} USING DELTA AS SELECT *, {expressao} AS _regra FROM {view_tipada}")

    barrados = {
        linha["_regra"]: linha["total"]
        for linha in spark.sql(
            f"SELECT _regra, count(*) AS total FROM {estagio} WHERE _regra IS NOT NULL GROUP BY _regra"
        ).collect()
    }
    for codigo, descricao, _ in regras:
        qualidade.append((tabela, codigo, descricao, int(barrados.get(codigo, 0))))

    spark.sql(
        f"""INSERT INTO {t('prata_rejeitos')}
            SELECT '{tabela}', _regra, to_json(struct(*)) FROM {estagio} WHERE _regra IS NOT NULL"""
    )

    particao = ", ".join(chaves)
    spark.sql(
        f"""CREATE OR REPLACE TEMP VIEW lote_prata_{tabela} AS
            SELECT {', '.join(colunas)} FROM (
              SELECT *, row_number() OVER (PARTITION BY {particao} ORDER BY _ordem DESC) AS _posicao
              FROM {estagio} WHERE _regra IS NULL
            ) WHERE _posicao = 1"""
    )
    validas = spark.sql(f"SELECT count(*) AS c FROM {estagio} WHERE _regra IS NULL").first()["c"]
    unicas = spark.sql(f"SELECT count(*) AS c FROM lote_prata_{tabela}").first()["c"]
    qualidade.append((tabela, "DUP", "Linhas repetidas pela chave (mantida a mais recente)", int(validas - unicas)))

    alvo = t(f"prata_{tabela}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {alvo} USING DELTA AS SELECT * FROM lote_prata_{tabela} WHERE 1 = 0")
    condicao = " AND ".join(f"alvo.{c} <=> novo.{c}" for c in chaves)
    spark.sql(
        f"""MERGE INTO {alvo} AS alvo USING lote_prata_{tabela} AS novo ON {condicao}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *"""
    )
    spark.sql(f"DROP TABLE {estagio}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Referências: municípios, tarifas e bandeiras

# COMMAND ----------

spark.sql(
    f"""CREATE OR REPLACE TEMP VIEW tipado_municipios AS
        SELECT try_cast(id AS INT) AS municipio_ibge, nome AS municipio, uf, data_coleta AS _ordem
        FROM {t('bronze_municipios')}"""
)
tratar(
    "municipios",
    "tipado_municipios",
    ["municipio_ibge"],
    [("M1", "Código IBGE ausente ou inválido", "municipio_ibge IS NULL")],
    ["municipio_ibge", "municipio", "uf"],
)

spark.sql(
    f"""CREATE OR REPLACE TEMP VIEW tipado_tarifas AS
        SELECT SigAgente AS distribuidora,
               try_cast(DatInicioVigencia AS DATE) AS inicio_vigencia,
               try_cast(DatFimVigencia AS DATE) AS fim_vigencia,
               try_cast(replace(VlrTUSD, ',', '.') AS DOUBLE) AS tusd_rs_mwh,
               try_cast(replace(VlrTE, ',', '.') AS DOUBLE) AS te_rs_mwh,
               data_coleta AS _ordem
        FROM {t('bronze_tarifas')}
        WHERE DscDetalhe = 'Não se aplica'"""  # SCEE é a tarifa de quem compensa geração própria
)
tratar(
    "tarifas",
    "tipado_tarifas",
    ["distribuidora", "inicio_vigencia"],
    [
        ("T1", "Vigência ausente ou invertida", "inicio_vigencia IS NULL OR fim_vigencia < inicio_vigencia"),
        ("T2", "Tarifa ausente ou não positiva", "tusd_rs_mwh IS NULL OR te_rs_mwh IS NULL OR tusd_rs_mwh + te_rs_mwh <= 0"),
    ],
    ["distribuidora", "inicio_vigencia", "fim_vigencia", "tusd_rs_mwh", "te_rs_mwh"],
)

spark.sql(
    f"""CREATE OR REPLACE TEMP VIEW tipado_bandeiras AS
        SELECT try_cast(DatCompetencia AS DATE) AS competencia,
               NomBandeiraAcionada AS bandeira,
               try_cast(replace(VlrAdicionalBandeira, ',', '.') AS DOUBLE) AS adicional_rs_mwh,
               data_coleta AS _ordem
        FROM {t('bronze_bandeiras')}"""
)
tratar(
    "bandeiras",
    "tipado_bandeiras",
    ["competencia"],
    [
        ("B1", "Competência inválida", "competencia IS NULL"),
        ("B2", "Adicional ausente ou negativo", "adicional_rs_mwh IS NULL OR adicional_rs_mwh < 0"),
    ],
    ["competencia", "bandeira", "adicional_rs_mwh"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clima
# MAGIC O Open-Meteo devolve listas paralelas: `arrays_zip` e `explode` geram uma linha por hora.

# COMMAND ----------

spark.sql(
    f"""CREATE OR REPLACE TEMP VIEW tipado_clima AS
        SELECT municipio_ibge,
               try_cast(h.time AS TIMESTAMP) AS instante,
               try_cast(h.temperature_2m AS DOUBLE) AS temperatura_c,
               try_cast(h.precipitation AS DOUBLE) AS precipitacao_mm,
               data_coleta AS _ordem
        FROM (
          SELECT municipio_ibge, data_coleta,
                 explode(arrays_zip(hourly.time, hourly.temperature_2m, hourly.precipitation)) AS h
          FROM {t('bronze_clima')}
        )"""
)
tratar(
    "clima",
    "tipado_clima",
    ["municipio_ibge", "instante"],
    [
        ("C1", "Instante inválido", "instante IS NULL"),
        ("C2", "Temperatura ausente ou fora de -10 a 50 °C", "temperatura_c IS NULL OR temperatura_c NOT BETWEEN -10 AND 50"),
    ],
    ["municipio_ibge", "instante", "temperatura_c", "precipitacao_mm"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cadastro

# COMMAND ----------

spark.sql(
    f"""CREATE OR REPLACE TEMP VIEW tipado_residencias AS
        SELECT residencia_id, nome_titular, cpf, email, telefone, cep,
               try_cast(municipio_ibge AS INT) AS municipio_ibge, distribuidora,
               try_cast(moradores AS INT) AS moradores,
               try_cast(area_m2 AS INT) AS area_m2,
               try_cast(dia_leitura AS INT) AS dia_leitura,
               try_cast(data_instalacao AS DATE) AS data_instalacao,
               data_coleta AS _ordem
        FROM {t('bronze_cadastro')}"""
)
tratar(
    "residencias",
    "tipado_residencias",
    ["residencia_id"],
    [("D1", "Residência sem município", "municipio_ibge IS NULL")],
    ["residencia_id", "nome_titular", "cpf", "email", "telefone", "cep", "municipio_ibge", "distribuidora",
     "moradores", "area_m2", "dia_leitura", "data_instalacao"],
)

spark.sql(
    f"""CREATE OR REPLACE TEMP VIEW tipado_aparelhos AS
        SELECT a.aparelho_id, residencia_id, a.tipo,
               try_cast(a.potencia_w AS INT) AS potencia_w,
               try_cast(a.horas_dia_declaradas AS DOUBLE) AS horas_dia_declaradas,
               try_cast(a.monitorado AS BOOLEAN) AS monitorado,
               try_cast(a.idade_anos AS INT) AS idade_anos,
               data_coleta AS _ordem
        FROM (SELECT residencia_id, data_coleta, explode(aparelhos) AS a FROM {t('bronze_cadastro')})"""
)
tratar(
    "aparelhos",
    "tipado_aparelhos",
    ["aparelho_id"],
    [("A1", "Potência ausente ou não positiva", "potencia_w IS NULL OR potencia_w <= 0")],
    ["aparelho_id", "residencia_id", "tipo", "potencia_w", "horas_dia_declaradas", "monitorado", "idade_anos"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leituras dos sensores

# COMMAND ----------

spark.sql(
    f"""CREATE OR REPLACE TEMP VIEW tipado_leituras AS
        SELECT b.residencia_id, b.ponto,
               try_cast(b.instante AS TIMESTAMP) AS instante,
               try_cast(replace(b.kwh, ',', '.') AS DOUBLE) AS kwh,
               b.arquivo_origem,
               r.residencia_id IS NOT NULL AS _residencia_ok,
               (b.ponto = 'geral' OR a.aparelho_id IS NOT NULL) AS _ponto_ok,
               b.arquivo_origem AS _ordem
        FROM {t('bronze_sensores')} b
        LEFT JOIN {t('prata_residencias')} r ON r.residencia_id = b.residencia_id
        LEFT JOIN {t('prata_aparelhos')} a ON a.aparelho_id = b.ponto"""
)
tratar(
    "leituras",
    "tipado_leituras",
    ["residencia_id", "ponto", "instante"],
    [
        ("L1", "kWh ausente ou não numérico", "kwh IS NULL"),
        ("L2", "kWh fora da faixa de 0 a 15 por hora", "kwh < 0 OR kwh > 15"),
        ("L3", "Residência não cadastrada", "NOT _residencia_ok"),
        ("L4", "Ponto de medição não cadastrado", "NOT _ponto_ok"),
        ("L5", "Instante inválido", "instante IS NULL"),
    ],
    ["residencia_id", "ponto", "instante", "kwh", "arquivo_origem"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contas de luz

# COMMAND ----------

spark.sql(
    f"""CREATE OR REPLACE TEMP VIEW tipado_contas AS
        SELECT conta_id, residencia_id,
               try_cast(data_leitura_anterior AS DATE) AS data_leitura_anterior,
               try_cast(data_leitura_atual AS DATE) AS data_leitura_atual,
               try_cast(kwh_faturado AS DOUBLE) AS kwh_faturado,
               data_coleta AS _ordem
        FROM {t('bronze_contas')}"""
)
tratar(
    "contas",
    "tipado_contas",
    ["conta_id"],
    [
        ("F1", "kWh faturado ausente ou não positivo", "kwh_faturado IS NULL OR kwh_faturado <= 0"),
        ("F2", "Datas de leitura invertidas", "data_leitura_atual <= data_leitura_anterior"),
    ],
    ["conta_id", "residencia_id", "data_leitura_anterior", "data_leitura_atual", "kwh_faturado"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado das regras de qualidade

# COMMAND ----------

agora = datetime.now(timezone.utc).replace(tzinfo=None)
(
    spark.createDataFrame(
        [(*linha, agora) for linha in qualidade],
        "tabela STRING, regra STRING, descricao STRING, total_barrado BIGINT, executado_em TIMESTAMP",
    )
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(t("prata_qualidade"))
)
display(spark.table(t("prata_qualidade")).orderBy("tabela", "regra"))
