# Databricks notebook source
# MAGIC %md
# MAGIC # SeguWatts: Ouro
# MAGIC Modelo dimensional, proteção LGPD, detecção de defeitos e tabelas consumidas pelo MVP.
# MAGIC
# MAGIC O sal do hash nunca fica no código. Defina o secret `seguwatts/lgpd_sal` ou preencha o widget.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("schema", "workspace.gs")
dbutils.widgets.text("base", "/Volumes/workspace/gs/chegada")
dbutils.widgets.text("lgpd_sal", "")
SCHEMA = dbutils.widgets.get("schema")
BASE = dbutils.widgets.get("base")


def t(nome):
    return f"{SCHEMA}.{nome}"


def obter_sal():
    try:
        return dbutils.secrets.get("seguwatts", "lgpd_sal")
    except Exception:
        sal = dbutils.widgets.get("lgpd_sal")
        if not sal:
            raise ValueError("Defina o secret seguwatts/lgpd_sal ou preencha o widget lgpd_sal.") from None
        return sal


SAL = obter_sal()
LIMITE_EXCESSO = 0.20
LIMITE_EXCESSO_DEMAIS = 0.25
LIMITE_CRITICO = 0.35
DIAS_REFERENCIA = 30

ROTULOS = {
    "geladeira": "Geladeira", "freezer": "Freezer", "ar_condicionado": "Ar-condicionado",
    "chuveiro": "Chuveiro", "tv": "TV", "iluminacao": "Iluminação", "maquina_lavar": "Máquina de lavar",
    "computador": "Computador", "micro_ondas": "Micro-ondas", "ferro": "Ferro de passar",
    "standby": "Aparelhos em espera", "demais_cargas": "Demais aparelhos",
}
rotulo_sql = "CASE tipo " + " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in ROTULOS.items()) + " END"


def salgado(coluna):
    return F.sha2(F.concat(F.lit(SAL), F.col(coluna)), 256)


def surrogate(coluna):
    return F.xxhash64(F.lit(SAL), F.col(coluna))


def salvar(view, tabela):
    spark.sql(f"CREATE OR REPLACE TABLE {t(tabela)} USING DELTA AS SELECT * FROM {view}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensões
# MAGIC `dim_residencia` guarda apenas hash e dados mascarados: nome, CPF, e-mail e telefone não passam para a Ouro.

# COMMAND ----------

spark.sql(
    f"""CREATE OR REPLACE TEMP VIEW v_bandeira_mes AS
        SELECT competencia, bandeira, adicional_rs_mwh FROM {t('prata_bandeiras')}"""
)
spark.sql(
    f"""CREATE OR REPLACE TEMP VIEW v_dim_data AS
        SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS sk_data,
               d AS data,
               dayofweek(d) AS dia_semana_num,
               element_at(array('Domingo','Segunda','Terça','Quarta','Quinta','Sexta','Sábado'), dayofweek(d)) AS dia_semana,
               dayofweek(d) IN (1, 7) AS fim_de_semana,
               date_format(d, 'yyyy-MM') AS ano_mes,
               month(d) AS mes,
               year(d) AS ano,
               coalesce(b.bandeira, 'Não publicada') AS bandeira,
               b.adicional_rs_mwh
        FROM (
          SELECT explode(sequence(CAST(min(instante) AS DATE), CAST(max(instante) AS DATE), INTERVAL 1 DAY)) AS d
          FROM {t('prata_leituras')}
        )
        LEFT JOIN v_bandeira_mes b ON b.competencia = trunc(d, 'MM')"""
)
salvar("v_dim_data", "dim_data")

spark.sql(
    f"""CREATE OR REPLACE TEMP VIEW v_dim_localidade AS
        SELECT DISTINCT r.municipio_ibge AS sk_localidade, m.municipio, m.uf, r.distribuidora
        FROM {t('prata_residencias')} r
        JOIN {t('prata_municipios')} m USING (municipio_ibge)"""
)
salvar("v_dim_localidade", "dim_localidade")

residencias = spark.table(t("prata_residencias")).select(
    "residencia_id",
    surrogate("residencia_id").alias("sk_residencia"),
    salgado("residencia_id").alias("residencia_hash"),
    salgado("cpf").alias("titular_hash"),
    F.concat(F.substring("nome_titular", 1, 2), F.lit("***")).alias("titular_mascarado"),
    F.concat(F.substring("email", 1, 1), F.lit("***@***")).alias("email_mascarado"),
    F.substring("cep", 1, 5).alias("cep_prefixo"),
    F.col("municipio_ibge").alias("sk_localidade"),
    "moradores",
    "area_m2",
    "dia_leitura",
    "data_instalacao",
)
residencias.createOrReplaceTempView("v_mapa_residencia")  # temporária: tem o id original
spark.sql("CREATE OR REPLACE TEMP VIEW v_dim_residencia AS SELECT * EXCEPT (residencia_id) FROM v_mapa_residencia")
salvar("v_dim_residencia", "dim_residencia")

aparelhos = (
    spark.table(t("prata_aparelhos"))
    .select("aparelho_id", "residencia_id", "tipo", "potencia_w", "horas_dia_declaradas", "monitorado", "idade_anos")
    .unionByName(
        spark.table(t("prata_residencias")).select(
            F.concat(F.col("residencia_id"), F.lit("-DEMAIS")).alias("aparelho_id"),
            "residencia_id",
            F.lit("demais_cargas").alias("tipo"),
            F.lit(None).cast("int").alias("potencia_w"),
            F.lit(None).cast("double").alias("horas_dia_declaradas"),
            F.lit(False).alias("monitorado"),
            F.lit(None).cast("int").alias("idade_anos"),
        )
    )
    .withColumn("sk_aparelho", surrogate("aparelho_id"))
    .withColumn("sk_residencia", surrogate("residencia_id"))
)
aparelhos.createOrReplaceTempView("v_mapa_aparelho")  # temporária: tem os ids originais
spark.sql(
    f"""CREATE OR REPLACE TEMP VIEW v_dim_aparelho AS
        SELECT sk_aparelho, sk_residencia, tipo, {rotulo_sql} AS rotulo, potencia_w, horas_dia_declaradas, idade_anos,
               monitorado,
               CASE WHEN monitorado THEN 'medido' WHEN tipo = 'demais_cargas' THEN 'residual' ELSE 'estimado' END AS origem_medicao
        FROM v_mapa_aparelho"""
)
salvar("v_dim_aparelho", "dim_aparelho")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fato horário
# MAGIC Guarda tomadas e `demais_cargas` (geral menos tomadas), nunca o sensor geral junto.
# MAGIC Assim, somar as linhas dá o consumo da casa sem contar nada duas vezes.

# COMMAND ----------

spark.sql(
    f"""CREATE OR REPLACE TEMP VIEW v_tarifa_dia AS
        SELECT data, distribuidora, tusd_rs_mwh, te_rs_mwh, adicional_rs_mwh, rs_kwh FROM (
          SELECT d.data, t.distribuidora, t.tusd_rs_mwh, t.te_rs_mwh, coalesce(d.adicional_rs_mwh, 0) AS adicional_rs_mwh,
                 (t.tusd_rs_mwh + t.te_rs_mwh + coalesce(d.adicional_rs_mwh, 0)) / 1000 AS rs_kwh,
                 row_number() OVER (PARTITION BY d.data, t.distribuidora ORDER BY t.inicio_vigencia DESC) AS posicao
          FROM dim_data_local d
          JOIN {t('prata_tarifas')} t ON d.data BETWEEN t.inicio_vigencia AND t.fim_vigencia
        ) WHERE posicao = 1""".replace("dim_data_local", t("dim_data"))
)

spark.sql(
    f"""CREATE OR REPLACE TEMP VIEW v_leitura_aditiva AS
        WITH tomadas AS (
          SELECT residencia_id, instante, sum(kwh) AS kwh FROM {t('prata_leituras')} WHERE ponto <> 'geral'
          GROUP BY residencia_id, instante
        )
        SELECT residencia_id, ponto, instante, kwh FROM {t('prata_leituras')} WHERE ponto <> 'geral'
        UNION ALL
        SELECT g.residencia_id, concat(g.residencia_id, '-DEMAIS') AS ponto, g.instante,
               greatest(g.kwh - coalesce(t.kwh, 0), 0) AS kwh
        FROM {t('prata_leituras')} g
        LEFT JOIN tomadas t ON t.residencia_id = g.residencia_id AND t.instante = g.instante
        WHERE g.ponto = 'geral'"""
)

spark.sql(
    f"""CREATE OR REPLACE TEMP VIEW v_fato_hora AS
        SELECT r.sk_residencia, a.sk_aparelho,
               CAST(date_format(l.instante, 'yyyyMMdd') AS INT) AS sk_data,
               r.sk_localidade,
               l.instante,
               hour(l.instante) AS hora,
               l.kwh,
               l.kwh * td.rs_kwh AS custo_rs,
               c.temperatura_c,
               CASE WHEN a.monitorado THEN 'medido' ELSE 'residual' END AS origem_medicao
        FROM v_leitura_aditiva l
        JOIN v_mapa_residencia r ON r.residencia_id = l.residencia_id
        JOIN v_mapa_aparelho a ON a.aparelho_id = l.ponto
        JOIN {t('dim_localidade')} loc ON loc.sk_localidade = r.sk_localidade
        LEFT JOIN v_tarifa_dia td ON td.data = CAST(l.instante AS DATE) AND td.distribuidora = loc.distribuidora
        LEFT JOIN {t('prata_clima')} c ON c.municipio_ibge = r.sk_localidade AND c.instante = l.instante"""
)
salvar("v_fato_hora", "fato_consumo_hora")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rateio por ciclo da conta
# MAGIC Aparelhos com tomada usam o valor medido. O restante da conta é dividido entre os
# MAGIC aparelhos sem sensor, na proporção de potência x horas declaradas.

# COMMAND ----------

spark.sql(
    f"""CREATE OR REPLACE TEMP VIEW v_rateio AS
        WITH ciclos AS (
          SELECT c.conta_id, r.sk_residencia, c.data_leitura_anterior, c.data_leitura_atual, c.kwh_faturado
          FROM {t('prata_contas')} c JOIN v_mapa_residencia r USING (residencia_id)
        ),
        no_ciclo AS (
          SELECT c.conta_id, f.sk_aparelho, sum(f.kwh) AS kwh, sum(f.custo_rs) AS custo
          FROM ciclos c
          JOIN {t('fato_consumo_hora')} f
            ON f.sk_residencia = c.sk_residencia
           AND CAST(f.instante AS DATE) > c.data_leitura_anterior
           AND CAST(f.instante AS DATE) <= c.data_leitura_atual
          GROUP BY c.conta_id, f.sk_aparelho
        ),
        custo_medio AS (
          SELECT conta_id, try_divide(sum(custo), sum(kwh)) AS rs_kwh FROM no_ciclo GROUP BY conta_id
        ),
        medido AS (
          SELECT n.conta_id, n.sk_aparelho, n.kwh, 'medido' AS origem_medicao
          FROM no_ciclo n JOIN {t('dim_aparelho')} a USING (sk_aparelho) WHERE a.monitorado
        ),
        soma_medido AS (SELECT conta_id, sum(kwh) AS kwh FROM medido GROUP BY conta_id),
        pesos AS (
          SELECT c.conta_id, a.sk_aparelho, a.potencia_w * a.horas_dia_declaradas AS peso,
                 sum(a.potencia_w * a.horas_dia_declaradas) OVER (PARTITION BY c.conta_id) AS peso_total
          FROM ciclos c JOIN {t('dim_aparelho')} a USING (sk_residencia)
          WHERE NOT a.monitorado AND a.tipo <> 'demais_cargas'
        ),
        estimado AS (
          SELECT p.conta_id, p.sk_aparelho,
                 greatest(c.kwh_faturado - coalesce(s.kwh, 0), 0) * try_divide(p.peso, p.peso_total) AS kwh,
                 'estimado' AS origem_medicao
          FROM pesos p
          JOIN ciclos c USING (conta_id)
          LEFT JOIN soma_medido s USING (conta_id)
        ),
        todos AS (SELECT * FROM medido UNION ALL SELECT * FROM estimado)
        SELECT c.sk_residencia, t.sk_aparelho,
               CAST(date_format(c.data_leitura_atual, 'yyyyMMdd') AS INT) AS sk_data,
               c.conta_id AS ciclo_ref,
               c.data_leitura_anterior, c.data_leitura_atual,
               t.kwh, t.kwh * m.rs_kwh AS custo_rs, t.origem_medicao
        FROM todos t JOIN ciclos c USING (conta_id) JOIN custo_medio m USING (conta_id)"""
)
# ciclo_ref contém o id da residência: troca por um hash antes de salvar.
spark.table("v_rateio").withColumn("ciclo_ref", salgado("ciclo_ref")).createOrReplaceTempView("v_rateio_seguro")
salvar("v_rateio_seguro", "fato_rateio_ciclo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detecção de defeitos
# MAGIC Cada aparelho é comparado com ele mesmo nos 30 primeiros dias de uso, descontando o efeito
# MAGIC da temperatura. O alerta sugere verificação e não afirma que há defeito.

# COMMAND ----------

spark.sql(
    f"""CREATE OR REPLACE TEMP VIEW v_diagnostico AS
        WITH dia AS (
          SELECT f.sk_aparelho, f.sk_residencia, a.tipo, d.data,
                 sum(f.kwh) AS kwh_dia,
                 count_if(f.kwh > coalesce(a.potencia_w, 0) / 1000 * 0.1) AS horas_ligado,
                 sum(CASE WHEN f.kwh > coalesce(a.potencia_w, 0) / 1000 * 0.1 THEN f.kwh END) AS kwh_ligado,
                 avg(f.temperatura_c) AS temperatura_media
          FROM {t('fato_consumo_hora')} f
          JOIN {t('dim_aparelho')} a USING (sk_aparelho)
          JOIN {t('dim_data')} d USING (sk_data)
          WHERE a.monitorado OR a.tipo = 'demais_cargas'
          GROUP BY f.sk_aparelho, f.sk_residencia, a.tipo, d.data
        ),
        uso AS (
          SELECT *, try_divide(kwh_ligado, horas_ligado) AS potencia_media,
                 row_number() OVER (PARTITION BY sk_aparelho ORDER BY data) AS n_uso
          FROM dia WHERE horas_ligado >= 1 AND temperatura_media IS NOT NULL
        ),
        base AS (
          SELECT sk_aparelho, count(*) AS dias_base,
                 regr_intercept(potencia_media, temperatura_media) AS intercepto,
                 regr_slope(potencia_media, temperatura_media) AS inclinacao,
                 avg(potencia_media) AS potencia_base,
                 min(temperatura_media) AS temp_min, max(temperatura_media) AS temp_max
          FROM uso WHERE n_uso <= {DIAS_REFERENCIA} GROUP BY sk_aparelho
        ),
        comparado AS (
          SELECT u.*, b.dias_base, b.potencia_base,
                 greatest(
                   coalesce(b.intercepto + b.inclinacao * least(greatest(u.temperatura_media, b.temp_min), b.temp_max), b.potencia_base),
                   b.potencia_base * 0.5
                 ) AS potencia_esperada
          FROM uso u JOIN base b USING (sk_aparelho)
          WHERE u.n_uso > {DIAS_REFERENCIA} AND b.dias_base = {DIAS_REFERENCIA}
        ),
        excesso AS (SELECT *, try_divide(potencia_media, potencia_esperada) - 1 AS excesso FROM comparado),
        limite AS (
          SELECT *, CASE WHEN tipo = 'demais_cargas' THEN {LIMITE_EXCESSO_DEMAIS} ELSE {LIMITE_EXCESSO} END AS limite FROM excesso
        ),
        janela AS (
          SELECT *,
                 avg(excesso) OVER w AS excesso_7d,
                 sum(CASE WHEN excesso >= limite THEN 1 ELSE 0 END) OVER w AS dias_acima,
                 count(*) OVER w AS dias_janela
          FROM limite
          WINDOW w AS (PARTITION BY sk_aparelho ORDER BY data ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
        )
        SELECT sk_aparelho, sk_residencia, tipo, data, kwh_dia, horas_ligado, temperatura_media,
               potencia_media, potencia_esperada, excesso, excesso_7d, dias_acima,
               (dias_janela = 7 AND excesso_7d >= limite AND dias_acima >= 5) AS alerta
        FROM janela"""
)
salvar("v_diagnostico", "ouro_diagnostico_aparelho_dia")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação contra o gabarito do simulador
# MAGIC Mede se a detecção encontra as falhas injetadas, sem expor os ids originais.

# COMMAND ----------

gabarito = spark.read.json(f"{BASE}/gabarito/falhas.json")
gabarito.withColumn(
    "aparelho_id",
    F.when(F.col("ponto") == "demais_cargas", F.concat("residencia_id", F.lit("-DEMAIS"))).otherwise(F.col("ponto")),
).createOrReplaceTempView("v_gabarito")

spark.sql(
    f"""CREATE OR REPLACE TEMP VIEW v_validacao AS
        WITH avaliaveis AS (SELECT DISTINCT sk_aparelho FROM {t('ouro_diagnostico_aparelho_dia')}),
        falhas AS (
          SELECT a.sk_aparelho, g.tipo_falha, to_date(g.inicio) AS inicio, CAST(g.intensidade AS DOUBLE) AS intensidade
          FROM v_gabarito g JOIN v_mapa_aparelho a USING (aparelho_id)
        ),
        alertas AS (
          SELECT sk_aparelho, min(data) AS primeiro_alerta FROM {t('ouro_diagnostico_aparelho_dia')}
          WHERE alerta GROUP BY sk_aparelho
        ),
        apos_inicio AS (
          SELECT d.sk_aparelho, min(d.data) AS deteccao
          FROM {t('ouro_diagnostico_aparelho_dia')} d JOIN falhas f USING (sk_aparelho)
          WHERE d.alerta AND d.data >= f.inicio GROUP BY d.sk_aparelho
        )
        SELECT av.sk_aparelho, ap.tipo, f.tipo_falha, f.inicio, f.intensidade,
               coalesce(ai.deteccao, al.primeiro_alerta) AS deteccao,
               datediff(ai.deteccao, f.inicio) AS dias_ate_detectar,
               CASE
                 WHEN f.sk_aparelho IS NOT NULL AND ai.deteccao IS NOT NULL THEN 'verdadeiro_positivo'
                 WHEN f.sk_aparelho IS NOT NULL THEN 'falso_negativo'
                 WHEN al.sk_aparelho IS NOT NULL THEN 'falso_positivo'
                 ELSE 'verdadeiro_negativo'
               END AS resultado
        FROM avaliaveis av
        JOIN {t('dim_aparelho')} ap USING (sk_aparelho)
        LEFT JOIN falhas f USING (sk_aparelho)
        LEFT JOIN alertas al USING (sk_aparelho)
        LEFT JOIN apos_inicio ai USING (sk_aparelho)"""
)
salvar("v_validacao", "ouro_validacao_deteccao")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabelas consumidas pelo MVP
# MAGIC Contrato em `docs/contrato_mvp.md`.

# COMMAND ----------

spark.sql(
    f"""CREATE OR REPLACE TEMP VIEW v_mvp_painel AS
        WITH mes AS (
          SELECT f.sk_residencia, d.ano_mes,
                 sum(f.kwh) AS kwh_total,
                 sum(f.custo_rs) AS custo_rs,
                 avg(f.temperatura_c) AS temperatura_media,
                 first(d.bandeira) AS bandeira,
                 count(DISTINCT d.data) AS dias_medidos
          FROM {t('fato_consumo_hora')} f JOIN {t('dim_data')} d USING (sk_data)
          GROUP BY f.sk_residencia, d.ano_mes
        )
        SELECT r.residencia_hash, m.ano_mes, round(m.kwh_total, 1) AS kwh_total, round(m.custo_rs, 2) AS custo_rs,
               round(m.temperatura_media, 1) AS temperatura_media, m.bandeira, m.dias_medidos,
               round(100 * try_divide(m.kwh_total - lag(m.kwh_total) OVER w, lag(m.kwh_total) OVER w), 1) AS variacao_kwh_pct
        FROM mes m JOIN {t('dim_residencia')} r USING (sk_residencia)
        WINDOW w AS (PARTITION BY m.sk_residencia ORDER BY m.ano_mes)"""
)
salvar("v_mvp_painel", "mvp_painel_mensal")

spark.sql(
    f"""CREATE OR REPLACE TEMP VIEW v_mvp_perfil AS
        WITH hora AS (
          SELECT f.sk_residencia, f.sk_data, f.hora, sum(f.kwh) AS kwh
          FROM {t('fato_consumo_hora')} f GROUP BY f.sk_residencia, f.sk_data, f.hora
        )
        SELECT r.residencia_hash, d.dia_semana_num, d.dia_semana, h.hora, round(avg(h.kwh), 3) AS kwh_medio
        FROM hora h
        JOIN {t('dim_data')} d USING (sk_data)
        JOIN {t('dim_residencia')} r USING (sk_residencia)
        GROUP BY r.residencia_hash, d.dia_semana_num, d.dia_semana, h.hora"""
)
salvar("v_mvp_perfil", "mvp_perfil_semana")

spark.sql(
    f"""CREATE OR REPLACE TEMP VIEW v_mvp_aparelhos AS
        WITH ultimo AS (
          SELECT sk_residencia, max(data_leitura_atual) AS data_leitura_atual
          FROM {t('fato_rateio_ciclo')} GROUP BY sk_residencia
        ),
        ciclo AS (
          SELECT f.*, sum(f.kwh) OVER (PARTITION BY f.sk_residencia) AS kwh_casa
          FROM {t('fato_rateio_ciclo')} f JOIN ultimo u USING (sk_residencia, data_leitura_atual)
        )
        SELECT r.residencia_hash, a.rotulo, a.tipo, c.origem_medicao, c.data_leitura_atual AS fim_ciclo,
               round(c.kwh, 1) AS kwh, round(c.custo_rs, 2) AS custo_rs,
               round(100 * try_divide(c.kwh, c.kwh_casa), 1) AS participacao_pct
        FROM ciclo c
        JOIN {t('dim_aparelho')} a USING (sk_aparelho)
        JOIN {t('dim_residencia')} r ON r.sk_residencia = c.sk_residencia"""
)
salvar("v_mvp_aparelhos", "mvp_aparelhos_ciclo")

spark.sql(
    f"""CREATE OR REPLACE TEMP VIEW v_mvp_alertas AS
        WITH ultimo_dia AS (SELECT max(data) AS data, max(CAST(date_format(data, 'yyyyMMdd') AS INT)) AS sk FROM {t('ouro_diagnostico_aparelho_dia')}),
        ultima AS (
          SELECT * FROM (
            SELECT d.*, row_number() OVER (PARTITION BY sk_aparelho ORDER BY data DESC) AS posicao
            FROM {t('ouro_diagnostico_aparelho_dia')} d
          ) WHERE posicao = 1
        ),
        ultimos7 AS (
          SELECT sk_aparelho, avg(kwh_dia - kwh_dia / (1 + greatest(excesso, 0))) AS kwh_excesso_dia FROM (
            SELECT *, row_number() OVER (PARTITION BY sk_aparelho ORDER BY data DESC) AS posicao
            FROM {t('ouro_diagnostico_aparelho_dia')}
          ) WHERE posicao <= 7 GROUP BY sk_aparelho
        ),
        primeira AS (
          SELECT sk_aparelho, min(data) AS desde FROM {t('ouro_diagnostico_aparelho_dia')} WHERE alerta GROUP BY sk_aparelho
        ),
        tarifa AS (
          SELECT distribuidora, rs_kwh FROM v_tarifa_dia WHERE data = (SELECT max(data) FROM v_tarifa_dia)
        ),
        status AS (
          SELECT a.sk_aparelho, a.sk_residencia, a.rotulo, a.tipo, a.origem_medicao,
                 u.alerta, u.excesso_7d, p.desde, s.kwh_excesso_dia,
                 CASE WHEN u.sk_aparelho IS NULL THEN 'sem_base'
                      WHEN u.alerta AND u.excesso_7d >= {LIMITE_CRITICO} THEN 'critico'
                      WHEN u.alerta THEN 'atencao'
                      ELSE 'normal' END AS status
          FROM {t('dim_aparelho')} a
          LEFT JOIN ultima u USING (sk_aparelho)
          LEFT JOIN ultimos7 s USING (sk_aparelho)
          LEFT JOIN primeira p USING (sk_aparelho)
          WHERE a.monitorado OR a.tipo = 'demais_cargas'
        )
        SELECT r.residencia_hash, s.rotulo, s.tipo, s.origem_medicao, s.status,
               CASE WHEN s.alerta THEN round(100 * s.excesso_7d, 0) END AS excesso_pct,
               CASE WHEN s.alerta THEN s.desde END AS desde,
               CASE WHEN s.alerta THEN round(s.kwh_excesso_dia * 30, 1) END AS kwh_excesso_mes,
               CASE WHEN s.alerta THEN round(s.kwh_excesso_dia * 30 * tar.rs_kwh, 2) END AS rs_excesso_mes,
               CASE
                 WHEN NOT coalesce(s.alerta, false) THEN NULL
                 WHEN s.tipo = 'demais_cargas' THEN concat('O consumo dos aparelhos sem sensor subiu cerca de ', CAST(round(100 * s.excesso_7d, 0) AS INT), '%. Veja se algo ficou ligado sem necessidade.')
                 ELSE concat(s.rotulo, ': consumo cerca de ', CAST(round(100 * s.excesso_7d, 0) AS INT), '% acima do próprio normal. Vale pedir uma verificação.')
               END AS mensagem
        FROM status s
        JOIN {t('dim_residencia')} r USING (sk_residencia)
        JOIN {t('dim_localidade')} l USING (sk_localidade)
        LEFT JOIN tarifa tar ON tar.distribuidora = l.distribuidora"""
)
salvar("v_mvp_alertas", "mvp_alertas_aparelho")

# COMMAND ----------

display(
    spark.sql(
        f"""SELECT tabela, linhas FROM (
              SELECT 'fato_consumo_hora' AS tabela, count(*) AS linhas FROM {t('fato_consumo_hora')}
              UNION ALL SELECT 'fato_rateio_ciclo', count(*) FROM {t('fato_rateio_ciclo')}
              UNION ALL SELECT 'dim_residencia', count(*) FROM {t('dim_residencia')}
              UNION ALL SELECT 'dim_aparelho', count(*) FROM {t('dim_aparelho')}
              UNION ALL SELECT 'dim_data', count(*) FROM {t('dim_data')}
              UNION ALL SELECT 'mvp_alertas_aparelho', count(*) FROM {t('mvp_alertas_aparelho')}
            )"""
    )
)
