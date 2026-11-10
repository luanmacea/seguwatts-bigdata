-- Databricks notebook source
-- q1: Em que dia da semana e horário as casas gastam mais?
SELECT dia_semana, hora, round(avg(kwh_medio), 3) AS kwh_medio_por_casa
FROM workspace.gs.mvp_perfil_semana
GROUP BY dia_semana, hora
ORDER BY kwh_medio_por_casa DESC
LIMIT 10;

-- COMMAND ----------

-- q2: Como o gasto muda de mês para mês, com o calor e a bandeira?
SELECT p.ano_mes,
       first(p.bandeira) AS bandeira,
       round(avg(p.temperatura_media), 1) AS temperatura_media,
       round(avg(p.kwh_total / p.dias_medidos), 2) AS kwh_dia_por_casa,
       round(avg(p.custo_rs / p.dias_medidos) * 30, 2) AS custo_30_dias_por_casa
FROM workspace.gs.mvp_painel_mensal p
GROUP BY p.ano_mes
ORDER BY p.ano_mes;

-- COMMAND ----------

-- q3: Qual aparelho pesa mais na conta? Medido na tomada ou estimado pelo rateio.
SELECT a.rotulo,
       f.origem_medicao,
       round(sum(f.kwh), 0) AS kwh_total,
       round(sum(f.custo_rs), 2) AS custo_total_rs,
       round(100 * sum(f.kwh) / sum(sum(f.kwh)) OVER (), 1) AS participacao_pct
FROM workspace.gs.fato_rateio_ciclo f
JOIN workspace.gs.dim_aparelho a USING (sk_aparelho)
GROUP BY a.rotulo, f.origem_medicao
ORDER BY kwh_total DESC;

-- COMMAND ----------

-- q4: Quais aparelhos estão gastando mais do que deveriam?
SELECT rotulo, status, count(*) AS aparelhos, round(avg(excesso_pct), 0) AS excesso_medio_pct,
       round(avg(rs_excesso_mes), 2) AS rs_excesso_medio_mes
FROM workspace.gs.mvp_alertas_aparelho
GROUP BY rotulo, status
ORDER BY rotulo, status;

-- COMMAND ----------

-- q5 (IMPACTO): Quanto as famílias economizam resolvendo os problemas apontados?
WITH alertas AS (
  SELECT residencia_hash, sum(kwh_excesso_mes) AS kwh_mes, sum(rs_excesso_mes) AS rs_mes
  FROM workspace.gs.mvp_alertas_aparelho
  WHERE status IN ('atencao', 'critico')
  GROUP BY residencia_hash
),
ultimo_mes AS (
  SELECT residencia_hash, kwh_total / dias_medidos * 30 AS kwh_30_dias
  FROM workspace.gs.mvp_painel_mensal
  WHERE ano_mes = (SELECT max(ano_mes) FROM workspace.gs.mvp_painel_mensal)
)
SELECT count(*) AS casas_com_alerta,
       round(sum(a.kwh_mes), 1) AS kwh_evitaveis_mes,
       round(sum(a.rs_mes), 2) AS rs_evitaveis_mes,
       round(sum(a.rs_mes) * 12, 2) AS rs_evitaveis_ano,
       round(avg(a.rs_mes), 2) AS economia_media_por_casa_mes,
       round(100 * sum(a.kwh_mes) / sum(u.kwh_30_dias), 1) AS reducao_pct_nessas_casas
FROM alertas a
JOIN ultimo_mes u USING (residencia_hash);

-- COMMAND ----------

-- q6 (validação): A detecção encontra as falhas injetadas pelo simulador?
WITH total AS (
  SELECT count_if(resultado = 'verdadeiro_positivo') AS vp,
         count_if(resultado = 'falso_positivo') AS fp,
         count_if(resultado = 'falso_negativo') AS fn,
         count_if(resultado = 'verdadeiro_negativo') AS vn,
         round(avg(dias_ate_detectar), 1) AS dias_medios_ate_detectar
  FROM workspace.gs.ouro_validacao_deteccao
)
SELECT vp, fp, fn, vn,
       round(100 * try_divide(vp, vp + fp), 1) AS precisao_pct,
       round(100 * try_divide(vp, vp + fn), 1) AS cobertura_pct,
       dias_medios_ate_detectar
FROM total;
