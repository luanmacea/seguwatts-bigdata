# Databricks notebook source
# MAGIC %md
# MAGIC # SeguWatts: exportação para o MVP
# MAGIC Grava as tabelas `mvp_*` em um único JSON. Baixe o arquivo do Volume e coloque em `mvp/dados/`.

# COMMAND ----------

import json
import os
from datetime import datetime, timezone

dbutils.widgets.text("schema", "workspace.gs")
dbutils.widgets.text("saida", "/Volumes/workspace/gs/chegada/mvp/seguwatts_mvp.json")
SCHEMA = dbutils.widgets.get("schema")
SAIDA = dbutils.widgets.get("saida")


def linhas(consulta):
    return [linha.asDict() for linha in spark.sql(consulta).collect()]


def agrupar(tabela):
    grupos = {}
    for linha in linhas(f"SELECT * FROM {SCHEMA}.{tabela}"):
        grupos.setdefault(linha.pop("residencia_hash"), []).append(linha)
    return grupos


painel, perfil, aparelhos, alertas = map(
    agrupar, ["mvp_painel_mensal", "mvp_perfil_semana", "mvp_aparelhos_ciclo", "mvp_alertas_aparelho"]
)

residencias = []
for casa in linhas(
    f"""SELECT r.residencia_hash, l.municipio, l.distribuidora, r.moradores
        FROM {SCHEMA}.dim_residencia r JOIN {SCHEMA}.dim_localidade l USING (sk_localidade)
        ORDER BY r.residencia_hash"""
):
    chave = casa["residencia_hash"]
    residencias.append(
        {
            **casa,
            "rotulo": f"Casa {chave[:6]}",
            "painel": sorted(painel.get(chave, []), key=lambda p: p["ano_mes"]),
            "perfil_semana": sorted(perfil.get(chave, []), key=lambda p: (p["dia_semana_num"], p["hora"])),
            "aparelhos": sorted(aparelhos.get(chave, []), key=lambda a: -a["kwh"]),
            "alertas": alertas.get(chave, []),
        }
    )

os.makedirs(os.path.dirname(SAIDA), exist_ok=True)
with open(SAIDA, "w", encoding="utf-8") as arquivo:
    json.dump(
        {"gerado_em": datetime.now(timezone.utc).isoformat(timespec="seconds"), "residencias": residencias},
        arquivo,
        ensure_ascii=False,
        default=str,
    )
print(f"{len(residencias)} residências exportadas para {SAIDA}")
