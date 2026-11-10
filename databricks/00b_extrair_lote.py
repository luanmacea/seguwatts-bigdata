# Databricks notebook source
# MAGIC %md
# MAGIC # SeguWatts: extração de lote
# MAGIC Os lotes sobem ao Volume pela interface, compactados para caber no limite de envio.
# MAGIC Este notebook recria as pastas `clima/`, `sensores/` etc. Informe os arquivos no widget.

# COMMAND ----------

import collections
import zipfile

dbutils.widgets.text("base", "/Volumes/workspace/gs/chegada")
dbutils.widgets.text("arquivos", "lote_1_parte01.zip,lote_1_parte02.zip")
BASE = dbutils.widgets.get("base")

extraidos = collections.Counter()
for nome in [n.strip() for n in dbutils.widgets.get("arquivos").split(",") if n.strip()]:
    with zipfile.ZipFile(f"{BASE}/{nome}") as compactado:
        for item in compactado.namelist():
            compactado.extract(item, BASE)
            extraidos[item.split("/")[0]] += 1

display(spark.createDataFrame(sorted(extraidos.items()), "fonte STRING, arquivos_extraidos LONG"))
