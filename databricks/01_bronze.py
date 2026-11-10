# Databricks notebook source
# MAGIC %md
# MAGIC # SeguWatts: Bronze
# MAGIC Auto Loader lê só os arquivos novos do Volume. O checkpoint garante que rodar
# MAGIC de novo não duplica nada. Rode depois de cada lote enviado.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("schema", "workspace.gs")
dbutils.widgets.text("base", "/Volumes/workspace/gs/chegada")
SCHEMA = dbutils.widgets.get("schema")
BASE = dbutils.widgets.get("base")

# kwh chega como número ou como texto ("0,123"); fixar STRING evita perder esses casos.
FONTES = {
    "clima": None,
    "tarifas": None,
    "bandeiras": None,
    "municipios": None,
    "cadastro": None,
    "sensores": "kwh STRING",
    "contas": None,
}

# COMMAND ----------


def carregar(fonte, dicas):
    leitor = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{BASE}/_controle/schema_{fonte}")
    )
    if dicas:
        leitor = leitor.option("cloudFiles.schemaHints", dicas)
    consulta = (
        leitor.load(f"{BASE}/{fonte}")
        .withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("carregado_em", F.current_timestamp())
        .writeStream.option("checkpointLocation", f"{BASE}/_controle/checkpoint_{fonte}")
        .trigger(availableNow=True)
        .toTable(f"{SCHEMA}.bronze_{fonte}")
    )
    consulta.awaitTermination()


for fonte, dicas in FONTES.items():
    carregar(fonte, dicas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contagens para o relatório
# MAGIC Anote antes e depois do 2º lote, e depois de rodar novamente sem lote novo.

# COMMAND ----------

contagens = spark.sql(
    " UNION ALL ".join(
        f"SELECT '{fonte}' AS fonte, count(*) AS linhas, count(DISTINCT arquivo_origem) AS arquivos "
        f"FROM {SCHEMA}.bronze_{fonte}"
        for fonte in FONTES
    )
)
display(contagens)
