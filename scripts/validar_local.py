"""Roda Prata, Ouro, consultas e exportação em Spark + Delta local, sem Databricks.

Executa os mesmos notebooks entregues. Só a Bronze é substituída por uma leitura
simples, porque o Auto Loader existe apenas no Databricks. Roda o pipeline duas
vezes para comprovar que a segunda execução não altera nada.

Uso, a partir da raiz do projeto:
docker run --rm -u root -v "$PWD:/app" -w /app apache/spark:4.0.1-python3 \
  /opt/spark/bin/spark-submit --driver-memory 5g \
  --packages io.delta:delta-spark_2.13:4.0.0 scripts/validar_local.py
"""
import json
import re
import shutil
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAIZ = Path(__file__).resolve().parent.parent
TRABALHO = RAIZ / "_validacao_local"
SAIDA_MVP = RAIZ / "mvp" / "dados" / "seguwatts_mvp.json"
FONTES = ["clima", "tarifas", "bandeiras", "municipios", "cadastro", "sensores", "contas"]
PROIBIDOS = ["Morador Exemplo", "@exemplo.invalid", "000.000.", "(11) 90000"]


class Widgets:
    def __init__(self, valores):
        self.valores = valores

    def text(self, nome, padrao):
        self.valores.setdefault(nome, padrao)

    def get(self, nome):
        return self.valores[nome]


class Segredos:
    def get(self, *_):
        raise RuntimeError("sem secrets localmente")


class DbUtils:
    def __init__(self, valores):
        self.widgets, self.secrets = Widgets(valores), Segredos()


def criar_spark():
    return (
        SparkSession.builder.master("local[4]")
        .appName("seguwatts-validacao")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.sources.default", "delta")
        .config("spark.sql.warehouse.dir", str(TRABALHO / "warehouse"))
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def carregar_bronze(spark):
    """Equivale ao Auto Loader, sem o controle incremental."""
    for fonte in FONTES:
        df = spark.read.json(str(RAIZ / "chegada" / fonte))
        if fonte == "sensores":
            df = df.withColumn("kwh", F.col("kwh").cast("string"))
        (
            df.withColumn("arquivo_origem", F.col("_metadata.file_path"))
            .withColumn("carregado_em", F.current_timestamp())
            .write.mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"gs.bronze_{fonte}")
        )


def rodar_notebook(nome, spark, widgets):
    codigo = (RAIZ / "databricks" / nome).read_text(encoding="utf-8")
    exibidos = []
    contexto = {
        "spark": spark,
        "dbutils": DbUtils(widgets),
        "display": lambda df: exibidos.append(df.collect()),
        "__name__": "__notebook__",
    }
    exec(compile(codigo, nome, "exec"), contexto)
    return exibidos


def consultas(spark):
    texto = (RAIZ / "databricks" / "04_consultas.sql").read_text(encoding="utf-8").replace("workspace.gs.", "gs.")
    resultado = {}
    for bloco in re.split(r"-- COMMAND -+", texto):
        titulo = re.search(r"-- (q\d)", bloco)
        sql = "\n".join(l for l in bloco.splitlines() if not l.strip().startswith("--")).strip().rstrip(";")
        if titulo and sql:
            resultado[titulo.group(1)] = spark.sql(sql).collect()
    return resultado


def verificar(spark):
    """Interrompe a validação na primeira regra de aceite quebrada."""
    geral = spark.sql(
        """SELECT round(sum(greatest(g.kwh, coalesce(t.kwh, 0))), 2) AS kwh FROM gs.prata_leituras g
           LEFT JOIN (SELECT residencia_id, instante, sum(kwh) kwh FROM gs.prata_leituras WHERE ponto <> 'geral'
                      GROUP BY residencia_id, instante) t USING (residencia_id, instante)
           WHERE g.ponto = 'geral'"""
    ).first()["kwh"]
    fato = spark.sql(
        """SELECT round(sum(kwh), 2) AS kwh FROM gs.fato_consumo_hora f
           WHERE EXISTS (SELECT 1 FROM gs.fato_consumo_hora x JOIN gs.dim_aparelho a USING (sk_aparelho)
                         WHERE a.tipo = 'demais_cargas' AND x.sk_residencia = f.sk_residencia AND x.instante = f.instante)"""
    ).first()["kwh"]
    assert abs(geral - fato) / geral < 0.001, f"Fato não bate com sensor geral: {geral} x {fato}"

    tabelas_ouro = [
        linha["tableName"]
        for linha in spark.sql("SHOW TABLES IN gs").collect()
        if re.match(r"(dim_|fato_|mvp_|ouro_)", linha["tableName"])
    ]
    for tabela in tabelas_ouro:
        conteudo = " ".join(str(l) for l in spark.table(f"gs.{tabela}").limit(2000).collect())
        for proibido in PROIBIDOS:
            assert proibido not in conteudo, f"Dado pessoal em gs.{tabela}: {proibido}"
    assert "R0001" not in " ".join(str(l) for l in spark.table("gs.fato_rateio_ciclo").limit(2000).collect())
    return {"kwh_geral": geral, "kwh_fato": fato, "tabelas_ouro": tabelas_ouro}


def contagens(spark):
    return {
        linha["tableName"]: spark.table(f"gs.{linha['tableName']}").count()
        for linha in spark.sql("SHOW TABLES IN gs").collect()
        if linha["tableName"].startswith(("prata_", "dim_", "fato_", "mvp_"))
        and linha["tableName"] not in ("prata_qualidade", "prata_rejeitos")
    }


def main():
    if TRABALHO.exists():
        shutil.rmtree(TRABALHO)
    TRABALHO.mkdir(parents=True)
    spark = criar_spark()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE DATABASE gs")
    widgets = {
        "schema": "gs",
        "base": str(RAIZ / "chegada"),
        "lgpd_sal": "sal-usado-so-na-validacao-local",
        "saida": str(SAIDA_MVP),
    }

    carregar_bronze(spark)
    rodar_notebook("02_prata.py", spark, widgets)
    primeira = contagens(spark)
    qualidade = sorted(tuple(l)[:4] for l in spark.table("gs.prata_qualidade").collect())

    rodar_notebook("02_prata.py", spark, widgets)
    rodar_notebook("03_ouro.py", spark, widgets)
    segunda = {k: v for k, v in contagens(spark).items() if k in primeira}
    divergentes = {k: (primeira[k], segunda[k]) for k in primeira if primeira[k] != segunda[k]}
    assert not divergentes, f"Segunda execução alterou contagens: {divergentes}"

    aceite = verificar(spark)
    resultados = consultas(spark)
    rodar_notebook("05_exportar_mvp.py", spark, widgets)

    resumo = {
        "contagens": contagens(spark),
        "qualidade": [list(q) for q in qualidade],
        "aceite": aceite,
        "consultas": {k: [r.asDict() for r in v] for k, v in resultados.items()},
    }
    (RAIZ / "docs" / "resultado_validacao_local.json").write_text(
        json.dumps(resumo, ensure_ascii=False, indent=1, default=str), encoding="utf-8"
    )
    for chave, valor in resumo["consultas"].items():
        print(chave, valor[:6], flush=True)
    print("VALIDACAO OK", flush=True)


if __name__ == "__main__":
    sys.exit(main())
