"""DAG diário do SeguWatts: coleta as fontes e grava na pasta chegada/.

Cada execução trata o dia anterior, já completo, no fuso de São Paulo.
Todas as configurações ficam em Variables; nada sensível aparece no código.
"""
import logging
from datetime import timedelta

import pendulum
import requests
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable
from airflow.utils.trigger_rule import TriggerRule

from seguwatts import fontes, simulador

FUSO = pendulum.timezone("America/Sao_Paulo")
log = logging.getLogger(__name__)


def _dia_coletado(logical_date) -> str:
    return (pendulum.instance(logical_date).in_timezone(FUSO) - timedelta(days=1)).format("YYYY-MM-DD")


def _pasta_chegada() -> str:
    return Variable.get("seguwatts_chegada", default="/opt/airflow/chegada")


def _executar(funcao, logical_date, **_):
    """Adapta o contexto do Airflow às funções, que só conhecem dia e pasta."""
    dia = _dia_coletado(logical_date)
    resultado = funcao(dia, _pasta_chegada())
    log.info("%s concluiu para %s: %s", funcao.__name__, dia, resultado)
    return str(resultado)


def _enviar_aviso(texto: str):
    """Envia ao webhook configurado; sem webhook, registra no log."""
    url = Variable.get("seguwatts_webhook_url", default="")
    log.info("Notificação: %s", texto)
    if url:
        requests.post(url, json={"content": texto, "text": texto}, timeout=15).raise_for_status()


def _notificar(ti, logical_date, **_):
    """A conferência só devolve resultado se todos os arquivos do dia chegaram."""
    dia = _dia_coletado(logical_date)
    if ti.xcom_pull(task_ids="conferir_chegada"):
        _enviar_aviso(f"SeguWatts: coleta de {dia} concluída")
    else:
        _enviar_aviso(f"SeguWatts: coleta de {dia} INCOMPLETA; veja as tarefas com falha")


def _ao_falhar(contexto):
    _enviar_aviso(f"SeguWatts: tarefa {contexto['task_instance'].task_id} falhou")


with DAG(
    dag_id="seguwatts_coleta",
    description="Coleta clima, tarifas, bandeiras, municípios e dados dos sensores",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 26, tz=FUSO),
    catchup=True,
    max_active_runs=1,  # as contas somam os dias anteriores, então a ordem importa
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": _ao_falhar,
    },
    tags=["seguwatts", "gs-2026"],
) as dag:
    inicio = EmptyOperator(task_id="inicio")

    paralelas = [
        PythonOperator(task_id=f"coletar_{nome}", python_callable=_executar, op_kwargs={"funcao": funcao})
        for nome, funcao in {
            "clima": fontes.coletar_clima,
            "tarifas": fontes.coletar_tarifas,
            "bandeiras": fontes.coletar_bandeiras,
            "municipios": fontes.coletar_municipios,
            "cadastro": simulador.gerar_cadastro,
        }.items()
    ]

    # Depende do clima: o consumo simulado reage à temperatura real do dia.
    sensores = PythonOperator(
        task_id="gerar_sensores", python_callable=_executar, op_kwargs={"funcao": simulador.gerar_sensores}
    )
    contas = PythonOperator(
        task_id="gerar_contas", python_callable=_executar, op_kwargs={"funcao": simulador.gerar_contas}
    )
    conferir = PythonOperator(
        task_id="conferir_chegada", python_callable=_executar, op_kwargs={"funcao": fontes.conferir_chegada}
    )
    notificar = PythonOperator(
        task_id="notificar", python_callable=_notificar, trigger_rule=TriggerRule.ALL_DONE, retries=0
    )

    inicio >> paralelas >> sensores >> contas >> conferir >> notificar
