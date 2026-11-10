"""Gera o histórico anterior ao início do DAG, com as mesmas funções usadas nele.

Clima, sensores e contas são gerados dia a dia. Tarifas, bandeiras, municípios e
cadastro são retratos: coletar várias vezes no mesmo instante não traria dado novo,
então entram uma única vez, na data inicial.

Uso: python scripts/carga_historica.py --inicio 2026-05-01 --fim 2026-10-24
"""
import argparse
import sys
import time
from datetime import date, timedelta
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ / "dags"))

from seguwatts import fontes, simulador  # noqa: E402
from seguwatts.arquivos import caminho  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--inicio", default="2026-05-01")
    parser.add_argument("--fim", default="2026-10-24")
    parser.add_argument("--chegada", default=str(RAIZ / "chegada"))
    args = parser.parse_args()
    chegada = Path(args.chegada)

    primeiro = args.inicio
    fontes.coletar_tarifas(primeiro, chegada)
    fontes.coletar_bandeiras(primeiro, chegada)
    fontes.coletar_municipios(primeiro, chegada)
    simulador.gerar_cadastro(primeiro, chegada)

    atual, fim = date.fromisoformat(args.inicio), date.fromisoformat(args.fim)
    while atual <= fim:
        dia = atual.isoformat()
        if not caminho(chegada, "clima", dia).exists():
            fontes.coletar_clima(dia, chegada)
            time.sleep(0.3)  # respeita a API gratuita
        simulador.gerar_sensores(dia, chegada)
        simulador.gerar_contas(dia, chegada)
        print(dia, flush=True)
        atual += timedelta(days=1)


if __name__ == "__main__":
    main()
