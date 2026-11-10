"""Separa chegada/ em lotes para enviar ao Volume do Databricks pela interface.

Uso: python scripts/montar_lotes.py --nome lote_1 --ate 2026-10-31
     python scripts/montar_lotes.py --nome lote_2 --de 2026-11-01

Dentro de cada lote, as pastas já têm o nome esperado no Volume.
"""
import argparse
import re
import shutil
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
DATA = re.compile(r"_(\d{4}-\d{2}-\d{2})\.json$")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--nome", required=True)
    parser.add_argument("--de", default="0000-00-00")
    parser.add_argument("--ate", default="9999-99-99")
    args = parser.parse_args()

    origem, destino = RAIZ / "chegada", RAIZ / "lotes" / args.nome
    total = 0
    for arquivo in sorted(origem.rglob("*.json")):
        relativo = arquivo.relative_to(origem)
        encontrado = DATA.search(arquivo.name)
        if encontrado:
            if not args.de <= encontrado.group(1) <= args.ate:
                continue
        elif args.de != "0000-00-00":  # o gabarito (sem data) vai só no primeiro lote
            continue
        alvo = destino / relativo
        alvo.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(arquivo, alvo)
        total += 1
    print(f"{total} arquivos em {destino}")


if __name__ == "__main__":
    main()
