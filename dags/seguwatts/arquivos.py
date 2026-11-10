"""Leitura e escrita dos arquivos da pasta chegada/."""
import json
import os
from pathlib import Path
from typing import Iterable


def caminho(chegada, fonte: str, dia: str) -> Path:
    return Path(chegada) / fonte / f"{fonte}_{dia}.json"


def escrever_ndjson(path: Path, registros: Iterable[dict]) -> Path:
    """Grava um JSON por linha. O rename evita arquivo pela metade se a tarefa cair."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporario = path.with_suffix(".tmp")
    with temporario.open("w", encoding="utf-8", newline="\n") as saida:
        for registro in registros:
            saida.write(json.dumps(registro, ensure_ascii=False, separators=(",", ":")) + "\n")
    os.replace(temporario, path)
    return path


def ler_ndjson(path: Path) -> list:
    with Path(path).open(encoding="utf-8") as entrada:
        return [json.loads(linha) for linha in entrada if linha.strip()]
