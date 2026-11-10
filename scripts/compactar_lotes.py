"""Compacta cada lote em partes menores que o limite de envio pelo navegador.

Uso, depois do montar_lotes.py: python scripts/compactar_lotes.py lote_1 lote_2
"""
import sys
import zipfile
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent / "lotes"
LIMITE = int(8.9 * 1024 * 1024)


def compactar(lote: str):
    parte = 1
    destino = RAIZ / f"{lote}_parte{parte:02d}.zip"
    arquivo = zipfile.ZipFile(destino, "w", zipfile.ZIP_DEFLATED, compresslevel=9)
    for origem in sorted((RAIZ / lote).rglob("*.json")):
        arquivo.write(origem, origem.relative_to(RAIZ / lote).as_posix())
        arquivo.fp.flush()
        if destino.stat().st_size > LIMITE:
            arquivo.close()
            parte += 1
            destino = RAIZ / f"{lote}_parte{parte:02d}.zip"
            arquivo = zipfile.ZipFile(destino, "w", zipfile.ZIP_DEFLATED, compresslevel=9)
    arquivo.close()
    print(f"{lote}: {parte} parte(s)")


if __name__ == "__main__":
    for nome in sys.argv[1:]:
        compactar(nome)
