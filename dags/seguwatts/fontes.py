"""Coleta das fontes externas reais e conferência da pasta chegada/."""
import json
from pathlib import Path

import requests

from seguwatts import config
from seguwatts.arquivos import caminho, escrever_ndjson


def _get_json(http, url, params=None):
    resposta = http.get(url, params=params, timeout=config.TIMEOUT_HTTP)
    resposta.raise_for_status()
    return resposta.json()


def coletar_clima(dia: str, chegada, http=requests) -> Path:
    dados = _get_json(
        http,
        config.OPEN_METEO_API,
        {
            "latitude": ",".join(str(m["lat"]) for m in config.MUNICIPIOS),
            "longitude": ",".join(str(m["lon"]) for m in config.MUNICIPIOS),
            "start_date": dia,
            "end_date": dia,
            "hourly": "temperature_2m,precipitation",
            "timezone": "America/Sao_Paulo",
        },
    )
    if isinstance(dados, dict):  # A API devolve objeto quando há um único local.
        dados = [dados]
    if len(dados) != len(config.MUNICIPIOS):
        raise ValueError(f"Open-Meteo devolveu {len(dados)} locais, esperava {len(config.MUNICIPIOS)}")

    registros = []
    for municipio, local in zip(config.MUNICIPIOS, dados):
        if all(t is None for t in local["hourly"]["temperature_2m"]):
            raise ValueError(f"Open-Meteo sem temperatura para {municipio['nome']} em {dia}")
        registros.append(
            {
                "municipio_ibge": municipio["ibge"],
                "municipio": municipio["nome"],
                "data_coleta": dia,
                "hourly": local["hourly"],
            }
        )
    return escrever_ndjson(caminho(chegada, "clima", dia), registros)


def _aneel(http, recurso: str, filtros: dict | None = None) -> list:
    """Busca paginada; a API limita o tamanho de cada resposta."""
    registros, deslocamento, tamanho = [], 0, 1000
    while True:
        params = {"resource_id": recurso, "limit": tamanho, "offset": deslocamento}
        if filtros:
            params["filters"] = json.dumps(filtros, ensure_ascii=False)
        pagina = _get_json(http, config.ANEEL_API, params)["result"]["records"]
        registros.extend(pagina)
        if len(pagina) < tamanho:
            return registros
        deslocamento += tamanho


def coletar_tarifas(dia: str, chegada, http=requests) -> Path:
    registros = []
    for distribuidora in config.DISTRIBUIDORAS:
        linhas = _aneel(
            http,
            config.ANEEL_RECURSO_TARIFAS,
            {
                "SigAgente": distribuidora,
                "DscSubGrupo": "B1",
                "DscSubClasse": "Residencial",
                "DscModalidadeTarifaria": "Convencional",
                "DscBaseTarifaria": "Tarifa de Aplicação",
            },
        )
        if not linhas:
            raise ValueError(f"ANEEL sem tarifa para {distribuidora}")
        registros.extend({**linha, "data_coleta": dia} for linha in linhas)
    return escrever_ndjson(caminho(chegada, "tarifas", dia), registros)


def coletar_bandeiras(dia: str, chegada, http=requests) -> Path:
    linhas = _aneel(http, config.ANEEL_RECURSO_BANDEIRAS)
    if not linhas:
        raise ValueError("ANEEL sem bandeiras tarifárias")
    return escrever_ndjson(caminho(chegada, "bandeiras", dia), [{**l, "data_coleta": dia} for l in linhas])


def coletar_municipios(dia: str, chegada, http=requests) -> Path:
    linhas = _get_json(http, config.IBGE_API)
    registros = [{"id": m["id"], "nome": m["nome"], "uf": "SP", "data_coleta": dia} for m in linhas]
    if not registros:
        raise ValueError("IBGE sem municípios")
    return escrever_ndjson(caminho(chegada, "municipios", dia), registros)


def conferir_chegada(dia: str, chegada) -> dict:
    """Falha se faltar arquivo do dia ou se uma fonte obrigatória vier vazia."""
    tamanhos, problemas = {}, []
    for fonte in config.FONTES_OBRIGATORIAS + config.FONTES_OPCIONALMENTE_VAZIAS:
        arquivo = caminho(chegada, fonte, dia)
        if not arquivo.exists():
            problemas.append(f"{fonte}: arquivo ausente")
            continue
        tamanhos[fonte] = arquivo.stat().st_size
        if fonte in config.FONTES_OBRIGATORIAS and tamanhos[fonte] == 0:
            problemas.append(f"{fonte}: arquivo vazio")
    if problemas:
        raise FileNotFoundError(f"Chegada incompleta em {dia}: " + "; ".join(problemas))
    return tamanhos
