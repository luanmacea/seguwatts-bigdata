import pytest

from seguwatts import config, fontes
from seguwatts.arquivos import caminho, escrever_ndjson, ler_ndjson

DIA = "2026-08-10"


class Resposta:
    def __init__(self, dados, erro=None):
        self.dados, self.erro = dados, erro

    def raise_for_status(self):
        if self.erro:
            raise self.erro

    def json(self):
        return self.dados


class Http:
    def __init__(self, *respostas):
        self.respostas, self.chamadas = list(respostas), []

    def get(self, url, params=None, timeout=None):
        self.chamadas.append((url, params, timeout))
        return Resposta(self.respostas.pop(0))


def clima_api(temperatura=20.0):
    return [{"hourly": {"time": ["t"] * 24, "temperature_2m": [temperatura] * 24, "precipitation": [0.0] * 24}}] * len(
        config.MUNICIPIOS
    )


def aneel(*registros):
    return {"result": {"records": list(registros)}}


def test_clima_grava_um_registro_por_municipio(tmp_path):
    arquivo = fontes.coletar_clima(DIA, tmp_path, Http(clima_api()))
    linhas = ler_ndjson(arquivo)
    assert arquivo.name == f"clima_{DIA}.json"
    assert [l["municipio_ibge"] for l in linhas] == [m["ibge"] for m in config.MUNICIPIOS]
    assert linhas[0]["data_coleta"] == DIA


def test_clima_sem_temperatura_falha_para_o_airflow_tentar_de_novo(tmp_path):
    with pytest.raises(ValueError):
        fontes.coletar_clima(DIA, tmp_path, Http(clima_api(None)))


def test_clima_com_numero_errado_de_locais_falha(tmp_path):
    with pytest.raises(ValueError):
        fontes.coletar_clima(DIA, tmp_path, Http(clima_api()[:2]))


def test_tarifas_consulta_cada_distribuidora(tmp_path):
    http = Http(*[aneel({"SigAgente": d}) for d in config.DISTRIBUIDORAS])
    linhas = ler_ndjson(fontes.coletar_tarifas(DIA, tmp_path, http))
    assert len(http.chamadas) == len(config.DISTRIBUIDORAS)
    assert {l["SigAgente"] for l in linhas} == set(config.DISTRIBUIDORAS)


def test_tarifas_sem_retorno_falha(tmp_path):
    with pytest.raises(ValueError):
        fontes.coletar_tarifas(DIA, tmp_path, Http(aneel()))


def test_bandeiras_percorre_todas_as_paginas(tmp_path):
    cheia = aneel(*[{"i": i} for i in range(1000)])
    final = aneel({"i": 1000})
    http = Http(cheia, final)
    assert len(ler_ndjson(fontes.coletar_bandeiras(DIA, tmp_path, http))) == 1001
    assert http.chamadas[1][1]["offset"] == 1000


def test_municipios_mantem_apenas_o_necessario(tmp_path):
    http = Http([{"id": 1, "nome": "A", "microrregiao": {"x": 1}}])
    assert ler_ndjson(fontes.coletar_municipios(DIA, tmp_path, http)) == [
        {"id": 1, "nome": "A", "uf": "SP", "data_coleta": DIA}
    ]


def _preencher(tmp_path, ignorar=(), vazias=("contas",)):
    for fonte in config.FONTES_OBRIGATORIAS + config.FONTES_OPCIONALMENTE_VAZIAS:
        if fonte not in ignorar:
            escrever_ndjson(caminho(tmp_path, fonte, DIA), [] if fonte in vazias else [{"a": 1}])


def test_conferir_aceita_contas_vazia(tmp_path):
    _preencher(tmp_path)
    assert fontes.conferir_chegada(DIA, tmp_path)["contas"] == 0


def test_conferir_acusa_arquivo_ausente(tmp_path):
    _preencher(tmp_path, ignorar=("sensores",))
    with pytest.raises(FileNotFoundError, match="sensores"):
        fontes.conferir_chegada(DIA, tmp_path)


def test_conferir_acusa_fonte_obrigatoria_vazia(tmp_path):
    _preencher(tmp_path, vazias=("contas", "clima"))
    with pytest.raises(FileNotFoundError, match="clima"):
        fontes.conferir_chegada(DIA, tmp_path)
