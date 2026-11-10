"""Gera as fontes que não existem abertas: cadastro, leituras de sensores e contas.

Não há base pública de consumo por residência, e dados pessoais reais não podem
ir para o repositório. Todo dado aqui é fictício, reproduzível pela semente e
influenciado pela temperatura real coletada do Open-Meteo.

Falhas injetadas ficam em chegada/gabarito/falhas.json, para medir a detecção.
"""
import json
import math
import random
from datetime import date, timedelta
from pathlib import Path

from seguwatts import config
from seguwatts.arquivos import caminho, escrever_ndjson, ler_ndjson

LIMITE_KWH_HORA = 15.0
RESIDENCIA_FANTASMA = "R9999"
PONTO_FANTASMA = "P-FANTASMA"
DEMAIS_CARGAS = "demais_cargas"

# tipo: (potência nominal em W, horas por dia declaradas pelo morador)
CATALOGO = {
    "geladeira": (150, 24.0),
    "freezer": (200, 24.0),
    "ar_condicionado": (1100, 3.0),
    "chuveiro": (5500, 0.5),
    "tv": (120, 4.0),
    "iluminacao": (100, 5.0),
    "maquina_lavar": (500, 0.5),
    "computador": (150, 4.0),
    "micro_ondas": (1200, 0.2),
    "ferro": (1000, 0.2),
    "standby": (30, 24.0),
}
MONITORADOS = ("geladeira", "freezer", "ar_condicionado")


def _rng(*partes) -> random.Random:
    return random.Random("|".join(str(p) for p in partes))


def _dia(texto: str) -> date:
    return date.fromisoformat(texto)


def gerar_residencias(n: int = config.N_RESIDENCIAS, seed: int = config.SEED) -> list:
    residencias = []
    for i in range(1, n + 1):
        rng = _rng(seed, "residencia", i)
        rid = f"R{i:04d}"
        municipio = rng.choice(config.MUNICIPIOS)

        tipos = ["geladeira", "chuveiro", "tv", "iluminacao", "maquina_lavar", "micro_ondas", "standby"]
        if rng.random() < 0.30:
            tipos.append("freezer")
        if rng.random() < 0.45:
            tipos.append("ar_condicionado")
        if rng.random() < 0.55:
            tipos.append("computador")
        if rng.random() < 0.50:
            tipos.append("ferro")

        aparelhos = []
        for n_aparelho, tipo in enumerate(tipos, start=1):
            potencia, horas = CATALOGO[tipo]
            aparelhos.append(
                {
                    "aparelho_id": f"{rid}-A{n_aparelho:02d}",
                    "tipo": tipo,
                    "potencia_w": round(potencia * rng.uniform(0.9, 1.15)),
                    "horas_dia_declaradas": round(horas * rng.uniform(0.8, 1.2), 2) if horas < 24 else horas,
                    "monitorado": tipo in MONITORADOS,
                    "idade_anos": rng.randint(0, 14),
                }
            )

        residencias.append(
            {
                "residencia_id": rid,
                "nome_titular": f"Morador Exemplo {i:04d}",
                "cpf": f"000.000.{i:03d}-00",
                "email": f"morador{i:04d}@exemplo.invalid",
                "telefone": f"(11) 90000-{i:04d}",
                "cep": f"{rng.randint(1000, 19999):05d}-000",
                "municipio_ibge": municipio["ibge"],
                "distribuidora": municipio["distribuidora"],
                "moradores": rng.randint(1, 6),
                "area_m2": rng.randint(32, 140),
                "dia_leitura": rng.randint(1, 28),
                "data_instalacao": config.DATA_INSTALACAO,
                "aparelhos": aparelhos,
            }
        )
    return residencias


def gerar_falhas(residencias: list, seed: int = config.SEED) -> list:
    """Sorteia defeitos com início depois do período de referência de cada aparelho."""
    falhas = []
    for residencia in residencias:
        rng = _rng(seed, "falha", residencia["residencia_id"])
        sorteio = rng.random()
        if sorteio < 0.22:
            monitorados = [a for a in residencia["aparelhos"] if a["monitorado"]]
            aparelho = rng.choice(monitorados)
            primeiro = date(2026, 10, 15) if aparelho["tipo"] == "ar_condicionado" else date(2026, 7, 15)
            janela = 10 if aparelho["tipo"] == "ar_condicionado" else 77
            falhas.append(
                {
                    "residencia_id": residencia["residencia_id"],
                    "ponto": aparelho["aparelho_id"],
                    "tipo_falha": "degradacao",
                    "inicio": (primeiro + timedelta(days=rng.randint(0, janela))).isoformat(),
                    "intensidade": round(rng.uniform(0.25, 0.45), 3),
                }
            )
        elif sorteio < 0.30:
            falhas.append(
                {
                    "residencia_id": residencia["residencia_id"],
                    "ponto": DEMAIS_CARGAS,
                    "tipo_falha": "carga_oculta",
                    "inicio": (date(2026, 7, 15) + timedelta(days=rng.randint(0, 77))).isoformat(),
                    "intensidade": rng.randint(80, 150),
                }
            )
    return falhas


def _temperatura_sazonal(dia: date, hora: int) -> float:
    """Usada só se o clima do dia não existir: a coleta real é sempre preferida."""
    anual = 22 + 4 * math.cos(2 * math.pi * (dia.timetuple().tm_yday - 15) / 365)
    return anual + 4 * math.sin(2 * math.pi * (hora - 9) / 24)


def _temperaturas(chegada, dia: str) -> dict:
    """{municipio_ibge: [24 temperaturas]} a partir do arquivo real de clima."""
    arquivo = caminho(chegada, "clima", dia)
    if not arquivo.exists():
        return {}
    resultado = {}
    for local in ler_ndjson(arquivo):
        temperaturas = local["hourly"]["temperature_2m"]
        if len(temperaturas) == 24 and all(t is not None for t in temperaturas):
            resultado[local["municipio_ibge"]] = temperaturas
    return resultado


def _fator_falha(falha: dict, dia: date) -> float:
    inicio = _dia(falha["inicio"])
    if dia < inicio:
        return 1.0
    return 1.0 + falha["intensidade"] * min(1.0, ((dia - inicio).days + 1) / 14)


def _perfil_horas(tipo: str, fim_de_semana: bool) -> list:
    """Peso relativo de uso de cada hora."""
    noite = [18, 19, 20, 21, 22]
    pesos = [0.0] * 24
    if tipo == "tv":
        horas = noite + ([13, 14, 15, 16, 17] if fim_de_semana else [])
    elif tipo == "iluminacao":
        horas = [5, 6] + noite + [23]
    elif tipo == "computador":
        horas = [9, 10, 11, 14, 15, 16] if not fim_de_semana else [10, 11, 15, 16, 20, 21]
    elif tipo == "micro_ondas":
        horas = [7, 12, 19]
    else:
        horas = list(range(24))
    for hora in horas:
        pesos[hora] = 1.0
    total = sum(pesos)
    return [p / total for p in pesos]


def _consumo_horario(residencia: dict, dia: date, temperaturas: list, falhas: dict, seed: int) -> dict:
    """{aparelho_id: [kWh em cada uma das 24 horas]}, mais a carga oculta se houver."""
    rid = residencia["residencia_id"]
    rng = _rng(seed, "consumo", rid, dia.isoformat())
    fim_de_semana = dia.weekday() >= 5
    tmax, tmin = max(temperaturas), min(temperaturas)
    limiar_ar = _rng(seed, "limiar", rid).uniform(26.5, 30.0)
    consumo = {}

    for aparelho in residencia["aparelhos"]:
        tipo, potencia = aparelho["tipo"], aparelho["potencia_w"]
        horas = [0.0] * 24

        if tipo in ("geladeira", "freezer"):
            ciclo = 0.38 if tipo == "geladeira" else 0.42
            for h in range(24):
                efeito_calor = 1 + 0.012 * (temperaturas[h] - 22)
                horas[h] = potencia / 1000 * ciclo * efeito_calor * rng.uniform(0.96, 1.04)

        elif tipo == "ar_condicionado":
            if tmax >= limiar_ar:
                janela = [19, 20, 21, 22, 23] + ([13, 14, 15, 16] if fim_de_semana else [])
                for h in janela:
                    if rng.random() < 0.8:
                        horas[h] = potencia / 1000 * rng.uniform(0.60, 0.80) * (1 + 0.008 * (temperaturas[h] - 26))

        elif tipo == "chuveiro":
            intensidade = 1.0 if tmin < 16 else 0.75
            for _ in range(residencia["moradores"]):
                h = rng.choice([6, 7, 8, 18, 19, 20, 21])
                horas[h] += potencia / 1000 * intensidade * rng.uniform(7, 12) / 60

        elif tipo == "maquina_lavar":
            if dia.weekday() == 5 or (dia.weekday() == 2 and rng.random() < 0.5):
                for h in (9, 10):
                    horas[h] = potencia / 1000 * rng.uniform(0.7, 1.0)

        elif tipo == "ferro":
            if dia.weekday() in (0, 6) and rng.random() < 0.7:
                horas[rng.choice([9, 10, 19, 20])] = potencia / 1000 * rng.uniform(0.3, 0.6)

        else:  # tv, iluminacao, computador, micro_ondas, standby
            energia = potencia / 1000 * aparelho["horas_dia_declaradas"] * rng.uniform(0.75, 1.25)
            if fim_de_semana and tipo in ("tv", "micro_ondas", "iluminacao"):
                energia *= 1.25
            for h, peso in enumerate(_perfil_horas(tipo, fim_de_semana)):
                horas[h] = energia * peso

        falha = falhas.get(aparelho["aparelho_id"])
        if falha:
            fator = _fator_falha(falha, dia)
            horas = [v * fator for v in horas]
        consumo[aparelho["aparelho_id"]] = horas

    oculta = falhas.get((rid, DEMAIS_CARGAS))
    if oculta and dia >= _dia(oculta["inicio"]):
        consumo["_carga_oculta"] = [oculta["intensidade"] / 1000] * 24
    return consumo


def _indice_falhas(falhas: list) -> dict:
    indice = {}
    for falha in falhas:
        chave = (falha["residencia_id"], DEMAIS_CARGAS) if falha["ponto"] == DEMAIS_CARGAS else falha["ponto"]
        indice[chave] = falha
    return indice


def _sujar(linha: dict, rng: random.Random) -> list:
    """Reproduz problemas de sensor reais. Devolve de 0 a 2 linhas."""
    sorteio = rng.random()
    if sorteio < 0.003:  # sensor offline: leitura perdida
        return []
    if sorteio < 0.007:
        return [{**linha, "kwh": None}]
    if sorteio < 0.009:
        return [{**linha, "kwh": -abs(linha["kwh"])}]
    if sorteio < 0.010:
        return [{**linha, "kwh": 99.9}]
    if sorteio < 0.015:
        return [{**linha, "kwh": str(linha["kwh"]).replace(".", ",")}]
    if sorteio < 0.018:
        return [linha, dict(linha)]
    return [linha]


def gerar_cadastro(dia: str, chegada, seed: int = config.SEED) -> Path:
    residencias = gerar_residencias(seed=seed)
    gabarito = Path(chegada) / "gabarito" / "falhas.json"
    escrever_ndjson(gabarito, gerar_falhas(residencias, seed))
    return escrever_ndjson(caminho(chegada, "cadastro", dia), [{**r, "data_coleta": dia} for r in residencias])


def gerar_sensores(dia: str, chegada, seed: int = config.SEED) -> Path:
    residencias = gerar_residencias(seed=seed)
    falhas = _indice_falhas(gerar_falhas(residencias, seed))
    temperaturas_reais = _temperaturas(chegada, dia)
    data = _dia(dia)
    linhas = []

    for residencia in residencias:
        rid = residencia["residencia_id"]
        rng = _rng(seed, "sujeira", rid, dia)
        temperaturas = temperaturas_reais.get(residencia["municipio_ibge"]) or [
            _temperatura_sazonal(data, h) for h in range(24)
        ]
        consumo = _consumo_horario(residencia, data, temperaturas, falhas, seed)
        monitorados = {a["aparelho_id"] for a in residencia["aparelhos"] if a["monitorado"]}

        for hora in range(24):
            instante = f"{dia}T{hora:02d}:00:00"
            total = sum(serie[hora] for serie in consumo.values())
            pontos = [("geral", total * rng.uniform(0.995, 1.005))] + [
                (ponto, consumo[ponto][hora]) for ponto in sorted(monitorados)
            ]
            for ponto, kwh in pontos:
                linhas.extend(
                    _sujar(
                        {
                            "leitura_id": f"{rid}|{ponto}|{instante}",
                            "residencia_id": rid,
                            "ponto": ponto,
                            "instante": instante,
                            "kwh": round(kwh, 4),
                            "firmware": "1.2.0",
                        },
                        rng,
                    )
                )

    rng_dia = _rng(seed, "fantasma", dia)
    linhas.append(
        {
            "leitura_id": f"{RESIDENCIA_FANTASMA}|geral|{dia}T12:00:00",
            "residencia_id": RESIDENCIA_FANTASMA,
            "ponto": "geral",
            "instante": f"{dia}T12:00:00",
            "kwh": round(rng_dia.uniform(0.1, 1.0), 4),
            "firmware": "1.2.0",
        }
    )
    linhas.append(
        {
            "leitura_id": f"R0001|{PONTO_FANTASMA}|{dia}T12:00:00",
            "residencia_id": "R0001",
            "ponto": PONTO_FANTASMA,
            "instante": f"{dia}T12:00:00",
            "kwh": round(rng_dia.uniform(0.1, 1.0), 4),
            "firmware": "1.2.0",
        }
    )
    return escrever_ndjson(caminho(chegada, "sensores", dia), linhas)


def _kwh_valido(valor) -> float | None:
    if isinstance(valor, str):
        valor = valor.replace(",", ".")
    try:
        numero = float(valor)
    except (TypeError, ValueError):
        return None
    return numero if 0 <= numero <= LIMITE_KWH_HORA else None


def _somas_gerais(chegada, dias: list) -> dict:
    """{(residencia_id, dia): kWh do sensor geral}, ignorando leitura inválida ou repetida."""
    somas, vistos = {}, set()
    for dia in dias:
        arquivo = caminho(chegada, "sensores", dia)
        if not arquivo.exists():
            continue
        for linha in ler_ndjson(arquivo):
            if linha["ponto"] != "geral" or linha["leitura_id"] in vistos:
                continue
            kwh = _kwh_valido(linha["kwh"])
            if kwh is None:
                continue
            vistos.add(linha["leitura_id"])
            chave = (linha["residencia_id"], dia)
            somas[chave] = somas.get(chave, 0.0) + kwh
    return somas


def _mes_anterior(data: date, dia_leitura: int) -> date:
    ano, mes = (data.year - 1, 12) if data.month == 1 else (data.year, data.month - 1)
    return date(ano, mes, dia_leitura)


def gerar_contas(dia: str, chegada, seed: int = config.SEED) -> Path:
    """Fecha a conta das residências cujo dia de leitura é hoje; só com ciclo completo."""
    data = _dia(dia)
    fechando = [r for r in gerar_residencias(seed=seed) if r["dia_leitura"] == data.day]
    contas = []

    if fechando:
        inicio_janela = _mes_anterior(data, data.day)
        dias = [(inicio_janela + timedelta(days=i + 1)).isoformat() for i in range((data - inicio_janela).days)]
        somas = _somas_gerais(chegada, dias)

        for residencia in fechando:
            rid = residencia["residencia_id"]
            presentes = [d for d in dias if (rid, d) in somas]
            if len(presentes) < len(dias) * 0.9:  # histórico insuficiente: não fecha a conta
                continue
            rng = _rng(seed, "conta", rid, dia)
            kwh = sum(somas[(rid, d)] for d in presentes) * (len(dias) / len(presentes)) * rng.uniform(0.985, 1.015)
            sorteio = rng.random()
            anterior, atual = inicio_janela.isoformat(), dia
            if sorteio < 0.02:
                kwh = 0.0
            elif sorteio < 0.04:
                anterior, atual = atual, anterior
            contas.append(
                {
                    "conta_id": f"{rid}-{dia[:7]}",
                    "residencia_id": rid,
                    "data_leitura_anterior": anterior,
                    "data_leitura_atual": atual,
                    "kwh_faturado": round(kwh, 2),
                    "data_coleta": dia,
                }
            )
    return escrever_ndjson(caminho(chegada, "contas", dia), contas)


def carregar_gabarito(chegada) -> list:
    arquivo = Path(chegada) / "gabarito" / "falhas.json"
    return [json.loads(l) for l in arquivo.read_text(encoding="utf-8").splitlines() if l.strip()]
