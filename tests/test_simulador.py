from datetime import date, timedelta

import pytest

from seguwatts import simulador
from seguwatts.arquivos import caminho, ler_ndjson

DIA = "2026-08-10"


def test_cadastro_e_deterministico():
    assert simulador.gerar_residencias(10, 7) == simulador.gerar_residencias(10, 7)
    assert simulador.gerar_residencias(10, 7) != simulador.gerar_residencias(10, 8)


def test_identidades_sao_claramente_ficticias():
    for residencia in simulador.gerar_residencias(5):
        assert residencia["nome_titular"].startswith("Morador Exemplo")
        assert residencia["email"].endswith("@exemplo.invalid")
        assert residencia["cpf"].startswith("000.000.")


def test_toda_casa_tem_ate_tres_tomadas_e_sempre_geladeira():
    for residencia in simulador.gerar_residencias():
        monitorados = [a["tipo"] for a in residencia["aparelhos"] if a["monitorado"]]
        assert "geladeira" in monitorados
        assert len(monitorados) <= 3


def test_falhas_comecam_depois_da_referencia():
    residencias = simulador.gerar_residencias()
    falhas = simulador.gerar_falhas(residencias)
    assert falhas
    tipos = {a["aparelho_id"]: a["tipo"] for r in residencias for a in r["aparelhos"]}
    for falha in falhas:
        minimo = date(2026, 10, 15) if tipos.get(falha["ponto"]) == "ar_condicionado" else date(2026, 7, 15)
        assert date.fromisoformat(falha["inicio"]) >= minimo


def test_sensores_sao_identicos_ao_repetir_o_dia(tmp_path):
    primeira = simulador.gerar_sensores(DIA, tmp_path).read_bytes()
    segunda = simulador.gerar_sensores(DIA, tmp_path).read_bytes()
    assert primeira == segunda


def test_sensores_tem_sujeira_de_cada_tipo(tmp_path):
    linhas = ler_ndjson(simulador.gerar_sensores(DIA, tmp_path))
    valores = [l["kwh"] for l in linhas]
    assert any(v is None for v in valores)
    assert any(isinstance(v, (int, float)) and v < 0 for v in valores)
    assert any(v == 99.9 for v in valores)
    assert any(isinstance(v, str) and "," in v for v in valores)
    ids = [l["leitura_id"] for l in linhas]
    assert len(ids) != len(set(ids))
    assert any(l["residencia_id"] == simulador.RESIDENCIA_FANTASMA for l in linhas)
    assert any(l["ponto"] == simulador.PONTO_FANTASMA for l in linhas)


def test_sensor_geral_nunca_fica_abaixo_das_tomadas(tmp_path):
    linhas = ler_ndjson(simulador.gerar_sensores(DIA, tmp_path))
    limpas = {}
    for linha in linhas:
        if linha["ponto"] != simulador.PONTO_FANTASMA and isinstance(linha["kwh"], float) and 0 <= linha["kwh"] < 15:
            limpas.setdefault((linha["residencia_id"], linha["instante"]), {})[linha["ponto"]] = linha["kwh"]
    casas = [p for p in limpas.values() if "geral" in p and len(p) > 1]
    assert casas
    for pontos in casas:
        tomadas = sum(v for k, v in pontos.items() if k != "geral")
        assert pontos["geral"] >= tomadas * 0.99


def test_consumo_diario_e_plausivel():
    residencias = simulador.gerar_residencias()
    medias = []
    for residencia in residencias:
        consumo = simulador._consumo_horario(residencia, date(2026, 8, 10), [20.0] * 24, {}, 42)
        medias.append(sum(sum(serie) for serie in consumo.values()))
    media = sum(medias) / len(medias)
    assert 4 < media < 20, f"média diária improvável: {media:.1f} kWh"


def test_falha_aumenta_consumo_do_aparelho_depois_da_rampa():
    residencia = simulador.gerar_residencias(1)[0]
    geladeira = next(a for a in residencia["aparelhos"] if a["tipo"] == "geladeira")
    falha = {"residencia_id": "R0001", "ponto": geladeira["aparelho_id"], "inicio": "2026-08-01", "intensidade": 0.4}
    dia = date(2026, 9, 1)
    normal = simulador._consumo_horario(residencia, dia, [20.0] * 24, {}, 42)[geladeira["aparelho_id"]]
    com_falha = simulador._consumo_horario(residencia, dia, [20.0] * 24, {geladeira["aparelho_id"]: falha}, 42)[
        geladeira["aparelho_id"]
    ]
    assert sum(com_falha) == pytest.approx(sum(normal) * 1.4)


def test_ar_condicionado_reage_ao_calor():
    residencia = next(
        r for r in simulador.gerar_residencias() if any(a["tipo"] == "ar_condicionado" for a in r["aparelhos"])
    )
    ar = next(a for a in residencia["aparelhos"] if a["tipo"] == "ar_condicionado")["aparelho_id"]
    dia = date(2026, 10, 10)
    frio = simulador._consumo_horario(residencia, dia, [18.0] * 24, {}, 42)[ar]
    calor = simulador._consumo_horario(residencia, dia, [33.0] * 24, {}, 42)[ar]
    assert sum(frio) == 0
    assert sum(calor) > 0


def test_conta_exige_ciclo_completo(tmp_path):
    residencia = simulador.gerar_residencias()[0]
    fechamento = date(2026, 8, residencia["dia_leitura"])
    simulador.gerar_sensores(fechamento.isoformat(), tmp_path)  # só um dia de histórico
    contas = ler_ndjson(simulador.gerar_contas(fechamento.isoformat(), tmp_path))
    assert all(c["residencia_id"] != residencia["residencia_id"] for c in contas)


def test_conta_fecha_com_historico_e_bate_com_sensor(tmp_path):
    residencia = simulador.gerar_residencias()[0]
    rid = residencia["residencia_id"]
    fechamento = date(2026, 8, residencia["dia_leitura"])
    inicio = date(2026, 7, residencia["dia_leitura"])
    dias = [(inicio + timedelta(days=i + 1)).isoformat() for i in range((fechamento - inicio).days)]
    for dia in dias:
        simulador.gerar_sensores(dia, tmp_path)
    contas = ler_ndjson(simulador.gerar_contas(fechamento.isoformat(), tmp_path))
    conta = next(c for c in contas if c["residencia_id"] == rid)
    somas = simulador._somas_gerais(tmp_path, dias)
    total = sum(v for (r, _), v in somas.items() if r == rid)
    if conta["kwh_faturado"] > 0:  # 2% das contas são zeradas de propósito
        assert conta["kwh_faturado"] == pytest.approx(total, rel=0.03)


def test_cadastro_grava_gabarito(tmp_path):
    simulador.gerar_cadastro(DIA, tmp_path)
    assert caminho(tmp_path, "cadastro", DIA).exists()
    assert simulador.carregar_gabarito(tmp_path)
