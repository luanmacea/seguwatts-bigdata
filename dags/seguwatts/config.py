"""Parâmetros do pipeline. Não contém segredos."""

MUNICIPIOS = [
    {"ibge": 3550308, "nome": "São Paulo", "lat": -23.55, "lon": -46.63, "distribuidora": "ELETROPAULO"},
    {"ibge": 3534401, "nome": "Osasco", "lat": -23.53, "lon": -46.79, "distribuidora": "ELETROPAULO"},
    {"ibge": 3509502, "nome": "Campinas", "lat": -22.91, "lon": -47.06, "distribuidora": "CPFL-PAULISTA"},
    {"ibge": 3543402, "nome": "Ribeirão Preto", "lat": -21.18, "lon": -47.81, "distribuidora": "CPFL-PAULISTA"},
    {"ibge": 3518800, "nome": "Guarulhos", "lat": -23.46, "lon": -46.53, "distribuidora": "EDP SP"},
    {"ibge": 3549904, "nome": "São José dos Campos", "lat": -23.19, "lon": -45.88, "distribuidora": "EDP SP"},
]

DISTRIBUIDORAS = sorted({m["distribuidora"] for m in MUNICIPIOS})

SEED = 42
N_RESIDENCIAS = 100
DATA_INSTALACAO = "2026-05-01"

# Identificadores públicos dos recursos no portal de dados abertos da ANEEL.
ANEEL_API = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search"
ANEEL_RECURSO_TARIFAS = "fcf2906c-7c32-4b9b-a637-054e7a5234f4"
ANEEL_RECURSO_BANDEIRAS = "0591b8f6-fe54-437b-b72b-1aa2efd46e42"

OPEN_METEO_API = "https://archive-api.open-meteo.com/v1/archive"
IBGE_API = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/SP/municipios"

FONTES_OBRIGATORIAS = ["clima", "tarifas", "bandeiras", "municipios", "cadastro", "sensores"]
# "contas" pode ficar vazio: nem todo dia há ciclo fechado.
FONTES_OPCIONALMENTE_VAZIAS = ["contas"]

TIMEOUT_HTTP = 60
