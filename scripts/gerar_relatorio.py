"""Gera o relatório da disciplina em PDF, na ordem pedida pelo enunciado.

Uso: python scripts/gerar_relatorio.py  (precisa de playwright e do Google Chrome)
Saída: docs/relatorio_bigdata_seguwatts.pdf
"""
import html
import re
from pathlib import Path

from playwright.sync_api import sync_playwright

RAIZ = Path(__file__).resolve().parent.parent
DOCS = RAIZ / "docs"
REPO = "https://github.com/luanmacea/seguwatts-bigdata"

INTEGRANTES = [
    ("Davi Passanha de Sousa Guerra", "RM551605"),
    ("Cauã Gonçalves de Jesus", "RM97648"),
    ("Luan Silveira Macea", "RM98290"),
    ("Rui Amorim Siqueira", "RM98436"),
    ("Luigi Ferrara Sinno", "RM98047"),
]

contador = {"n": 0}


def figura(arquivo, legenda):
    contador["n"] += 1
    return (
        f'<figure><img src="prints/{arquivo}" alt="{legenda}">'
        f'<figcaption><b>Figura {contador["n"]}.</b> {legenda}</figcaption></figure>'
    )


def _consultas_sql():
    texto = (RAIZ / "databricks" / "04_consultas.sql").read_text(encoding="utf-8")
    resultado = {}
    for bloco in re.split(r"-- COMMAND -+", texto):
        nome = re.search(r"-- (q\d)", bloco)
        if nome:
            sql = "\n".join(l for l in bloco.strip().splitlines() if not l.startswith("-- Databricks"))
            resultado[nome.group(1)] = sql.strip()
    return resultado


SQL = _consultas_sql()


def consulta(nome, legenda):
    """SQL completo seguido do print do resultado no Databricks."""
    return (
        f'<div style="break-inside: avoid"><pre>{html.escape(SQL[nome])}</pre>'
        + figura(f"db_{nome}_tabela.png", legenda)
        + "</div>"
    )


def tabela(cabecalho, linhas):
    cab = "".join(f"<th>{c}</th>" for c in cabecalho)
    corpo = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in l) + "</tr>" for l in linhas)
    return f"<table><thead><tr>{cab}</tr></thead><tbody>{corpo}</tbody></table>"


def requisito(numero, titulo, texto, *figuras):
    return f'<div class="req"><h3>Requisito {numero:02d}: {titulo}</h3><p>{texto}</p>{"".join(figuras)}</div>'


CSS = """
@page { size: A4; margin: 16mm 15mm 18mm; }
* { box-sizing: border-box; }
body { font: 10.5pt/1.5 "Segoe UI", Roboto, Arial, sans-serif; color: #1b1b1b; margin: 0; }
h1 { font-size: 24pt; margin: 0 0 4px; }
h2 { font-size: 15pt; margin: 22px 0 8px; padding-bottom: 4px; border-bottom: 2px solid #2a78d6; break-after: avoid; }
h3 { font-size: 11.5pt; margin: 14px 0 4px; break-after: avoid; }
p { margin: 4px 0 8px; }
.capa { height: 250mm; display: flex; flex-direction: column; justify-content: center; }
.capa .sub { font-size: 13pt; color: #444; margin-bottom: 28px; }
.capa .disc { font-size: 11pt; color: #444; margin-top: 28px; }
table { border-collapse: collapse; width: 100%; margin: 6px 0 12px; font-size: 9.5pt; break-inside: avoid; }
th, td { border: 1px solid #cfcfcf; padding: 4px 7px; text-align: left; vertical-align: top; }
th { background: #eef3fb; }
figure { margin: 8px 0 14px; break-inside: avoid; }
figure img { max-width: 100%; max-height: 118mm; border: 1px solid #cfcfcf; display: block; }
figcaption { font-size: 9pt; color: #444; margin-top: 4px; }
code { font-family: Consolas, monospace; font-size: 9pt; background: #f1f1f1; padding: 0 3px; }
pre { font-family: Consolas, monospace; font-size: 8.5pt; background: #f6f6f6; border: 1px solid #ddd; padding: 8px; white-space: pre-wrap; break-inside: avoid; }
.fluxo { display: flex; flex-wrap: wrap; align-items: center; gap: 6px; margin: 10px 0; break-inside: avoid; }
.caixa { border: 1.5px solid #2a78d6; border-radius: 6px; padding: 6px 8px; font-size: 9pt; text-align: center; background: #f4f8fe; }
.seta { color: #2a78d6; font-weight: bold; }
.req { break-inside: auto; }
.quebra { break-before: page; }
.destaque { background: #eef3fb; border-left: 4px solid #2a78d6; padding: 8px 12px; margin: 8px 0; }
"""


def montar():
    caixas = [
        "Open-Meteo, ANEEL, IBGE<br>e simulador",
        "Airflow 3<br>DAG diário",
        "pasta chegada/",
        "Volume Databricks<br>(2 lotes pela interface)",
        "Bronze<br>Auto Loader",
        "Prata<br>MERGE + qualidade",
        "Ouro<br>fato + dimensões + hash",
        "Tabelas mvp_*<br>e JSON",
        "Tela do cidadão",
    ]
    fluxo = '<span class="seta">→</span>'.join(f'<div class="caixa">{c}</div>' for c in caixas)

    partes = [
        f"""<section class="capa">
        <h1>SeguWatts</h1>
        <div class="sub">Componente Dados: pipeline de Big Data para uso consciente de energia residencial</div>
        {tabela(["Nome completo", "RM"], INTEGRANTES)}
        <div class="disc">Global Solution 2026.2 · FIAP · Engenharia de Software, 4º ano<br>
        Big Data Architecture &amp; Data Integration<br>
        Repositório: <a href="{REPO}">{REPO}</a></div>
        </section>""",
        f"""<h2 class="quebra">01. Integrantes</h2>{tabela(["Nome completo", "RM"], INTEGRANTES)}""",
        """<h2>02. O produto e as perguntas que o MVP responde</h2>
        <p class="destaque">O SeguWatts mostra ao morador quando e onde a casa gasta energia, quanto isso custa e qual aparelho
        pode estar com defeito, antes que o problema apareça na conta.</p>
        <p><b>Cidadão:</b> famílias de renda baixa e média no estado de São Paulo, para quem a conta de luz pesa no orçamento.
        <b>ODS:</b> 10 (redução das desigualdades), 11 (cidades sustentáveis) e 13 (ação climática).</p>"""
        + tabela(
            ["#", "Pergunta do cidadão", "Consulta"],
            [
                ["1", "Em que dia da semana e horário minha casa gasta mais?", "q1"],
                ["2", "Como meu gasto muda de mês para mês, com o calor e a bandeira tarifária?", "q2"],
                ["3", "Qual aparelho pesa mais na minha conta?", "q3"],
                ["4", "Algum aparelho está gastando mais do que deveria?", "q4"],
                ["5", "Quanto eu economizo se resolver esses problemas? (impacto)", "q5"],
            ],
        )
        + """<p><b>Como a casa é medida:</b> um sensor geral no quadro de energia e sensores de tomada em até três aparelhos
        críticos (geladeira, freezer e ar-condicionado), todos com leitura por hora, mais a conta de luz e o cadastro de aparelhos.
        O consumo dos demais aparelhos é o sensor geral menos as tomadas, e cada aparelho sem sensor recebe uma parte da conta
        proporcional a potência × horas de uso declaradas.</p>""",
        f"""<h2>03. Diagrama do fluxo de dados</h2><div class="fluxo">{fluxo}</div>
        <p>O Databricks Free Edition não se conecta ao Airflow. Por isso os arquivos de <code>chegada/</code> sobem ao Volume
        pela interface, em lotes. Como o navegador limita o tamanho de cada envio, cada lote foi compactado
        (<code>scripts/compactar_lotes.py</code>) e o notebook <code>00b_extrair_lote</code> recria as pastas dentro do Volume.</p>""",
        "<h2>04. Fontes de dados</h2>"
        + tabela(
            ["Fonte", "Origem", "Formato", "Frequência", "Tipo"],
            [
                ["clima", "Open-Meteo Archive API (6 municípios de SP)", "JSON", "Diária", "Externa real"],
                ["tarifas", "ANEEL Dados Abertos: tarifas B1 residenciais", "JSON", "Diária", "Externa real"],
                ["bandeiras", "ANEEL Dados Abertos: acionamento de bandeiras", "JSON", "Diária", "Externa real"],
                ["municipios", "IBGE API de Localidades", "JSON", "Diária", "Externa real"],
                ["sensores", "Simulador documentado (leituras horárias)", "JSON", "Diária", "Simulada"],
                ["contas", "Simulador documentado (contas de luz)", "JSON", "Diária", "Simulada"],
                ["cadastro", "Simulador documentado (moradores e aparelhos)", "JSON aninhado", "Diária", "Simulada"],
            ],
        )
        + """<p><b>Por que há dados simulados:</b> não existe base aberta de consumo por residência, e dados pessoais reais não
        podem ser publicados. O simulador (<code>dags/seguwatts/simulador.py</code>) é reproduzível pela semente, faz a geladeira e
        o ar-condicionado reagirem à temperatura real do Open-Meteo, usa identidades claramente fictícias e injeta sujeira
        proposital e defeitos registrados num gabarito, que permitem medir se a detecção funciona.</p>""",
        '<h2 class="quebra">05. Requisitos 01 a 09</h2>',
        requisito(
            1,
            "Duas ou mais fontes, ao menos uma externa",
            "Quatro fontes externas reais (Open-Meteo, ANEEL tarifas, ANEEL bandeiras e IBGE) e três simuladas, todas ligadas ao "
            "problema: consumo, custo da energia e clima. Cada fonte tem uma tarefa própria no DAG.",
            figura("airflow_graph.png", "DAG <code>seguwatts_coleta</code> no Airflow 3: uma tarefa de coleta por fonte."),
        ),
        requisito(
            2,
            "DAG agendado gravando na pasta chegada/",
            "O DAG roda <code>@daily</code> com <code>catchup</code>, coleta o dia anterior e grava um arquivo por fonte com a data no nome, "
            "por exemplo <code>chegada/clima/clima_2026-11-08.json</code>. O histórico de 01/05 a 24/10 foi gerado pelas mesmas funções "
            "(<code>scripts/carga_historica.py</code>); o DAG executou de 25/10 a 08/11, com 15 execuções concluídas.",
            figura("airflow_grid.png", "Visão Grid: execuções diárias concluídas com sucesso em todas as tarefas."),
        ),
        requisito(
            3,
            "Paralelismo, verificação dos arquivos e chaves fora do código",
            "As cinco coletas rodam em paralelo depois de <code>inicio</code> (Figura 1). <code>conferir_chegada</code> falha se faltar "
            "algum arquivo do dia ou se uma fonte obrigatória vier vazia, e <code>notificar</code> roda sempre (<code>all_done</code>), "
            "enviando o resultado ao webhook configurado. As fontes usadas não exigem chave de API; mesmo assim, toda configuração sensível "
            "fica em Airflow Variables, nunca no código:",
        )
        + "<pre>def _pasta_chegada() -> str:\n    return Variable.get(\"seguwatts_chegada\", default=\"/opt/airflow/chegada\")\n\n"
        "def _enviar_aviso(texto: str):\n    url = Variable.get(\"seguwatts_webhook_url\", default=\"\")</pre>",
        requisito(
            4,
            "Dois ou mais lotes enviados ao Volume",
            "Lote 1: dias de 01/05 a 31/10 (585 arquivos, em 2 partes compactadas). Lote 2: dias de 01/11 a 08/11 (56 arquivos). "
            "Cada lote foi enviado pela interface e extraído no Volume em momentos diferentes.",
            figura("db_volume_lote1.png", "Volume <code>workspace.gs.chegada</code> com o lote 1 enviado e as pastas por fonte já extraídas."),
            figura("db_extrair_lote2.png", "Extração do lote 2: 8 arquivos novos por fonte, de 01/11 a 08/11."),
        ),
        requisito(
            5,
            "Bronze incremental e sem duplicação",
            "Auto Loader (<code>cloudFiles</code>, <code>availableNow</code>) com checkpoint por fonte: só arquivos novos entram. "
            "As contagens antes e depois do 2º lote e da reexecução estão na seção 07.",
        ),
        requisito(
            6,
            "Prata: tipos, nulos e inválidos tratados, MERGE pela chave e regras de qualidade",
            "Cada tabela recebe tipos corretos (<code>try_cast</code>, vírgula decimal convertida), remove duplicatas pela chave e faz "
            "<code>MERGE</code>. Dezesseis regras com contagem: as linhas barradas vão para <code>prata_rejeitos</code> e os totais para "
            "<code>prata_qualidade</code>. Em <code>prata_leituras</code>, por exemplo, foram barradas 4.921 leituras sem kWh válido, "
            "3.386 fora da faixa de 0 a 15 kWh/h, 192 de residência inexistente e 192 de ponto de medição inexistente.",
            figura("db_prata_qualidade.png", "Tabela <code>prata_qualidade</code>: total barrado por regra e linhas repetidas removidas."),
        ),
        requisito(
            7,
            "Ouro dimensional e 5 consultas SQL",
            "Fato <code>fato_consumo_hora</code> (residência × aparelho × hora, 1,2 milhão de linhas) e dimensões <code>dim_data</code>, "
            "<code>dim_localidade</code>, <code>dim_residencia</code> e <code>dim_aparelho</code>, além do fato <code>fato_rateio_ciclo</code>. "
            "O fato guarda as tomadas e as demais cargas, nunca o sensor geral junto, para não somar nada em dobro. "
            "As consultas estão na seção 08.",
            figura("db_tabelas.png", "Tabelas do schema <code>workspace.gs</code>: Bronze, Prata, dimensões, fatos e tabelas do MVP."),
        ),
        requisito(
            8,
            "LGPD na Ouro e uso dos dados pelo MVP",
            "Na Ouro não há nome, CPF, e-mail, telefone nem o identificador original. Titular e residência viram SHA-256 com sal; "
            "o sal fica no secret, no widget ou num arquivo gerado no Volume, nunca no repositório. O MVP consome apenas as tabelas "
            "<code>mvp_*</code>, exportadas em JSON (contrato em <code>docs/contrato_mvp.md</code>).",
            figura("db_lgpd_dim_residencia.png", "<code>dim_residencia</code>: identificadores em hash, titular e e-mail mascarados, CEP com 5 dígitos."),
            figura("db_mvp_alertas.png", "<code>mvp_alertas_aparelho</code>, tabela da Ouro usada pela tela de alertas."),
            figura("db_exportar_mvp.png", "Exportação das tabelas <code>mvp_*</code> para o JSON consumido pela tela."),
            figura("mvp_tela.png", "Tela do cidadão: alerta, horário de pico, custo mensal e peso de cada aparelho."),
        ),
        requisito(
            9,
            "README e organização do repositório",
            f"Repositório público em <a href=\"{REPO}\">{REPO}</a>, com <code>dags/</code>, <code>databricks/</code>, "
            "<code>amostras/</code>, <code>mvp/</code>, testes e README com integrantes, arquitetura e passo a passo de reprodução. "
            "Sem chaves, senhas ou dados pessoais reais.",
            figura("github_repositorio.png", "Repositório público no GitHub."),
        ),
        '<h2 class="quebra">06. Prints do Airflow</h2>'
        + figura("airflow_graph.png", "Visão Graph: <code>inicio</code>, cinco coletas em paralelo, sensores, contas, conferência e notificação.")
        + figura("airflow_grid.png", "Visão Grid: execuções diárias concluídas, todas as tarefas em verde."),
        "<h2>07. Contagens da Bronze</h2>"
        + tabela(
            ["Momento", "bronze_sensores", "bronze_clima", "bronze_contas", "Arquivos de sensores"],
            [
                ["Depois do lote 1", "1.183.854", "1.104", "508", "184"],
                ["Depois do lote 2", "1.235.331", "1.152", "532", "192"],
                ["3ª execução, sem lote novo", "1.235.331", "1.152", "532", "192"],
            ],
        )
        + "<p>O lote 2 acrescentou só os 8 dias novos, e a execução sem lote novo não mudou nenhuma contagem: a Bronze é incremental e não duplica.</p>"
        + figura("db_bronze_lote1.png", "Contagens da Bronze depois do lote 1.")
        + figura("db_bronze_lote2.png", "Contagens da Bronze depois do lote 2.")
        + figura("db_bronze_reexecucao.png", "Contagens da Bronze na execução seguinte, sem lote novo: valores idênticos."),
        '<h2 class="quebra">08. Consultas SQL, resultados e interpretação</h2>',
        "<h3>q1: em que dia da semana e horário as casas gastam mais?</h3>"
        + consulta("q1", "q1: as dez combinações de dia e hora com maior consumo médio por casa.")
        + "<p>O pico é às 19h em todos os dias da semana, com 0,84 kWh por casa na quarta-feira. O início da noite concentra banho, TV "
        "e iluminação, e é o melhor horário para orientar mudanças de hábito.</p>",
        "<h3>q2: como o gasto muda de mês para mês?</h3>"
        + consulta("q2", "q2: consumo diário, custo equivalente a 30 dias, temperatura e bandeira por mês.")
        + "<p>O consumo vai de 7,55 kWh/dia por casa em junho a 8,12 em novembro. A variação é pequena porque o calor aumenta "
        "geladeira e ar-condicionado e o frio aumenta o chuveiro. A bandeira amarela de maio a setembro encareceu o kWh; a de novembro "
        "ainda não foi publicada pela ANEEL.</p>",
        "<h3>q3: qual aparelho pesa mais na conta?</h3>"
        + consulta("q3", "q3: participação de cada aparelho no consumo, medido na tomada ou estimado pelo rateio da conta.")
        + "<p>O chuveiro responde por 36,8% do consumo (estimado) e a geladeira por 17,7% (medido). O chuveiro é o maior peso, mas a "
        "geladeira é o maior consumo medido e o alvo mais claro de manutenção.</p>",
        "<h3>q4: algum aparelho está gastando mais do que deveria?</h3>"
        + consulta("q4", "q4: aparelhos por status de alerta, com excesso médio e custo extra por mês.")
        + "<p>Há alertas em 20 geladeiras, 5 freezers e 5 casas com consumo sem explicação nos aparelhos sem sensor. Cada aparelho é "
        "comparado com ele mesmo nos 30 primeiros dias de uso, descontando a temperatura. Os 16 aparelhos de ar-condicionado sem 30 dias de "
        "uso ficam como <code>sem_base</code> e não geram alerta sem fundamento.</p>",
        "<h3>q5 (impacto): quanto as famílias economizam?</h3>"
        + consulta("q5", "q5: economia possível se os problemas apontados forem resolvidos.")
        + "<p class=\"destaque\">30 casas com alerta poderiam deixar de gastar 799,4 kWh e R$ 621,80 por mês (R$ 7.461,60 por ano), "
        "em média R$ 20,73 por casa, uma redução de 10,5% no consumo dessas famílias.</p>",
        "<h3>q6 (validação): a detecção encontra as falhas injetadas?</h3>"
        + consulta("q6", "q6: comparação dos alertas com o gabarito do simulador.")
        + "<p>Foram 31 falhas detectadas, nenhum alarme falso e nenhuma perdida, em média 9,9 dias após o início. Uma das 32 falhas "
        "injetadas não pôde ser avaliada por falta de 30 dias de uso. Esse resultado vale para dados simulados, em que os defeitos são "
        "nítidos; em uso real, a taxa tende a ser menor.</p>",
        f"""<h2 class="quebra">09. Conclusão técnica</h2>
        <p>O pipeline coleta quatro fontes reais e três simuladas no Airflow, envia lotes ao Volume e trata os dados em camadas no
        Databricks. A Bronze é incremental e não duplica. A Prata tipa, deduplica, aplica dezesseis regras de qualidade com contagem e faz
        MERGE. A Ouro entrega um modelo dimensional sem contagem dupla, protege dados pessoais com hash e máscara e alimenta a tela
        do cidadão pelas tabelas <code>mvp_*</code>.</p>
        <p><b>Decisões principais:</b> sensores híbridos, porque um sensor geral lendo por hora não separa o consumo por aparelho;
        detecção explicável, que compara cada aparelho com ele mesmo e sugere verificação em vez de afirmar defeito; e LGPD aplicada
        na Ouro.</p>
        <p><b>Limitações:</b> sensores, contas e cadastro são simulados; o custo não inclui ICMS, PIS e COFINS; e um aparelho que já
        começa com defeito parece normal, porque a referência são os primeiros 30 dias de uso.</p>
        <p><b>Repositório:</b> <a href="{REPO}">{REPO}</a></p>""",
    ]
    return f'<!DOCTYPE html><html lang="pt-BR"><head><meta charset="utf-8"><title>SeguWatts: relatório de Big Data</title><style>{CSS}</style></head><body>{"".join(partes)}</body></html>'


def main():
    html = DOCS / "relatorio_bigdata_seguwatts.html"
    html.write_text(montar(), encoding="utf-8")
    with sync_playwright() as p:
        navegador = p.chromium.launch(channel="chrome", headless=True)
        pagina = navegador.new_page()
        pagina.goto(html.as_uri())
        pagina.wait_for_load_state("networkidle")
        pagina.pdf(
            path=str(DOCS / "relatorio_bigdata_seguwatts.pdf"),
            format="A4",
            print_background=True,
            display_header_footer=True,
            header_template="<span></span>",
            footer_template='<div style="font-size:8pt;width:100%;text-align:center;color:#666">'
            'SeguWatts · Big Data · <span class="pageNumber"></span>/<span class="totalPages"></span></div>',
            margin={"top": "16mm", "bottom": "18mm", "left": "15mm", "right": "15mm"},
        )
        navegador.close()
    print(DOCS / "relatorio_bigdata_seguwatts.pdf")


if __name__ == "__main__":
    main()
