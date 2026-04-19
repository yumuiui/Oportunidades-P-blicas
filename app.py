"""
NextSupply — Banco de Dados de Oportunidades Públicas
ZIPs ficam em data/zips/ no repositório — carregados automaticamente.
"""

import re
import zipfile
from io import BytesIO
from pathlib import Path
from collections import defaultdict

import pandas as pd
import pdfplumber
import streamlit as st

# ─────────────────────────────────────────────────────────────────────────────
# CAMINHOS
# ─────────────────────────────────────────────────────────────────────────────

BASE_DIR     = Path(__file__).parent
ZIPS_DIR     = BASE_DIR / "data" / "zips"
PIPEFY_PATH  = BASE_DIR / "data" / "pipefy_latest.xlsx"
ANALISE_PATH = BASE_DIR / "data" / "analise_precos.xlsx"
GERAL_PATH   = BASE_DIR / "data" / "planilha_geral.xlsx"

ZIPS_DIR.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="NextSupply | OPS Database", page_icon="🔧", layout="wide")

FASES_LANCADAS   = {"Enviado pro Trello", "LANÇADO", "PEDIDO DE COTAÇÃO ENVIADO", "COTADAS TOTALMENTE"}
FASES_DECLINADAS = {"Declinadas"}
FASES_ADIADAS    = {"ADIADAS/RELANÇAR"}
FASES_ANDAMENTO  = {"RECEBIDAS"}

ESCOPOS = {
    "Valvula":        ["válvula","valve","globo","gaveta","borboleta","esfera","retenção","alívio","controle"],
    "Bomba":          ["bomba","pump","centrífuga","submersível","centrif"],
    "Motor":          ["motor","elétrico","trifásico","monofásico"],
    "Transmissor":    ["transmissor","transmitter","sensor","medidor","transdutor"],
    "Filtro":         ["filtro","filter","coalescer","separador"],
    "Rele":           ["relé","relay","rele"],
    "Eletrico":       ["disjuntor","chave","quadro","painel","elétrico","cabo","conector"],
    "Instrumentacao": ["instrumento","controlador","indicador","manômetro","pressostato"],
    "Mangueira":      ["mangueira","hose","flexível","tubing"],
    "Rolamento":      ["rolamento","bearing","mancal"],
    "Compressor":     ["compressor","compressora"],
    "Atuador":        ["atuador","actuator"],
    "Iluminacao":     ["luminária","lanterna","lighting","led","lâmpada"],
    "Camera":         ["câmera","camera","cftv","vigilância"],
    "Incendio":       ["incêndio","extintor","sprinkler","hidrante"],
    "EPI":            ["capacete","luva","botina","óculos protet"],
    "Guindaste":      ["guindaste","crane","içamento","talha"],
    "HVAC":           ["hvac","climatizador","ar condicionado","ventilação"],
    "Servico":        ["serviço","manutenção","inspeção","reparo"],
}

FABRICANTES_CONHECIDOS = [
    "EMERSON","ABB","SIEMENS","PARKER","SKF","ENDRESS","DANFOSS","FESTO","IFM",
    "ROCKWELL","SCHNEIDER","YOKOGAWA","HONEYWELL","FISHER","FLOWSERVE","CRANE",
    "VELAN","CAMERON","SPIRAX","SEATRAX","DONALDSON","BALLUFF","STAHL","EATON",
    "RITTAL","CISCO","STEYR","PELICAN","ENERPAC","ASCO","EXHEAT","KONGSBERG",
    "YAMADA","OMEGA","FLUKE","VEGA","KROHNE","BURKERT","SAMSON","METSO",
    "SULZER","GRUNDFOS","XYLEM","KSB","ITT","ARMSTRONG","NOSHOK",
]

# ─────────────────────────────────────────────────────────────────────────────
# CSS
# ─────────────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
:root {
    --orange:#E8630A; --bg:#06111f; --surface:#0c1d30;
    --text:#e8eef6; --muted:#7a95b0;
}
.stApp {
    background: radial-gradient(ellipse 60% 40% at 0% 0%,rgba(232,99,10,0.10),transparent),
                linear-gradient(180deg,#060f1c 0%,#040b16 100%);
    color:var(--text);
}
.block-container{max-width:1400px;padding-top:1.5rem;}
[data-testid="stHeader"]{background:transparent;}
[data-testid="stSidebar"]{background:#0c1d30 !important;border-right:1px solid rgba(232,99,10,0.2) !important;}
[data-testid="stSidebar"] *{color:#e8eef6 !important;}
.stTabs [data-baseweb="tab-list"]{background:transparent;border-bottom:1px solid rgba(232,99,10,0.2);gap:.5rem;}
.stTabs [data-baseweb="tab"]{background:rgba(12,29,48,.6);border:1px solid rgba(232,99,10,.15);border-radius:8px 8px 0 0;color:#7a95b0 !important;padding:.5rem 1.2rem;}
.stTabs [aria-selected="true"]{background:rgba(232,99,10,.12) !important;border-color:rgba(232,99,10,.4) !important;color:#E8630A !important;}
[data-testid="stDataFrame"]{border:1px solid rgba(232,99,10,.2);border-radius:10px;}
.metric-card{background:linear-gradient(135deg,#0c1d30,#112540);border:1px solid rgba(232,99,10,.22);border-radius:16px;padding:1.4rem 1.2rem;text-align:center;}
.metric-card .val{font-size:2.4rem;font-weight:700;line-height:1;}
.metric-card .pct{font-size:.72rem;margin-top:3px;}
.metric-card .lbl{font-size:.78rem;color:#7a95b0;margin-top:.4rem;text-transform:uppercase;letter-spacing:.06em;}
.metric-card .sub{font-size:.82rem;color:#a0b4c8;margin-top:.3rem;}
.info-box{background:rgba(232,99,10,.07);border:1px solid rgba(232,99,10,.2);border-radius:10px;padding:.85rem 1.1rem;margin-bottom:1rem;font-size:.87rem;color:#c8d8e8;line-height:1.6;}
.warn-box{background:rgba(251,191,36,.07);border:1px solid rgba(251,191,36,.25);border-radius:10px;padding:.85rem 1.1rem;margin-bottom:1rem;font-size:.87rem;color:#fde68a;}
.ok-box{background:rgba(34,197,94,.07);border:1px solid rgba(34,197,94,.25);border-radius:10px;padding:.85rem 1.1rem;margin-bottom:1rem;font-size:.87rem;color:#bbf7d0;}
.section-header{font-size:1rem;font-weight:600;color:#E8630A;border-bottom:1px solid rgba(232,99,10,.18);padding-bottom:.4rem;margin:1.2rem 0 .8rem;}
.stButton>button{background:linear-gradient(135deg,#E8630A,#c4510a) !important;color:white !important;border:none !important;border-radius:8px !important;font-weight:600 !important;}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# FUNÇÕES DE EXTRAÇÃO
# ─────────────────────────────────────────────────────────────────────────────

def extrair_numero_op(texto):
    m = re.search(r'\b(7\d{9})\b', texto)
    return m.group(1) if m else None

def extrair_prazo(texto):
    m = re.search(r'(?:Prazo|Data[- ]Limite|Encerramento)[:\s]+(\d{2}/\d{2}/\d{4})', texto, re.IGNORECASE)
    return m.group(1) if m else ""

def extrair_fabricante(texto):
    m = re.search(r'(?:FABRICANTE|Fabricante|Marca)[:\s]+([A-Z][A-Z0-9 &\-\.]{1,30})', texto)
    if m:
        fab = m.group(1).strip().rstrip(".")
        if len(fab) > 2:
            return fab
    texto_upper = texto.upper()
    for fab in FABRICANTES_CONHECIDOS:
        if fab in texto_upper:
            return fab
    return "Nao identificado"

def extrair_escopo(texto):
    texto_lower = texto.lower()
    encontrados = [esc for esc, kws in ESCOPOS.items() if any(kw in texto_lower for kw in kws)]
    return ", ".join(encontrados) if encontrados else "Outros"

def extrair_descricao(texto):
    m = re.search(r'(?:Descrição|Objeto)[:\s]+(.{10,100})', texto, re.IGNORECASE)
    if m:
        return m.group(1).strip()[:100]
    linhas = [l.strip() for l in texto.split('\n') if len(l.strip()) > 15]
    return linhas[1][:100] if len(linhas) > 1 else ""

def classificar_fase(fase):
    if fase in FASES_LANCADAS:   return "Lancada"
    if fase in FASES_DECLINADAS: return "Declinada"
    if fase in FASES_ADIADAS:    return "Adiada"
    if fase in FASES_ANDAMENTO:  return "Em andamento"
    return "Outro"

# ─────────────────────────────────────────────────────────────────────────────
# CARREGAMENTO
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def carregar_todos_zips():
    arquivos_zip = sorted(ZIPS_DIR.glob("*.zip"))
    if not arquivos_zip:
        return pd.DataFrame(columns=["op","fabricante","escopo","prazo","descricao","arquivo_zip"])
    ops = []
    for zip_path in arquivos_zip:
        try:
            with zipfile.ZipFile(zip_path) as zf:
                pdfs = [n for n in zf.namelist() if n.lower().endswith('.pdf')]
                for pdf_name in pdfs:
                    try:
                        with zf.open(pdf_name) as pf:
                            pdf_bytes = pf.read()
                        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
                            texto = "\n".join(p.extract_text() or "" for p in pdf.pages[:3])
                        op = extrair_numero_op(texto)
                        if op:
                            ops.append({
                                "op":          op,
                                "fabricante":  extrair_fabricante(texto),
                                "escopo":      extrair_escopo(texto),
                                "prazo":       extrair_prazo(texto),
                                "descricao":   extrair_descricao(texto),
                                "arquivo_zip": zip_path.name,
                            })
                    except Exception:
                        pass
        except Exception:
            pass
    if not ops:
        return pd.DataFrame(columns=["op","fabricante","escopo","prazo","descricao","arquivo_zip"])
    return pd.DataFrame(ops).drop_duplicates(subset=["op"])


@st.cache_data(show_spinner=False)
def carregar_pipefy():
    if not PIPEFY_PATH.exists():
        return None
    df = pd.read_excel(PIPEFY_PATH)
    df.columns  = df.columns.str.strip()
    df["op"]        = df["Titulo"].astype(str).str.strip() if "Titulo" in df.columns else df["Título"].astype(str).str.strip()
    df["fase"]      = df["Fase atual"].astype(str).str.strip()
    df["etiquetas"] = df["Etiquetas"].fillna("").astype(str)
    df["criado_em"] = pd.to_datetime(df["Criado em"], errors="coerce")
    return df[["op","fase","etiquetas","criado_em","Criador"]]


@st.cache_data(show_spinner=False)
def carregar_analise():
    if not ANALISE_PATH.exists():
        return None
    dados    = pd.read_excel(ANALISE_PATH, sheet_name="DADOS")
    lancados = pd.read_excel(ANALISE_PATH, sheet_name="LANCADOS") if "LANCADOS" in pd.ExcelFile(ANALISE_PATH).sheet_names else pd.DataFrame()
    dados.columns = dados.columns.str.strip()
    return dados, lancados


@st.cache_data(show_spinner=False)
def carregar_geral():
    if not GERAL_PATH.exists():
        return None
    df = pd.read_excel(GERAL_PATH, sheet_name="Comercial")
    df.columns = df.columns.str.strip()
    return df


def cruzar(df_zips, df_pipefy):
    merged = df_zips.merge(df_pipefy, on="op", how="left")
    merged["fase"]      = merged["fase"].fillna("Ignorada")
    merged["etiquetas"] = merged["etiquetas"].fillna("")
    merged["status"]    = merged["fase"].apply(
        lambda f: "Ignorada" if f == "Ignorada" else classificar_fase(f)
    )
    return merged

# ─────────────────────────────────────────────────────────────────────────────
# CABEÇALHO
# ─────────────────────────────────────────────────────────────────────────────

c1, c2 = st.columns([1, 9])
with c1:
    st.markdown("<div style='font-size:3rem;margin-top:.3rem'>🔧</div>", unsafe_allow_html=True)
with c2:
    st.markdown("# NextSupply")
    st.markdown("<span style='color:#7a95b0;font-size:.9rem'>Banco de Oportunidades Públicas · Comparativo ZIP vs Pipefy · Histórico de Cotações</span>", unsafe_allow_html=True)
st.markdown("---")

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## Status dos Dados")

    n_zips = len(list(ZIPS_DIR.glob("*.zip")))
    st.markdown(f"**ZIPs:** {'✅ ' + str(n_zips) + ' arquivo(s)' if n_zips else '❌ Nenhum em data/zips/'}")
    if n_zips:
        with st.expander(f"Ver {n_zips} ZIPs"):
            for z in sorted(ZIPS_DIR.glob("*.zip")):
                st.caption(z.name)

    st.markdown("---")
    st.markdown("**Pipefy**")
    st.success("✅ pipefy_latest.xlsx") if PIPEFY_PATH.exists() else st.error("❌ Não encontrado")
    novo_pipefy = st.file_uploader("Atualizar Pipefy", type=["xlsx"], key="up_pipefy")
    if novo_pipefy:
        PIPEFY_PATH.parent.mkdir(parents=True, exist_ok=True)
        PIPEFY_PATH.write_bytes(novo_pipefy.read())
        carregar_pipefy.clear()
        st.success("Atualizado!")
        st.rerun()

    st.markdown("---")
    st.markdown("**Analise de Precos**")
    st.success("✅ analise_precos.xlsx") if ANALISE_PATH.exists() else st.warning("⚠️ Não encontrado")
    novo_analise = st.file_uploader("Atualizar Analise", type=["xlsx"], key="up_analise")
    if novo_analise:
        ANALISE_PATH.parent.mkdir(parents=True, exist_ok=True)
        ANALISE_PATH.write_bytes(novo_analise.read())
        carregar_analise.clear()
        st.success("Atualizado!")
        st.rerun()

    st.markdown("---")
    st.markdown("**Planilha Geral (Dispensas)**")
    st.success("✅ planilha_geral.xlsx") if GERAL_PATH.exists() else st.warning("⚠️ Não encontrado")
    novo_geral = st.file_uploader("Atualizar Planilha Geral", type=["xlsx"], key="up_geral")
    if novo_geral:
        GERAL_PATH.parent.mkdir(parents=True, exist_ok=True)
        GERAL_PATH.write_bytes(novo_geral.read())
        carregar_geral.clear()
        st.success("Atualizado!")
        st.rerun()

    st.markdown("---")
    st.caption("Novos ZIPs: copie para data/zips/ e faça push no GitHub Desktop.")

# ─────────────────────────────────────────────────────────────────────────────
# DADOS PRINCIPAIS
# ─────────────────────────────────────────────────────────────────────────────

with st.spinner("Lendo ZIPs e extraindo dados dos PDFs..."):
    df_zips = carregar_todos_zips()
df_pipefy_data = carregar_pipefy()

# ─────────────────────────────────────────────────────────────────────────────
# ABAS
# ─────────────────────────────────────────────────────────────────────────────

tab1, tab2, tab3 = st.tabs(["📊  Comparativo", "🔍  Histórico de Preços", "📄  Consulta da Sophia"])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — COMPARATIVO
# ══════════════════════════════════════════════════════════════════════════════

with tab1:
    if df_zips.empty:
        st.markdown("<div class='warn-box'>⚠️ Nenhum ZIP em <code>data/zips/</code>. Adicione via GitHub Desktop.</div>", unsafe_allow_html=True)
    elif df_pipefy_data is None:
        st.markdown("<div class='warn-box'>⚠️ Relatório Pipefy não encontrado. Faça upload na barra lateral.</div>", unsafe_allow_html=True)
    else:
        df = cruzar(df_zips, df_pipefy_data)
        contagem = df["status"].value_counts()
        total    = len(df)

        metricas = [
            ("Lancadas",     contagem.get("Lancada", 0),      "#4ade80", "Enviadas pro Trello"),
            ("Declinadas",   contagem.get("Declinada", 0),    "#f87171", "Trabalhadas e recusadas"),
            ("Ignoradas",    contagem.get("Ignorada", 0),     "#9ca3af", "Recebidas, nao trabalhadas"),
            ("Adiadas",      contagem.get("Adiada", 0),       "#fbbf24", "Para relancar"),
            ("Em andamento", contagem.get("Em andamento", 0), "#60a5fa", "No Pipefy agora"),
        ]
        cols = st.columns(5)
        for col, (lbl, val, cor, sub) in zip(cols, metricas):
            pct = f"{val/total*100:.0f}%" if total else "-"
            col.markdown(f"""
            <div class='metric-card'>
                <div class='val' style='color:{cor}'>{val}</div>
                <div class='pct' style='color:{cor}'>{pct} do total</div>
                <div class='lbl'>{lbl}</div>
                <div class='sub'>{sub}</div>
            </div>""", unsafe_allow_html=True)

        st.markdown(f"<br><div class='info-box'><strong>{total}</strong> OPs nos ZIPs &nbsp;·&nbsp; <strong>{len(df_pipefy_data)}</strong> no Pipefy &nbsp;·&nbsp; <strong>{n_zips}</strong> ZIP(s)</div>", unsafe_allow_html=True)

        st.markdown("<div class='section-header'>🔎 Filtros</div>", unsafe_allow_html=True)
        f1, f2, f3, f4 = st.columns(4)

        filtro_status = f1.selectbox("Status", ["Todos"] + sorted(df["status"].unique()))

        todos_escopos = set()
        for e in df["escopo"].dropna():
            for s in str(e).split(","):
                s = s.strip()
                if s: todos_escopos.add(s)
        filtro_escopo = f2.selectbox("Escopo", ["Todos"] + sorted(todos_escopos))

        todos_fabs = sorted(df["fabricante"].dropna().unique())
        filtro_fab = f3.selectbox("Fabricante", ["Todos"] + todos_fabs)

        busca_op = f4.text_input("Buscar n° OP")

        df_f = df.copy()
        if filtro_status != "Todos":  df_f = df_f[df_f["status"] == filtro_status]
        if filtro_escopo != "Todos":  df_f = df_f[df_f["escopo"].str.contains(filtro_escopo, na=False)]
        if filtro_fab    != "Todos":  df_f = df_f[df_f["fabricante"] == filtro_fab]
        if busca_op:                  df_f = df_f[df_f["op"].str.contains(busca_op)]

        # Por Escopo
        st.markdown("<div class='section-header'>📈 Por Escopo</div>", unsafe_allow_html=True)
        escopo_status = defaultdict(lambda: defaultdict(int))
        for _, row in df.iterrows():
            escs = [e.strip() for e in str(row["escopo"]).split(",") if e.strip()]
            if not escs: escs = ["Outros"]
            for esc in escs:
                escopo_status[esc][row["status"]] += 1
        linhas = []
        for esc, sd in escopo_status.items():
            tot = sum(sd.values())
            linhas.append({
                "Escopo": esc,
                "Lancadas": sd.get("Lancada",0),
                "Declinadas": sd.get("Declinada",0),
                "Ignoradas": sd.get("Ignorada",0),
                "Adiadas": sd.get("Adiada",0),
                "Total": tot,
                "Taxa lancamento": f"{sd.get('Lancada',0)/tot*100:.0f}%" if tot else "-",
            })
        st.dataframe(pd.DataFrame(linhas).sort_values("Total", ascending=False), use_container_width=True, hide_index=True)

        # Por Fabricante
        st.markdown("<div class='section-header'>🏭 Por Fabricante</div>", unsafe_allow_html=True)
        fab_status = defaultdict(lambda: defaultdict(int))
        for _, row in df.iterrows():
            fab_status[row["fabricante"]][row["status"]] += 1
        linhas_fab = []
        for fab, sd in fab_status.items():
            tot = sum(sd.values())
            linhas_fab.append({
                "Fabricante": fab,
                "Lancadas": sd.get("Lancada",0),
                "Declinadas": sd.get("Declinada",0),
                "Ignoradas": sd.get("Ignorada",0),
                "Adiadas": sd.get("Adiada",0),
                "Total": tot,
            })
        st.dataframe(pd.DataFrame(linhas_fab).sort_values("Total", ascending=False), use_container_width=True, hide_index=True)

        # Detalhamento
        st.markdown(f"<div class='section-header'>📋 Detalhamento por OP ({len(df_f)} registros)</div>", unsafe_allow_html=True)
        colunas = [c for c in ["op","status","fabricante","escopo","prazo","descricao","fase","arquivo_zip"] if c in df_f.columns]
        df_show = df_f[colunas].copy()
        df_show.columns = ["N° OP","Status","Fabricante","Escopo","Prazo","Descricao","Fase Pipefy","Arquivo ZIP"][:len(colunas)]
        st.dataframe(df_show, use_container_width=True, hide_index=True)

        csv = df_show.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Exportar (.csv)", data=csv, file_name="nextsupply_ops.csv", mime="text/csv")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — HISTÓRICO DE PREÇOS
# ══════════════════════════════════════════════════════════════════════════════

with tab2:
    st.markdown("<div class='section-header'>🔍 Busca no Histórico de Cotações</div>", unsafe_allow_html=True)
    st.markdown("<div class='info-box'>Busca na <strong>Analise de Precos</strong> e na <strong>Planilha Geral de Dispensas</strong> (uso consultivo).</div>", unsafe_allow_html=True)

    analise = carregar_analise()
    geral   = carregar_geral()

    if analise is None and geral is None:
        st.info("Nenhum arquivo de histórico. Faça upload na barra lateral.")
    else:
        c_b, c_t = st.columns([3, 1])
        termo      = c_b.text_input("Part Number, Fabricante ou N° OP", placeholder="Ex: FMU41, EMERSON, 7004565050")
        tipo_busca = c_t.selectbox("Buscar em", ["Ambos", "Analise de Precos", "Planilha Geral"])

        if termo:
            t = termo.lower()
            if analise and tipo_busca in ("Ambos", "Analise de Precos"):
                dados, _ = analise
                st.markdown("#### Analise de Precos")
                cols_b = [c for c in ["OP","PART NUMBER","FABRICANTE"] if c in dados.columns]
                mask = dados[cols_b].apply(lambda col: col.astype(str).str.lower().str.contains(t, na=False)).any(axis=1)
                res  = dados[mask]
                if not res.empty:
                    show = [c for c in ["DATA","OP","ITEM","PART NUMBER","FABRICANTE","NOSSO PRECO UNIT.","NOSSO PRECO TOTAL","NOSSO PRAZO","Resultado Esperado"] if c in res.columns]
                    if not show:
                        show = list(res.columns[:8])
                    st.dataframe(res[show], use_container_width=True, hide_index=True)
                    st.caption(f"{len(res)} registros")
                else:
                    st.info("Nenhum resultado na Analise de Precos.")

            if geral is not None and tipo_busca in ("Ambos", "Planilha Geral"):
                st.markdown("#### Planilha Geral — Dispensas (uso consultivo)")
                cols_b = [c for c in ["Número da OP","PN","FABRICANTE","DESCRIÇÃO BREVE"] if c in geral.columns]
                mask = geral[cols_b].apply(lambda col: col.astype(str).str.lower().str.contains(t, na=False)).any(axis=1)
                res_g = geral[mask]
                if not res_g.empty:
                    show = [c for c in ["Número da OP","ITEM","FABRICANTE","PN","DESCRIÇÃO BREVE","VALOR UNITÁRIO","VALOR TOTAL","COTADOR"] if c in res_g.columns]
                    st.dataframe(res_g[show], use_container_width=True, hide_index=True)
                    st.caption(f"{len(res_g)} registros (dispensas)")
                else:
                    st.info("Nenhum resultado na Planilha Geral.")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — MÓDULO SOPHIA
# ══════════════════════════════════════════════════════════════════════════════

with tab3:
    st.markdown("<div class='section-header'>📄 Consulta da Sophia</div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='info-box'>"
        "Suba o PDF de uma oportunidade publica e descubra:<br>"
        "✅ Ja foi lancada? &nbsp; ❌ Foi declinada? &nbsp; ⚪ Nunca trabalhamos?<br>"
        "E tambem verifica se existe historico de preco na Analise de Precos."
        "</div>",
        unsafe_allow_html=True,
    )

    pdf_sophia = st.file_uploader("📎 Suba aqui o PDF da oportunidade", type=["pdf"], key="sophia_pdf")

    if pdf_sophia:
        with st.spinner("Lendo PDF..."):
            try:
                conteudo_pdf = pdf_sophia.read()
                with pdfplumber.open(BytesIO(conteudo_pdf)) as pdf:
                    texto = "\n".join(p.extract_text() or "" for p in pdf.pages)

                op_num     = extrair_numero_op(texto)
                fabricante = extrair_fabricante(texto)
                escopo     = extrair_escopo(texto)
                prazo      = extrair_prazo(texto)

                st.markdown("<div class='section-header'>📌 Dados identificados no PDF</div>", unsafe_allow_html=True)
                ca, cb, cc, cd = st.columns(4)
                ca.metric("N° OP",      op_num or "Nao identificado")
                cb.metric("Fabricante", fabricante)
                cc.metric("Escopo",     escopo)
                cd.metric("Prazo",      prazo or "-")

                st.markdown("---")

                # STATUS NO PIPEFY
                st.markdown("<div class='section-header'>📊 Status no Pipefy</div>", unsafe_allow_html=True)

                if df_pipefy_data is not None and op_num:
                    linha = df_pipefy_data[df_pipefy_data["op"] == op_num]
                    if not linha.empty:
                        fase   = linha.iloc[0]["fase"]
                        etiq   = linha.iloc[0]["etiquetas"]
                        criado = linha.iloc[0]["criado_em"]
                        status = classificar_fase(fase)

                        box_map = {
                            "Lancada":      ("ok-box",   "✅ LANCADA"),
                            "Declinada":    ("warn-box", "❌ DECLINADA"),
                            "Adiada":       ("warn-box", "⏳ ADIADA"),
                            "Em andamento": ("info-box", "🔄 EM ANDAMENTO"),
                            "Outro":        ("info-box", "ℹ️ NO PIPEFY"),
                        }
                        box_class, titulo = box_map.get(status, ("info-box", "ℹ️ NO PIPEFY"))
                        criado_str = str(criado)[:10] if pd.notna(criado) else "-"
                        st.markdown(
                            f"<div class='{box_class}'>"
                            f"<strong>{titulo}</strong><br>"
                            f"Fase: <strong>{fase}</strong><br>"
                            f"Etiquetas: {etiq}<br>"
                            f"Criado em: {criado_str}"
                            f"</div>",
                            unsafe_allow_html=True,
                        )
                    else:
                        if not df_zips.empty and op_num in df_zips["op"].values:
                            st.markdown("<div class='warn-box'>⚪ <strong>IGNORADA</strong> — Recebida no ZIP mas nunca trabalhada no Pipefy.</div>", unsafe_allow_html=True)
                        else:
                            st.markdown("<div class='warn-box'>⚪ <strong>NAO ENCONTRADA</strong> — Nao esta nos ZIPs nem no Pipefy.</div>", unsafe_allow_html=True)
                elif df_pipefy_data is None:
                    st.info("Carregue o relatório Pipefy na barra lateral para verificar o status.")
                else:
                    st.warning("N° de OP nao identificado no PDF.")

                # HISTÓRICO DE PREÇO
                st.markdown("<div class='section-header'>💰 Historico na Analise de Precos</div>", unsafe_allow_html=True)
                analise = carregar_analise()
                if analise is None:
                    st.info("Carregue a Analise de Precos na barra lateral.")
                else:
                    dados, _ = analise
                    termos = [t for t in [op_num, fabricante] if t and t != "Nao identificado"]
                    resultados = []
                    for termo in termos:
                        cols_b = [c for c in ["OP","PART NUMBER","FABRICANTE"] if c in dados.columns]
                        mask   = dados[cols_b].apply(
                            lambda col: col.astype(str).str.upper().str.contains(termo.upper(), na=False)
                        ).any(axis=1)
                        resultados.append(dados[mask])

                    df_hist = pd.concat(resultados).drop_duplicates() if resultados else pd.DataFrame()
                    if not df_hist.empty:
                        st.success(f"✅ {len(df_hist)} registros históricos encontrados!")
                        show = [c for c in ["DATA","OP","ITEM","PART NUMBER","FABRICANTE","NOSSO PRECO UNIT.","NOSSO PRAZO","Resultado Esperado"] if c in df_hist.columns]
                        if not show:
                            show = list(df_hist.columns[:8])
                        st.dataframe(df_hist[show], use_container_width=True, hide_index=True)
                    else:
                        st.info("⚪ Nenhum historico de preco encontrado para esta oportunidade.")

            except Exception as e:
                st.error(f"Erro ao processar PDF: {e}")
