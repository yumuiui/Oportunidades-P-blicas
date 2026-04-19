"""
NextSupply — Banco de Dados de Oportunidades Públicas
ZIPs ficam em data/zips/ no repositório — carregados automaticamente.
Pipefy em data/pipefy_latest.xlsx — atualizado quando necessário.
"""

import re
import zipfile
import tempfile
from io import BytesIO
from pathlib import Path
from collections import defaultdict

import pandas as pd
import pdfplumber
import streamlit as st

# ─────────────────────────────────────────────────────────────────────────────
# CAMINHOS — pastas dentro do repositório
# ─────────────────────────────────────────────────────────────────────────────

BASE_DIR   = Path(__file__).parent
ZIPS_DIR   = BASE_DIR / "data" / "zips"
PIPEFY_PATH = BASE_DIR / "data" / "pipefy_latest.xlsx"
ANALISE_PATH = BASE_DIR / "data" / "analise_precos.xlsx"
GERAL_PATH   = BASE_DIR / "data" / "planilha_geral.xlsx"

ZIPS_DIR.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="NextSupply | OPS Database",
    page_icon="🔧",
    layout="wide",
)

FASES_LANCADAS   = {"Enviado pro Trello", "LANÇADO", "PEDIDO DE COTAÇÃO ENVIADO", "COTADAS TOTALMENTE"}
FASES_DECLINADAS = {"Declinadas"}
FASES_ADIADAS    = {"ADIADAS/RELANÇAR"}
FASES_ANDAMENTO  = {"RECEBIDAS"}

# ─────────────────────────────────────────────────────────────────────────────
# CSS — Identidade NextSupply
# ─────────────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
:root {
    --bg:      #06111f;
    --surface: #0c1d30;
    --surface2:#112540;
    --orange:  #E8630A;
    --border:  rgba(232,99,10,0.28);
    --text:    #e8eef6;
    --muted:   #7a95b0;
}

.stApp {
    background:
        radial-gradient(ellipse 60% 40% at 0% 0%, rgba(232,99,10,0.10), transparent),
        linear-gradient(180deg, #060f1c 0%, #040b16 100%);
    color: var(--text);
}
.block-container { max-width: 1400px; padding-top: 1.5rem; }
[data-testid="stHeader"] { background: transparent; }

[data-testid="stSidebar"] {
    background: #0c1d30 !important;
    border-right: 1px solid rgba(232,99,10,0.2) !important;
}
[data-testid="stSidebar"] * { color: #e8eef6 !important; }

/* Abas */
.stTabs [data-baseweb="tab-list"] {
    background: transparent;
    border-bottom: 1px solid rgba(232,99,10,0.2);
    gap: 0.5rem;
}
.stTabs [data-baseweb="tab"] {
    background: rgba(12,29,48,0.6);
    border: 1px solid rgba(232,99,10,0.15);
    border-radius: 8px 8px 0 0;
    color: #7a95b0 !important;
    padding: 0.5rem 1.2rem;
}
.stTabs [aria-selected="true"] {
    background: rgba(232,99,10,0.12) !important;
    border-color: rgba(232,99,10,0.4) !important;
    color: #E8630A !important;
}

/* Dataframe */
[data-testid="stDataFrame"] { border: 1px solid rgba(232,99,10,0.2); border-radius: 10px; }

/* Cards de métricas */
.metric-card {
    background: linear-gradient(135deg, #0c1d30, #112540);
    border: 1px solid rgba(232,99,10,0.22);
    border-radius: 16px;
    padding: 1.4rem 1.2rem;
    text-align: center;
    height: 100%;
}
.metric-card .val  { font-size: 2.4rem; font-weight: 700; line-height: 1; }
.metric-card .pct  { font-size: 0.72rem; margin-top: 3px; }
.metric-card .lbl  { font-size: 0.78rem; color: #7a95b0; margin-top: 0.4rem; text-transform: uppercase; letter-spacing: 0.06em; }
.metric-card .sub  { font-size: 0.82rem; color: #a0b4c8; margin-top: 0.3rem; }

/* Badges de status */
.badge-lancada   { background:rgba(34,197,94,0.15);  color:#4ade80; border:1px solid rgba(34,197,94,0.3);  border-radius:20px; padding:2px 10px; font-size:0.75rem; white-space:nowrap; }
.badge-declinada { background:rgba(239,68,68,0.15);  color:#f87171; border:1px solid rgba(239,68,68,0.3);  border-radius:20px; padding:2px 10px; font-size:0.75rem; white-space:nowrap; }
.badge-ignorada  { background:rgba(156,163,175,0.15);color:#9ca3af; border:1px solid rgba(156,163,175,0.3);border-radius:20px; padding:2px 10px; font-size:0.75rem; white-space:nowrap; }
.badge-adiada    { background:rgba(251,191,36,0.15); color:#fbbf24; border:1px solid rgba(251,191,36,0.3); border-radius:20px; padding:2px 10px; font-size:0.75rem; white-space:nowrap; }
.badge-andamento { background:rgba(96,165,250,0.15); color:#60a5fa; border:1px solid rgba(96,165,250,0.3); border-radius:20px; padding:2px 10px; font-size:0.75rem; white-space:nowrap; }

/* Caixas informativas */
.info-box {
    background: rgba(232,99,10,0.07);
    border: 1px solid rgba(232,99,10,0.2);
    border-radius: 10px;
    padding: 0.85rem 1.1rem;
    margin-bottom: 1rem;
    font-size: 0.87rem;
    color: #c8d8e8;
    line-height: 1.6;
}
.warn-box {
    background: rgba(251,191,36,0.07);
    border: 1px solid rgba(251,191,36,0.25);
    border-radius: 10px;
    padding: 0.85rem 1.1rem;
    margin-bottom: 1rem;
    font-size: 0.87rem;
    color: #fde68a;
}
.section-header {
    font-size: 1rem;
    font-weight: 600;
    color: #E8630A;
    border-bottom: 1px solid rgba(232,99,10,0.18);
    padding-bottom: 0.4rem;
    margin: 1.2rem 0 0.8rem;
}

/* Botões */
.stButton>button {
    background: linear-gradient(135deg, #E8630A, #c4510a) !important;
    color: white !important;
    border: none !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
}
.stButton>button:hover { opacity: 0.88 !important; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# FUNÇÕES UTILITÁRIAS
# ─────────────────────────────────────────────────────────────────────────────

def extrair_numero_op(texto: str) -> str | None:
    m = re.search(r'\b(7\d{9})\b', texto)
    return m.group(1) if m else None

def extrair_prazo(texto: str) -> str:
    m = re.search(r'(?:Prazo|Data[- ]Limite|Encerramento)[:\s]+(\d{2}/\d{2}/\d{4})', texto, re.IGNORECASE)
    return m.group(1) if m else ""

def extrair_descricao(texto: str) -> str:
    m = re.search(r'(?:Descrição|Objeto)[:\s]+(.{10,80})', texto, re.IGNORECASE)
    if m:
        return m.group(1).strip()[:80]
    linhas = [l.strip() for l in texto.split('\n') if len(l.strip()) > 15]
    return linhas[1][:80] if len(linhas) > 1 else ""

# ─────────────────────────────────────────────────────────────────────────────
# CARREGAMENTO DE DADOS (com cache)
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def carregar_todos_zips() -> pd.DataFrame:
    """
    Lê todos os ZIPs em data/zips/ e extrai os números de OP.
    Cache é invalidado quando a lista de arquivos muda.
    """
    arquivos_zip = sorted(ZIPS_DIR.glob("*.zip"))
    if not arquivos_zip:
        return pd.DataFrame(columns=["op","arquivo_zip","prazo","descricao"])

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
                            texto = "\n".join(p.extract_text() or "" for p in pdf.pages[:2])
                        op = extrair_numero_op(texto)
                        if op:
                            ops.append({
                                "op":          op,
                                "arquivo_zip": zip_path.name,
                                "prazo":       extrair_prazo(texto),
                                "descricao":   extrair_descricao(texto),
                            })
                    except Exception:
                        pass
        except Exception:
            pass

    if not ops:
        return pd.DataFrame(columns=["op","arquivo_zip","prazo","descricao"])
    return pd.DataFrame(ops).drop_duplicates(subset=["op"])


@st.cache_data(show_spinner=False)
def carregar_pipefy_arquivo() -> pd.DataFrame | None:
    if not PIPEFY_PATH.exists():
        return None
    df = pd.read_excel(PIPEFY_PATH)
    df.columns = df.columns.str.strip()
    df["op"]        = df["Título"].astype(str).str.strip()
    df["fase"]      = df["Fase atual"].astype(str).str.strip()
    df["etiquetas"] = df["Etiquetas"].fillna("").astype(str)
    df["criado_em"] = pd.to_datetime(df["Criado em"], errors="coerce")
    return df[["op","fase","etiquetas","criado_em","Criador"]]


@st.cache_data(show_spinner=False)
def carregar_analise_arquivo() -> tuple[pd.DataFrame, pd.DataFrame] | None:
    if not ANALISE_PATH.exists():
        return None
    dados    = pd.read_excel(ANALISE_PATH, sheet_name="DADOS")
    lancados = pd.read_excel(ANALISE_PATH, sheet_name="LANÇADOS")
    dados.columns    = dados.columns.str.strip()
    lancados.columns = lancados.columns.str.strip()
    return dados, lancados


@st.cache_data(show_spinner=False)
def carregar_geral_arquivo() -> pd.DataFrame | None:
    if not GERAL_PATH.exists():
        return None
    df = pd.read_excel(GERAL_PATH, sheet_name="Comercial")
    df.columns = df.columns.str.strip()
    return df


def classificar_fase(fase: str) -> str:
    if fase in FASES_LANCADAS:   return "Lançada"
    if fase in FASES_DECLINADAS: return "Declinada"
    if fase in FASES_ADIADAS:    return "Adiada"
    if fase in FASES_ANDAMENTO:  return "Em andamento"
    return "Outro"


def cruzar(df_zips: pd.DataFrame, df_pipefy: pd.DataFrame) -> pd.DataFrame:
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
    st.markdown(
        "<div style='font-size:3rem;margin-top:0.3rem'>🔧</div>",
        unsafe_allow_html=True
    )
with c2:
    st.markdown("# NextSupply")
    st.markdown(
        "<span style='color:#7a95b0;font-size:0.9rem'>"
        "Banco de Oportunidades Públicas · Comparativo ZIP vs Pipefy · Histórico de Cotações"
        "</span>",
        unsafe_allow_html=True,
    )

st.markdown("---")

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR — status dos arquivos + atualização do Pipefy
# ─────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## 📁 Status dos Dados")

    # ZIPs
    n_zips = len(list(ZIPS_DIR.glob("*.zip")))
    st.markdown(
        f"**ZIPs carregados:** {'✅ ' + str(n_zips) + ' arquivo(s)' if n_zips else '❌ Nenhum ZIP em data/zips/'}"
    )
    if n_zips:
        zips_lista = sorted(ZIPS_DIR.glob("*.zip"))
        with st.expander(f"Ver os {n_zips} ZIPs"):
            for z in zips_lista:
                st.caption(z.name)

    st.markdown("---")

    # Pipefy
    st.markdown("**Relatório Pipefy**")
    if PIPEFY_PATH.exists():
        st.success("✅ pipefy_latest.xlsx encontrado")
    else:
        st.error("❌ Não encontrado em data/")

    st.markdown("**Atualizar Pipefy** (substitui o atual):")
    novo_pipefy = st.file_uploader("", type=["xlsx"], key="upload_pipefy", label_visibility="collapsed")
    if novo_pipefy:
        PIPEFY_PATH.parent.mkdir(parents=True, exist_ok=True)
        PIPEFY_PATH.write_bytes(novo_pipefy.read())
        carregar_pipefy_arquivo.clear()
        st.success("✅ Pipefy atualizado!")
        st.rerun()

    st.markdown("---")

    # Análise de Preços
    st.markdown("**Análise de Preços**")
    if ANALISE_PATH.exists():
        st.success("✅ analise_precos.xlsx encontrado")
    else:
        st.warning("⚠️ Não encontrado — funcionalidades de preço desabilitadas")
    novo_analise = st.file_uploader("Substituir Análise de Preços", type=["xlsx"], key="upload_analise", label_visibility="collapsed")
    if novo_analise:
        ANALISE_PATH.parent.mkdir(parents=True, exist_ok=True)
        ANALISE_PATH.write_bytes(novo_analise.read())
        carregar_analise_arquivo.clear()
        st.success("✅ Análise de Preços atualizada!")
        st.rerun()

    st.markdown("---")

    # Planilha Geral
    st.markdown("**Planilha Geral (Dispensas)**")
    if GERAL_PATH.exists():
        st.success("✅ planilha_geral.xlsx encontrado")
    else:
        st.warning("⚠️ Não encontrado")
    novo_geral = st.file_uploader("Substituir Planilha Geral", type=["xlsx"], key="upload_geral", label_visibility="collapsed")
    if novo_geral:
        GERAL_PATH.parent.mkdir(parents=True, exist_ok=True)
        GERAL_PATH.write_bytes(novo_geral.read())
        carregar_geral_arquivo.clear()
        st.success("✅ Planilha Geral atualizada!")
        st.rerun()

    st.markdown("---")
    st.markdown(
        "<div style='font-size:0.75rem;color:#5a7590;line-height:1.6'>"
        "📌 Para adicionar ZIPs novos:<br>"
        "Coloque os arquivos em <code>data/zips/</code><br>"
        "e faça push para o GitHub.<br>"
        "O app recarrega automaticamente."
        "</div>",
        unsafe_allow_html=True,
    )

# ─────────────────────────────────────────────────────────────────────────────
# CARREGAR DADOS
# ─────────────────────────────────────────────────────────────────────────────

with st.spinner("Carregando ZIPs do repositório…"):
    df_zips = carregar_todos_zips()

df_pipefy = carregar_pipefy_arquivo()

# ─────────────────────────────────────────────────────────────────────────────
# ABAS PRINCIPAIS
# ─────────────────────────────────────────────────────────────────────────────

tab1, tab2, tab3 = st.tabs([
    "📊  Comparativo",
    "🔍  Histórico de Preços",
    "📄  Consulta da Sophia",
])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — COMPARATIVO
# ══════════════════════════════════════════════════════════════════════════════

with tab1:
    if df_zips.empty:
        st.markdown("""
        <div class='warn-box'>
            ⚠️ Nenhum ZIP encontrado em <code>data/zips/</code>.<br>
            Adicione os ZIPs do Petronect nessa pasta no repositório e faça push.
        </div>
        """, unsafe_allow_html=True)

    elif df_pipefy is None:
        st.markdown("""
        <div class='warn-box'>
            ⚠️ Relatório do Pipefy não encontrado.<br>
            Faça upload na barra lateral para habilitar o comparativo.
        </div>
        """, unsafe_allow_html=True)

    else:
        df = cruzar(df_zips, df_pipefy)
        contagem = df["status"].value_counts()
        total    = len(df)

        # ── Métricas ──
        metricas = [
            ("Lançadas",     contagem.get("Lançada", 0),      "#4ade80", "Enviadas pro Trello"),
            ("Declinadas",   contagem.get("Declinada", 0),    "#f87171", "Trabalhadas e recusadas"),
            ("Ignoradas",    contagem.get("Ignorada", 0),     "#9ca3af", "Recebidas, não trabalhadas"),
            ("Adiadas",      contagem.get("Adiada", 0),       "#fbbf24", "Para relançar"),
            ("Em andamento", contagem.get("Em andamento", 0), "#60a5fa", "No Pipefy agora"),
        ]
        cols = st.columns(5)
        for col, (lbl, val, cor, sub) in zip(cols, metricas):
            pct = f"{val/total*100:.0f}%" if total else "—"
            col.markdown(f"""
            <div class='metric-card'>
                <div class='val' style='color:{cor}'>{val}</div>
                <div class='pct' style='color:{cor}'>{pct} do total</div>
                <div class='lbl'>{lbl}</div>
                <div class='sub'>{sub}</div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown(
            f"<br><div class='info-box'>"
            f"<strong>{total}</strong> OPs únicas nos ZIPs &nbsp;·&nbsp; "
            f"<strong>{len(df_pipefy)}</strong> registros no Pipefy &nbsp;·&nbsp; "
            f"<strong>{n_zips}</strong> arquivo(s) ZIP carregado(s)"
            f"</div>",
            unsafe_allow_html=True,
        )

        # ── Filtros ──
        st.markdown("<div class='section-header'>🔎 Filtros</div>", unsafe_allow_html=True)
        f1, f2, f3 = st.columns(3)

        status_opts  = ["Todos"] + sorted(df["status"].unique())
        filtro_status = f1.selectbox("Status", status_opts)

        todas_etiquetas = set()
        for e in df["etiquetas"].dropna():
            for tag in str(e).split(","):
                t = tag.strip()
                if t and t.lower() != "nan":
                    todas_etiquetas.add(t)
        etiqueta_opts    = ["Todas"] + sorted(todas_etiquetas)
        filtro_etiqueta  = f2.selectbox("Categoria (Etiqueta Pipefy)", etiqueta_opts)
        busca_op         = f3.text_input("Buscar nº OP")

        df_filtrado = df.copy()
        if filtro_status   != "Todos":
            df_filtrado = df_filtrado[df_filtrado["status"] == filtro_status]
        if filtro_etiqueta != "Todas":
            df_filtrado = df_filtrado[df_filtrado["etiquetas"].str.contains(filtro_etiqueta, na=False)]
        if busca_op:
            df_filtrado = df_filtrado[df_filtrado["op"].str.contains(busca_op)]

        # ── Por Categoria ──
        st.markdown("<div class='section-header'>📈 Distribuição por Categoria</div>", unsafe_allow_html=True)

        etiqueta_status = defaultdict(lambda: defaultdict(int))
        for _, row in df.iterrows():
            tags = [t.strip() for t in str(row["etiquetas"]).split(",")
                    if t.strip() and t.strip().lower() != "nan"]
            if not tags:
                tags = ["Sem categoria"]
            for tag in tags:
                etiqueta_status[tag][row["status"]] += 1

        linhas = []
        for tag, sd in etiqueta_status.items():
            total_tag = sum(sd.values())
            tx_lancamento = f"{sd.get('Lançada',0)/total_tag*100:.0f}%" if total_tag else "—"
            linhas.append({
                "Categoria":       tag,
                "Lançadas":        sd.get("Lançada", 0),
                "Declinadas":      sd.get("Declinada", 0),
                "Ignoradas":       sd.get("Ignorada", 0),
                "Adiadas":         sd.get("Adiada", 0),
                "Em andamento":    sd.get("Em andamento", 0),
                "Total":           total_tag,
                "Taxa lançamento": tx_lancamento,
            })
        df_cat = pd.DataFrame(linhas).sort_values("Total", ascending=False)
        st.dataframe(df_cat, use_container_width=True, hide_index=True)

        # ── Tabela detalhada ──
        st.markdown(
            f"<div class='section-header'>📋 Detalhamento por OP "
            f"<span style='font-size:0.82rem;font-weight:400;color:#7a95b0'>({len(df_filtrado)} registros)</span>"
            f"</div>",
            unsafe_allow_html=True,
        )

        colunas_show = ["op","status","etiquetas","prazo","descricao","fase","arquivo_zip"]
        colunas_show = [c for c in colunas_show if c in df_filtrado.columns]
        df_display   = df_filtrado[colunas_show].copy()
        df_display.columns = ["Nº OP","Status","Etiquetas","Prazo","Descrição","Fase Pipefy","Arquivo ZIP"][: len(colunas_show)]

        st.dataframe(df_display, use_container_width=True, hide_index=True)

        csv = df_display.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Exportar tabela completa (.csv)",
            data=csv, file_name="nextsupply_ops_comparativo.csv", mime="text/csv",
        )

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — HISTÓRICO DE PREÇOS
# ══════════════════════════════════════════════════════════════════════════════

with tab2:
    st.markdown("<div class='section-header'>🔍 Busca no Histórico de Cotações</div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='info-box'>"
        "Busca na <strong>Análise de Preços</strong> e na <strong>Planilha Geral de Dispensas</strong> (uso consultivo). "
        "Útil para saber se um item já foi cotado antes, a que preço e qual foi o resultado."
        "</div>",
        unsafe_allow_html=True,
    )

    analise = carregar_analise_arquivo()
    geral   = carregar_geral_arquivo()

    if analise is None and geral is None:
        st.info("Nenhum arquivo de histórico encontrado. Faça upload na barra lateral.")
    else:
        c_busca, c_tipo = st.columns([3, 1])
        termo      = c_busca.text_input("Part Number, Fabricante ou Nº OP", placeholder="Ex: FMU41, EMERSON, 7004565050")
        tipo_busca = c_tipo.selectbox("Buscar em", ["Ambos", "Análise de Preços", "Planilha Geral"])

        if termo:
            t = termo.lower()

            if analise and tipo_busca in ("Ambos", "Análise de Preços"):
                dados, _ = analise
                st.markdown("#### Análise de Preços")
                cols_b = [c for c in ["OP","PART NUMBER","FABRICANTE"] if c in dados.columns]
                mask   = dados[cols_b].apply(lambda col: col.astype(str).str.lower().str.contains(t, na=False)).any(axis=1)
                res    = dados[mask]
                if not res.empty:
                    show = [c for c in ["DATA","OP","ITEM","PART NUMBER","FABRICANTE","NOSSO PREÇO UNIT.","NOSSO PREÇO TOTAL","NOSSO PRAZO","Resultado Esperado"] if c in res.columns]
                    st.dataframe(res[show], use_container_width=True, hide_index=True)
                    st.caption(f"{len(res)} registros encontrados")
                else:
                    st.info("Nenhum resultado na Análise de Preços.")

            if geral is not None and tipo_busca in ("Ambos", "Planilha Geral"):
                st.markdown("#### Planilha Geral — Dispensas *(uso consultivo)*")
                cols_b = [c for c in ["Número da OP","PN","FABRICANTE","DESCRIÇÃO BREVE"] if c in geral.columns]
                mask   = geral[cols_b].apply(lambda col: col.astype(str).str.lower().str.contains(t, na=False)).any(axis=1)
                res_g  = geral[mask]
                if not res_g.empty:
                    show = [c for c in ["Número da OP","ITEM","FABRICANTE","PN","DESCRIÇÃO BREVE","VALOR UNITÁRIO","VALOR TOTAL","COTADOR"] if c in res_g.columns]
                    st.dataframe(res_g[show], use_container_width=True, hide_index=True)
                    st.caption(f"{len(res_g)} registros (dispensas — uso consultivo)")
                else:
                    st.info("Nenhum resultado na Planilha Geral.")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — MÓDULO SOPHIA
# ══════════════════════════════════════════════════════════════════════════════

with tab3:
    st.markdown("<div class='section-header'>📄 Consulta Rápida — Sophia</div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='info-box'>"
        "Suba o PDF de uma oportunidade pública → o sistema identifica os fabricantes e Part Numbers "
        "e verifica automaticamente se já existe histórico de preço na Análise de Preços."
        "</div>",
        unsafe_allow_html=True,
    )

    analise = carregar_analise_arquivo()
    if analise is None:
        st.info("Faça upload da Análise de Preços na barra lateral para habilitar este módulo.")
    else:
        pdf_sophia = st.file_uploader("📎 PDF da oportunidade", type=["pdf"], key="sophia_pdf")

        if pdf_sophia:
            with st.spinner("Lendo PDF…"):
                try:
                    with pdfplumber.open(BytesIO(pdf_sophia.read())) as pdf:
                        texto = "\n".join(p.extract_text() or "" for p in pdf.pages)

                    op_num = extrair_numero_op(texto)

                    col_a, col_b = st.columns(2)
                    with col_a:
                        if op_num:
                            st.success(f"✅ Oportunidade: **{op_num}**")
                        else:
                            st.warning("Nº de OP não identificado no PDF.")

                    FABRICANTES = [
                        "EMERSON","ABB","SIEMENS","PARKER","SKF","ENDRESS","DANFOSS",
                        "FESTO","IFM","ROCKWELL","SCHNEIDER","YOKOGAWA","HONEYWELL",
                        "FISHER","FLOWSERVE","CRANE","VELAN","CAMERON","SPIRAX",
                        "SEATRAX","DONALDSON","BALLUFF","STAHL","EATON","RITTAL",
                        "CISCO","STEYR","PELICAN","SCHOENROCK","ENERPAC",
                    ]
                    fabs = [f for f in FABRICANTES if f in texto.upper()]

                    with col_b:
                        st.markdown("**Fabricantes identificados:**")
                        if fabs:
                            st.markdown(" · ".join(f"`{f}`" for f in fabs))
                        else:
                            st.caption("Nenhum fabricante da lista identificado")

                    # Busca histórica
                    st.markdown("<div class='section-header'>Histórico na Análise de Preços</div>", unsafe_allow_html=True)
                    dados, _ = analise

                    termos_busca = fabs + ([op_num] if op_num else [])
                    if not termos_busca:
                        st.info("Nenhum fabricante ou OP identificado para buscar.")
                    else:
                        resultados = []
                        for termo in termos_busca:
                            cols_b = [c for c in ["OP","PART NUMBER","FABRICANTE"] if c in dados.columns]
                            mask   = dados[cols_b].apply(
                                lambda col: col.astype(str).str.upper().str.contains(termo.upper(), na=False)
                            ).any(axis=1)
                            resultados.append(dados[mask])

                        df_hist = pd.concat(resultados).drop_duplicates() if resultados else pd.DataFrame()

                        if not df_hist.empty:
                            st.success(f"✅ {len(df_hist)} registros históricos encontrados!")
                            show = [c for c in ["DATA","OP","ITEM","PART NUMBER","FABRICANTE","NOSSO PREÇO UNIT.","NOSSO PRAZO","Resultado Esperado"] if c in df_hist.columns]
                            st.dataframe(df_hist[show], use_container_width=True, hide_index=True)
                        else:
                            st.info("⚪ Nenhum histórico encontrado para esta oportunidade.")

                except Exception as e:
                    st.error(f"Erro ao processar PDF: {e}")
