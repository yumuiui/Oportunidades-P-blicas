"""
Value OPS — Banco de Dados de Oportunidades Públicas
Compara ZIPs recebidos vs Pipefy | Consulta histórico de preços
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
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Value OPS | Banco de Oportunidades",
    page_icon="📊",
    layout="wide",
)

FASES_LANCADAS = {"Enviado pro Trello", "LANÇADO", "PEDIDO DE COTAÇÃO ENVIADO", "COTADAS TOTALMENTE"}
FASES_DECLINADAS = {"Declinadas"}
FASES_ADIADAS = {"ADIADAS/RELANÇAR"}
FASES_RECEBIDAS = {"RECEBIDAS"}

# ─────────────────────────────────────────────────────────────────────────────
# CSS
# ─────────────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
:root {
    --bg: #06111f; --surface: #0c1d30; --surface2: #112540;
    --orange: #E8630A; --border: rgba(232,99,10,0.28);
    --text: #e8eef6; --muted: #7a95b0;
}
.stApp {
    background: radial-gradient(ellipse 60% 40% at 0% 0%, rgba(232,99,10,0.10), transparent),
                linear-gradient(180deg, #060f1c 0%, #040b16 100%);
    color: var(--text);
}
.block-container { max-width: 1400px; padding-top: 1.5rem; }
[data-testid="stHeader"] { background: transparent; }
[data-testid="stSidebar"] { background: #0c1d30 !important; border-right: 1px solid rgba(232,99,10,0.2) !important; }

.metric-card {
    background: linear-gradient(135deg, #0c1d30, #112540);
    border: 1px solid rgba(232,99,10,0.25);
    border-radius: 16px;
    padding: 1.4rem 1.6rem;
    text-align: center;
}
.metric-card .val { font-size: 2.4rem; font-weight: 700; line-height: 1; }
.metric-card .lbl { font-size: 0.78rem; color: #7a95b0; margin-top: 0.4rem; text-transform: uppercase; letter-spacing: 0.06em; }
.metric-card .sub { font-size: 0.85rem; color: #a0b4c8; margin-top: 0.3rem; }

.badge-lancada   { background: rgba(34,197,94,0.15);  color: #4ade80; border: 1px solid rgba(34,197,94,0.3);  border-radius: 20px; padding: 2px 10px; font-size: 0.75rem; }
.badge-declinada { background: rgba(239,68,68,0.15);  color: #f87171; border: 1px solid rgba(239,68,68,0.3);  border-radius: 20px; padding: 2px 10px; font-size: 0.75rem; }
.badge-ignorada  { background: rgba(156,163,175,0.15);color: #9ca3af; border: 1px solid rgba(156,163,175,0.3);border-radius: 20px; padding: 2px 10px; font-size: 0.75rem; }
.badge-adiada    { background: rgba(251,191,36,0.15); color: #fbbf24; border: 1px solid rgba(251,191,36,0.3); border-radius: 20px; padding: 2px 10px; font-size: 0.75rem; }
.badge-andamento { background: rgba(96,165,250,0.15); color: #60a5fa; border: 1px solid rgba(96,165,250,0.3); border-radius: 20px; padding: 2px 10px; font-size: 0.75rem; }

.section-header {
    font-size: 1.05rem; font-weight: 600; color: #E8630A;
    border-bottom: 1px solid rgba(232,99,10,0.2);
    padding-bottom: 0.5rem; margin-bottom: 1rem;
}
.info-box {
    background: rgba(232,99,10,0.08); border: 1px solid rgba(232,99,10,0.2);
    border-radius: 10px; padding: 0.9rem 1.1rem; margin-bottom: 1rem;
    font-size: 0.88rem; color: #c8d8e8;
}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# FUNÇÕES DE EXTRAÇÃO
# ─────────────────────────────────────────────────────────────────────────────

def extrair_numero_op(texto: str) -> str | None:
    """Extrai número de OP de 10 dígitos do texto de um PDF."""
    m = re.search(r'\b(7\d{9})\b', texto)
    return m.group(1) if m else None

def extrair_prazo(texto: str) -> str:
    """Extrai prazo/data limite da OP."""
    m = re.search(r'(?:Prazo|Data[- ]Limite|Encerramento)[:\s]+(\d{2}/\d{2}/\d{4})', texto, re.IGNORECASE)
    return m.group(1) if m else ""

def extrair_descricao_breve(texto: str) -> str:
    """Extrai uma descrição resumida da OP."""
    m = re.search(r'(?:Descrição|Objeto)[:\s]+(.{10,80})', texto, re.IGNORECASE)
    if m:
        return m.group(1).strip()[:80]
    linhas = [l.strip() for l in texto.split('\n') if len(l.strip()) > 15]
    return linhas[1][:80] if len(linhas) > 1 else ""

@st.cache_data(show_spinner=False)
def processar_zips(arquivos_bytes: list[tuple[str, bytes]]) -> pd.DataFrame:
    """Processa lista de ZIPs e retorna DataFrame com OPs encontradas."""
    ops = []
    for nome_zip, conteudo in arquivos_bytes:
        try:
            with zipfile.ZipFile(BytesIO(conteudo)) as zf:
                pdfs = [n for n in zf.namelist() if n.lower().endswith('.pdf')]
                for pdf_name in pdfs:
                    try:
                        with zf.open(pdf_name) as pdf_file:
                            pdf_bytes = pdf_file.read()
                        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
                            texto = "\n".join(p.extract_text() or "" for p in pdf.pages[:2])
                        op = extrair_numero_op(texto)
                        if op:
                            ops.append({
                                "op": op,
                                "arquivo_zip": nome_zip,
                                "pdf": pdf_name,
                                "prazo": extrair_prazo(texto),
                                "descricao": extrair_descricao_breve(texto),
                            })
                    except Exception:
                        pass
        except Exception:
            pass
    if not ops:
        return pd.DataFrame(columns=["op","arquivo_zip","pdf","prazo","descricao"])
    df = pd.DataFrame(ops).drop_duplicates(subset=["op"])
    return df

@st.cache_data(show_spinner=False)
def carregar_pipefy(conteudo: bytes) -> pd.DataFrame:
    """Carrega e normaliza o relatório do Pipefy."""
    df = pd.read_excel(BytesIO(conteudo))
    df.columns = df.columns.str.strip()
    df["op"] = df["Título"].astype(str).str.strip()
    df["fase"] = df["Fase atual"].astype(str).str.strip()
    df["etiquetas"] = df["Etiquetas"].fillna("").astype(str)
    df["criado_em"] = pd.to_datetime(df["Criado em"], errors="coerce")
    return df[["op","fase","etiquetas","criado_em","Criador"]]

def classificar_fase(fase: str) -> str:
    if fase in FASES_LANCADAS:   return "Lançada"
    if fase in FASES_DECLINADAS: return "Declinada"
    if fase in FASES_ADIADAS:    return "Adiada"
    if fase in FASES_RECEBIDAS:  return "Em andamento"
    return "Outro"

def cruzar(df_zips: pd.DataFrame, df_pipefy: pd.DataFrame) -> pd.DataFrame:
    """Cruza ZIPs com Pipefy e classifica cada OP."""
    merged = df_zips.merge(df_pipefy, on="op", how="left")
    merged["fase"] = merged["fase"].fillna("Ignorada")
    merged["etiquetas"] = merged["etiquetas"].fillna("")
    merged["status"] = merged["fase"].apply(
        lambda f: "Ignorada" if f == "Ignorada" else classificar_fase(f)
    )
    return merged

@st.cache_data(show_spinner=False)
def carregar_analise(conteudo: bytes) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Carrega abas DADOS e LANÇADOS da Análise de Preços."""
    dados = pd.read_excel(BytesIO(conteudo), sheet_name="DADOS")
    lancados = pd.read_excel(BytesIO(conteudo), sheet_name="LANÇADOS")
    dados.columns   = dados.columns.str.strip()
    lancados.columns= lancados.columns.str.strip()
    return dados, lancados

@st.cache_data(show_spinner=False)
def carregar_planilha_geral(conteudo: bytes) -> pd.DataFrame:
    """Carrega aba Comercial da Planilha Geral."""
    df = pd.read_excel(BytesIO(conteudo), sheet_name="Comercial")
    df.columns = df.columns.str.strip()
    return df

# ─────────────────────────────────────────────────────────────────────────────
# CABEÇALHO
# ─────────────────────────────────────────────────────────────────────────────

col_logo, col_titulo = st.columns([1, 8])
with col_logo:
    st.markdown("## 📊")
with col_titulo:
    st.markdown("# Value OPS — Banco de Oportunidades")
    st.markdown("<span style='color:#7a95b0;font-size:0.9rem'>Licitações Públicas · Comparativo ZIP vs Pipefy · Histórico de Preços</span>", unsafe_allow_html=True)

st.markdown("---")

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR — UPLOADS
# ─────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### 📁 Carregar Arquivos")
    st.markdown("<div class='info-box'>Carregue os arquivos uma vez — o sistema mantém em memória enquanto a sessão estiver aberta.</div>", unsafe_allow_html=True)

    st.markdown("**1. ZIPs do Petronect**")
    uploaded_zips = st.file_uploader(
        "Todos os ZIPs recebidos",
        type=["zip"], accept_multiple_files=True,
        key="zips", label_visibility="collapsed"
    )

    st.markdown("**2. Relatório Pipefy** (.xlsx)")
    uploaded_pipefy = st.file_uploader(
        "Relatório Pipefy", type=["xlsx"], key="pipefy", label_visibility="collapsed"
    )

    st.markdown("**3. Análise de Preços** (.xlsx) — opcional")
    uploaded_analise = st.file_uploader(
        "Análise de Preços", type=["xlsx"], key="analise", label_visibility="collapsed"
    )

    st.markdown("**4. Planilha Geral** (.xlsx) — opcional")
    uploaded_geral = st.file_uploader(
        "Planilha Geral", type=["xlsx"], key="geral", label_visibility="collapsed"
    )

    st.markdown("---")
    processar = st.button("⚡ Processar ZIPs", use_container_width=True, type="primary")

# ─────────────────────────────────────────────────────────────────────────────
# PROCESSAMENTO
# ─────────────────────────────────────────────────────────────────────────────

if "df_resultado" not in st.session_state:
    st.session_state.df_resultado = None
if "df_pipefy" not in st.session_state:
    st.session_state.df_pipefy = None

if processar and uploaded_zips and uploaded_pipefy:
    with st.spinner("Lendo PDFs dentro dos ZIPs…"):
        arquivos = [(f.name, f.read()) for f in uploaded_zips]
        df_zips = processar_zips(arquivos)

    with st.spinner("Carregando Pipefy…"):
        df_pipefy = carregar_pipefy(uploaded_pipefy.read())
        st.session_state.df_pipefy = df_pipefy

    with st.spinner("Cruzando dados…"):
        resultado = cruzar(df_zips, df_pipefy)
        st.session_state.df_resultado = resultado

    st.success(f"✅ {len(df_zips)} OPs encontradas nos ZIPs · {len(df_pipefy)} registros no Pipefy")

elif processar:
    st.warning("Carregue os ZIPs e o Relatório Pipefy antes de processar.")

# ─────────────────────────────────────────────────────────────────────────────
# ABAS PRINCIPAIS
# ─────────────────────────────────────────────────────────────────────────────

tab1, tab2, tab3 = st.tabs(["📊 Comparativo", "🔍 Histórico de Preços", "📄 Consulta da Sophia"])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — COMPARATIVO
# ══════════════════════════════════════════════════════════════════════════════

with tab1:
    df = st.session_state.df_resultado

    if df is None:
        st.markdown("""
        <div style='text-align:center; padding: 4rem 2rem; color: #7a95b0;'>
            <div style='font-size:3rem;'>📂</div>
            <div style='font-size:1.1rem; margin-top:1rem;'>
                Carregue os ZIPs e o Relatório Pipefy na barra lateral<br>e clique em <strong style='color:#E8630A'>Processar ZIPs</strong>
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        contagem = df["status"].value_counts()
        total = len(df)

        # ── Métricas ──
        cols = st.columns(5)
        metricas = [
            ("Lançadas",     contagem.get("Lançada", 0),     "#4ade80", "Enviadas pro Trello"),
            ("Declinadas",   contagem.get("Declinada", 0),   "#f87171", "Trabalhadas e recusadas"),
            ("Ignoradas",    contagem.get("Ignorada", 0),    "#9ca3af", "Recebidas, não trabalhadas"),
            ("Adiadas",      contagem.get("Adiada", 0),      "#fbbf24", "Para relançar"),
            ("Em andamento", contagem.get("Em andamento", 0),"#60a5fa", "No Pipefy agora"),
        ]
        for col, (lbl, val, cor, sub) in zip(cols, metricas):
            pct = f"{val/total*100:.0f}%" if total else "—"
            col.markdown(f"""
            <div class='metric-card'>
                <div class='val' style='color:{cor}'>{val}</div>
                <div style='color:{cor};font-size:0.7rem;margin-top:2px'>{pct} do total</div>
                <div class='lbl'>{lbl}</div>
                <div class='sub'>{sub}</div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown(f"<br><div class='info-box'>Total de OPs nos ZIPs: <strong>{total}</strong> &nbsp;·&nbsp; Total no Pipefy: <strong>{len(st.session_state.df_pipefy)}</strong></div>", unsafe_allow_html=True)

        # ── Filtros ──
        st.markdown("<div class='section-header'>🔎 Filtros</div>", unsafe_allow_html=True)
        f1, f2, f3 = st.columns(3)

        status_opts = ["Todos"] + sorted(df["status"].unique().tolist())
        filtro_status = f1.selectbox("Status", status_opts)

        todas_etiquetas = set()
        for e in df["etiquetas"].dropna():
            for tag in str(e).split(","):
                t = tag.strip()
                if t and t != "nan":
                    todas_etiquetas.add(t)
        etiqueta_opts = ["Todas"] + sorted(todas_etiquetas)
        filtro_etiqueta = f2.selectbox("Etiqueta (categoria)", etiqueta_opts)

        busca_op = f3.text_input("Buscar por Nº OP")

        df_filtrado = df.copy()
        if filtro_status != "Todos":
            df_filtrado = df_filtrado[df_filtrado["status"] == filtro_status]
        if filtro_etiqueta != "Todas":
            df_filtrado = df_filtrado[df_filtrado["etiquetas"].str.contains(filtro_etiqueta, na=False)]
        if busca_op:
            df_filtrado = df_filtrado[df_filtrado["op"].str.contains(busca_op)]

        st.markdown(f"<div style='color:#7a95b0;font-size:0.85rem;margin-bottom:0.5rem'>{len(df_filtrado)} registros encontrados</div>", unsafe_allow_html=True)

        # ── Análise por Etiqueta ──
        st.markdown("<div class='section-header'>📈 Distribuição por Categoria (Etiqueta)</div>", unsafe_allow_html=True)

        etiqueta_status = defaultdict(lambda: defaultdict(int))
        for _, row in df.iterrows():
            tags = [t.strip() for t in str(row["etiquetas"]).split(",") if t.strip() and t.strip() != "nan"]
            if not tags:
                tags = ["Sem etiqueta"]
            for tag in tags:
                etiqueta_status[tag][row["status"]] += 1

        linhas = []
        for tag, status_dict in etiqueta_status.items():
            total_tag = sum(status_dict.values())
            linhas.append({
                "Categoria": tag,
                "Lançadas":    status_dict.get("Lançada", 0),
                "Declinadas":  status_dict.get("Declinada", 0),
                "Ignoradas":   status_dict.get("Ignorada", 0),
                "Adiadas":     status_dict.get("Adiada", 0),
                "Total":       total_tag,
            })
        df_etiquetas = pd.DataFrame(linhas).sort_values("Total", ascending=False)
        st.dataframe(df_etiquetas, use_container_width=True, hide_index=True)

        # ── Tabela detalhada ──
        st.markdown("<div class='section-header'>📋 Detalhamento por OP</div>", unsafe_allow_html=True)

        BADGE = {
            "Lançada":     "<span class='badge-lancada'>Lançada</span>",
            "Declinada":   "<span class='badge-declinada'>Declinada</span>",
            "Ignorada":    "<span class='badge-ignorada'>Ignorada</span>",
            "Adiada":      "<span class='badge-adiada'>Adiada</span>",
            "Em andamento":"<span class='badge-andamento'>Em andamento</span>",
        }

        df_display = df_filtrado[["op","status","etiquetas","prazo","descricao","fase","arquivo_zip"]].copy()
        df_display.columns = ["Nº OP","Status","Etiquetas","Prazo","Descrição","Fase Pipefy","Arquivo ZIP"]

        st.dataframe(df_display, use_container_width=True, hide_index=True)

        # ── Export ──
        csv = df_display.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Exportar tabela (.csv)",
            data=csv, file_name="ops_comparativo.csv", mime="text/csv"
        )

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — HISTÓRICO DE PREÇOS
# ══════════════════════════════════════════════════════════════════════════════

with tab2:
    st.markdown("<div class='section-header'>🔍 Busca no Histórico de Cotações</div>", unsafe_allow_html=True)
    st.markdown("<div class='info-box'>Busca na <strong>Análise de Preços</strong> e na <strong>Planilha Geral</strong> (dispensas). Útil para saber se um item já foi cotado e a que preço.</div>", unsafe_allow_html=True)

    if not uploaded_analise and not uploaded_geral:
        st.info("Carregue a Análise de Preços e/ou a Planilha Geral na barra lateral para habilitar esta busca.")
    else:
        col_busca, col_tipo = st.columns([3, 1])
        termo = col_busca.text_input("Digite Part Number, Fabricante ou Nº OP", placeholder="Ex: FMU41, EMERSON, 7004565050")
        tipo_busca = col_tipo.selectbox("Buscar em", ["Ambos", "Análise de Preços", "Planilha Geral"])

        if termo:
            termo_lower = termo.lower()

            if uploaded_analise and tipo_busca in ("Ambos", "Análise de Preços"):
                st.markdown("#### Resultados — Análise de Preços")
                try:
                    dados, lancados = carregar_analise(uploaded_analise.read())
                    colunas_busca = ["OP","PART NUMBER","FABRICANTE"]
                    colunas_busca = [c for c in colunas_busca if c in dados.columns]
                    mask = dados[colunas_busca].apply(
                        lambda col: col.astype(str).str.lower().str.contains(termo_lower, na=False)
                    ).any(axis=1)
                    res = dados[mask]
                    if not res.empty:
                        cols_show = [c for c in ["DATA","OP","ITEM","PART NUMBER","FABRICANTE","NOSSO PREÇO UNIT.","NOSSO PREÇO TOTAL","NOSSO PRAZO","Resultado Esperado"] if c in res.columns]
                        st.dataframe(res[cols_show], use_container_width=True, hide_index=True)
                        st.caption(f"{len(res)} registros encontrados")
                    else:
                        st.info("Nenhum resultado encontrado na Análise de Preços.")
                except Exception as e:
                    st.error(f"Erro ao carregar Análise de Preços: {e}")

            if uploaded_geral and tipo_busca in ("Ambos", "Planilha Geral"):
                st.markdown("#### Resultados — Planilha Geral (Dispensas, consultivo)")
                try:
                    comercial = carregar_planilha_geral(uploaded_geral.read())
                    colunas_busca_g = [c for c in ["Número da OP","PN","FABRICANTE","DESCRIÇÃO BREVE"] if c in comercial.columns]
                    mask_g = comercial[colunas_busca_g].apply(
                        lambda col: col.astype(str).str.lower().str.contains(termo_lower, na=False)
                    ).any(axis=1)
                    res_g = comercial[mask_g]
                    if not res_g.empty:
                        cols_g = [c for c in ["Número da OP","ITEM","FABRICANTE","PN","DESCRIÇÃO BREVE","VALOR UNITÁRIO","VALOR TOTAL","COTADOR","ITEM RECUSADO"] if c in res_g.columns]
                        st.dataframe(res_g[cols_g], use_container_width=True, hide_index=True)
                        st.caption(f"{len(res_g)} registros encontrados (dispensas — uso consultivo)")
                    else:
                        st.info("Nenhum resultado na Planilha Geral.")
                except Exception as e:
                    st.error(f"Erro ao carregar Planilha Geral: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — MÓDULO SOPHIA
# ══════════════════════════════════════════════════════════════════════════════

with tab3:
    st.markdown("<div class='section-header'>📄 Consulta Rápida da Sophia</div>", unsafe_allow_html=True)
    st.markdown("<div class='info-box'>Sophia sobe o PDF de uma oportunidade pública → o sistema extrai os itens e verifica automaticamente se já existe análise de preço para eles.</div>", unsafe_allow_html=True)

    if not uploaded_analise:
        st.info("Carregue a Análise de Preços na barra lateral para habilitar este módulo.")
    else:
        pdf_sophia = st.file_uploader("📎 Suba o PDF da oportunidade aqui", type=["pdf"], key="sophia_pdf")

        if pdf_sophia:
            with st.spinner("Lendo PDF…"):
                try:
                    with pdfplumber.open(BytesIO(pdf_sophia.read())) as pdf:
                        texto_completo = "\n".join(p.extract_text() or "" for p in pdf.pages)

                    op_num = extrair_numero_op(texto_completo)
                    if op_num:
                        st.success(f"✅ Oportunidade identificada: **{op_num}**")
                    else:
                        st.warning("Número de OP não identificado no PDF.")

                    # Extrair Part Numbers (padrões comuns)
                    pns = list(set(re.findall(r'\b[A-Z0-9\-]{6,20}\b', texto_completo)))
                    fabricantes_conhecidos = ["EMERSON","ABB","SIEMENS","PARKER","SKF","ENDRESS","DANFOSS","FESTO","IFM","ROCKWELL","SCHNEIDER","YOKOGAWA","HONEYWELL","FISHER","FLOWSERVE","CRANE","VELAN","CAMERON","SPIRAX"]
                    fabs_encontrados = [f for f in fabricantes_conhecidos if f in texto_completo.upper()]

                    col_a, col_b = st.columns(2)
                    with col_a:
                        st.markdown("**Fabricantes identificados no PDF:**")
                        if fabs_encontrados:
                            for f in fabs_encontrados:
                                st.markdown(f"- {f}")
                        else:
                            st.caption("Nenhum fabricante da lista identificado")

                    with col_b:
                        st.markdown(f"**Part Numbers detectados:** {len(pns)}")
                        st.caption("Primeiros 10: " + ", ".join(pns[:10]))

                    # Buscar na Análise de Preços
                    st.markdown("---")
                    st.markdown("#### 🔎 Verificando histórico na Análise de Preços…")

                    dados, _ = carregar_analise(uploaded_analise.read())

                    resultados = []
                    termos = fabs_encontrados + ([op_num] if op_num else [])
                    for termo in termos:
                        colunas_busca = [c for c in ["OP","PART NUMBER","FABRICANTE"] if c in dados.columns]
                        mask = dados[colunas_busca].apply(
                            lambda col: col.astype(str).str.upper().str.contains(termo.upper(), na=False)
                        ).any(axis=1)
                        resultados.append(dados[mask])

                    if resultados:
                        df_encontrado = pd.concat(resultados).drop_duplicates()
                        if not df_encontrado.empty:
                            st.success(f"✅ {len(df_encontrado)} registros históricos encontrados!")
                            cols_show = [c for c in ["DATA","OP","ITEM","PART NUMBER","FABRICANTE","NOSSO PREÇO UNIT.","NOSSO PRAZO","Resultado Esperado"] if c in df_encontrado.columns]
                            st.dataframe(df_encontrado[cols_show], use_container_width=True, hide_index=True)
                        else:
                            st.info("⚪ Nenhum histórico encontrado para esta oportunidade.")

                except Exception as e:
                    st.error(f"Erro ao processar PDF: {e}")
