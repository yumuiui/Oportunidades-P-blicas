# NextSupply — OPS Database

Banco de dados de oportunidades públicas do Petronect.

## Estrutura de pastas

```
├── app.py
├── requirements.txt
└── data/
    ├── zips/                   ← todos os ZIPs do Petronect ficam aqui
    │   ├── ops_2026-01-15.zip
    │   ├── ops_2026-01-16.zip
    │   └── ...
    ├── pipefy_latest.xlsx      ← último export do Pipefy (atualizar pelo app)
    ├── analise_precos.xlsx     ← Análise de Preços v5 (atualizar pelo app)
    └── planilha_geral.xlsx     ← Planilha Geral de Lançamentos (atualizar pelo app)
```

## Como adicionar novos ZIPs

1. Coloque o arquivo `.zip` na pasta `data/zips/`
2. Faça `git add . && git commit -m "add zip dd-mm-yyyy" && git push`
3. O Streamlit Cloud atualiza automaticamente

## Como atualizar o Pipefy / Planilhas

Use o upload na barra lateral do próprio app — substitui o arquivo em `data/` automaticamente.

## Deploy (Streamlit Cloud)

1. Crie repositório privado no GitHub
2. Conecte em [share.streamlit.io](https://share.streamlit.io)
3. Main file: `app.py`
