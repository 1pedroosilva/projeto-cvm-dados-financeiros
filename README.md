# Projeto CVM - Dados Financeiros

[![CI](https://github.com/1pedroosilva/projeto-cvm-dados-financeiros/actions/workflows/ci.yml/badge.svg)](https://github.com/1pedroosilva/projeto-cvm-dados-financeiros/actions/workflows/ci.yml)


![Testes Databricks](https://github.com/1pedroosilva/projeto-cvm-dados-financeiros/actions/workflows/testes_integracao.yml/badge.svg)


## Visão Geral

Projeto de gestão e análise de dados financeiros de companhias abertas brasileiras, extraídos do portal de **Dados Abertos da CVM** (Comissão de Valores Mobiliários).

O pipeline implementa a **arquitetura medalhão** (Bronze → Silver → Gold) com análises exploratórias intermediárias para validação de qualidade antes de promover dados para camadas downstream.

---

## Objetivo

Construir uma **arquitetura de dados em camadas (medalhão)** para ingestão, transformação e análise de demonstrações financeiras padronizadas (DFP) de empresas de capital aberto no Brasil.

---

## Estrutura do Projeto

    projeto-cvm-dados-financeiros/
    ├── .github/
    │   └── workflows/
    │       └── ci.yml
    ├── 00_documentacao/
    │   ├── evolucao_projeto.md
    │   ├── tecnica/
    │   │   ├── arquitetura.md
    │   │   └── guardrails.md
    │   └── negocio/
    │       └── dicionario_dados.md
    ├── 01_bronze/
    │   ├── 101_cvm_dfp_dre.py
    │   ├── 102_cvm_dfp_bpa.py
    │   └── 103_cvm_dfp_bpp.py
    ├── 02_silver/
    │   ├── 201_cvm_dfp_dre.py
    │   ├── 202_cvm_dfp_bpa.py
    │   └── 203_cvm_dfp_bpp.py
    ├── 04_exploracao/
    │   └── EDA_001_analise_dre_silver.ipynb
    ├── 05_apoio/
    │   ├── 000_orquestrador_pipeline.py
    │   ├── 001_ddl_create_tables.py
    │   ├── 002_ddl_controle_ingestao.py
    │   ├── 003_download_cvm_para_landing.py
    │   ├── 099_ddl_table_comments.py
    │   └── config_parametros.py
    ├── 06_testes/
    │   ├── criar_schemas_teste.py
    │   ├── test_integracao_dre.py
    │   └── TEMPLATE_github_workflow.yml
    ├── resources/
    │   └── jobs/
    │       └── job_pipeline_cvm.yml
    ├── tests/
    │   └── test_config_parametros.py
    ├── databricks.yml
    ├── ruff.toml
    └── README.md

---

## Fontes de Dados

### CVM - Demonstrações Financeiras Padronizadas (DFP)

* **Portal**: [Dados Abertos CVM](https://dados.cvm.gov.br/)
* **Demonstrações Implementadas**:
  - **DRE** (Demonstração do Resultado do Exercício)
  - **BPA** (Balanço Patrimonial Ativo)
  - **BPP** (Balanço Patrimonial Passivo)

---

## Deploy e Execução

O pipeline roda como um Databricks Job (`Pipeline CVM - DFP`) gerenciado via **Databricks Asset Bundles (DABs)**, definido em `databricks.yml` e `resources/jobs/job_pipeline_cvm.yml`. Mudanças em tasks, schedule ou notificações são feitas nesses arquivos e aplicadas com `databricks bundle deploy`, não editando o job diretamente na interface do Databricks.

Detalhes técnicos completos (sincronização de notebooks, compute serverless, modo de edição) em [00_documentacao/tecnica/arquitetura.md](00_documentacao/tecnica/arquitetura.md).

---

## Testes de Integração

O workflow `testes_integracao.yml` executa os notebooks do pipeline (Bronze → Silver) no Databricks e valida o funcionamento E2E.

**Trigger**: Manual (`workflow_dispatch`) — não consome DBU em todo push.

**Configuração necessária** (apenas uma vez):

1. Gerar token no Databricks: **User Settings** → **Developer** → **Access tokens** → **Generate new token**
2. Configurar secrets no GitHub: **Settings** → **Secrets and variables** → **Actions** → **New repository secret**:
   ```
   DATABRICKS_HOST = https://dbc-a4218100-86c9.cloud.databricks.com
   DATABRICKS_TOKEN = <seu-token-gerado>
   ```

**Execução**: GitHub Actions → **Testes de Integração** → **Run workflow** → escolher ambiente (ci/dev)

---

## Status Atual

### Implementado

**Infraestrutura e Governança:**

✓ Estrutura de pastas numerada (00_, 01_, 02_, 03_, 04_, 05_)  
✓ **Landing Zone** em Unity Catalog Volume (`/Volumes/workspace/proj_cvm/landing/`)  
✓ Scripts DDL (001_ddl_create_tables.py + 002_ddl_controle_ingestao.py)  
✓ Tabela de **controle de ingestão** (`proj_cvm_05_apoio.controle_ingestao`)  
✓ **Configuração centralizada** (`config_parametros.py`)  
✓ **Orquestrador de pipeline** (`000_orquestrador_pipeline.py`)  
✓ Download para Landing Zone (`003_download_cvm_para_landing.py`)  
✓ Documentação de tabelas (`099_ddl_table_comments.py`)  
✓ **CI/CD automatizado** (GitHub Actions - `.github/workflows/ci.yml`)  
✓ **Testes unitários** (`tests/test_config_parametros.py`)  
✓ **Linting e formatação** (Ruff - `ruff.toml`)

**Pipeline de Dados:**

✓ **Bronze - DRE** com versionamento (notebook `101_cvm_dfp_dre.py`)
  - Tabela: `proj_cvm_01_bronze.101_dre_dfp`
  - **Append-only** com colunas `_versao_ingestao`, `_last_modified_cvm`, `_ingest_ts`

✓ **Bronze - BPA** com versionamento (notebook `102_cvm_dfp_bpa.py`)
  - Tabela: `proj_cvm_01_bronze.102_bpa_dfp`
  - **Append-only** com colunas `_versao_ingestao`, `_last_modified_cvm`, `_ingest_ts`

✓ **Silver - DRE** transformada (notebook `201_cvm_dfp_dre.py`)
  - Tabela: `proj_cvm_02_silver.201_dre_dfp`
  - Filtro de versão mais recente + **Gravação atômica por período**

✓ **Silver - BPA** transformada (notebook `202_cvm_dfp_bpa.py`)
  - Tabela: `proj_cvm_02_silver.202_bpa_dfp`
  - Filtro de versão mais recente + **Gravação atômica por período**

✓ **Silver - BPP** transformada (notebook `203_cvm_dfp_bpp.py`)
  - Tabela: `proj_cvm_02_silver.203_bpp_dfp`
  - Filtro de versão mais recente + **Gravação atômica por período**

**Documentação:**

✓ Projeto: README.md + arquitetura.md + dicionario_dados.md + guardrails.md  
✓ Operacional: **evolucao_projeto.md** (registro cronológico de evolução)

**Análises Exploratórias:**

✓ **EDA_001_analise_dre_silver** (análise de qualidade da camada Silver DRE)
  - 9 frentes de validação investigadas
  - Detalhes em `04_exploracao/EDA_001_analise_dre_silver.ipynb`

---

## Próximos Passos

* Normalizar escalas monetárias na transformação Silver DRE
* Adicionar classificação hierárquica (**TOTALIZADORA/ANALÍTICA**) na Silver DRE
* Executar **backfill** Bronze DRE para período 2021-2023
* Implementar camada **Gold** (métricas e KPIs)
* Expandir ingestão para outras demonstrações (**DFC**, **DMPL**)
* Criar dashboards de análise no **Genie Spaces**

---

## Documentação Técnica

Para detalhes sobre arquitetura, padrões, convenções e fluxo de dados:

* **Frameworks e Padrões Universais (Tipo 1 - Conceitual)**: Ver projeto [databricks-genie-skills](https://github.com/1pedroosilva/databricks-genie-skills)
  - Skills reutilizáveis entre projetos (nomenclaturas, estrutura notebooks, revisão código 4 frentes, resiliência operacional, arquitetura medalhão, Unity Catalog, protocolo atualização)
  - Investigação técnica completa sobre **Databricks Genie Code Skill Registry**
  - Frameworks universais não duplicados neste projeto (fonte única)

* **Arquitetura e Padrões Técnicos**: Ver [00_documentacao/tecnica/arquitetura.md](00_documentacao/tecnica/arquitetura.md)
  - Camadas Medalhão (**Bronze/Silver/Gold**)
  - **Landing Zone** e versionamento
  - Stack tecnológico
  - Convenções de numeração (notebooks, tabelas)
  - Princípio **DRY** em nomenclatura
  - Estrutura de notebooks
  - Estratégias de gravação

* **Evolução do Projeto**: Ver [00_documentacao/evolucao_projeto.md](00_documentacao/evolucao_projeto.md)
  - Registro cronológico de desenvolvimento
  - Decisões arquiteturais
  - Aprendizados técnicos


* **Dicionário de Dados**: Ver [00_documentacao/negocio/dicionario_dados.md](00_documentacao/negocio/dicionario_dados.md)
  - Metadados de negócio
