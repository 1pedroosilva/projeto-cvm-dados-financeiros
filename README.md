# Projeto CVM - Dados Financeiros

## Visão Geral

Projeto de gestão e análise de dados financeiros de companhias abertas brasileiras, extraídos do portal de **Dados Abertos da CVM** (Comissão de Valores Mobiliários).

O pipeline implementa a **arquitetura medalhão** (Bronze → Silver → Gold) com análises exploratórias intermediárias para validação de qualidade antes de promover dados para camadas downstream

> **Insight-chave**: A análise exploratória identificou **3 problemas críticos** na camada Silver antes de avançar para Gold — escalas monetárias não normalizadas (erro 1000x), hierarquia contábil sem classificação (risco de duplicação), e perda de histórico 2021-2023. O ciclo **ACHADO → DECISÃO → CÓDIGO → VALIDAÇÃO** provou valor real ao detectar bugs que queries downstream silenciosamente propagariam.

---

## Objetivo

Construir uma **arquitetura de dados em camadas (medallão)** para ingestão, transformação e análise de demonstrações financeiras padronizadas (DFP) de empresas de capital aberto no Brasil.

---

## Estrutura do Projeto

    projeto-cvm-dados-financeiros/
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

**Pipeline de Dados:**

✓ **Bronze - DRE** com versionamento (notebook `101_cvm_dfp_dre.py`)
  - Tabela: `proj_cvm_01_bronze.101_dre_dfp`
  - **Append-only** com colunas `_versao_ingestao`, `_last_modified_cvm`, `_ingest_ts`

✓ **Bronze - BPA** com versionamento (notebook `102_cvm_dfp_bpa.py`)
  - Tabela: `proj_cvm_01_bronze.102_bpa_dfp`
  - **Append-only** com colunas `_versao_ingestao`, `_last_modified_cvm`, `_ingest_ts`

✓ **Silver - DRE** transformada (notebook `201_cvm_dfp_dre.py`)
  - Tabela: `proj_cvm_02_silver.201_dre_dfp`
  - Filtro de versão mais recente + **DELETE WHERE + APPEND**

✓ **Silver - BPA** transformada (notebook `202_cvm_dfp_bpa.py`)
  - Tabela: `proj_cvm_02_silver.202_bpa_dfp`
  - Filtro de versão mais recente + **DELETE WHERE + APPEND**

**Documentação:**

✓ Projeto: README.md + arquitetura.md + dicionario_dados.md + guardrails.md  
✓ Operacional: **evolucao_projeto.md** (registro cronológico de evolução)

**Análises Exploratórias:**

✓ **EDA_001_analise_dre_silver** (9 frentes investigadas sobre Silver DRE)
  - Identificados **3 problemas críticos**: escalas monetárias não normalizadas, hierarquia contábil sem classificação, perda de dados históricos 2021-2023
  - Validações confirmadas: **completude**, **chave única**, **integridade hierárquica**, **versões**

---

## Análise Exploratória e Qualidade de Dados

A análise exploratória do Silver DRE aplicou o ciclo **ACHADO → DECISÃO → CÓDIGO → VALIDAÇÃO** para identificar problemas de qualidade antes de avançar para a camada Gold. 

O notebook **EDA_001** investigou **9 frentes** (escalas monetárias, hierarquia contábil, cobertura temporal, completude, chave única, integridade hierárquica, versionamento, duplicações estruturais, e consistência de grupos) e identificou **3 problemas críticos** que impactam análises downstream.

### Problemas Identificados na Camada Silver

| Achado | Impacto | Ação | Evidência |
|--------|---------|------|-----------|
| Escalas monetárias não normalizadas (Bronze preserva escala original: unidade vs milhares) | Erro de magnitude **1000x** em queries de soma/agregação | Normalizar escala na transformação Silver | EDA_001, célula "Escala Monetária" |
| Hierarquia contábil sem classificação de grupo (CD_CONTA não indica se é conta **TOTALIZADORA** ou **ANALÍTICA**) | Risco de duplicar valor ao somar contas-pai + contas-filho na mesma agregação | Adicionar classificação hierárquica na Silver (campo **TIPO_CONTA**) | EDA_001, célula "Hierarquia Contábil" |
| Perda de histórico 2021-2023 (Bronze contém apenas 2024) | Análises de tendência e comparativos multi-anuais inviabilizados | Executar **backfill 2021-2023** na Bronze. Causa ainda não determinada, requer investigação do notebook 101 (hipóteses: falha silenciosa no processamento, truncamento acidental, parâmetro modificado) | EDA_001, célula "Cobertura Temporal" |

### Rigor Metodológico

> Durante a validação hierárquica, uma **premissa oculta** foi identificada: a célula de descoberta encontrou uma amostra com filtros específicos (ORDEM_EXERC, GRUPO_DFP), mas a célula de validação seguinte retranscreveu manualmente os filtros SQL e omitiu essas colunas, testando uma **fatia diferente da amostra** sem perceber. 
> 
> Corrigido mediante **herança explícita de filtros**. Documentado em evolucao_projeto.md (17/08). Este tipo de bug de processo — premissas ocultas em queries — ilustra o valor do ciclo de validação rigorosa aplicado no projeto.

O notebook completo (9 frentes investigadas, código validado) está disponível em `04_exploracao/EDA_001_analise_dre_silver.ipynb`.

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
  - Camadas Medallão (**Bronze/Silver/Gold**)
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
