# Projeto CVM - Dados Financeiros

## Visão Geral
Projeto de gestão e análise de dados financeiros de companhias abertas brasileiras, extraídos do portal de Dados Abertos da Comissão de Valores Mobiliários (CVM).

## Objetivo
Construir uma arquitetura de dados em camadas (medallão) para ingestão, transformação e análise de demonstrações financeiras padronizadas (DFP) de empresas de capital aberto no Brasil.

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
    │   └── 202_cvm_dfp_bpa.py
    ├── 03_gold/
    ├── 04_apoio/
    │   ├── 000_orquestrador_pipeline.py
    │   ├── 001_ddl_create_tables.py
    │   ├── 002_ddl_controle_ingestao.py
    │   ├── 003_download_cvm_para_landing.py
    │   ├── 099_ddl_table_comments.py
    │   └── config_parametros.py
    └── README.md

## Fontes de Dados

### CVM - Demonstrações Financeiras Padronizadas (DFP)
* **Portal**: [Dados Abertos CVM](https://dados.cvm.gov.br/)
* **Demonstrações Implementadas**:
  - DRE (Demonstração do Resultado do Exercício)
  - BPA (Balanço Patrimonial Ativo)

## Status Atual

### Implementado

**Infraestrutura e Governança:**
- ✅ Estrutura de pastas numerada (00_, 01_, 02_, 03_, 04_, 05_)
- ✅ **Landing Zone** em Unity Catalog Volume (`/Volumes/workspace/proj_cvm/landing/`)
- ✅ Scripts DDL (001_ddl_create_tables.py + 002_ddl_controle_ingestao.py)
- ✅ Tabela de controle de ingestão (`proj_cvm_05_apoio.controle_ingestao`)
- ✅ Configuração centralizada (`config_parametros.py`)
- ✅ Orquestrador de pipeline (`000_orquestrador_pipeline.py`)
- ✅ Download para Landing Zone (`003_download_cvm_para_landing.py`)
- ✅ Documentação de tabelas (`099_ddl_table_comments.py`)

**Pipeline de Dados:**
- ✅ **Bronze** - DRE com versionamento (notebook `101_cvm_dfp_dre.py`)
  - Tabela: `proj_cvm_01_bronze.101_dre_dfp`
  - Append-only com colunas `_versao_ingestao`, `_last_modified_cvm`, `_ingest_ts`
- ✅ **Bronze** - BPA com versionamento (notebook `102_cvm_dfp_bpa.py`)
  - Tabela: `proj_cvm_01_bronze.102_bpa_dfp`
  - Append-only com colunas `_versao_ingestao`, `_last_modified_cvm`, `_ingest_ts`
- ✅ **Silver** - DRE transformada (notebook `201_cvm_dfp_dre.py`)
  - Tabela: `proj_cvm_02_silver.201_dre_dfp`
  - Filtro de versão mais recente + DELETE WHERE + APPEND
- ✅ **Silver** - BPA transformada (notebook `202_cvm_dfp_bpa.py`)
  - Tabela: `proj_cvm_02_silver.202_bpa_dfp`
  - Filtro de versão mais recente + DELETE WHERE + APPEND

**Documentação:**
- ✅ Projeto: README.md + arquitetura.md + dicionario_dados.md + guardrails.md
- ✅ Operacional: evolucao_projeto.md (registro cronológico de evolução)

### Próximos Passos
- ⏳ Implementar camada Gold (métricas e KPIs)
- ⏳ Expandar ingestão para outras demonstrações (BPP, DFC, DMPL)
- ⏳ Criar dashboards de análise no Genie Spaces

## Documentação Técnica

Para detalhes sobre arquitetura, padrões, convenções e fluxo de dados:

* **Frameworks e Padrões Universais (Tipo 1 - Conceitual)**: Ver projeto [databricks-genie-skills](https://github.com/1pedroosilva/databricks-genie-skills)
  - Skills reutilizáveis entre projetos (nomenclaturas, estrutura notebooks, revisão código 4 frentes, resiliência operacional, arquitetura medalhão, Unity Catalog, protocolo atualização)
  - Investigação técnica completa sobre Databricks Genie Code Skill Registry
  - Frameworks universais não duplicados neste projeto (fonte única)

* **Arquitetura e Padrões Técnicos**: Ver [00_documentacao/tecnica/arquitetura.md](00_documentacao/tecnica/arquitetura.md)
  - Camadas Medallão (Bronze/Silver/Gold)
  - Landing Zone e versionamento
  - Stack tecnológico
  - Convenções de numeração (notebooks, tabelas)
  - Princípio DRY em nomenclatura
  - Estrutura de notebooks
  - Estratégias de gravação

* **Evolução do Projeto**: Ver [00_documentacao/evolucao_projeto.md](00_documentacao/evolucao_projeto.md)
  - Registro cronológico de desenvolvimento
  - Decisões arquiteturais
  - Aprendizados técnicos

* **Referência de IDs**: Ver [00_documentacao/referencia_ids.md](00_documentacao/referencia_ids.md)
  - Registro centralizado de IDs de todos os assets do projeto

* **Dicionário de Dados**: Ver [00_documentacao/negocio/dicionario_dados.md](00_documentacao/negocio/dicionario_dados.md)
  - Metadados de negócio