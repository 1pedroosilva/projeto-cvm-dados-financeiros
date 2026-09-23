
[![CI](https://github.com/1pedroosilva/projeto-cvm-dados-financeiros/actions/workflows/ci.yml/badge.svg)](https://github.com/1pedroosilva/projeto-cvm-dados-financeiros/actions/workflows/ci.yml)
[![Testes Databricks](https://github.com/1pedroosilva/projeto-cvm-dados-financeiros/actions/workflows/testes_integracao.yml/badge.svg)](https://github.com/1pedroosilva/projeto-cvm-dados-financeiros/actions/workflows/testes_integracao.yml)

# Projeto CVM - Dados Financeiros

Pipeline de ingestão e transformação de demonstrações financeiras de companhias abertas brasileiras, publicadas pela Comissão de Valores Mobiliários (CVM). Arquitetura medalhão implementada em Databricks com Delta Lake e Unity Catalog — Bronze e Silver em produção, Gold planejada.

> **📋 [Estado Atual do Projeto](00_documentacao/tecnica/estado_atual.md)** — Retrato de hoje: jobs ativos, notebooks em produção, utilitários e aposentados (sem histórico).

## O que é este projeto

Processa demonstrações financeiras padronizadas (DFP) da CVM:
* **DRE** (Demonstração do Resultado do Exercício) - receitas, despesas e resultado
* **BPA** (Balanço Patrimonial Ativo) - ativos
* **BPP** (Balanço Patrimonial Passivo) - passivos e patrimônio líquido

Os dados são extraídos do [Portal de Dados Abertos da CVM](https://dados.cvm.gov.br/), processados em camadas (bronze → silver) e armazenados em Unity Catalog para análise.

## Arquitetura

### Camadas de Dados

```
[CVM Portal] → [Landing Zone] → [Bronze] → [Silver] → [Gold (planejada)]
     ZIP          UC Volume      Raw Data   Curated    KPIs
```

**Bronze** (`01_bronze/`): Preservação dos dados brutos da fonte
* 3 notebooks: `101_cvm_dfp_dre`, `102_cvm_dfp_bpa`, `103_cvm_dfp_bpp`
* Tabelas: `{SCHEMA_BRONZE}.101_dre_dfp`, `102_bpa_dfp`, `103_bpp_dfp` (via config_parametros)
* Estratégia: APPEND-ONLY (histórico completo, idempotente)
* Guardrails: arquivo vazio, schema inválido

**Silver** (`02_silver/`): Dados limpos, tipados e enriquecidos
* 3 notebooks: `201_cvm_dfp_dre`, `202_cvm_dfp_bpa`, `203_cvm_dfp_bpp`
* Tabelas: `{SCHEMA_SILVER}.201_dre_dfp`, `202_bpa_dfp`, `203_bpp_dfp` (via config_parametros)
* Transformações: conversão de tipos, normalização de escala monetária, filtro de duplicatas, colunas derivadas (ANO, TRIMESTRE, MES)
* Estratégia: REPLACE WHERE (substituição atômica por período)
* Guardrails: bronze vazia para o ano

**Gold** (`03_gold/`): Planejada (métricas e KPIs) — não implementada

**Landing Zone**: `VOLUME_LANDING_DFP` (config_parametros) - preservação de arquivos originais ZIP com metadados HTTP

### Estrutura do Repositório

```
projeto-cvm-dados-financeiros/
├── .github/workflows/
│   ├── ci.yml                   # Ruff + pytest (push/PR no main)
│   └── testes_integracao.yml    # Deploy + run testes E2E (manual)
├── 00_documentacao/
│   ├── evolucao_projeto.md      # Histórico e decisões
│   ├── tecnica/
│   │   ├── arquitetura.md            # Especificação técnica completa
│   │   ├── decisoes_arquiteturais.md # Decisões de design e trade-offs
│   │   ├── estado_atual.md           # Retrato do pipeline hoje
│   │   └── guardrails.md             # Validações de qualidade
│   └── negocio/
│       └── dicionario_dados.md  # Conceitos de negócio CVM/DFP
├── 01_bronze/                   # Ingestão bruta (3 notebooks)
├── 02_silver/                   # Transformação (3 notebooks)
├── 03_gold/                     # Agregação (planejada)
│   └── README.md                # Descrição da camada planejada
├── 04_exploracao/               # Análises exploratórias (3 notebooks EDA)
├── 05_apoio/
│   ├── 000_orquestrador_pipeline.py
│   ├── 001_ddl_create_tables.py
│   ├── 002_ddl_controle_ingestao.py
│   ├── 003_download_cvm_para_landing.py
│   ├── 004_verificacao_diaria_landing.py
│   ├── 099_ddl_table_comments.py
│   ├── ambientes.json           # Resolução de ambiente e carga
│   └── config_parametros.py
├── 06_testes/
│   └── test_integracao_dre.py   # Validação E2E Bronze→Silver
├── resources/jobs/
│   ├── job_pipeline_semanal.yml      # Bronze→Silver semanal (6 tasks)
│   ├── job_verificacao_diaria.yml    # Verificação diária landing zone
│   └── job_testes_integracao.yml     # Testes E2E
├── tests/
│   └── test_config_parametros.py
├── databricks.yml               # Configuração DAB
├── ruff.toml                    # Linter
└── LICENSE                      # MIT
```

## Stack Tecnológico

* **Plataforma**: Databricks (Serverless Compute)
* **Armazenamento**: Delta Lake + Unity Catalog
* **Processamento**: Apache Spark (PySpark)
* **Orquestração**: Databricks Workflows (Databricks Asset Bundles)
* **Governança**: Unity Catalog (schemas, volumes, controle de ingestão)

## Configuração

### Databricks Asset Bundle (DAB)

O projeto usa DAB para gerenciar infraestrutura como código. 3 ambientes declarados em `ambientes.json`:

**dev** (padrão, instanciado):
* Catálogo: `workspace`
* Prefixo de schema: `proj_cvm_dev`
* Landing Zone: `/Volumes/workspace/proj_cvm/landing`

**test** (instanciado):
* Catálogo: `workspace`
* Prefixo de schema: `proj_cvm_test`

**prod** (declarado, não instanciado):
* Catálogo: `workspace`
* Prefixo de schema: `proj_cvm_prod`

Configuração em `databricks.yml` e `resources/jobs/*.yml`.

## Execução

### Via Databricks Workflows (Recomendado)

O job `pipeline_semanal` orquestra Bronze→Silver para DRE, BPA e BPP em três trilhos paralelos (6 tasks). Orquestração e download não são tasks deste job — o download é feito pelo job `verificacao_diaria`.

**Jobs**:

* `pipeline_semanal` — Bronze→Silver, semanal às segundas 07:00 (América/São_Paulo). Ativo no target dev; pausado no target test
* `verificacao_diaria` — Verificação da landing zone, diário às 06:00 (América/São_Paulo). Ativo no target dev; pausado no target test

O target test existe para o job `testes_integracao_cvm` (executado sob demanda via GitHub Actions). Os jobs `pipeline_semanal` e `verificacao_diaria` são deployados no target test mas ficam pausados — não há execução automática agendada em test.

**Deploy via DAB**:
```bash
# Validar configuração
databricks bundle validate -t dev

# Deploy para ambiente dev
databricks bundle deploy -t dev

# Executar job manualmente
databricks bundle run pipeline_semanal -t dev
```

### Testes de Integração

Job `testes_integracao_cvm` valida pipeline Bronze→Silver para DRE (ano 2021):

```bash
# Deploy job de testes
databricks bundle deploy -t test

# Executar testes
databricks bundle run testes_integracao_cvm -t test
```

## Validações e Qualidade

### Guardrails

**Bronze**:
* Arquivo vazio → PARA (preserva Bronze)
* Schema inválido (colunas críticas faltando) → PARA
* Implementação: função `validar_e_projetar_schema()` em `config_parametros.py`

**Silver**:
* Bronze vazia para o ano → SKIP (preserva Silver)

Detalhes em [`00_documentacao/tecnica/guardrails.md`](00_documentacao/tecnica/guardrails.md).

### Rastreamento

Tabela de controle `{SCHEMA_APOIO}.controle_ingestao` (via config_parametros) registra:
* Cada ingestão (fonte, ano, timestamp, versão)
* Erros (status ERROR, mensagem truncada em 500 chars)
* Metadados da fonte (last_modified via HTTP)

Tabela `{SCHEMA_APOIO}.observabilidade_execucoes` (via config_parametros) registra:
* Métricas detalhadas de execução (etapa, fonte, duração, registros processados)
* Contexto do job (job_id, run_id, task_key)
* Status (SUCCESS, ERROR, SKIPPED, PARTIAL)

## Fonte de Dados

**Origem**: [Portal de Dados Abertos da CVM](https://dados.cvm.gov.br/)

**URL**: `https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_{ANO}.zip`

**Formato**: ZIP contendo CSVs (encoding ISO-8859-1, separador `;`)

**Periodicidade**: Anual (DFP = Demonstrações Financeiras Padronizadas anuais)

**Demonstrações processadas**:
* `dfp_cia_aberta_DRE_con_{ANO}.csv` - DRE consolidada
* `dfp_cia_aberta_BPA_con_{ANO}.csv` - Balanço Patrimonial Ativo consolidado
* `dfp_cia_aberta_BPP_con_{ANO}.csv` - Balanço Patrimonial Passivo consolidado

Detalhes sobre estrutura dos dados e conceitos de negócio em [`00_documentacao/negocio/dicionario_dados.md`](00_documentacao/negocio/dicionario_dados.md).

## Documentação Complementar

* **Arquitetura técnica**: [`00_documentacao/tecnica/arquitetura.md`](00_documentacao/tecnica/arquitetura.md)
* **Decisões arquiteturais**: [`00_documentacao/tecnica/decisoes_arquiteturais.md`](00_documentacao/tecnica/decisoes_arquiteturais.md)
* **Estado atual do projeto**: [`00_documentacao/tecnica/estado_atual.md`](00_documentacao/tecnica/estado_atual.md)
* **Guardrails e validações**: [`00_documentacao/tecnica/guardrails.md`](00_documentacao/tecnica/guardrails.md)
* **Dicionário de dados e negócio**: [`00_documentacao/negocio/dicionario_dados.md`](00_documentacao/negocio/dicionario_dados.md)

## Licença

MIT License - veja [`LICENSE`](LICENSE) para detalhes.