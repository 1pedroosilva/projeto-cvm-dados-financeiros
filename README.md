
[![CI](https://github.com/1pedroosilva/projeto-cvm-dados-financeiros/actions/workflows/ci.yml/badge.svg)](https://github.com/1pedroosilva/projeto-cvm-dados-financeiros/actions/workflows/ci.yml)
[![Testes Databricks](https://github.com/1pedroosilva/projeto-cvm-dados-financeiros/actions/workflows/testes_integracao.yml/badge.svg)](https://github.com/1pedroosilva/projeto-cvm-dados-financeiros/actions/workflows/testes_integracao.yml)

# Projeto CVM - Dados Financeiros

Pipeline de ingestão e transformação de demonstrações financeiras de companhias abertas brasileiras, publicadas pela Comissão de Valores Mobiliários (CVM). Arquitetura medalhão (bronze, silver, gold) implementada em Databricks com Delta Lake e Unity Catalog.

## O que é este projeto

Processa demonstrações financeiras padronizadas (DFP) da CVM:
* **DRE** (Demonstração do Resultado do Exercício) - receitas, despesas e resultado
* **BPA** (Balanço Patrimonial Ativo) - ativos
* **BPP** (Balanço Patrimonial Passivo) - passivos e patrimônio líquido

Os dados são extraídos do [Portal de Dados Abertos da CVM](https://dados.cvm.gov.br/), processados em camadas (bronze → silver) e armazenados em Unity Catalog para análise.

## Arquitetura

### Camadas de Dados

```
[CVM Portal] → [Landing Zone] → [Bronze] → [Silver] → [Gold]
     ZIP          UC Volume      Raw Data   Curated    KPIs
```

**Bronze** (`01_bronze/`): Preservação dos dados brutos da fonte
* 3 notebooks: `101_cvm_dfp_dre`, `102_cvm_dfp_bpa`, `103_cvm_dfp_bpp`
* Tabelas: `proj_cvm_01_bronze.101_dre_dfp`, `102_bpa_dfp`, `103_bpp_dfp`
* Estratégia: APPEND-ONLY (histórico completo, idempotente)
* Guardrails: arquivo vazio, schema inválido

**Silver** (`02_silver/`): Dados limpos, tipados e enriquecidos
* 3 notebooks: `201_cvm_dfp_dre`, `202_cvm_dfp_bpa`, `203_cvm_dfp_bpp`
* Tabelas: `proj_cvm_02_silver.201_dre_dfp`, `202_bpa_dfp`, `203_bpp_dfp`
* Transformações: conversão de tipos, normalização de escala monetária, filtro de duplicatas, colunas derivadas (ANO, TRIMESTRE, MES)
* Estratégia: REPLACE WHERE (substituição atômica por período)
* Guardrails: bronze vazia para o ano

**Gold** (`03_gold/`): Em desenvolvimento (métricas e KPIs)

**Landing Zone**: `/Volumes/workspace/proj_cvm/landing/dfp/` - preservação de arquivos originais ZIP com metadados HTTP

### Estrutura do Repositório

```
projeto-cvm-dados-financeiros/
├── 00_documentacao/
│   ├── tecnica/
│   │   ├── arquitetura.md      # Especificação técnica completa
│   │   └── guardrails.md       # Validações de qualidade
│   └── negocio/
│       └── dicionario_dados.md # Conceitos de negócio CVM/DFP
├── 01_bronze/                   # Ingestão bruta (3 notebooks)
├── 02_silver/                   # Transformação (3 notebooks)
├── 03_gold/                     # Agregação (em desenvolvimento)
├── 04_exploracao/               # Análises exploratórias
├── 05_apoio/
│   ├── 000_orquestrador_pipeline.py
│   ├── 001_ddl_create_tables.py
│   ├── 002_ddl_controle_ingestao.py
│   ├── 003_download_cvm_para_landing.py
│   ├── 099_ddl_table_comments.py
│   └── config_parametros.py
├── resources/jobs/
│   ├── job_pipeline_cvm.yml          # Pipeline completo (8 tasks)
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
* **Orquestração**: Databricks Workflows (Databricks Asset Bundle)
* **Governança**: Unity Catalog (schemas, volumes, controle de ingestão)

## Configuração

### Databricks Asset Bundle (DAB)

O projeto usa DAB para gerenciar infraestrutura como código. 3 ambientes configurados:

**dev** (padrão):
* Catálogo: `workspace`
* Schemas: `proj_cvm_dev_01_bronze`, `proj_cvm_dev_02_silver`
* Landing Zone: `/Volumes/workspace/proj_cvm/landing`

**prod**:
* Catálogo: `workspace`
* Schemas: `proj_cvm_01_bronze`, `proj_cvm_02_silver`

**ci**:
* Catálogo: `workspace`
* Schemas: `proj_cvm_ci_01_bronze`, `proj_cvm_ci_02_silver`

Configuração em `databricks.yml` e `resources/jobs/*.yml`.

## Execução

### Via Databricks Workflows (Recomendado)

O job `pipeline_cvm_completo` orquestra o pipeline completo:

1. **Orquestração**: Detecção inteligente de anos a processar (tabela de controle)
2. **Download**: Arquivos CVM para Landing Zone
3. **Bronze**: Ingestão paralela de DRE, BPA, BPP
4. **Silver**: Transformação paralela de DRE, BPA, BPP

**Schedule**: Diário às 3h (América/São_Paulo), pausado por padrão.

**Deploy via DAB**:
```bash
# Validar configuração
databricks bundle validate -t dev

# Deploy para ambiente dev
databricks bundle deploy -t dev

# Executar job manualmente
databricks bundle run pipeline_cvm_completo -t dev
```

### Testes de Integração

Job `testes_integracao_cvm` valida pipeline Bronze→Silver para DRE (ano 2010):

```bash
# Deploy job de testes
databricks bundle deploy -t ci

# Executar testes
databricks bundle run testes_integracao_cvm -t ci
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

Tabela de controle `proj_cvm_05_apoio.controle_ingestao` registra:
* Cada ingestão (fonte, ano, timestamp, versão)
* Erros (status ERROR, mensagem truncada em 500 chars)
* Metadados da fonte (last_modified via HTTP)

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
* **Guardrails e validações**: [`00_documentacao/tecnica/guardrails.md`](00_documentacao/tecnica/guardrails.md)
* **Dicionário de dados e negócio**: [`00_documentacao/negocio/dicionario_dados.md`](00_documentacao/negocio/dicionario_dados.md)

## Licença

MIT License - veja [`LICENSE`](LICENSE) para detalhes.