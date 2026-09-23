# Estado Atual do Projeto CVM

> **Última atualização:** 23/09/2026 (pause de jobs agendados do target test)  
> **Retrato do pipeline hoje — sem histórico, sem justificativas.**

---

## Jobs em Produção

| Job | Target | ID | Schedule | Pause | Notebooks Executados |
|-----|--------|-----|----------|-------|---------------------|
| **CVM - Verificação Diária Landing Zone** | dev | 348419458655416 | Diário 06:00 BRT | UNPAUSED | `05_apoio/004_verificacao_diaria_landing.py` |
| **CVM - Pipeline Completo Semanal** | dev | 890867014997453 | Segunda 07:00 BRT | UNPAUSED | `01_bronze/101_cvm_dfp_dre.py`<br>`01_bronze/102_cvm_dfp_bpa.py`<br>`01_bronze/103_cvm_dfp_bpp.py`<br>`02_silver/201_cvm_dfp_dre.py`<br>`02_silver/202_cvm_dfp_bpa.py`<br>`02_silver/203_cvm_dfp_bpp.py` |
| **[test] Testes de Integração - Pipeline CVM** | dev | 987359705402401 | Manual (GitHub Actions) | — | `05_apoio/001_ddl_create_tables.py`<br>`01_bronze/101_cvm_dfp_dre.py`<br>`02_silver/201_cvm_dfp_dre.py`<br>`06_testes/test_integracao_dre.py` |
| **CVM - Verificação Diária Landing Zone** | test | 770519249168919 | Diário 06:00 BRT | **PAUSED** | `05_apoio/004_verificacao_diaria_landing.py` |
| **CVM - Pipeline Completo Semanal** | test | 684250943587739 | Segunda 07:00 BRT | **PAUSED** | `01_bronze/101_cvm_dfp_dre.py`<br>`01_bronze/102_cvm_dfp_bpa.py`<br>`01_bronze/103_cvm_dfp_bpp.py`<br>`02_silver/201_cvm_dfp_dre.py`<br>`02_silver/202_cvm_dfp_bpa.py`<br>`02_silver/203_cvm_dfp_bpp.py` |
| **[test] Testes de Integração - Pipeline CVM** | test | 474378723473259 | Manual (GitHub Actions) | — | `05_apoio/001_ddl_create_tables.py`<br>`01_bronze/101_cvm_dfp_dre.py`<br>`02_silver/201_cvm_dfp_dre.py`<br>`06_testes/test_integracao_dre.py` |

---

## Notebooks do Projeto

### 🟢 Em Produção (executados por jobs)

| Camada | Notebook | Função |
|--------|----------|--------|
| Bronze | `101_cvm_dfp_dre.py` | Ingestão DRE |
| Bronze | `102_cvm_dfp_bpa.py` | Ingestão BPA |
| Bronze | `103_cvm_dfp_bpp.py` | Ingestão BPP |
| Silver | `201_cvm_dfp_dre.py` | Transformação DRE |
| Silver | `202_cvm_dfp_bpa.py` | Transformação BPA |
| Silver | `203_cvm_dfp_bpp.py` | Transformação BPP |
| Apoio | `001_ddl_create_tables.py` | DDL unificado (schemas + tabelas) |
| Apoio | `004_verificacao_diaria_landing.py` | Download incremental CVM (HEAD + Last-Modified) |
| Testes | `test_integracao_dre.py` | Validação E2E Bronze→Silver |

---

### 🟡 Utilitários Manuais (executados sob demanda)

| Camada | Notebook | Função |
|--------|----------|--------|
| Apoio | `002_ddl_controle_ingestao.py` | Validação idempotente da tabela `controle_ingestao` |
| Apoio | `099_ddl_table_comments.py` | Adicionar COMMENT ON TABLE/COLUMN no catálogo UC |
| Exploração | `EDA_002_analise_bpa_silver.py` | Análise exploratória BPA |
| Exploração | `EDA_003_analise_bpp_silver.py` | Análise exploratória BPP |

---

### 🔴 Aposentados (mantidos para uso manual específico)

| Camada | Notebook | Ex-Função | Substituído Por |
|--------|----------|-----------|----------------|
| Apoio | `000_orquestrador_pipeline.py` | Detecção centralizada de anos a processar | Detecção distribuída via `inicializar_anos_processar()` em cada notebook |
| Apoio | `003_download_cvm_para_landing.py` | Download direto sem comparação de Last-Modified | `004_verificacao_diaria_landing.py` (com HEAD + Last-Modified) |

**Uso atual dos aposentados:** Download forçado de ano específico (003), validação manual de lógica legada (000).

---

### 📦 Histórico (não executado, mantido como registro)

| Camada | Notebook | Ex-Função | Formato |
|--------|----------|-----------|--------|
| Exploração | `EDA_001_analise_dre_silver.ipynb` | Análise exploratória DRE Silver (investigação original que motivou correções de escala e hierarquia) | `.ipynb` (único no repo, exceção à padronização `.py`) |

---

## Arquivos de Configuração

| Arquivo | Função |
|---------|--------|
| `05_apoio/config_parametros.py` | Parâmetros globais: anos, URLs, schemas UC, resolução de ambiente |
| `05_apoio/ambientes.json` | Mapeamento de ambientes (dev/test/prod) → prefixos de schema |
| `databricks.yml` | Bundle: targets (dev/test/prod), jobs, variáveis |
| `resources/jobs/*.yml` | Definições de jobs (verificacao_diaria, pipeline_semanal, testes_integracao) |

---

## Estrutura de Dados (Unity Catalog)

### Schemas

| Ambiente | Bronze | Silver | Apoio |
|----------|--------|--------|-------|
| **dev** | `proj_cvm_dev_01_bronze` | `proj_cvm_dev_02_silver` | `proj_cvm_dev_05_apoio` |
| **test** | `proj_cvm_test_01_bronze` | `proj_cvm_test_02_silver` | `proj_cvm_test_05_apoio` |
| **prod** | `proj_cvm_01_bronze` | `proj_cvm_02_silver` | `proj_cvm_05_apoio` |

### Tabelas

| Camada | Tabela | Descrição |
|--------|--------|-----------|
| Bronze | `101_dre_dfp` | Demonstração de Resultado (raw) |
| Bronze | `102_bpa_dfp` | Balanço Patrimonial Ativo (raw) |
| Bronze | `103_bpp_dfp` | Balanço Patrimonial Passivo (raw) |
| Silver | `201_dre_dfp` | DRE estruturado + hierarquia |
| Silver | `202_bpa_dfp` | BPA estruturado + hierarquia |
| Silver | `203_bpp_dfp` | BPP estruturado + hierarquia |
| Apoio | `controle_ingestao` | Registro de processamento (ano, status, timestamp) |
| Apoio | `observabilidade_execucoes` | Métricas detalhadas de execução (populada por `registrar_observabilidade_execucao()` nos 6 notebooks Bronze/Silver: 101, 102, 103, 201, 202, 203) |

### Volume

* **Landing Zone:** `/Volumes/workspace/proj_cvm/landing/dfp/`  
  Compartilhado entre dev/test/prod (dados brutos CVM são imutáveis)

---

## CI/CD

| Workflow | Trigger | Função |
|----------|---------|--------|
| `.github/workflows/ci.yml` | Push/PR no `main` | Ruff lint + pytest (funções puras) |
| `.github/workflows/testes_integracao.yml` | Manual (workflow_dispatch) | Deploy bundle → Run job teste → Validação E2E |

---

**Histórico e decisões arquiteturais:** Ver [`evolucao_projeto.md`](../evolucao_projeto.md), [`arquitetura.md`](arquitetura.md) e [`decisoes_arquiteturais.md`](decisoes_arquiteturais.md).
