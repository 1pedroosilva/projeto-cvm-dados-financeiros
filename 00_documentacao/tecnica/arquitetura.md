# Arquitetura Técnica - Projeto CVM Dados Financeiros

## Visão Geral

Este documento descreve a arquitetura técnica do projeto de ingestão e processamento de dados financeiros da CVM (Comissão de Valores Mobiliários).

## Arquitetura de Dados

### Padrão Medalhão (Medallion Architecture)

O projeto segue a arquitetura medalhão, um padrão consolidado em lakehouse que organiza dados em três camadas progressivas:

```
[Fonte: CVM] → [Bronze] → [Silver] → [Gold] → [Consumo]
```

#### Camada Bronze (01_bronze/)
* **Objetivo**: Captura de dados brutos da fonte
* **Características**:
  - Preserva estrutura original da fonte
  - Sem transformações ou validações
  - Histórico completo (append-only quando possível)
  - Rastreabilidade total da origem
* **Formato**: Delta Lake
* **Schema**: `workspace.bronze`
* **Numeração**: Notebooks `1XX_` e tabelas `workspace.bronze.1XX_`
* **Retenção**: Longo prazo (dados origem preservados)

#### Camada Silver (02_silver/)
* **Objetivo**: Dados limpos, validados e padronizados
* **Características**:
  - Normalização de tipos de dados
  - Remoção de duplicatas
  - Tratamento de valores nulos e outliers
  - Padronização de nomenclaturas
  - Enriquecimento com dados de referência
* **Formato**: Delta Lake
* **Schema**: `workspace.silver`
* **Numeração**: Notebooks `2XX_` e tabelas `workspace.silver.2XX_`
* **Retenção**: Médio/longo prazo

#### Camada Gold (03_gold/)
* **Objetivo**: Dados agregados e otimizados para consumo
* **Características**:
  - Agregações e cálculos de métricas
  - Visões orientadas a casos de uso
  - Desnormalização para performance
  - KPIs e indicadores de negócio
* **Formato**: Delta Lake
* **Schema**: `workspace.gold`
* **Numeração**: Notebooks `3XX_` e tabelas `workspace.gold.3XX_`
* **Retenção**: Conforme necessidade de negócio

## Stack Tecnológico

### Plataforma
* **Databricks**: Plataforma de lakehouse unificada
* **Workspace**: Ambiente de desenvolvimento e produção
* **Unity Catalog**: Governança e catálogo de dados

### Processamento
* **Apache Spark**: Motor de processamento distribuído
* **Delta Lake**: Formato de armazenamento com suporte ACID
* **Python**: Linguagem principal para ETL e orquestração
* **SQL**: Queries analíticas e transformações
* **Pandas**: Manipulação de datasets menores e integração com APIs

### Notebooks
* **Databricks Notebooks**: Ambiente de desenvolvimento interativo
* **Serverless Compute**: Execução sem necessidade de clusters dedicados

## Fluxo de Dados

**Visão Geral do Pipeline**:
```
[CVM Portal] → [Landing Zone] → [Bronze] → [Silver] → [Gold] → [Consumo]
     ↓              ↓              ↓           ↓          ↓
  ZIP/CSV      Preservação    Versionado   Limpo    Agregado
```

### 0. Landing Zone (Preservação)

**Objetivo**: Preservar arquivos originais da fonte sem alteração

**Localização**: Unity Catalog Volume `/Volumes/workspace/proj_cvm/landing/dfp/{ano}/`

**Pipeline**:
1. **Download** via `003_download_cvm_para_landing.py`
   - URL: `https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_{ano}.zip`
   - Destino: Volume UC (particionado por ano)
   - **Estratégia Spark Connect/Serverless**:
     * Gravação direta no Volume UC via `open()` (sem staging em `/tmp`)
     * Criação de diretórios com `os.makedirs(ano_path, exist_ok=True)`
     * Download em memória via `urllib.request.urlopen()`
     * Compatível com Serverless Compute (não usa filesystem local)
2. **Metadados** gravados em `_metadata.json`
   - `last_modified` (timestamp HTTP)
   - `content_length` (tamanho do arquivo)
   - `download_timestamp` (quando foi baixado)
3. **Rastreamento** em tabela de controle `proj_cvm_05_apoio.controle_ingestao`

**Estrutura**:
```
/Volumes/workspace/proj_cvm/landing/dfp/
├── 2020/
│   ├── dfp_cia_aberta_2020.zip
│   └── _metadata.json
├── 2021/
│   ├── dfp_cia_aberta_2021.zip
│   └── _metadata.json
```

**Benefícios**:
* Reprocessabilidade completa (arquivo original sempre disponível)
* Auditoria de mudanças na fonte (via `last_modified`)
* Separação entre download (custoso) e processamento (repetível)

---

### 1. Bronze (Ingestão Idempotente)

**Objetivo**: Captura bruta com 1 versão por ano (DELETE+APPEND)

**Notebooks**:
* `101_cvm_dfp_dre.py` → Tabela `proj_cvm_01_bronze.101_dre_dfp`
* `102_cvm_dfp_bpa.py` → Tabela `proj_cvm_01_bronze.102_bpa_dfp`

**Pipeline**:
1. **Leitura** do ZIP na Landing Zone
   - Extração em memória (`zipfile`, `io.BytesIO`)
   - Leitura de CSV com `pandas` (encoding ISO-8859-1)
   - Conversão para Spark DataFrame
2. **Validação**: Arquivo vazio? Schema inválido? → PARA (Bronze preservada)
3. **Enriquecimento com metadados**:
   ```python
   df_bronze = (
       df_raw
       .withColumn("_versao_ingestao", lit(1))  # fixo em 1 (sempre sobrescreve)
       .withColumn("_last_modified_cvm", lit(last_modified_ts))
       .withColumn("_ingest_ts", current_timestamp())
   )
   ```
4. **Gravação idempotente**: `DELETE WHERE ano + APPEND`
   ```python
   spark.sql(f"DELETE FROM proj_cvm_01_bronze.101_dre_dfp WHERE year(DT_REFER) = {ano}")
   df_bronze.write.mode("append").saveAsTable("proj_cvm_01_bronze.101_dre_dfp")
   ```
   - Sempre 1 versão por ano
   - Rodar 10x = rodar 1x (idempotência)
   - Sem acúmulo de versões duplicadas

**Características**:
* **Idempotente**: DELETE WHERE ano garante 1 versão por ano, sem duplicatas técnicas
* **Auto-corretivo**: Bugs não acumulam lixo - próxima execução limpa
* **Fail-safe**: Guardrails validam ANTES do DELETE (dados preservados em caso de erro)
* **Sem validação de last_modified**: Simplifica lógica, confiança na idempotência estrutural

> **Guardrails**: Validações detalhadas em [guardrails.md](guardrails.md)

**Estratégia de Gravação**: `DELETE WHERE ano + APPEND`

---

### 2. Silver (Transformação e Limpeza)

**Objetivo**: Dados limpos, validados e prontos para análise

**Notebooks**:
* `201_cvm_dfp_dre.py` → Tabela `proj_cvm_02_silver.201_dre_dfp`
* `202_cvm_dfp_bpa.py` → Tabela `proj_cvm_02_silver.202_bpa_dfp`

**Pipeline**:
1. **Leitura direta da Bronze** (sem Window Function - Bronze idempotente tem 1 versão)
   ```python
   df_bronze = spark.table("proj_cvm_01_bronze.101_dre_dfp").filter(year(col("DT_REFER")) == ano)
   ```
2. **Transformações**:
   - Conversão de tipos (`DT_REFER` → date, `VL_CONTA` → double)
   - Remoção de duplicatas (`distinct()`)
   - Filtro de nulos (campos críticos)
   - Enriquecimento (colunas `ANO`, `TRIMESTRE`, `MES`, `DT_PROCESSAMENTO`)
3. **Gravação**: `DELETE WHERE ano + APPEND`
   - Reprocessa apenas o período afetado
   - Preserva histórico de outros períodos

**Características**:
* **Curada**: Sem duplicatas, tipos corretos
* **Simples**: Sem Window Function (Bronze idempotente)
* **Fail-safe**: Guardrail protege Silver de DELETE sem dados

> **Guardrails**: Validações detalhadas em [guardrails.md](guardrails.md)

**Estratégia de Gravação**: `DELETE WHERE ano = {ano_processado}` + `APPEND`

---

### 3. Gold (Agregação)

**Objetivo**: Métricas de negócio e KPIs

**Métricas planejadas**:
* KPIs financeiros por empresa (margem, rentabilidade)
* Comparações setoriais
* Evolução temporal de indicadores
* Rankings e benchmarks

**Estratégia de Gravação**: `DELETE WHERE` + `APPEND` ou `INSERT OVERWRITE` (depende do caso)

---

### Orquestração

**Coordenação**: `000_orquestrador_pipeline.py`

**Pre-flight checks**:
1. Validar existência de arquivos na Landing Zone
2. Verificar última ingestão (tabela de controle)
3. Detectar novos períodos ou correções
4. Executar notebooks na ordem correta

**Tabela de Controle**: `proj_cvm_05_apoio.controle_ingestao`
* Rastreia cada ingestão (ano, timestamp, versão)
* Detecta mudanças via `last_modified`
* Evita reprocessamento desnecessário

---

### Configuração Centralizada

**Arquivo**: `05_apoio/config_parametros.py`

**Objetivo**: Ponto único de configuração para todo o pipeline (URLs, paths, schemas, contratos de dados)

#### Inicialização de ANOS_PROCESSAR

**Padrão obrigatório** (desde 31/07/2026): Chamada explícita de `inicializar_anos_processar()`

**Razão**: `config_parametros.py` não executa código Spark no import (compatibilidade com contextos não-Spark)

**Uso em notebooks**:
```python
# Célula 2: INICIALIZAÇÃO E IMPORTS
%run ../05_apoio/config_parametros  # Notebook em 01_bronze/ ou 02_silver/
# ou
%run ./config_parametros  # Notebook em 05_apoio/

# Inicializar ANOS_PROCESSAR (se ainda não foi inicializado)
if ANOS_PROCESSAR is None:
    inicializar_anos_processar()
```

**Função `inicializar_anos_processar()`**:

* **Detecção inteligente**: Consulta tabela de controle (`controle_ingestao`) para detectar anos com arquivos baixados
* **Override opcional**: Aceita argumento `force_anos` ou variável de ambiente `ANOS_PROCESSAR_OVERRIDE`
* **Silent mode**: Parâmetro `silent=True` suprime saída (usar em jobs automáticos)
* **Idempotente**: Pode ser chamada múltiplas vezes sem efeito colateral

**Exemplo de override**:
```python
# Forçar processamento de anos específicos
inicializar_anos_processar(force_anos=[2023, 2024])
```

**Benefícios**:
* ✅ Compatível com Spark Connect (não requer Spark no import)
* ✅ Testabilidade (config pode ser importado sem cluster ativo)
* ✅ Flexibilidade (fácil override para testes ou reprocessamento)
* ✅ Centralização (lógica de detecção em um único lugar)

---

## Pipeline Implementado

### Infraestrutura

**Landing Zone** (`/Volumes/workspace/proj_cvm/landing/dfp/`):
* Preservação de arquivos originais da CVM
* Metadados HTTP (`_metadata.json` por ano)
* Versionamento automático de arquivos atualizados

**Scripts de Apoio** (`05_apoio/`):
* `000_orquestrador_pipeline.py` - Detecção inteligente de períodos
* `001_ddl_create_tables.py` - Criação de schemas e tabelas Unity Catalog
* `002_ddl_controle_ingestao.py` - Tabela de controle de ingestão
* `003_download_cvm_para_landing.py` - Download e preservação na Landing Zone
* `099_ddl_table_comments.py` - Documentação de metadados
* `config_parametros.py` - Configuração centralizada

### Camada Bronze

**Notebooks e Tabelas:**

| Notebook | Tabela UC | Demonstração |
| --- | --- | --- |
| `101_cvm_dfp_dre.py` | `proj_cvm_01_bronze.101_dre_dfp` | DRE (Resultado do Exercício) |
| `102_cvm_dfp_bpa.py` | `proj_cvm_01_bronze.102_bpa_dfp` | BPA (Balanço Patrimonial Ativo) |
| `103_cvm_dfp_bpp.py` | `proj_cvm_01_bronze.103_bpp_dfp` | BPP (Balanço Patrimonial Passivo) |

**Características Técnicas:**
* **Origem**: Leitura de Landing Zone (`/Volumes/workspace/proj_cvm/landing/dfp/{ano}/`)
* **Versionamento**: Colunas `_versao_ingestao`, `_last_modified_cvm`, `_ingest_ts`
* **Estratégia**: APPEND-ONLY (histórico completo preservado)
* **Controle**: Registro em `proj_cvm_05_apoio.controle_ingestao`
* **Idempotência**: Mesma versão de arquivo gera mesma versão de dados

### Camada Silver

**Notebooks e Tabelas:**

| Notebook | Tabela UC | Demonstração |
| --- | --- | --- |
| `201_cvm_dfp_dre.py` | `proj_cvm_02_silver.201_dre_dfp` | DRE transformada |
| `202_cvm_dfp_bpa.py` | `proj_cvm_02_silver.202_bpa_dfp` | BPA transformada |

**Características Técnicas:**
* **Filtro de versão**: Window Function (ROW_NUMBER) para selecionar versão mais recente
* **Projeção explícita**: `.select()` de todas as colunas do DDL (descarta extras de Bronze)
* **Transformações**: Conversão de tipos, normalização, colunas derivadas (ANO, TRIMESTRE, MES)
* **Estratégia**: DELETE WHERE ano + APPEND (idempotência por período)
* **Particionamento**: Por ano (`ANO`)

### Camada Gold

**Status**: Em desenvolvimento
* Métricas e KPIs de negócio
* Agregações para consumo analítico

## Padrões de Desenvolvimento

### Importação de Módulos Python Compartilhados

**Padrão obrigatório**: Usar `%run` com caminho relativo

**Razão**: Portabilidade entre workspaces/contas — caminhos absolutos com e-mail hardcoded quebram ao migrar ambientes.

**Sintaxe correta**:
```python
# Notebook em 01_bronze/ ou 02_silver/
%run ../05_apoio/config_parametros

# Notebook em 05_apoio/
%run ./config_parametros
```

**❌ PROIBIDO**: Caminho absoluto ou `open()` + `exec()`
```python
# ❌ Quebra portabilidade (e-mail hardcoded)
with open('/Workspace/Users/<user-email>/.../config_parametros.py', 'r') as f:
    exec(f.read())
```

---

### Acesso a Unity Catalog Volumes

**Compatibilidade Spark Connect/Serverless**: A partir de 31/07/2026, o projeto foi refatorado para compatibilidade total com Spark Connect e Serverless Compute.

#### Estratégia Atual (Spark Connect Compliant)

**Namespace Único**: Usar `/Volumes/` diretamente com APIs Python padrão

**Regras**:

* **Criar diretórios**: `os.makedirs()` com path `/Volumes/`
  ```python
  # ✅ Correto (Spark Connect/Serverless)
  import os
  os.makedirs("/Volumes/workspace/proj_cvm/landing/dfp/2025", exist_ok=True)
  ```

* **Leitura/escrita de arquivos**: Python built-in (`open()`) com path `/Volumes/`
  ```python
  # ✅ Correto (Spark Connect/Serverless)
  with open("/Volumes/workspace/proj_cvm/landing/dfp/2025/dados.zip", "wb") as f:
      f.write(conteudo)
  ```

* **Listar arquivos**: `os.listdir()` ou `os.path.isfile()`
  ```python
  # ✅ Correto (Spark Connect/Serverless)
  import os
  for arquivo in os.listdir("/Volumes/workspace/proj_cvm/landing/dfp/2025"):
      print(arquivo)
  ```

**CRÍTICO - Restrições Spark Connect**:
* ❌ **Filesystem local bloqueado**: Paths como `/tmp/`, `/home/`, qualquer path fora de `/Workspace/` gera `LocalFilesystemAccessDeniedException`
* ❌ **Sem staging intermediário**: Não usar `/tmp` para download + copy. Gravar diretamente no destino final
* ❌ **dbutils.fs.cp() de /tmp**: Operação bloqueada em Serverless
* ✅ **APIs Python padrão**: `open()`, `os.makedirs()`, `os.listdir()` funcionam perfeitamente com `/Volumes/`

**Benefícios da abordagem atual**:
* Compatível com Serverless Compute (sem necessidade de cluster dedicado)
* Código Python idiomático (sem dependência de `dbutils`)
* Execução mais rápida (sem I/O intermediário em `/tmp`)

#### Estratégia Legada (Pré-Spark Connect)

**Namespace Dual** (descontinuado para novos notebooks):
1. **`/Volumes/`** → `dbutils.fs.*`
2. **`/dbfs/Volumes/`** → Python built-in

**Nota**: Notebooks criados antes de 31/07/2026 podem ainda usar `dbutils.fs.mkdirs()` e `/dbfs/` prefix. Ambas as abordagens funcionam, mas a estratégia atual é preferida para compatibilidade Serverless.

---

### Guardrails e Validações

**Descrição completa**: Ver [guardrails.md](guardrails.md)

**Resumo**: Validações executadas ANTES de modificar dados (Bronze: arquivo vazio, schema inválido; Silver: Bronze vazia). Protege contra perda de dados e corrupção de schema.

---

### Schema Evolution e Limitações Delta Lake

**Limitação Arquitetural**: Delta Lake **NÃO suporta** `ALTER TABLE ... ALTER COLUMN ... TYPE` para mudança de tipo de coluna existente.

**Razão técnica**: Parquet (formato subjacente) é imutável por arquivo. Mudar tipo de coluna requereria reescrever todos os arquivos Parquet da tabela, operação não suportada pela API Delta.

**Impacto Real**:
* Tentativa de `ALTER COLUMN TYPE` gera erro: `[NOT_SUPPORTED_CHANGE_COLUMN] ALTER TABLE ALTER/CHANGE COLUMN is not supported for changing...`
* Único caminho para corrigir tipo: DROP + CREATE (perde metadados de criação)
* Alternativa CTAS (Create Table As Select) também cria nova tabela, perde metadados

#### Estratégia de Migration Implementada

**Objetivo**: Código executável em qualquer ambiente (novo ou existente) sem intervenção manual.

**Implementação** (`001_ddl_create_tables.py`):

```python
def apply_schema_migration_if_needed():
    """Aplica migrações de schema de forma idempotente.
    
    Migrations:
    - 001 (31/07/2026): Correção tipos STRING → INT/TIMESTAMP
    """
    migrations = [
        ("proj_cvm_05_apoio.controle_ingestao", "last_modified_cvm", "TIMESTAMP"),
        ("proj_cvm_02_silver.201_dre_dfp", "VERSAO", "INT"),
        ("proj_cvm_02_silver.201_dre_dfp", "CD_CVM", "INT"),
        # ...
    ]
    
    for table, column, new_type in migrations:
        try:
            spark.sql(f"ALTER TABLE {table} ALTER COLUMN {column} TYPE {new_type}")
        except Exception as e:
            # Tratamento de erros esperados
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
                pass  # Tabela ainda não existe, CREATE fará o correto
            elif "NOT_SUPPORTED_CHANGE_COLUMN" in str(e):
                pass  # Limitação Delta Lake, requer DROP+CREATE manual
```

**Comportamento**:
1. **Ambiente novo** (clone do repo): Migration tenta ALTER (falha silenciosamente, tabelas não existem), CREATE funciona com tipos corretos
2. **Ambiente existente** (workspace com tabelas antigas): Migration detecta `NOT_SUPPORTED_CHANGE_COLUMN`, reporta que requer DROP+CREATE
3. **Idempotente**: Pode executar múltiplas vezes sem efeito colateral

#### Tradeoff Documentado

**Decisão arquitetural**: Priorizar schema correto sobre preservação de metadados.

**Justificativa**:
* Para **portfólio**: Código executável do zero (funciona em qualquer ambiente novo) > `created_time` de tabelas antigas
* Em **produção real**: DROP+CREATE seria feito em maintenance window documentado
* Demonstra conhecimento de limitação real do Delta Lake (diferencial técnico)

**Alternativas consideradas e rejeitadas**:
1. Manter schema STRING: Preserva metadados, mas DDL diverge de implementação (confuso, não profissional)
2. CTAS sem DROP: Cria nova tabela, perde metadados igualmente, não resolve o problema
3. Documentar como "precisa intervenção manual": Quebra objetivo de código executável

**Resultado**: Qualquer pessoa que clonar o repositório e executar o pipeline terá sucesso imediato, sem configuração manual.

---

### Convenção de Numeração (OBRIGATÓRIA)

#### Pastas
* **Formato**: 2 dígitos + nome descritivo
* **Exemplos**: `00_documentacao`, `01_bronze`, `02_silver`, `03_gold`, `04_analises_exploratorias`, `05_apoio`
* **Objetivo**: Forçar ordenação lógica (não alfabética)
* **Estrutura atual**:
  - `00_documentacao/` - Documentação do projeto
  - `01_bronze/` - Notebooks de ingestão (camada bronze)
  - `02_silver/` - Notebooks de transformação (camada silver)
  - `03_gold/` - Notebooks de agregação (camada gold)
  - `04_analises_exploratorias/` - Notebooks de EDA
  - `05_apoio/` - Scripts de infraestrutura (DDL, orquestrador, config, download)

#### Notebooks
* **Formato**: 3 dígitos + nome descritivo autocontido
* **Padrão**: `[X][YY]_[descricao]`
  - `X` = camada (1=bronze, 2=silver, 3=gold)
  - `YY` = sequência (01-99)
* **Exemplos**: 
  - `101_cvm_dfp_dre` (bronze, primeiro notebook)
  - `102_cvm_itr_dre` (bronze, segundo notebook)
  - `201_transformacao_dre` (silver, primeiro notebook)
  - `301_kpis_dre` (gold, primeiro notebook)
* **Benefícios**:
  - Rastreabilidade clara: `101` → `201` → `301` tratam o mesmo dado
  - Ordenação perfeita
  - Consistência visual
  - Capacidade adequada (99 notebooks por camada)

#### Tabelas Unity Catalog
* **Formato**: `workspace.[camada].[XXX]_[nome_tabela]`
* **Exemplos**:
  - `workspace.bronze.101_dre_consolidada`
  - `workspace.silver.201_dre_limpa`
  - `workspace.gold.301_indicadores_empresas`
* **Regra**: Numeração da tabela segue o notebook que a cria

### Princípio DRY em Nomenclatura

**DRY = Don't Repeat Yourself**

O nome do arquivo NÃO deve repetir informações já explícitas na estrutura de diretórios.

**✅ Correto**:
```
01_bronze/101_cvm_dfp_dre.py
```

**❌ Errado**:
```
01_bronze/101_cvm_dfp_dre_bronze.ipynb  # "bronze" é redundante!
```

A pasta já diz que é bronze, o nome do arquivo não precisa repetir.

### Estrutura de Notebooks

**Estrutura padrão**:
1. Célula 1 (Markdown): Documentação
2. Célula 2 (Python): INICIALIZAÇÃO E IMPORTS
3. Demais células: Transformações

**Células de transformação**:
```python
# df_[nome]: [O que a célula faz]
# Justificativa: [Por que essa transformação é necessária]

df_resultado = spark.sql("""
    SELECT ...
    FROM ...
""")
```

### Nomenclatura

* **DataFrames**: `df_[descricao_autocontida]`
  - Exemplo: `df_dre_consolidada` (não `df_bronze_dre`)
  - NÃO incluir camada quando notebook é dedicado a uma camada
  - Foco: descrever o DADO, não a infraestrutura
* **Títulos de células**: SEMPRE EM MAIÚSCULO

### Governança

* **Schemas Unity Catalog**:
  - `workspace.bronze` - Dados brutos
  - `workspace.silver` - Dados transformados
  - `workspace.gold` - Dados agregados
* **Controle de versão**: Git (apenas código consolidado)
* **Documentação**: Arquivos Markdown no próprio projeto

---

### Requisitos de Resiliência

**Princípio**: O projeto adota os padrões de resiliência operacional definidos no projeto [databricks-genie-skills](https://github.com/1pedroosilva/databricks-genie-skills) (skill `resiliencia-operacional`).

**Implementação obrigatória em notebooks de produção**:
* Retry logic com exponential backoff para chamadas HTTP/APIs
* Tratamento granular de erros (try/except por unidade de trabalho)
* Logging estruturado (timestamp, contexto, status)
* Checkpointing via tabela de controle (`proj_cvm_05_apoio.controle_ingestao`)
* Validação de pré-requisitos antes de processar
* Auto-ajuste de períodos (detecção inteligente de pendentes)
* Parametrização externa (config em arquivos Python)

**Referência técnica completa**: Ver skill `resiliencia-operacional` no projeto [databricks-genie-skills](https://github.com/1pedroosilva/databricks-genie-skills)

## Segurança e Compliance

* **Dados Públicos**: Dados da CVM são públicos, sem restrições de acesso
* **Unity Catalog**: Controle de acesso em nível de schema/tabela
* **Auditoria**: Delta Lake mantém histórico de mudanças (time travel)

## Escalabilidade

* **Serverless Compute**: Escala automática conforme demanda
* **Delta Lake**: Otimizações automáticas (compactação, indexação)
* **Particionamento**: A ser implementado conforme crescimento de dados

## Monitoramento

* **Job Runs**: Histórico de execuções disponível no Databricks
* **Delta History**: Auditoria de mudanças nas tabelas
* **Logs**: Logs de execução de notebooks

## Próximas Evoluções Técnicas

1. **Orquestração**: Databricks Workflows para agendamento
2. **Incremental Load**: Mudança de overwrite para append incremental
3. **Data Quality**: Validações automáticas com Great Expectations
4. **Particionamento**: Particionamento por ano para performance
5. **Otimização**: Z-ordering para queries frequentes