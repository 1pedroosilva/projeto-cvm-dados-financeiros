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
* **Schema**: `{SCHEMA_BRONZE}`
* **Numeração**: Notebooks `1XX_` e tabelas `{SCHEMA_BRONZE}.1XX_`
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
* **Schema**: `{SCHEMA_SILVER}`
* **Numeração**: Notebooks `2XX_` e tabelas `{SCHEMA_SILVER}.2XX_`
* **Retenção**: Médio/longo prazo

#### Camada Gold (03_gold/)
* **Objetivo**: Dados agregados e otimizados para consumo
* **Características**:
  - Agregações e cálculos de métricas
  - Visões orientadas a casos de uso
  - Desnormalização para performance
  - KPIs e indicadores de negócio
* **Formato**: Delta Lake
* **Schema**: `{SCHEMA_GOLD}`
* **Numeração**: Notebooks `3XX_` e tabelas `{SCHEMA_GOLD}.3XX_`
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

**Localização**: Unity Catalog Volume `{VOLUME_LANDING_DFP}/{ano}/`

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
3. **Rastreamento** em tabela de controle `{SCHEMA_APOIO}.controle_ingestao`

**Estrutura**:
```
{VOLUME_LANDING_DFP}/
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

**Objetivo**: Captura bruta com histórico completo (APPEND-ONLY)

**Notebooks**:
* `101_cvm_dfp_dre.py` → Tabela `{SCHEMA_BRONZE}.101_dre_dfp`
* `102_cvm_dfp_bpa.py` → Tabela `{SCHEMA_BRONZE}.102_bpa_dfp`
* `103_cvm_dfp_bpp.py` → Tabela `{SCHEMA_BRONZE}.103_bpp_dfp`

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
4. **Gravação**: `APPEND-ONLY` (histórico completo)
   ```python
   df_bronze.write.mode("append").saveAsTable("{SCHEMA_BRONZE}.101_dre_dfp")
   ```
   - Preserva histórico de versões (múltiplas execuções = múltiplas versões)
   - Silver aplica Window Function para selecionar versão mais recente
   - Idempotência garantida via controle + verificação de dados reais

**Características**:
* **Idempotente**: APPEND-ONLY preserva histórico, Silver filtra versão mais recente
* **Auto-corretivo**: Bugs não acumulam lixo - próxima execução corrige
* **Fail-safe**: Guardrails validam ANTES do APPEND (dados preservados em caso de erro)
* **Verificação de Last-Modified**: Compara `_metadata.json` da landing contra `controle_ingestao` para detectar republicações

> **Guardrails**: Validações detalhadas em [guardrails.md](guardrails.md)

**Estratégia de Gravação**: `APPEND-ONLY` (histórico completo preservado)

---

### 2. Silver (Transformação e Limpeza)

**Objetivo**: Dados limpos, validados e prontos para análise

**Notebooks**:
* `201_cvm_dfp_dre.py` → Tabela `{SCHEMA_SILVER}.201_dre_dfp`
* `202_cvm_dfp_bpa.py` → Tabela `{SCHEMA_SILVER}.202_bpa_dfp`
* `203_cvm_dfp_bpp.py` → Tabela `{SCHEMA_SILVER}.203_bpp_dfp`

**Pipeline**:
1. **Leitura da Bronze com Window Function** (filtro de versão mais recente via `_versao_ingestao`)
   ```python
   df_bronze = spark.table("{SCHEMA_BRONZE}.101_dre_dfp").filter(year(col("DT_REFER")) == ano)
   ```
2. **Transformações**:
   - Conversão de tipos (`DT_REFER` → date, `VL_CONTA` → double)
   - Normalização de escala monetária (`VL_CONTA` × 1000 quando `ESCALA_MOEDA = "MIL"`)
   - Filtro de nulos (campos críticos)
   - Enriquecimento temporal (colunas `ANO`, `TRIMESTRE`, `MES`, `DT_PROCESSAMENTO`)
   - Enriquecimento hierárquico (colunas `ST_CONTA_FIXA`, `NIVEL_CONTA`, `CD_CONTA_PAI`, `CD_CONTA_RAIZ` derivadas de `CD_CONTA`)
3. **Gravação**: `REPLACE WHERE ano`
   - Substituição atômica por período: Delta Lake garante operação all-or-nothing
   - Elimina janela de vulnerabilidade entre DELETE e APPEND
   - Reprocessa apenas o período afetado, preserva histórico de outros períodos
   - Preserva histórico de outros períodos

**Características**:
* **Curada**: Sem duplicatas, tipos corretos
* **Versionada**: Window Function seleciona versão mais recente da Bronze
* **Fail-safe**: Guardrail protege Silver de processar Bronze vazia

> **Guardrails**: Validações detalhadas em [guardrails.md](guardrails.md)

**Estratégia de Gravação**: `REPLACE WHERE` (substituição atômica por período)

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

**Detecção**: Cada notebook Bronze/Silver chama `inicializar_anos_processar()` de `config_parametros.py`

**Detecção inteligente** (leitura local, sem HTTP):
1. Consulta tabela de controle (`controle_ingestao`) para identificar anos processados
2. Verifica dados reais na tabela destino (não confia só no SUCCESS)
3. Compara `Last-Modified` do `_metadata.json` na landing contra `last_modified_cvm` do controle
4. Três estados: (1) nunca processado, (2) fantasma (SUCCESS sem dados), (3) republicado (metadado diverge do controle)
5. Aplica janela temporal (`JANELA_ANOS_RELEVANTE`)
6. Consolida 6 fontes: 3 Bronze + 3 Silver

> **Nota**: O notebook `000_orquestrador_pipeline.py` existe em `05_apoio/` mas foi removido do job de produção (890867014997453) em 19/09/2026. A detecção agora é distribuída — cada notebook chama `inicializar_anos_processar()` independentemente. Busca por arquivos novos na CVM (HTTP) é responsabilidade do job diário de download.
>
> **004_verificacao_diaria_landing**: Itera a janela temporal diretamente (`range(ano_atual - JANELA_ANOS_RELEVANTE, ano_atual + 1)`), sem depender de `inicializar_anos_processar()`. Esta independência é intencional: o notebook que descobre mudanças na fonte não pode receber a lista de quem já assumiu que nada mudou.

**Tabela de Controle**: `{SCHEMA_APOIO}.controle_ingestao`
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
# Célula 2: Carregar configurações
%run ../05_apoio/config_parametros  # Notebook em 01_bronze/ ou 02_silver/
# ou
%run ./config_parametros  # Notebook em 05_apoio/

# Célula 3: Inicializar Anos a Processar
# CARGA (do config_parametros) controla a janela de anos:
#   incremental (padrão): detecção inteligente via controle_ingestao
#   completa: força todos os anos disponíveis (2021-ano corrente)
ANOS_PROCESSAR = inicializar_anos_processar()
```

**Função `inicializar_anos_processar()`**:

* **Detecção inteligente**: Consulta tabela de controle (`controle_ingestao`) para detectar anos com arquivos baixados
* **Override opcional**: Aceita argumento `force_anos` (uso interno) ou variável de ambiente `CARGA=completa` para forçar todos os anos
* **Silent mode**: Parâmetro `silent=True` suprime saída (usar em jobs automáticos)
* **Idempotente**: Pode ser chamada múltiplas vezes sem efeito colateral

**Exemplo de override**:
```python
# Reprocessar todos os anos: CARGA=completa
# Via widget UI: criar widget CARGA com valor "completa"
# Ou via variável de ambiente: export CARGA=completa
# O eixo CARGA e resolvido no config_parametros via ambientes.json
```

**Benefícios**:
* ✅ Compatível com Spark Connect (não requer Spark no import)
* ✅ Testabilidade (config pode ser importado sem cluster ativo)
* ✅ Flexibilidade (fácil override para testes ou reprocessamento)
* ✅ Centralização (lógica de detecção em um único lugar)

---

## Pipeline Implementado

### Infraestrutura

**Landing Zone** (`{VOLUME_LANDING_DFP}`):
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
| `101_cvm_dfp_dre.py` | `{SCHEMA_BRONZE}.101_dre_dfp` | DRE (Resultado do Exercício) |
| `102_cvm_dfp_bpa.py` | `{SCHEMA_BRONZE}.102_bpa_dfp` | BPA (Balanço Patrimonial Ativo) |
| `103_cvm_dfp_bpp.py` | `{SCHEMA_BRONZE}.103_bpp_dfp` | BPP (Balanço Patrimonial Passivo) |

**Características Técnicas:**
* **Origem**: Leitura de Landing Zone (`{VOLUME_LANDING_DFP}/{ano}/`)
* **Versionamento**: Colunas `_versao_ingestao`, `_last_modified_cvm`, `_ingest_ts`
* **Estratégia**: APPEND-ONLY (histórico completo preservado)
* **Controle**: Registro em `{SCHEMA_APOIO}.controle_ingestao`
* **Idempotência**: Mesma versão de arquivo gera mesma versão de dados

### Camada Silver

**Notebooks e Tabelas:**

| Notebook | Tabela UC | Demonstração |
| --- | --- | --- |
| `201_cvm_dfp_dre.py` | `{SCHEMA_SILVER}.201_dre_dfp` | DRE transformada |
| `202_cvm_dfp_bpa.py` | `{SCHEMA_SILVER}.202_bpa_dfp` | BPA transformada |
| `203_cvm_dfp_bpp.py` | `{SCHEMA_SILVER}.203_bpp_dfp` | BPP transformada |

**Características Técnicas:**
* **Filtro de versão**: Window Function (ROW_NUMBER) para selecionar versão mais recente
* **Projeção explícita**: `.select()` de todas as colunas do DDL (descarta extras de Bronze)
* **Transformações**: Conversão de tipos, normalização de escala monetária (MIL → reais), colunas derivadas (ANO, TRIMESTRE, MES), enriquecimento hierárquico (ST_CONTA_FIXA, NIVEL_CONTA, CD_CONTA_PAI, CD_CONTA_RAIZ)
* **Estratégia**: REPLACE WHERE (substituição atômica por período - elimina janela de vulnerabilidade do DELETE+APPEND)
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
  os.makedirs(f"{VOLUME_LANDING_DFP}/2025", exist_ok=True)
  ```

* **Leitura/escrita de arquivos**: Python built-in (`open()`) com path `/Volumes/`
  ```python
  # ✅ Correto (Spark Connect/Serverless)
  with open(f"{VOLUME_LANDING_DFP}/2025/dados.zip", "wb") as f:
      f.write(conteudo)
  ```

* **Listar arquivos**: `os.listdir()` ou `os.path.isfile()`
  ```python
  # ✅ Correto (Spark Connect/Serverless)
  import os
  for arquivo in os.listdir(f"{VOLUME_LANDING_DFP}/2025"):
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
        ("{SCHEMA_APOIO}.controle_ingestao", "last_modified_cvm", "TIMESTAMP"),
        ("{SCHEMA_SILVER}.201_dre_dfp", "VERSAO", "INT"),
        ("{SCHEMA_SILVER}.201_dre_dfp", "CD_CVM", "INT"),
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
* **Reprodutibilidade**: código executável do zero (funciona em qualquer ambiente novo) > `created_time` de tabelas antigas
* **Ambiente com tabelas existentes**: DROP+CREATE executado em janela de manutenção documentada
* **Limitação do Delta Lake**: ALTER COLUMN TYPE não suporta essa mudança de tipo

**Alternativas consideradas e rejeitadas**:
1. Manter schema STRING: Preserva metadados, mas DDL diverge de implementação (tipos declarados não correspondem aos casts no código)
2. CTAS sem DROP: Cria nova tabela, perde metadados igualmente, não resolve o problema
3. Documentar como "precisa intervenção manual": Quebra objetivo de código executável

**Resultado**: Qualquer pessoa que clonar o repositório e executar o pipeline terá sucesso imediato, sem configuração manual.

---

### Convenção de Numeração (OBRIGATÓRIA)

#### Pastas
* **Formato**: 2 dígitos + nome descritivo
* **Exemplos**: `00_documentacao`, `01_bronze`, `02_silver`, `03_gold`, `04_exploracao`, `05_apoio`
* **Objetivo**: Forçar ordenação lógica (não alfabética)
* **Estrutura atual**:
  - `00_documentacao/` - Documentação do projeto
  - `01_bronze/` - Notebooks de ingestão (camada bronze)
  - `02_silver/` - Notebooks de transformação (camada silver)
  - `03_gold/` - Notebooks de agregação (camada gold)
  - `04_exploracao/` - Notebooks de EDA
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
* **Formato**: `{SCHEMA_[CAMADA]}.[XXX]_[nome_tabela]` (prefixo derivado do ambiente via config_parametros)
* **Exemplos**:
  - `{SCHEMA_BRONZE}.101_dre_dfp`
  - `{SCHEMA_SILVER}.201_dre_dfp`
  - `{SCHEMA_GOLD}.301_indicadores_empresas`
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
  - `{SCHEMA_BRONZE}` - Dados brutos
  - `{SCHEMA_SILVER}` - Dados transformados
  - `{SCHEMA_GOLD}` - Dados agregados
* **Controle de versão**: Git (apenas código consolidado)
* **Documentação**: Arquivos Markdown no próprio projeto

---

### Requisitos de Resiliência

**Princípio**: O projeto adota os padrões de resiliência operacional definidos no projeto [databricks-genie-skills](https://github.com/1pedroosilva/databricks-genie-skills) (skill `resiliencia-operacional`).

**Implementação obrigatória em notebooks de produção**:
* Retry logic com exponential backoff para chamadas HTTP/APIs
* Tratamento granular de erros (try/except por unidade de trabalho)
* Logging estruturado (timestamp, contexto, status)
* Checkpointing via tabela de controle (`{SCHEMA_APOIO}.controle_ingestao`)
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

## Deploy e Infraestrutura

> **[SUPERSEDED em 19/09/2026 — orquestrador removido do job; ver seção Orquestração]**

O pipeline é implantado via **Databricks Asset Bundles (DABs)**, definido em `databricks.yml` (raiz do projeto) e `resources/jobs/job_pipeline_cvm.yml`. O Job `Pipeline CVM - DFP` (id `661897477878521`), originalmente criado manualmente na UI, foi adotado pelo bundle via `databricks bundle deployment bind` — não foi recriado, preservando histórico de execuções.

**Características do deploy gerenciado por bundle:**
* **`edit_mode: UI_LOCKED`**: o job não pode mais ser editado diretamente na interface do Databricks. Mudanças em tasks, schedule ou notificações exigem editar o YAML e rodar `databricks bundle deploy --target <dev|prod>`
* **Sincronização de notebooks**: o job executa cópias dos notebooks sincronizadas pelo bundle (`/Workspace/Users/<usuario>/.bundle/projeto-cvm-dados-financeiros/<target>/files/...`), não os arquivos originais do projeto diretamente. Uma edição no notebook original só passa a valer para o job após um novo `bundle deploy`
* **Compute serverless obrigatório**: o workspace não suporta cluster clássico (`new_cluster`); as tasks do job rodam sem especificação de cluster
* **Modo `development`**: o target `dev` prefixa o nome do job com `[dev <usuario>]` automaticamente

**Targets declarados**: `dev` (padrão, prefixo `proj_cvm_dev`, único instanciado), `test` (prefixo `proj_cvm_test`, não instanciado) e `prod` (prefixo `proj_cvm_prod`, não instanciado), todos no mesmo workspace.

## Parametrizacao de Ambiente

Todos os nomes de catalogo, schema, tabela e volume sao derivados de `ambientes.json` via `config_parametros`. Nenhum notebook monta nome por conta propria.

**Fonte unica**: `05_apoio/ambientes.json` define catalogo, prefixo de schema por ambiente, camadas (bronze/silver/gold/apoio) e schema do volume. O `config_parametros` le o JSON em tempo de execucao e deriva:
* `CATALOG_NAME`, `SCHEMA_BRONZE`, `SCHEMA_SILVER`, `SCHEMA_GOLD`, `SCHEMA_APOIO`
* `SCHEMA_VOLUME` (schema do volume da landing zone, compartilhado entre ambientes)
* `VOLUME_LANDING`, `VOLUME_LANDING_DFP`
* `TABELA_CONTROLE` (catalogo.schema.controle_ingestao)
* `AMBIENTE`, `CARGA` (eixos independentes: ambiente e tipo de carga)

**Eixos independentes**:
* `AMBIENTE` (dev/test/prod): define catalogo e prefixo de schema
* `CARGA` (incremental/completa): controla a janela de anos processada

**DDL unificado**: O notebook `001_ddl_create_tables.py` cria todos os schemas e tabelas do projeto em uma unica passagem idempotente, incluindo `controle_ingestao`. Antes, essa tabela era criada apenas no `002_ddl_controle_ingestao.py`; sem ela, a primeira execucao do orquestrador em schema novo quebrava (`get_novos_anos_para_processar` faz SELECT na tabela). O 002 permanece como validacao idempotente.

## Próximas Evoluções Técnicas

1. **Data Quality**: Validações automáticas com Great Expectations
2. **Particionamento**: Particionamento por ano para performance
3. **Otimização**: Z-ordering para queries frequentes
