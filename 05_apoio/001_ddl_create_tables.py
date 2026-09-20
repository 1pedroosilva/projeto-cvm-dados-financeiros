# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,DOCUMENTAÇÃO
# MAGIC %md
# MAGIC # Criação de Schemas e Tabelas Unity Catalog
# MAGIC
# MAGIC ## Objetivo
# MAGIC Estabelecer a estrutura de dados do projeto via DDL explícito. Cria schemas das três camadas (Bronze, Silver, Gold) e define todas as tabelas com tipos de dados, comentários e estrutura completa antes de qualquer ingestão.
# MAGIC
# MAGIC ## Abordagem de Governança
# MAGIC * **Separação DDL/DML**: Infraestrutura (CREATE TABLE) separada de dados (INSERT/APPEND) - padrão de governança bancária
# MAGIC * **Schema explícito**: Todos os tipos de dados declarados manualmente (STRING, INT, DOUBLE, DATE, TIMESTAMP)
# MAGIC * **Idempotência**: `CREATE IF NOT EXISTS` permite reexecução sem erros
# MAGIC * **Metadados técnicos**: Colunas `_versao_ingestao`, `_last_modified_cvm`, `_ingest_ts`, `_source_file` para auditoria
# MAGIC
# MAGIC ## Estrutura Criada
# MAGIC * **Schemas**: `proj_cvm_01_bronze`, `proj_cvm_02_silver`, `proj_cvm_03_gold`
# MAGIC * **Tabelas Bronze**: `101_dre_dfp`, `102_bpa_dfp` (append-only com versionamento)
# MAGIC * **Tabelas Silver**: `201_dre_dfp`, `202_bpa_dfp` (versão mais recente + enriquecimento)
# MAGIC
# MAGIC ## Função
# MAGIC Script de apoio - Setup de infraestrutura Unity Catalog

# COMMAND ----------

# DBTITLE 1,CRIAÇÃO DE SCHEMAS
# Criação de schemas Bronze, Silver e Gold
spark.sql("""
CREATE SCHEMA IF NOT EXISTS proj_cvm_01_bronze
COMMENT 'Camada Bronze - Ingestão bruta de dados da CVM sem transformações'
""")

spark.sql("""
CREATE SCHEMA IF NOT EXISTS proj_cvm_02_silver
COMMENT 'Camada Silver - Dados transformados, limpos e padronizados'
""")

spark.sql("""
CREATE SCHEMA IF NOT EXISTS proj_cvm_03_gold
COMMENT 'Camada Gold - Métricas de negócio e agregações para análise'
""")

print("✅ Schemas criados com sucesso")

# COMMAND ----------

# DBTITLE 1,SCHEMA EVOLUTION - Migrations Idempotentes
# ============================================================================
# SCHEMA EVOLUTION: Aplicar correções de tipo em ambientes existentes
# ============================================================================
# Garante que código funciona tanto em ambiente novo quanto em workspace
# com versões anteriores das tabelas. ALTER TABLE preserva dados e metadados.
# Idempotente: pode ser executado múltiplas vezes sem efeito colateral.

def apply_schema_migration_if_needed():
    """Aplica migrações de schema de forma idempotente.
    
    Migrations:
    - 001 (31/07/2026): Correção tipos STRING → INT/TIMESTAMP
      - Motivo: Alinhamento DDL com transformações + detecção de atualizações
      - Tabelas: Silver (VERSAO, CD_CVM), Controle (last_modified_cvm)
    """
    
    migrations = [
        # Migration 001: Correção tipos STRING → INT/TIMESTAMP (31/07/2026)
        ("proj_cvm_05_apoio.controle_ingestao", "last_modified_cvm", "TIMESTAMP",
         "Permite comparação direta com datetime HTTP Last-Modified"),
        ("proj_cvm_02_silver.201_dre_dfp", "VERSAO", "INT",
         "Alinhamento com cast aplicado na transformação Silver"),
        ("proj_cvm_02_silver.201_dre_dfp", "CD_CVM", "INT",
         "Alinhamento com cast aplicado na transformação Silver"),
        ("proj_cvm_02_silver.202_bpa_dfp", "VERSAO", "INT",
         "Alinhamento com cast aplicado na transformação Silver"),
        ("proj_cvm_02_silver.202_bpa_dfp", "CD_CVM", "INT",
         "Alinhamento com cast aplicado na transformação Silver"),
    ]
    
    print("="*80)
    print("SCHEMA EVOLUTION - Aplicando migrations (idempotente)")
    print("="*80)
    
    for table, column, new_type, reason in migrations:
        try:
            spark.sql(f"ALTER TABLE {table} ALTER COLUMN {column} TYPE {new_type}")
            print(f"✅ {table}.{column} → {new_type}")
            print(f"   Motivo: {reason}")
        except Exception as e:
            error_msg = str(e)
            
            # Tabela não existe ainda (será criada pelo CREATE abaixo)
            if "TABLE_OR_VIEW_NOT_FOUND" in error_msg or "does not exist" in error_msg:
                print(f"⏭️  {table}.{column} - Tabela não existe (será criada)")
            
            # Coluna já está no tipo correto
            elif "Cannot safely cast" in error_msg or "same type" in error_msg:
                print(f"✓  {table}.{column} - Já está como {new_type}")
            
            # Outro erro (reportar mas não falhar)
            else:
                print(f"⚠️  {table}.{column} - {error_msg[:100]}")
    
    # Migration 002: Adição de colunas TIPO_CONTA e TIPO_ESTRUTURAL (20/09/2026)
    # Identificado pelas EDAs 001/002/003 como correção necessária para evitar
    # dupla contagem em somas (totalizadoras + analíticas) e distinguir
    # contas aditivas (soma de filhas) de derivadas (fórmula entre irmãs)
    add_column_migrations = [
        ("proj_cvm_02_silver.201_dre_dfp", "TIPO_CONTA", "STRING",
         "Classificação TOTALIZADORA vs ANALITICA (EDA 001)"),
        ("proj_cvm_02_silver.201_dre_dfp", "TIPO_ESTRUTURAL", "STRING",
         "Classificação ADITIVA vs DERIVADA para totalizadoras da DRE (EDA 001)"),
        ("proj_cvm_02_silver.202_bpa_dfp", "TIPO_CONTA", "STRING",
         "Classificação TOTALIZADORA vs ANALITICA (EDA 002)"),
        ("proj_cvm_02_silver.203_bpp_dfp", "TIPO_CONTA", "STRING",
         "Classificação TOTALIZADORA vs ANALITICA (EDA 003)"),
    ]
    
    for table, column, col_type, reason in add_column_migrations:
        try:
            spark.sql(f"ALTER TABLE {table} ADD COLUMNS ({column} {col_type})")
            print(f"✅ {table}.{column} adicionada ({col_type})")
            print(f"   Motivo: {reason}")
        except Exception as e:
            error_msg = str(e)
            if "TABLE_OR_VIEW_NOT_FOUND" in error_msg or "does not exist" in error_msg:
                print(f"⏭️  {table}.{column} - Tabela não existe (será criada)")
            elif "already exists" in error_msg or "DUPLICATE_COLUMN" in error_msg:
                print(f"✓  {table}.{column} - Coluna já existe")
            else:
                print(f"⚠️  {table}.{column} - {error_msg[:100]}")
    
    print("="*80)
    print("Migrations concluídas. Prosseguindo com CREATE TABLE IF NOT EXISTS...")
    print("="*80)

# Executar migrations antes de criar tabelas
apply_schema_migration_if_needed()

# COMMAND ----------

# DBTITLE 1,CRIAÇÃO DA TABELA BRONZE - 101_dre_dfp
# Tabela Bronze: DRE (dados brutos as-is + metadados técnicos de ingestão)
spark.sql("""
CREATE TABLE IF NOT EXISTS proj_cvm_01_bronze.101_dre_dfp (
  CNPJ_CIA STRING,
  DT_REFER STRING,
  VERSAO STRING,
  DENOM_CIA STRING,
  CD_CVM STRING,
  GRUPO_DFP STRING,
  MOEDA STRING,
  ESCALA_MOEDA STRING,
  ORDEM_EXERC STRING,
  DT_INI_EXERC STRING,
  DT_FIM_EXERC STRING,
  CD_CONTA STRING,
  DS_CONTA STRING,
  VL_CONTA STRING,
  ST_CONTA_FIXA STRING,
  _versao_ingestao INT,
  _last_modified_cvm STRING,
  _ingest_ts TIMESTAMP,
  _source_file STRING
)
USING DELTA
COMMENT 'DRE consolidada - Dados brutos extraídos do portal CVM. Mantém estrutura original + metadados técnicos + versionamento (_versao_ingestao preserva TODAS as versões).'
""")

print("✅ Tabela proj_cvm_01_bronze.101_dre_dfp criada")

# COMMAND ----------

# DBTITLE 1,CRIAÇÃO DA TABELA BRONZE - 102_bpa_dfp
# Tabela Bronze: BPA (dados brutos as-is + metadados técnicos de ingestão)
spark.sql("""
CREATE TABLE IF NOT EXISTS proj_cvm_01_bronze.102_bpa_dfp (
  CNPJ_CIA STRING,
  DT_REFER STRING,
  VERSAO STRING,
  DENOM_CIA STRING,
  CD_CVM STRING,
  GRUPO_DFP STRING,
  MOEDA STRING,
  ESCALA_MOEDA STRING,
  ORDEM_EXERC STRING,
  -- DT_INI_EXERC removida: BPA não contém na fonte CVM (snapshot, não período)
  DT_FIM_EXERC STRING,
  CD_CONTA STRING,
  DS_CONTA STRING,
  VL_CONTA STRING,
  ST_CONTA_FIXA STRING,
  _versao_ingestao INT,
  _last_modified_cvm STRING,
  _ingest_ts TIMESTAMP,
  _source_file STRING
)
USING DELTA
COMMENT 'BPA consolidado - Dados brutos extraídos do portal CVM. Mantém estrutura original + metadados técnicos + versionamento (_versao_ingestao preserva TODAS as versões).'
""")

print("✅ Tabela proj_cvm_01_bronze.102_bpa_dfp criada")

# COMMAND ----------

# DBTITLE 1,CRIAÇÃO DA TABELA BRONZE - 103_bpp_dfp
# Tabela Bronze: BPP (dados brutos as-is + metadados técnicos de ingestão)
spark.sql("""
CREATE TABLE IF NOT EXISTS proj_cvm_01_bronze.103_bpp_dfp (
  CNPJ_CIA STRING,
  DT_REFER STRING,
  VERSAO STRING,
  DENOM_CIA STRING,
  CD_CVM STRING,
  GRUPO_DFP STRING,
  MOEDA STRING,
  ESCALA_MOEDA STRING,
  ORDEM_EXERC STRING,
  DT_FIM_EXERC STRING,
  CD_CONTA STRING,
  DS_CONTA STRING,
  VL_CONTA STRING,
  ST_CONTA_FIXA STRING,
  _versao_ingestao INT,
  _last_modified_cvm STRING,
  _ingest_ts TIMESTAMP,
  _source_file STRING
)
USING DELTA
COMMENT 'BPP consolidado - Dados brutos extraídos do portal CVM. Mantém estrutura original + metadados técnicos + versionamento (_versao_ingestao preserva TODAS as versões).'
""")

print("✅ Tabela proj_cvm_01_bronze.103_bpp_dfp criada")

# COMMAND ----------

# DBTITLE 1,CRIACAO DA TABELA SILVER - 203_bpp_dfp
# Tabela Silver: BPP transformada (dados limpos e enriquecidos, particionada por ANO)
spark.sql("""
CREATE TABLE IF NOT EXISTS proj_cvm_02_silver.203_bpp_dfp (
  CNPJ_CIA STRING,
  DT_REFER DATE,
  VERSAO INT,
  DENOM_CIA STRING,
  CD_CVM INT,
  GRUPO_DFP STRING,
  MOEDA STRING,
  ESCALA_MOEDA STRING,
  ORDEM_EXERC STRING,
  DT_FIM_EXERC DATE,
  CD_CONTA STRING,
  DS_CONTA STRING,
  VL_CONTA DOUBLE,
  ANO INT,
  TRIMESTRE INT,
  MES INT,
  DT_PROCESSAMENTO TIMESTAMP,
  ST_CONTA_FIXA STRING,
  NIVEL_CONTA INT,
  CD_CONTA_PAI STRING,
  CD_CONTA_RAIZ STRING,
  TIPO_CONTA STRING
)
USING DELTA
PARTITIONED BY (ANO)
COMMENT 'BPP transformado - Dados limpos, tipados e enriquecidos com colunas temporais. Particionada por ano para DELETE+APPEND incremental eficiente.'
""")

print("✅ Tabela proj_cvm_02_silver.203_bpp_dfp criada")

# COMMAND ----------

# DBTITLE 1,CRIACAO DA TABELA SILVER - 201_dre_dfp
# Tabela Silver: DRE transformada (dados limpos e enriquecidos, particionada por ANO)
spark.sql("""
CREATE TABLE IF NOT EXISTS proj_cvm_02_silver.201_dre_dfp (
  CNPJ_CIA STRING,
  DT_REFER DATE,
  VERSAO INT,
  DENOM_CIA STRING,
  CD_CVM INT,
  GRUPO_DFP STRING,
  MOEDA STRING,
  ESCALA_MOEDA STRING,
  ORDEM_EXERC STRING,
  DT_INI_EXERC DATE,
  DT_FIM_EXERC DATE,
  CD_CONTA STRING,
  DS_CONTA STRING,
  VL_CONTA DOUBLE,
  ANO INT,
  TRIMESTRE INT,
  MES INT,
  DT_PROCESSAMENTO TIMESTAMP,
  ST_CONTA_FIXA STRING,
  NIVEL_CONTA INT,
  CD_CONTA_PAI STRING,
  CD_CONTA_RAIZ STRING,
  TIPO_CONTA STRING,
  TIPO_ESTRUTURAL STRING
)
USING DELTA
PARTITIONED BY (ANO)
COMMENT 'DRE transformada - Dados limpos, tipados e enriquecidos com colunas temporais. Particionada por ano para MERGE incremental eficiente.'
""")

print("✅ Tabela proj_cvm_02_silver.201_dre_dfp criada")

# COMMAND ----------

# DBTITLE 1,CRIACAO DA TABELA SILVER - 202_bpa_dfp
# Tabela Silver: BPA transformada (dados limpos e enriquecidos, particionada por ANO)
spark.sql("""
CREATE TABLE IF NOT EXISTS proj_cvm_02_silver.202_bpa_dfp (
  CNPJ_CIA STRING,
  DT_REFER DATE,
  VERSAO INT,
  DENOM_CIA STRING,
  CD_CVM INT,
  GRUPO_DFP STRING,
  MOEDA STRING,
  ESCALA_MOEDA STRING,
  ORDEM_EXERC STRING,
  -- DT_INI_EXERC removida: BPA não contém na fonte CVM (snapshot, não período)
  DT_FIM_EXERC DATE,
  CD_CONTA STRING,
  DS_CONTA STRING,
  VL_CONTA DOUBLE,
  ANO INT,
  TRIMESTRE INT,
  MES INT,
  DT_PROCESSAMENTO TIMESTAMP,
  ST_CONTA_FIXA STRING,
  NIVEL_CONTA INT,
  CD_CONTA_PAI STRING,
  CD_CONTA_RAIZ STRING,
  TIPO_CONTA STRING
)
USING DELTA
PARTITIONED BY (ANO)
COMMENT 'BPA transformado - Dados limpos, tipados e enriquecidos com colunas temporais. Particionada por ano para DELETE+APPEND incremental eficiente.'
""")

print("✅ Tabela proj_cvm_02_silver.202_bpa_dfp criada")

# COMMAND ----------

# DBTITLE 1,CRIAÇÃO DA TABELA DE OBSERVABILIDADE
# ============================================================================
# TABELA DE OBSERVABILIDADE (schema de apoio)
# ============================================================================
# Rastreia todas as execuções do pipeline com métricas detalhadas
# Alimentada pelo notebook 004_verificacao_diaria_landing e pelos notebooks bronze/silver

# Schema de apoio (criado também no 002_ddl_controle_ingestao — idempotente)
spark.sql("""
CREATE SCHEMA IF NOT EXISTS proj_cvm_05_apoio
COMMENT 'Schema para tabelas de apoio, controle e configuração do pipeline'
""")

# Tabela de observabilidade: maior que controle_ingestao (8 cols → 26 cols)
spark.sql("""
CREATE TABLE IF NOT EXISTS proj_cvm_05_apoio.observabilidade_execucoes (
  -- Identificação da execução
  id_execucao STRING COMMENT 'UUID único por execução de task',
  job_id BIGINT COMMENT 'ID do job no Databricks',
  job_name STRING COMMENT 'Nome do job',
  run_id BIGINT COMMENT 'ID da execução (run) do job',
  task_key STRING COMMENT 'Chave da task dentro do job',
  notebook_path STRING COMMENT 'Caminho do notebook executado',

  -- Contexto do pipeline
  etapa STRING COMMENT 'Etapa: verificacao, download, bronze, silver',
  fonte STRING COMMENT 'Fonte: dre, bpa, bpp, landing',
  ano INT COMMENT 'Ano processado (NULL se múltiplos anos)',

  -- Execução
  inicio_ts TIMESTAMP COMMENT 'Início da execução',
  fim_ts TIMESTAMP COMMENT 'Fim da execução',
  duracao_segundos DOUBLE COMMENT 'Duração em segundos',
  status STRING COMMENT 'SUCCESS, ERROR, SKIPPED, PARTIAL',

  -- Métricas de arquivos (landing zone)
  arquivos_verificados INT COMMENT 'Número de arquivos verificados na CVM',
  arquivos_baixados INT COMMENT 'Número de arquivos baixados',
  arquivos_arquivados INT COMMENT 'Número de arquivos arquivados (versionamento)',
  arquivos_ignorados INT COMMENT 'Número de arquivos já atualizados',

  -- Métricas de dados (bronze/silver)
  registros_processados BIGINT COMMENT 'Número de registros processados',
  bytes_baixados BIGINT COMMENT 'Bytes baixados da CVM',
  bytes_arquivados BIGINT COMMENT 'Bytes arquivados (versionamento)',

  -- Metadados da fonte CVM
  last_modified_cvm TIMESTAMP COMMENT 'Last-Modified do arquivo na CVM',
  url_cvm STRING COMMENT 'URL do arquivo CVM',

  -- Erro
  tipo_erro STRING COMMENT 'Tipo do erro (ex: HTTPError, ValueError)',
  mensagem_erro STRING COMMENT 'Mensagem de erro (até 2000 chars)',

  -- Ambiente
  trigger_type STRING COMMENT 'Tipo de trigger (SCHEDULED, ONE_TIME, etc.)',
  parametros STRING COMMENT 'Parâmetros do job (JSON)',

  -- Auditoria
  created_at TIMESTAMP COMMENT 'Timestamp de criação do registro'
)
USING DELTA
COMMENT 'Observabilidade - registra todas as execuções do pipeline CVM com métricas detalhadas de arquivos, dados e erros'
""")

print("✅ Tabela proj_cvm_05_apoio.observabilidade_execucoes criada")