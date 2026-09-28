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
# MAGIC Estabelecer a estrutura de dados do projeto via DDL explícito.
# MAGIC
# MAGIC ## O que é criado
# MAGIC * **4 schemas**: Bronze, Silver, Gold e Apoio
# MAGIC * **3 tabelas Bronze**: 101_dre_dfp, 102_bpa_dfp, 103_bpp_dfp
# MAGIC * **3 tabelas Silver**: 201_dre_dfp, 202_bpa_dfp, 203_bpp_dfp
# MAGIC * **3 tabelas de apoio**: controle_ingestao, observabilidade_execucoes, observabilidade_guardrails
# MAGIC
# MAGIC ## Características
# MAGIC * **Idempotente**: todas as instruções usam `CREATE IF NOT EXISTS`
# MAGIC * **Sem migrations**: não contém `ALTER TABLE` nem `apply_schema_migration_if_needed()`
# MAGIC * **Ambientes com tabelas antigas**: fazer `DROP TABLE` manual antes de rodar este DDL

# COMMAND ----------

# DBTITLE 1,CARREGAR CONFIG
# MAGIC %run ./config_parametros

# COMMAND ----------

# DBTITLE 1,CRIAÇÃO DE SCHEMAS
spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA_BRONZE}
COMMENT 'Camada Bronze - Ingestão bruta de dados da CVM sem transformações'
""")

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA_SILVER}
COMMENT 'Camada Silver - Dados transformados, limpos e padronizados'
""")

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA_GOLD}
COMMENT 'Camada Gold - Métricas de negócio e agregações para análise'
""")

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA_APOIO}
COMMENT 'Schema de apoio - tabelas de controle, observabilidade e configuração do pipeline'
""")

print("✅ Schemas criados: Bronze, Silver, Gold, Apoio")

# COMMAND ----------

# DBTITLE 1,BRONZE - 101_dre_dfp
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_BRONZE}.101_dre_dfp (
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
) USING DELTA
COMMENT 'DRE consolidada - Dados brutos da CVM. Append-only com versionamento.'
""")

# COMMAND ----------

# DBTITLE 1,BRONZE - 102_bpa_dfp
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_BRONZE}.102_bpa_dfp (
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
) USING DELTA
COMMENT 'BPA consolidado - Dados brutos da CVM. Append-only com versionamento.'
""")

# COMMAND ----------

# DBTITLE 1,BRONZE - 103_bpp_dfp
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_BRONZE}.103_bpp_dfp (
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
) USING DELTA
COMMENT 'BPP consolidado - Dados brutos da CVM. Append-only com versionamento.'
""")

# COMMAND ----------

# DBTITLE 1,SILVER - 201_dre_dfp
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_SILVER}.201_dre_dfp (
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
) USING DELTA
PARTITIONED BY (ANO)
COMMENT 'DRE transformada - Dados limpos, tipados e enriquecidos. Particionada por ano.'
""")

# COMMAND ----------

# DBTITLE 1,SILVER - 202_bpa_dfp
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_SILVER}.202_bpa_dfp (
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
) USING DELTA
PARTITIONED BY (ANO)
COMMENT 'BPA transformado - Dados limpos, tipados e enriquecidos. Particionada por ano.'
""")

# COMMAND ----------

# DBTITLE 1,SILVER - 203_bpp_dfp
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_SILVER}.203_bpp_dfp (
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
) USING DELTA
PARTITIONED BY (ANO)
COMMENT 'BPP transformado - Dados limpos, tipados e enriquecidos. Particionada por ano.'
""")

# COMMAND ----------

# DBTITLE 1,APOIO - controle_ingestao
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_APOIO}.controle_ingestao (
  fonte STRING COMMENT 'Identificador da fonte (dre, bpa, bpp, dre_silver, etc.)',
  ano INT COMMENT 'Ano fiscal do arquivo',
  arquivo STRING COMMENT 'Nome do arquivo ou tabela destino',
  last_modified_cvm TIMESTAMP COMMENT 'Last-Modified HTTP do arquivo na CVM',
  versao_ingestao INT COMMENT 'Versão sequencial de ingestão',
  ingest_ts TIMESTAMP COMMENT 'Timestamp da ingestão',
  status STRING COMMENT 'SUCCESS, FAILED',
  mensagem STRING COMMENT 'Mensagem de erro ou observações'
) USING DELTA
COMMENT 'Controle de ingestão - rastreia cada ingestão fonte/ano e detecta atualizações CVM'
""")

# COMMAND ----------

# DBTITLE 1,APOIO - observabilidade_execucoes
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_APOIO}.observabilidade_execucoes (
  id_execucao STRING COMMENT 'UUID único por execução',
  job_id BIGINT COMMENT 'ID do job Databricks',
  job_name STRING COMMENT 'Nome do job',
  run_id BIGINT COMMENT 'ID da run do job',
  task_key STRING COMMENT 'Chave da task dentro do job',
  notebook_path STRING COMMENT 'Caminho do notebook executado',
  etapa STRING COMMENT 'verificacao, download, bronze, silver',
  fonte STRING COMMENT 'dre, bpa, bpp, landing',
  ano INT COMMENT 'Ano processado (NULL se múltiplos)',
  inicio_ts TIMESTAMP COMMENT 'Início da execução',
  fim_ts TIMESTAMP COMMENT 'Fim da execução',
  duracao_segundos DOUBLE COMMENT 'Duração em segundos',
  status STRING COMMENT 'SUCCESS, ERROR, SKIPPED, PARTIAL',
  arquivos_verificados INT COMMENT 'Arquivos verificados na CVM (landing)',
  arquivos_baixados INT COMMENT 'Arquivos baixados (landing)',
  arquivos_arquivados INT COMMENT 'Arquivos arquivados (landing)',
  arquivos_ignorados INT COMMENT 'Arquivos já atualizados (landing)',
  registros_processados BIGINT COMMENT 'Registros processados (bronze/silver)',
  bytes_baixados BIGINT COMMENT 'Bytes baixados da CVM (landing)',
  bytes_arquivados BIGINT COMMENT 'Bytes arquivados (landing)',
  last_modified_cvm TIMESTAMP COMMENT 'Last-Modified do arquivo CVM',
  url_cvm STRING COMMENT 'URL do arquivo CVM',
  tipo_erro STRING COMMENT 'Tipo do erro (ex: ValueError)',
  mensagem_erro STRING COMMENT 'Mensagem de erro (até 2000 chars)',
  trigger_type STRING COMMENT 'SCHEDULED, ONE_TIME, etc.',
  parametros STRING COMMENT 'Parâmetros do job (JSON)',
  created_at TIMESTAMP COMMENT 'Timestamp de criação do registro'
) USING DELTA
COMMENT 'Observabilidade - execuções do pipeline CVM com métricas de arquivos, dados e erros'
""")

# COMMAND ----------

# DBTITLE 1,APOIO - observabilidade_guardrails
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_APOIO}.observabilidade_guardrails (
  id_check STRING COMMENT 'UUID único por check',
  id_execucao STRING COMMENT 'FK para observabilidade_execucoes.id_execucao',
  etapa STRING COMMENT 'bronze, silver, gold',
  fonte STRING COMMENT 'dre, bpa, bpp',
  ano INT COMMENT 'Ano validado',
  nome_guardrail STRING COMMENT 'Nome descritivo do guardrail',
  tipo_check STRING COMMENT 'schema_check, row_count, uniqueness, empty_file, empty_table',
  resultado STRING COMMENT 'PASS, FAIL, WARN',
  esperado STRING COMMENT 'Valor esperado',
  encontrado STRING COMMENT 'Valor encontrado',
  registros_afetados BIGINT COMMENT 'Registros que falharam',
  detalhes STRING COMMENT 'Mensagem adicional',
  ts_check TIMESTAMP COMMENT 'Momento do check'
) USING DELTA
COMMENT 'Observabilidade - validações de qualidade de dados do pipeline CVM'
""")

# COMMAND ----------

# DBTITLE 1,CONFIRMAÇÃO FINAL
print("✅ DDL concluído\n")

print("=== Tabelas Bronze ===")
spark.sql(f"SHOW TABLES IN {SCHEMA_BRONZE}").show(truncate=False)

print("=== Tabelas Silver ===")
spark.sql(f"SHOW TABLES IN {SCHEMA_SILVER}").show(truncate=False)

print("=== Tabelas de Apoio ===")
spark.sql(f"SHOW TABLES IN {SCHEMA_APOIO}").show(truncate=False)