# Databricks notebook source
# DBTITLE 1,DOCUMENTAÇÃO
# MAGIC %md
# MAGIC # Tabela de Controle de Ingestão
# MAGIC
# MAGIC ## Objetivo
# MAGIC Criar tabela de controle que rastreia execuções do pipeline CVM - permite detecção inteligente de:
# MAGIC * Novos anos disponíveis na CVM
# MAGIC * Arquivos atualizados (via metadado HTTP Last-Modified)
# MAGIC * Status de cada ingestão (SUCCESS, ERROR)
# MAGIC
# MAGIC ## Estrutura (8 colunas)
# MAGIC * **fonte** (STRING): Identificador da fonte (dre, bpa, bpp)
# MAGIC * **ano** (INT): Ano fiscal do arquivo
# MAGIC * **arquivo** (STRING): Nome do arquivo baixado (ex: `dfp_cia_aberta_2021.zip`)
# MAGIC * **last_modified_cvm** (TIMESTAMP): Data de última modificação na CVM (header HTTP Last-Modified)
# MAGIC * **versao_ingestao** (INT): Versão sequencial de ingestão (incrementa a cada reprocessamento)
# MAGIC * **ingest_ts** (TIMESTAMP): Timestamp da ingestão
# MAGIC * **status** (STRING): Status da ingestão (SUCCESS, ERROR)
# MAGIC * **mensagem** (STRING): Mensagem de erro ou observações (truncada em 500 chars)

# COMMAND ----------

# DBTITLE 1,CARREGAR CONFIG
# MAGIC %run ./config_parametros

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criação da Tabela

# COMMAND ----------

# DBTITLE 1,CRIAÇÃO DA TABELA DE CONTROLE
# CRIAÇÃO DO SCHEMA DE APOIO
spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA_APOIO}
COMMENT 'Schema para tabelas de apoio, controle e configuração do pipeline'
""")

print(f"✅ Schema {SCHEMA_APOIO} criado")

# CRIAÇÃO DA TABELA DE CONTROLE
# Tabela registra metadados de cada execução do pipeline
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_APOIO}.controle_ingestao (
  fonte STRING COMMENT 'Identificador da fonte de dados (ex: dre, bpa)',
  ano INT COMMENT 'Ano fiscal do arquivo',
  arquivo STRING COMMENT 'Nome do arquivo baixado',
  last_modified_cvm TIMESTAMP COMMENT 'Data de última modificação do arquivo na CVM (header Last-Modified)',
  versao_ingestao INT COMMENT 'Versão sequencial de ingestão',
  ingest_ts TIMESTAMP COMMENT 'Timestamp da ingestão',
  status STRING COMMENT 'Status da ingestão (SUCCESS, ERROR)',
  mensagem STRING COMMENT 'Mensagem de erro ou observações'
)
USING DELTA
COMMENT 'Controle de ingestão - Rastreia quando cada fonte/ano foi processado e detecta atualizações na CVM'
""")

print(f"✅ Tabela {SCHEMA_APOIO}.controle_ingestao criada com sucesso")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação

# COMMAND ----------

# DBTITLE 1,VALIDAÇÃO DA ESTRUTURA
# VALIDAÇÃO DA ESTRUTURA
# Exibe schema da tabela criada para confirmar estrutura
display(spark.table(f"{SCHEMA_APOIO}.controle_ingestao").limit(0))

# COMMAND ----------

# DBTITLE 1,DESCRIBE da tabela
# DESCRIBE da tabela para ver tipos e comentários
spark.sql(f"DESCRIBE TABLE {SCHEMA_APOIO}.controle_ingestao").show(truncate=False)