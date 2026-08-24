# Databricks notebook source
# DBTITLE 1,DOCUMENTAÇÃO
# MAGIC %md
# MAGIC # ============================================================================
# MAGIC # Teste de Integração E2E - Pipeline DRE (Bronze → Silver)
# MAGIC # ============================================================================
# MAGIC # Propósito: Validar execução ponta a ponta do pipeline DRE
# MAGIC # Escopo: Bronze ingestão + Silver transformação + Validações de qualidade
# MAGIC # Dados: Ano 2010 (volume mínimo, execução rápida)
# MAGIC # Ambientes: Schemas isolados via SCHEMA_SUFFIX (produção vs teste)
# MAGIC # ============================================================================

# COMMAND ----------

# DBTITLE 1,CARREGAR CONFIGURAÇÕES
# CARREGAR CONFIGURAÇÕES
%run ../05_apoio/config_parametros

# COMMAND ----------

# DBTITLE 1,IMPORTS
# IMPORTS
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Confirmar que schemas de teste estão configurados
print(f"ℹ️  Schemas configurados:")
print(f"   Bronze: {SCHEMA_BRONZE}")
print(f"   Silver: {SCHEMA_SILVER}")
print(f"   Apoio: {SCHEMA_APOIO}")
print(f"   Sufixo: '{SCHEMA_SUFFIX}'")

# COMMAND ----------

# DBTITLE 1,VALIDAÇÃO 1: Bronze tem dados
# ============================================================================
# VALIDAÇÃO 1: Tabela Bronze existe e tem dados
# ============================================================================
# Objetivo: Confirmar que ingestão Bronze funcionou
# Critério: COUNT(*) > 0 na tabela Bronze DRE para ano 2010
# ============================================================================

print("✅ Validando: Tabela Bronze existe e tem dados...")

tabela_bronze = f"{SCHEMA_BRONZE}.101_dre_dfp"
df_bronze = spark.table(tabela_bronze)
count_bronze = df_bronze.count()

assert count_bronze > 0, f"❌ FALHA: Tabela Bronze vazia ({tabela_bronze})"

print(f"   ✅ Bronze tem {count_bronze:,} registros")
print(f"   ✓ Validação 1 passou")

# COMMAND ----------

# DBTITLE 1,VALIDAÇÃO 2: Silver tem dados
# ============================================================================
# VALIDAÇÃO 2: Tabela Silver existe e tem dados
# ============================================================================
# Objetivo: Confirmar que transformação Silver funcionou
# Critério: COUNT(*) > 0 na tabela Silver DRE para ano 2010
# ============================================================================

print("✅ Validando: Tabela Silver existe e tem dados...")

tabela_silver = f"{SCHEMA_SILVER}.201_dre_dfp"
df_silver = spark.table(tabela_silver)
count_silver = df_silver.count()

assert count_silver > 0, f"❌ FALHA: Tabela Silver vazia ({tabela_silver})"

print(f"   ✅ Silver tem {count_silver:,} registros")
print(f"   ✓ Validação 2 passou")

# COMMAND ----------

# DBTITLE 1,VALIDAÇÃO 3: Sem perda de dados
# ============================================================================
# VALIDAÇÃO 3: Contagens Bronze = Silver (sem perda de dados)
# ============================================================================
# Objetivo: Confirmar que transformação não perdeu registros
# Critério: Mesmo número de linhas em Bronze e Silver
# Nota: Assume-se que Bronze tem apenas versão mais recente para 2010
#       (orquestrador não reprocessa anos, então _versao_ingestao é única)
# ============================================================================

print("✅ Validando: Contagens Bronze = Silver...")

assert count_bronze == count_silver, (
    f"❌ FALHA: Perda de dados na transformação\n"
    f"   Bronze: {count_bronze:,} registros\n"
    f"   Silver: {count_silver:,} registros\n"
    f"   Diferença: {count_bronze - count_silver:,} registros perdidos"
)

print(f"   ✅ Contagens batem: {count_bronze:,} registros")
print(f"   ✓ Validação 3 passou")

# COMMAND ----------

# DBTITLE 1,VALIDAÇÃO 4: PKs únicas
# ============================================================================
# VALIDAÇÃO 4: PKs únicas (CNPJ_CIA + DT_REFER + CD_CONTA)
# ============================================================================
# Objetivo: Confirmar que não há duplicação de chaves primárias
# Critério: Cada combinação (CNPJ_CIA, DT_REFER, CD_CONTA) aparece 1x
# ============================================================================

print("✅ Validando: PKs únicas em Silver...")

df_duplicatas = (
    df_silver
    .groupBy("cnpj_cia", "dt_refer", "cd_conta")
    .count()
    .filter("count > 1")
)

count_duplicatas = df_duplicatas.count()

assert count_duplicatas == 0, (
    f"❌ FALHA: PKs duplicadas em Silver\n"
    f"   {count_duplicatas} combinações duplicadas:\n"
    f"{df_duplicatas.show(20, truncate=False)}"
)

print(f"   ✅ Nenhuma PK duplicada")
print(f"   ✓ Validação 4 passou")

# COMMAND ----------

# DBTITLE 1,VALIDAÇÃO 5: Metadados populados
# ============================================================================
# VALIDAÇÃO 5: Colunas de metadados populadas (Bronze)
# ============================================================================
# Objetivo: Confirmar que colunas de auditoria estão preenchidas
# Critério: _versao_ingestao, _last_modified_cvm, _ingest_ts não-nulos
# ============================================================================

print("✅ Validando: Colunas de metadados populadas em Bronze...")

# Validar _versao_ingestao
count_versao_null = df_bronze.filter("_versao_ingestao IS NULL").count()
assert count_versao_null == 0, f"❌ FALHA: {count_versao_null} registros com _versao_ingestao NULL"

# Validar _ingest_ts
count_ingest_ts_null = df_bronze.filter("_ingest_ts IS NULL").count()
assert count_ingest_ts_null == 0, f"❌ FALHA: {count_ingest_ts_null} registros com _ingest_ts NULL"

# Validar _last_modified_cvm
count_last_modified_null = df_bronze.filter("_last_modified_cvm IS NULL").count()
assert count_last_modified_null == 0, f"❌ FALHA: {count_last_modified_null} registros com _last_modified_cvm NULL"

print(f"   ✅ Todas as colunas de metadados estão populadas")
print(f"   ✓ Validação 5 passou")

# COMMAND ----------

# DBTITLE 1,RESUMO DOS TESTES
# ============================================================================
# RESUMO DOS TESTES
# ============================================================================

print("\n" + "="*80)
print("✅ TODOS OS TESTES PASSARAM")
print("="*80)
print(f"\n📈 Estatísticas:")
print(f"   • Registros Bronze: {count_bronze:,}")
print(f"   • Registros Silver: {count_silver:,}")
print(f"   • Schemas testados: Bronze, Silver")
print(f"   • Sufixo aplicado: '{SCHEMA_SUFFIX}'")
print(f"\n✓ Pipeline DRE funcionando corretamente (Bronze → Silver)\n")

# COMMAND ----------

