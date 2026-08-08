# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Transformação Silver - DRE DFP CVM
# MAGIC
# MAGIC ## Objetivo
# MAGIC Transformar dados brutos de **Demonstrações de Resultados do Exercício (DRE)** das **Demonstrações Financeiras Padronizadas (DFP)** da CVM, movendo-os da camada **bronze** para a camada **silver**.
# MAGIC
# MAGIC ## Camada Silver - Princípios
# MAGIC A camada silver aplica:
# MAGIC * **Limpeza**: remoção de nulls críticos, duplicados e inconsistências
# MAGIC * **Padronização**: tipos de dados corretos, formatos consistentes
# MAGIC * **Enriquecimento**: adição de colunas calculadas, categorizações
# MAGIC * **Validação**: garantia de qualidade e integridade dos dados
# MAGIC
# MAGIC ## Entrada
# MAGIC * **Tabela Bronze**: `proj_cvm_01_bronze.101_dre_dfp`
# MAGIC * Dados brutos conforme extraídos da CVM
# MAGIC
# MAGIC ## Saída
# MAGIC * **Tabela Silver**: `proj_cvm_02_silver.201_dre_dfp`
# MAGIC * Dados limpos, padronizados e prontos para análise e agregações

# COMMAND ----------

# DBTITLE 1,INICIALIZAÇÃO E IMPORTS
%run ../04_apoio/config_parametros

# COMMAND ----------

# DBTITLE 1,Inicializar Anos a Processar
# Inicializar ANOS_PROCESSAR (se ainda não foi inicializado)
if ANOS_PROCESSAR is None:
    inicializar_anos_processar()

from pyspark.sql import Window
from pyspark.sql.functions import col, to_date, year, quarter, month, row_number, current_timestamp
from pyspark.sql.types import DoubleType, IntegerType

# COMMAND ----------

# DBTITLE 1,CONFIGURAÇÃO E PARÂMETROS
# ANOS_PROCESSAR: Lista de anos definida pelo orquestrador
# Processa apenas anos que tiveram nova ingestão em Bronze

print("="*80)
print("SILVER - DRE (201)")
print("="*80)
print(f"Anos a processar: {ANOS_PROCESSAR}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,LOOP - PROCESSAMENTO POR ANO
# Loop: Processar cada ano da lista ANOS_PROCESSAR
# Para cada ano: filtra versão mais recente, transforma, grava

for ano in ANOS_PROCESSAR:
    print(f"\n{'='*80}")
    print(f"Processando ano: {ano}")
    print("="*80)

    # ETAPA 1: Filtro de versionamento (Window Function)
    # Particiona por chave natural (CNPJ + DT_REFER + CD_CONTA + ORDEM_EXERC) e pega versão mais recente
    print("[1/4] Aplicando filtro de versionamento...")

    df_bronze = spark.table("proj_cvm_01_bronze.101_dre_dfp") \
        .filter(year(col("DT_REFER")) == ano)

    window_spec = Window.partitionBy(
        "CNPJ_CIA", "DT_REFER", "CD_CONTA", "ORDEM_EXERC"
    ).orderBy(col("_versao_ingestao").desc())

    df_versao_atual = df_bronze.withColumn(
        "_row_num", row_number().over(window_spec)
    ).filter(col("_row_num") == 1).drop("_row_num")

    # ETAPA 2: Transformações (padronização, limpeza, enriquecimento)
    print("[2/4] Aplicando transformações...")

    df_transformado = df_versao_atual \
        .withColumn("DT_REFER", to_date(col("DT_REFER"), "yyyy-MM-dd")) \
        .withColumn("DT_INI_EXERC", to_date(col("DT_INI_EXERC"), "yyyy-MM-dd")) \
        .withColumn("DT_FIM_EXERC", to_date(col("DT_FIM_EXERC"), "yyyy-MM-dd")) \
        .withColumn("VL_CONTA", col("VL_CONTA").cast(DoubleType())) \
        .withColumn("VERSAO", col("VERSAO").cast(IntegerType())) \
        .withColumn("CD_CVM", col("CD_CVM").cast(IntegerType())) \
        .distinct() \
        .filter(
            col("CNPJ_CIA").isNotNull() &
            col("DT_REFER").isNotNull() &
            col("CD_CONTA").isNotNull() &
            col("VL_CONTA").isNotNull()
        ) \
        .withColumn("ANO", year(col("DT_REFER"))) \
        .withColumn("TRIMESTRE", quarter(col("DT_REFER"))) \
        .withColumn("MES", month(col("DT_REFER"))) \
        .withColumn("DT_PROCESSAMENTO", current_timestamp())

    # PROJEÇÃO EXPLÍCITA: Garante que DataFrame corresponde ao schema Silver
    # Qualquer coluna extra no DataFrame é automaticamente descartada
    # Se Bronze mudar, Silver não quebra
    df_silver = df_transformado.select(
        "CNPJ_CIA",
        "DT_REFER",
        "VERSAO",
        "DENOM_CIA",
        "CD_CVM",
        "GRUPO_DFP",
        "MOEDA",
        "ESCALA_MOEDA",
        "ORDEM_EXERC",
        "DT_INI_EXERC",
        "DT_FIM_EXERC",
        "CD_CONTA",
        "DS_CONTA",
        "VL_CONTA",
        "ANO",
        "TRIMESTRE",
        "MES",
        "DT_PROCESSAMENTO"
    )

    # ETAPA 3: DELETE WHERE + APPEND (idempotência por período)
    print("[3/4] Gravando na tabela Silver...")

    spark.sql(f"DELETE FROM proj_cvm_02_silver.201_dre_dfp WHERE ANO = {ano}")

    df_silver.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("proj_cvm_02_silver.201_dre_dfp")

    print(f"   ✓ Ano {ano} gravado com sucesso")

    # ETAPA 4: Atualizar tabela de controle
    print("[4/4] Registrando processamento...")

    spark.sql(f"""
        INSERT INTO proj_cvm_04_apoio.controle_ingestao
            (fonte, ano, arquivo, last_modified_cvm, versao_ingestao, ingest_ts, status, mensagem)
        VALUES (
            'dre_silver',
            {ano},
            'proj_cvm_02_silver.201_dre_dfp',
            NULL,
            1,
            current_timestamp(),
            'SUCCESS',
            NULL
        )
    """)

    print(f"   ✓ Processamento registrado")

print(f"\n{'='*80}")
print(f"SILVER DRE - PROCESSAMENTO CONCLUÍDO")
print(f"Anos processados: {ANOS_PROCESSAR}")
print("="*80)

# COMMAND ----------

from pyspark.sql.functions import col, year

df_bronze = spark.table("proj_cvm_01_bronze.101_dre_dfp")
anos_disponiveis = df_bronze.select(year(col("DT_REFER")).alias("ANO")).distinct().orderBy("ANO")

display(anos_disponiveis)
