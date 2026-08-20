# Databricks notebook source
# DBTITLE 1,Documentação
# MAGIC %md
# MAGIC # Transformação Silver - BPA DFP CVM
# MAGIC
# MAGIC ## Objetivo
# MAGIC Transformar dados brutos de **Balanço Patrimonial Ativo (BPA)** das **Demonstrações Financeiras Padronizadas (DFP)** da CVM, movendo-os da camada **bronze** para a camada **silver**.
# MAGIC
# MAGIC ## Camada Silver - Princípios
# MAGIC A camada silver aplica:
# MAGIC * **Versionamento**: Filtra versão mais recente de cada registro usando Window Function
# MAGIC * **Limpeza**: remoção de nulls críticos, duplicados e inconsistências
# MAGIC * **Padronização**: conversão de tipos de dados (datas, numéricos, strings)
# MAGIC * **Enriquecimento**: cálculo de colunas derivadas úteis para análises
# MAGIC
# MAGIC ## Transformações Aplicadas
# MAGIC 1. **Filtro de versionamento**: Window Function (PARTITION BY chave natural, ORDER BY _versao_ingestao DESC)
# MAGIC 2. **Conversão de tipos**: Datas (date), valores numéricos (double)
# MAGIC 3. **Remoção de duplicados**: Distinct de registros idênticos
# MAGIC 4. **Tratamento de nulls**: Remoção de registros com campos obrigatórios nulos
# MAGIC 5. **Colunas calculadas**: Ano, trimestre, mês extraídos de DT_REFER
# MAGIC
# MAGIC ## Estratégia de Gravação
# MAGIC **REPLACE WHERE**: Substituição atômica por período - Delta Lake garante operação all-or-nothing, eliminando janela de vulnerabilidade

# COMMAND ----------

# DBTITLE 1,INICIALIZAÇÃO E IMPORTS
# MAGIC %run ../05_apoio/config_parametros

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
print("SILVER - BPA (202)")
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

    df_bronze = spark.table("proj_cvm_01_bronze.102_bpa_dfp") \
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
        .withColumn("DT_FIM_EXERC", to_date(col("DT_FIM_EXERC"), "yyyy-MM-dd")) \
        .withColumn("VL_CONTA", col("VL_CONTA").cast(DoubleType())) \
        .withColumn("VERSAO", col("VERSAO").cast(IntegerType())) \
        .withColumn("CD_CVM", col("CD_CVM").cast(IntegerType())) \
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
        # "DT_INI_EXERC" removida: BPA não contém na fonte CVM
        "DT_FIM_EXERC",
        "CD_CONTA",
        "DS_CONTA",
        "VL_CONTA",
        "ANO",
        "TRIMESTRE",
        "MES",
        "DT_PROCESSAMENTO"
    )

    # ETAPA 3: REPLACE WHERE (substituição atômica por período)
    print("[3/4] Gravando na tabela Silver...")

    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"ANO = {ano}") \
        .saveAsTable("proj_cvm_02_silver.202_bpa_dfp")

    print(f"   ✓ Ano {ano} gravado com sucesso")

    # ETAPA 4: Atualizar tabela de controle
    print("[4/4] Registrando processamento...")

    spark.sql(f"""
        INSERT INTO proj_cvm_05_apoio.controle_ingestao
            (fonte, ano, arquivo, last_modified_cvm, versao_ingestao, ingest_ts, status, mensagem)
        VALUES (
            'bpa_silver',
            {ano},
            'proj_cvm_02_silver.202_bpa_dfp',
            NULL,
            1,
            current_timestamp(),
            'SUCCESS',
            NULL
        )
    """)

    print(f"   ✓ Processamento registrado")

print(f"\n{'='*80}")
print(f"SILVER BPA - PROCESSAMENTO CONCLUÍDO")
print(f"Anos processados: {ANOS_PROCESSAR}")
print("="*80)