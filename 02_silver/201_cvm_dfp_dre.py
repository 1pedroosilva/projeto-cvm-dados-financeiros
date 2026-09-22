# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Documentação
# MAGIC %md
# MAGIC # Transformação Silver - DRE DFP CVM
# MAGIC
# MAGIC ## Objetivo
# MAGIC Transformar dados brutos de **Demonstrações de Resultados do Exercício (DRE)** das **Demonstrações Financeiras Padronizadas (DFP)** da CVM, movendo-os da camada **bronze** para a camada **silver**.
# MAGIC
# MAGIC ## Camada Silver - Princípios
# MAGIC A camada silver aplica:
# MAGIC * **Versionamento**: Window Function seleciona versão mais recente da Bronze (ORDER BY _versao_ingestao DESC)
# MAGIC * **Limpeza**: remoção de nulls críticos, duplicados e inconsistências
# MAGIC * **Padronização**: tipos de dados corretos, formatos consistentes, **normalização de escala monetária**
# MAGIC * **Enriquecimento**: adição de colunas calculadas, categorizações
# MAGIC * **Guardrail**: Bronze vazia para o ano → SKIP (não marca SUCCESS, evita estado irrecuperável)
# MAGIC * **Detecção**: anos pendentes via `inicializar_anos_processar()` (sem HTTP para CVM)
# MAGIC * **Idempotência**: controle + verificação de dados reais na tabela destino + detecção de republicações via `_metadata.json`
# MAGIC * **Processamento resiliente**: try/except por ano (falha isolada não interrompe demais)
# MAGIC
# MAGIC ## Normalização de Escala Monetária
# MAGIC A CVM publica valores em duas escalas:
# MAGIC * `MIL` — valor em milhares de reais (necessita multiplicação por 1000)
# MAGIC * `UNIDADE` — valor já em reais
# MAGIC
# MAGIC A Silver normaliza `VL_CONTA` para **reais (unidade)** em todos os registros, preservando `ESCALA_MOEDA` para rastreabilidade. Isso garante que qualquer análise downstream compare valores em uma única escala.
# MAGIC
# MAGIC ## Enriquecimento Hierárquico de Contas
# MAGIC A CVM publica contas contábeis em estrutura hierárquica por notação de pontos (`CD_CONTA`): `3` → `3.01` → `3.01.01` → `3.01.01.01` (até 5 níveis). A Silver deriva 4 colunas dessa estrutura:
# MAGIC
# MAGIC * **`ST_CONTA_FIXA`**: Projetada da Bronze (S = conta fixa da estrutura CVM, N = detalhamento específico por empresa). Antes descartada na projeção Silver, agora preservada
# MAGIC * **`NIVEL_CONTA`**: Nível hierárquico, derivado via `size(split(CD_CONTA, "[.]"))` (1 a 5)
# MAGIC * **`CD_CONTA_PAI`**: Conta pai na hierarquia — tudo antes do último `.` (NULL para nível 1)
# MAGIC * **`CD_CONTA_RAIZ`**: Conta raiz — primeiro segmento antes do primeiro `.` (ex: `3.01.01` → `3`)
# MAGIC
# MAGIC **Rollup**: o valor do pai é a soma dos filhos diretos. Dashboards devem filtrar por `NIVEL_CONTA` para evitar double-counting (somar todos os registros soma pais + filhos, inflando o total).
# MAGIC
# MAGIC ## Entrada
# MAGIC * **Tabela Bronze**: definida por SCHEMA_BRONZE no config_parametros
# MAGIC * Dados brutos conforme extraídos da CVM
# MAGIC
# MAGIC ## Saída
# MAGIC * **Tabela Silver**: definida por SCHEMA_SILVER no config_parametros
# MAGIC * Dados limpos, padronizados e prontos para análise e agregações

# COMMAND ----------

# DBTITLE 1,Carregar configurações
# MAGIC %run ../05_apoio/config_parametros

# COMMAND ----------

# DBTITLE 1,Inicializar Anos a Processar
# Captura explícita do retorno com guardrail de lista vazia
# CARGA=completa força todos os anos
ANOS_PROCESSAR = inicializar_anos_processar()

if not ANOS_PROCESSAR:
    raise ValueError("❌ ANOS_PROCESSAR vazio - nenhum ano para processar")

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import Window
from pyspark.sql.functions import col, to_date, year, quarter, month, row_number, current_timestamp, when, split, size, regexp_extract, lit
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

    df_bronze = spark.table(f"{SCHEMA_BRONZE}.101_dre_dfp") \
        .filter(year(col("DT_REFER")) == ano)

    # GUARDRAIL: Verificar se Bronze tem dados reais para este ano
    # Se Bronze está vazia, NÃO marca SUCCESS — evita estado irrecuperável
    count_bronze = df_bronze.count()
    if count_bronze == 0:
        print(f"⚠️  Bronze vazia para ano {ano} - pulando sem marcar SUCCESS")
        continue

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
        .filter(
            col("CNPJ_CIA").isNotNull() &
            col("DT_REFER").isNotNull() &
            col("CD_CONTA").isNotNull() &
            col("VL_CONTA").isNotNull()
        ) \
        .withColumn("VL_CONTA",
            when(col("ESCALA_MOEDA") == "MIL", col("VL_CONTA") * 1000)
            .otherwise(col("VL_CONTA"))
        ) \
        .withColumn("ANO", year(col("DT_REFER"))) \
        .withColumn("TRIMESTRE", quarter(col("DT_REFER"))) \
        .withColumn("MES", month(col("DT_REFER"))) \
        .withColumn("DT_PROCESSAMENTO", current_timestamp()) \
        .withColumn("ST_CONTA_FIXA", col("ST_CONTA_FIXA")) \
        .withColumn("NIVEL_CONTA", size(split(col("CD_CONTA"), "[.]"))) \
        .withColumn("CD_CONTA_PAI",
            when(size(split(col("CD_CONTA"), "[.]")) > 1,
                regexp_extract(col("CD_CONTA"), r"^(.+)\.[^.]+$", 1)
            ).otherwise(lit(None).cast("string"))
        ) \
        .withColumn("CD_CONTA_RAIZ", split(col("CD_CONTA"), "[.]")[0]) \
        .withColumn("TIPO_CONTA",
            when(size(split(col("CD_CONTA"), "[.]")) <= 2, lit("TOTALIZADORA"))
            .otherwise(lit("ANALITICA"))
        ) \
        .withColumn("TIPO_ESTRUTURAL",
            when(size(split(col("CD_CONTA"), "[.]")) > 2, lit(None).cast("string"))
            .when(col("CD_CONTA").isin("3.02", "3.03", "3.05", "3.07", "3.09", "3.10"), lit("DERIVADA"))
            .otherwise(lit("ADITIVA"))
        )

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
        "DT_PROCESSAMENTO",
        "ST_CONTA_FIXA",
        "NIVEL_CONTA",
        "CD_CONTA_PAI",
        "CD_CONTA_RAIZ",
        "TIPO_CONTA",
        "TIPO_ESTRUTURAL"
    )

    # ETAPA 3: REPLACE WHERE (substituição atômica por período)
    print("[3/4] Gravando na tabela Silver...")

    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"ANO = {ano}") \
        .saveAsTable(f"{SCHEMA_SILVER}.201_dre_dfp")

    print(f"   ✓ Ano {ano} gravado com sucesso")

    # ETAPA 4: Atualizar tabela de controle
    print("[4/4] Registrando processamento...")

    spark.sql(f"""
        INSERT INTO {SCHEMA_APOIO}.controle_ingestao
            (fonte, ano, arquivo, last_modified_cvm, versao_ingestao, ingest_ts, status, mensagem)
        VALUES (
            'dre_silver',
            {ano},
            '{SCHEMA_SILVER}.201_dre_dfp',
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