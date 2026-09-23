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
import logging
import time

# COMMAND ----------

# DBTITLE 1,CONFIGURAÇÃO E PARÂMETROS
# Configurar logging estruturado e exibir parâmetros do processamento
# Logging estruturado permite rastreabilidade completa da execução

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

logger.info("="*80)
logger.info("SILVER - DRE (201) - Transformação DRE")
logger.info("="*80)
logger.info(f"Anos a processar: {ANOS_PROCESSAR}")
logger.info(f"Total de anos: {len(ANOS_PROCESSAR)}")
logger.info("="*80)

# COMMAND ----------

# DBTITLE 1,ORQUESTRAÇÃO RESILIENTE
# Orquestração resiliente: try/except POR ano para isolamento de falhas
# Se um ano falhar, os demais continuam sendo processados

anos_sucesso = []
anos_falha = []

for ano in ANOS_PROCESSAR:
    inicio = time.time()
    logger.info(f"\n{'='*80}")
    logger.info(f"[INÍCIO] Processando ano={ano}")
    logger.info("="*80)
    
    try:
        # ETAPA 1: Filtro de versionamento (Window Function)
        logger.info("[1/4] Aplicando filtro de versionamento...")

        df_bronze = spark.table(f"{SCHEMA_BRONZE}.101_dre_dfp") \
            .filter(year(col("DT_REFER")) == ano)

        # GUARDRAIL: Verificar se Bronze tem dados reais para este ano
        count_bronze = df_bronze.count()
        if count_bronze == 0:
            logger.warning(f"[SKIP] Bronze vazia para ano {ano} - pulando sem marcar SUCCESS")
            continue

        window_spec = Window.partitionBy(
            "CNPJ_CIA", "DT_REFER", "CD_CONTA", "ORDEM_EXERC"
        ).orderBy(col("_versao_ingestao").desc())

        df_versao_atual = df_bronze.withColumn(
            "_row_num", row_number().over(window_spec)
        ).filter(col("_row_num") == 1).drop("_row_num")

        # ETAPA 2: Transformações (padronização, limpeza, enriquecimento)
        logger.info("[2/4] Aplicando transformações...")

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

        count_registros = df_silver.count()
        logger.info(f"[TRANSFORMAÇÃO] DataFrame Silver criado: {count_registros:,} registros")

        # ETAPA 3: REPLACE WHERE (substituição atômica por período)
        logger.info("[3/4] Gravando na tabela Silver...")

        df_silver.write \
            .format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", f"ANO = {ano}") \
            .saveAsTable(f"{SCHEMA_SILVER}.201_dre_dfp")

        logger.info(f"[GRAVAÇÃO] ✓ Ano {ano} gravado com sucesso")

        # ETAPA 4: Atualizar tabela de controle
        logger.info("[4/4] Registrando processamento...")

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

        duracao = time.time() - inicio
        anos_sucesso.append(ano)
        logger.info(f"[SUCESSO] ano={ano} | duração={duracao:.2f}s | registros={count_registros:,}")
        
        # Registrar observabilidade (Grupo B)
        registrar_observabilidade_execucao(
            etapa='silver',
            fonte='dre_silver',
            ano=ano,
            inicio_epoch=inicio,
            duracao_segundos=duracao,
            status='SUCCESS',
            registros_processados=count_registros
        )
        
    except Exception as e:
        duracao = time.time() - inicio
        anos_falha.append((ano, str(e)))
        logger.error(f"[FALHA] ano={ano} | duração={duracao:.2f}s | erro={str(e)}")
        
        # Registrar observabilidade (Grupo B)
        registrar_observabilidade_execucao(
            etapa='silver',
            fonte='dre_silver',
            ano=ano,
            inicio_epoch=inicio,
            duracao_segundos=duracao,
            status='ERROR',
            tipo_erro=type(e).__name__,
            mensagem_erro=str(e)
        )
        
        # Registrar falha na tabela de controle
        try:
            spark.sql(f"""
                INSERT INTO {SCHEMA_APOIO}.controle_ingestao
                    (fonte, ano, arquivo, last_modified_cvm, versao_ingestao, ingest_ts, status, mensagem)
                VALUES (
                    'dre_silver',
                    {ano},
                    '{SCHEMA_SILVER}.201_dre_dfp',
                    NULL,
                    NULL,
                    current_timestamp(),
                    'FAILED',
                    '{str(e).replace("'", "''")}'
                )
            """)
        except:
            pass  # Se falhar ao registrar, não interromper processamento

# COMMAND ----------

# DBTITLE 1,RELATÓRIO FINAL
