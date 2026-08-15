# Databricks notebook source
# DBTITLE 1,Documentação
# MAGIC %md
# MAGIC # Transformação Silver - BPP DFP CVM
# MAGIC
# MAGIC ## Objetivo
# MAGIC Transformar dados brutos de **Balanço Patrimonial Passivo (BPP)** das **Demonstrações Financeiras Padronizadas (DFP)** da CVM, movendo-os da camada **bronze** para a camada **silver**.
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
# MAGIC **DELETE WHERE + APPEND**: Por cada ano processado, deleta registros existentes daquele ano e adiciona novos (idempotência por período)

# COMMAND ----------

# DBTITLE 1,INICIALIZAÇÃO E IMPORTS
# Carregar configurações e funções compartilhadas
# Disponibiliza: inicializar_anos_processar, schemas UC, parâmetros do projeto

%run ../05_apoio/config_parametros

from pyspark.sql import Window
from pyspark.sql.functions import col, to_date, year, quarter, month, row_number, current_timestamp
from pyspark.sql.types import DoubleType, IntegerType
import logging
import time
from datetime import datetime

# Configurar logging estruturado
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# COMMAND ----------

# DBTITLE 1,Inicializar Anos a Processar
# ANOS_PROCESSAR: Lista de anos detectada pelo orquestrador
# Processa apenas anos que tiveram nova ingestão em Bronze

if ANOS_PROCESSAR is None:
    ANOS_PROCESSAR = inicializar_anos_processar()

if not ANOS_PROCESSAR:
    raise ValueError("❌ ANOS_PROCESSAR vazio - nenhum ano para processar")

# COMMAND ----------

# DBTITLE 1,CONFIGURAÇÃO E PARÂMETROS
# Resumo de configuração e parâmetros do processamento
# Exibe anos detectados e tabela de origem para auditabilidade

logger.info("="*80)
logger.info("SILVER - BPP (203) - Balanço Patrimonial Passivo")
logger.info("="*80)
logger.info(f"Anos a processar: {ANOS_PROCESSAR}")
logger.info(f"Tabela origem (Bronze): proj_cvm_01_bronze.103_bpp_dfp")
logger.info(f"Tabela destino (Silver): proj_cvm_02_silver.203_bpp_dfp")
logger.info(f"Total de anos: {len(ANOS_PROCESSAR)}")
logger.info("="*80)

# COMMAND ----------

# DBTITLE 1,LOOP - PROCESSAMENTO POR ANO
# Processamento resiliente: try/except POR ano para isolamento de falhas
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
        # Particiona por chave natural e seleciona versão mais recente via Window Function
        logger.info("[1/4] Aplicando filtro de versionamento...")
        
        df_bronze = spark.table("proj_cvm_01_bronze.103_bpp_dfp") \
            .filter(year(col("DT_REFER")) == ano)
        
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
        # "DT_INI_EXERC" removida: BPP não contém na fonte CVM
        "DT_FIM_EXERC",
        "CD_CONTA",
        "DS_CONTA",
        "VL_CONTA",
        "ANO",
        "TRIMESTRE",
        "MES",
        "DT_PROCESSAMENTO"
        )
        
        count_registros = df_silver.count()
        logger.info(f"[TRANSFORMAÇÃO] DataFrame Silver criado: {count_registros:,} registros")
        
        # ETAPA 3: DELETE WHERE + APPEND (idempotência por período)
        logger.info("[3/4] Gravando na tabela Silver...")
        
        spark.sql(f"DELETE FROM proj_cvm_02_silver.203_bpp_dfp WHERE ANO = {ano}")
        
        df_silver.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("proj_cvm_02_silver.203_bpp_dfp")
        
        logger.info(f"[GRAVAÇÃO] ✓ Ano {ano} gravado com sucesso")
        
        # ETAPA 4: Atualizar tabela de controle
        logger.info("[4/4] Registrando processamento...")
        
        spark.sql(f"""
            INSERT INTO proj_cvm_05_apoio.controle_ingestao
                (fonte, ano, arquivo, last_modified_cvm, versao_ingestao, ingest_ts, status, mensagem)
            VALUES (
                'bpp_silver',
                {ano},
                'proj_cvm_02_silver.203_bpp_dfp',
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
        
    except Exception as e:
        duracao = time.time() - inicio
        anos_falha.append((ano, str(e)))
        logger.error(f"[FALHA] ano={ano} | duração={duracao:.2f}s | erro={str(e)}")
        
        # Registrar falha na tabela de controle
        try:
            spark.sql(f"""
                INSERT INTO proj_cvm_05_apoio.controle_ingestao
                    (fonte, ano, arquivo, last_modified_cvm, versao_ingestao, ingest_ts, status, mensagem)
                VALUES (
                    'bpp_silver',
                    {ano},
                    'proj_cvm_02_silver.203_bpp_dfp',
                    NULL,
                    NULL,
                    current_timestamp(),
                    'FAILED',
                    '{str(e).replace("'", "''")}'
                )
            """)
        except:
            pass  # Se falhar ao registrar, não interromper processamento dos próximos anos

# Relatório final
logger.info(f"\n{'='*80}")
logger.info(f"SILVER BPP - PROCESSAMENTO CONCLUÍDO")
logger.info(f"{'='*80}")
logger.info(f"📊 Resumo: {len(anos_sucesso)} sucessos, {len(anos_falha)} falhas")
logger.info(f"✅ Anos processados com sucesso: {anos_sucesso}")

if anos_falha:
    logger.warning(f"\n⚠️ Anos que falharam:")
    for ano, erro in anos_falha:
        logger.warning(f"  - {ano}: {erro}")
else:
    logger.info("✅ Todos os anos foram processados com sucesso!")

logger.info("="*80)