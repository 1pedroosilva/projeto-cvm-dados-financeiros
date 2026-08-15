# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,DOCUMENTAÇÃO
# MAGIC %md
# MAGIC # Ingestão de Dados Financeiros - Balanço Patrimonial Passivo
# MAGIC
# MAGIC ## Objetivo
# MAGIC Este notebook realiza a **ingestão de dados de Balanço Patrimonial Passivo (BPP) consolidado** de companhias abertas brasileiras, publicados pela Comissão de Valores Mobiliários (CVM) através das Demonstrações Financeiras Padronizadas (DFP).
# MAGIC
# MAGIC ## Fonte dos Dados
# MAGIC * **Órgão**: Comissão de Valores Mobiliários (CVM) - órgão regulador do mercado de capitais brasileiro
# MAGIC * **Portal**: Dados Abertos CVM
# MAGIC * **Documento**: Demonstrações Financeiras Padronizadas (DFP) - relatórios anuais obrigatórios
# MAGIC * **Demonstração específica**: Balanço Patrimonial Passivo (BPP) - passivos consolidados
# MAGIC
# MAGIC ## Conteúdo
# MAGIC * Extração de dados do portal de dados abertos da CVM
# MAGIC * Processamento de arquivo ZIP em memória
# MAGIC * Carga dos dados na camada bronze do Unity Catalog
# MAGIC
# MAGIC ## Função
# MAGIC Camada **Bronze** - Ingestão bruta mantendo a estrutura original fornecida pela fonte oficial (CVM).

# COMMAND ----------

# DBTITLE 1,CARREGAR CONFIGURAÇÕES
# MAGIC %run ../05_apoio/config_parametros

# COMMAND ----------

# DBTITLE 1,INICIALIZAR ANOS A PROCESSAR
# ANOS_PROCESSAR: Lista de anos detectada automaticamente pelo orquestrador
# Detecta quais anos têm arquivos novos ou atualizados na Landing Zone

ANOS_PROCESSAR = inicializar_anos_processar()
if not ANOS_PROCESSAR:
    raise ValueError("❌ ANOS_PROCESSAR vazio - nenhum ano para processar")

# COMMAND ----------

# DBTITLE 1,IMPORTS
import zipfile
import io
import pandas as pd
from pyspark.sql.functions import current_timestamp, lit, year
import json
import logging
import time
from datetime import datetime

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
logger.info("BRONZE - BPP (103) - Balanço Patrimonial Passivo")
logger.info("="*80)
logger.info(f"Anos a processar: {ANOS_PROCESSAR}")
logger.info(f"Landing Zone: {VOLUME_LANDING_DFP}")
logger.info(f"Total de anos: {len(ANOS_PROCESSAR)}")
logger.info("="*80)

# COMMAND ----------

# DBTITLE 1,VALIDAÇÃO DE PRÉ-REQUISITOS
# validar_prerequisitos: Valida existência de arquivos antes de processar
# Falha rápida evita desperdício de recursos em processamento que vai falhar

def validar_prerequisitos(ano):
    """
    Valida que arquivos ZIP e metadados existem antes de processar.
    Retorna (metadados_dict) se válido, levanta FileNotFoundError se inválido.
    """
    ano_path = f"{VOLUME_LANDING_DFP}/{ano}"
    zip_path = f"{ano_path}/dfp_cia_aberta_{ano}.zip"
    metadata_path = f"{ano_path}/_metadata.json"
    
    # Validar metadados
    try:
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Metadados não encontrados: {metadata_path}")
    
    # Validar ZIP
    try:
        with open(zip_path, 'rb') as f:
            pass
    except FileNotFoundError:
        raise FileNotFoundError(f"Arquivo ZIP não encontrado: {zip_path}")
    
    return metadata

# COMMAND ----------

# DBTITLE 1,EXTRAÇÃO DE DADOS
# extrair_dados: Lê arquivo ZIP da Landing Zone e converte para pandas
# Spark Connect compatível (usa pandas como intermediário para leitura de ZIP)

def extrair_dados(ano):
    """
    Extrai dados do arquivo ZIP para DataFrame pandas.
    Retorna DataFrame pandas com os dados brutos do CSV.
    """
    ano_path = f"{VOLUME_LANDING_DFP}/{ano}"
    zip_path = f"{ano_path}/dfp_cia_aberta_{ano}.zip"
    
    logger.info(f"[EXTRAÇÃO] Lendo arquivo ZIP...")
    with open(zip_path, 'rb') as f:
        zip_bytes = io.BytesIO(f.read())
    
    with zipfile.ZipFile(zip_bytes) as z:
        with z.open(f"dfp_cia_aberta_BPP_con_{ano}.csv") as csv_file:
            df_pandas = pd.read_csv(csv_file, sep=";", encoding="ISO-8859-1")
    
    logger.info(f"[EXTRAÇÃO] Registros extraídos: {len(df_pandas):,}")
    return df_pandas

# COMMAND ----------

# DBTITLE 1,TRANSFORMAÇÃO BRONZE
# transformar_bronze: Converte pandas→Spark, valida schema e adiciona metadados técnicos
# Aplica guardrail de validação de colunas essenciais (descarta extras automaticamente)

def transformar_bronze(df_pandas, ano, versao_atual, last_modified_cvm):
    """
    Transforma DataFrame pandas em DataFrame Spark Bronze com metadados técnicos.
    Retorna DataFrame Spark pronto para gravação.
    """
    logger.info(f"[TRANSFORMAÇÃO] Convertendo pandas → Spark...")
    df_raw = spark.createDataFrame(df_pandas)
    
    logger.info(f"[GUARDRAIL] Validando schema e projetando colunas essenciais...")
    df_validado = validar_e_projetar_schema(
        df_raw,
        COLUNAS_ESSENCIAIS_BPP,
        f"BPP {ano}"
    )
    
    # Adicionar metadados técnicos
    df_bronze = df_validado \
        .withColumn("_versao_ingestao", lit(versao_atual)) \
        .withColumn("_last_modified_cvm", lit(last_modified_cvm)) \
        .withColumn("_ingest_ts", current_timestamp()) \
        .withColumn("_source_file", lit(f"dfp_cia_aberta_BPP_con_{ano}.csv"))
    
    count_registros = df_bronze.count()
    logger.info(f"[TRANSFORMAÇÃO] DataFrame Bronze criado: {count_registros:,} registros")
    
    return df_bronze, count_registros

# COMMAND ----------

# DBTITLE 1,GRAVAÇÃO DELTA
# gravar_delta: Grava DataFrame na tabela Bronze com estratégia APPEND
# Bronze nunca deleta dados - preserva histórico completo de todas as versões

def gravar_delta(df_bronze, ano, versao_atual):
    """
    Grava DataFrame Bronze na tabela Delta.
    Estratégia: APPEND puro (Silver filtra versão mais recente via Window Function).
    """
    logger.info(f"[GRAVAÇÃO] Gravando na tabela Bronze...")
    df_bronze.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("proj_cvm_01_bronze.103_bpp_dfp")
    
    logger.info(f"[GRAVAÇÃO] ✓ Ano {ano} gravado com sucesso (versão {versao_atual})")

# COMMAND ----------

# DBTITLE 1,REGISTRO DE CONTROLE
# registrar_controle: Registra status de processamento na tabela de controle
# Permite rastreabilidade e detecção de períodos pendentes/falhos

def registrar_controle_sucesso(ano, last_modified_cvm, versao_atual):
    """
    Registra processamento bem-sucedido na tabela de controle.
    """
    spark.sql(f"""
        INSERT INTO proj_cvm_05_apoio.controle_ingestao
            (fonte, ano, arquivo, last_modified_cvm, versao_ingestao, ingest_ts, status, mensagem)
        VALUES (
            'bpp',
            {ano},
            'dfp_cia_aberta_{ano}.zip',
            '{last_modified_cvm}',
            {versao_atual},
            current_timestamp(),
            'SUCCESS',
            NULL
        )
    """)

def registrar_controle_falha(ano, erro):
    """
    Registra falha de processamento na tabela de controle.
    """
    try:
        spark.sql(f"""
            INSERT INTO proj_cvm_05_apoio.controle_ingestao
                (fonte, ano, arquivo, last_modified_cvm, versao_ingestao, ingest_ts, status, mensagem)
            VALUES (
                'bpp',
                {ano},
                'dfp_cia_aberta_{ano}.zip',
                NULL,
                NULL,
                current_timestamp(),
                'FAILED',
                '{str(erro).replace("'", "''")}'
            )
        """)
    except:
        pass  # Se falhar ao registrar, não interromper processamento

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
        # 1. Validar pré-requisitos
        logger.info(f"[VALIDAÇÃO] Verificando arquivos necessários...")
        metadata = validar_prerequisitos(ano)
        last_modified_cvm = metadata.get('last_modified_cvm')
        
        # 2. Buscar próxima versão de ingestão para este ano
        versao_atual = spark.sql(f"""
            SELECT COALESCE(MAX(_versao_ingestao), 0) + 1 as proxima_versao
            FROM proj_cvm_01_bronze.103_bpp_dfp
            WHERE year(DT_REFER) = {ano}
        """).collect()[0]['proxima_versao']
        
        logger.info(f"[METADADOS] Versão de ingestão: {versao_atual}")
        logger.info(f"[METADADOS] Last-Modified CVM: {last_modified_cvm}")
        
        # 3. Extrair dados
        df_pandas = extrair_dados(ano)
        
        # 4. Transformar para Bronze
        df_bronze, count_registros = transformar_bronze(
            df_pandas, ano, versao_atual, last_modified_cvm
        )
        
        # 5. Gravar Delta
        gravar_delta(df_bronze, ano, versao_atual)
        
        # 6. Registrar controle de sucesso
        registrar_controle_sucesso(ano, last_modified_cvm, versao_atual)
        
        duracao = time.time() - inicio
        anos_sucesso.append(ano)
        logger.info(f"[SUCESSO] ano={ano} | duração={duracao:.2f}s | registros={count_registros:,}")
        
    except Exception as e:
        duracao = time.time() - inicio
        anos_falha.append((ano, str(e)))
        logger.error(f"[FALHA] ano={ano} | duração={duracao:.2f}s | erro={str(e)}")
        
        # Registrar falha na tabela de controle
        registrar_controle_falha(ano, e)

# Relatório final
logger.info(f"\n{'='*80}")
logger.info(f"BRONZE BPP - PROCESSAMENTO CONCLUÍDO")
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