# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Documentação
# MAGIC %md
# MAGIC # Ingestão de Dados Financeiros - Demonstração de Resultado do Exercício
# MAGIC
# MAGIC ## Objetivo
# MAGIC Este notebook realiza a **ingestão de dados de Demonstração do Resultado do Exercício (DRE) consolidada** de companhias abertas brasileiras, publicados pela Comissão de Valores Mobiliários (CVM) através das Demonstrações Financeiras Padronizadas (DFP).
# MAGIC
# MAGIC ## Fonte dos Dados
# MAGIC * **Órgão**: Comissão de Valores Mobiliários (CVM) - órgão regulador do mercado de capitais brasileiro
# MAGIC * **Portal**: Dados Abertos CVM
# MAGIC * **Documento**: Demonstrações Financeiras Padronizadas (DFP) - relatórios anuais obrigatórios
# MAGIC * **Demonstração específica**: Demonstração do Resultado do Exercício (DRE) - receitas, despesas e resultado consolidado
# MAGIC
# MAGIC ## Conteúdo
# MAGIC * Detecção automática de anos pendentes via `inicializar_anos_processar()` (sem HTTP para CVM)
# MAGIC * Extração de arquivo ZIP da Landing Zone em memória
# MAGIC * Guardrails: arquivo vazio → PARA, schema inválido → PARA
# MAGIC * Carga na camada bronze via APPEND-ONLY (preserva histórico de versões)
# MAGIC * Idempotência: controle + verificação de dados reais na tabela destino + comparação de Last-Modified (`_metadata.json` vs `controle_ingestao`) para detectar republicações
# MAGIC * Processamento resiliente: try/except por ano (falha isolada não interrompe demais)
# MAGIC
# MAGIC ## Função
# MAGIC Camada **Bronze** - Ingestão bruta mantendo a estrutura original fornecida pela fonte oficial (CVM).

# COMMAND ----------

# DBTITLE 1,CARREGAR CONFIGURAÇÕES
# MAGIC %run ../05_apoio/config_parametros

# COMMAND ----------

# DBTITLE 1,Inicializar Anos a Processar
# Captura explícita do retorno com guardrail de lista vazia
# CARGA=completa força todos os anos
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
logger.info("BRONZE - DRE (101) - Demonstração do Resultado do Exercício")
logger.info("="*80)
logger.info(f"Anos a processar: {ANOS_PROCESSAR}")
logger.info(f"Landing Zone: {VOLUME_LANDING_DFP}")
logger.info(f"Total de anos: {len(ANOS_PROCESSAR)}")
logger.info("="*80)

# COMMAND ----------

# DBTITLE 1,ORQUESTRAÇÃO RESILIENTE
# Loop: Processar cada ano da lista ANOS_PROCESSAR
# Lê arquivo ZIP da Landing Zone (já baixado por 004_verificacao_diaria_landing)

# Rastreamento de sucesso/falha
anos_sucesso = []
anos_falha = []

for ano in ANOS_PROCESSAR:
    inicio = time.time()
    logger.info(f"\n{'='*80}")
    logger.info(f"[INÍCIO] Processando ano={ano}")
    logger.info("="*80)
    
    try:
        # Caminhos
        ano_path = f"{VOLUME_LANDING_DFP}/{ano}"
        zip_path = f"{ano_path}/dfp_cia_aberta_{ano}.zip"
        metadata_path = f"{ano_path}/_metadata.json"
        
        # GUARDRAIL: Validação de pré-requisitos
        import os
        if not os.path.exists(zip_path):
            raise FileNotFoundError(f"Arquivo ZIP não encontrado: {zip_path}")
        if not os.path.exists(metadata_path):
            raise FileNotFoundError(f"Metadados não encontrados: {metadata_path}")

        # Ler metadados HTTP do arquivo
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)

        last_modified_cvm = metadata.get('last_modified_cvm')

        # IDEMPOTÊNCIA: Verificar se já processado
        ja_processado = spark.sql(f"""
            SELECT COUNT(*) as count
            FROM {SCHEMA_APOIO}.controle_ingestao
            WHERE fonte = 'dre'
              AND ano = {ano}
              AND last_modified_cvm = '{last_modified_cvm}'
              AND status = 'SUCCESS'
        """).collect()[0]['count']
        
        if ja_processado > 0:
            logger.info(f"⏭️  [SKIP] Ano {ano} já processado com esta versão CVM")
            anos_sucesso.append(ano)
            continue
        
        # Buscar próxima versão de ingestão para este ano
        versao_atual = spark.sql(f"""
            SELECT COALESCE(MAX(_versao_ingestao), 0) + 1 as proxima_versao
            FROM {SCHEMA_BRONZE}.101_dre_dfp
            WHERE year(DT_REFER) = {ano}
        """).collect()[0]['proxima_versao']
        
        logger.info(f"[METADADOS] Versão de ingestão: {versao_atual}")
        logger.info(f"[METADADOS] Last-Modified CVM: {last_modified_cvm}")

        # Ler ZIP diretamente da Landing Zone (Spark Connect compatível)
        # Não usa /tmp - lê direto do /Volumes/ path com open()
        with open(zip_path, 'rb') as f:
            zip_bytes = io.BytesIO(f.read())

        with zipfile.ZipFile(zip_bytes) as z:
            with z.open(f"dfp_cia_aberta_DRE_con_{ano}.csv") as csv_file:
                df_pandas = pd.read_csv(csv_file, sep=";", encoding="ISO-8859-1")
        
        # RECONCILIAÇÃO: Contagem inicial
        count_extraido = len(df_pandas)
        logger.info(f"[EXTRAÇÃO] Registros extraídos: {count_extraido:,}")

        # TRANSFORMAÇÃO PARA SPARK COM VERSIONAMENTO
        df_raw = spark.createDataFrame(df_pandas)

        # RECONCILIAÇÃO: Validar conversão pandas → Spark
        count_spark = df_raw.count()
        logger.info(f"[RECONCILIAÇÃO] Extraído: {count_extraido:,} | Spark: {count_spark:,}")
        assert count_extraido == count_spark, f"❌ Perda na conversão: {count_extraido} → {count_spark}"

        # GUARDRAIL DE VALIDAÇÃO DE SCHEMA
        df_validado = validar_e_projetar_schema(
            df_raw,
            COLUNAS_ESSENCIAIS_DRE,
            f"DRE {ano}"
        )

        # RECONCILIAÇÃO: Validar que schema não rejeitou registros
        count_validado = df_validado.count()
        logger.info(f"[RECONCILIAÇÃO] Pós-validação: {count_validado:,}")
        if count_validado < count_spark:
            logger.warning(f"⚠️  {count_spark - count_validado} registros rejeitados na validação")

        # ADICIONAR METADADOS TÉCNICOS
        df_bronze = df_validado \
            .withColumn("_versao_ingestao", lit(versao_atual)) \
            .withColumn("_last_modified_cvm", lit(last_modified_cvm)) \
            .withColumn("_ingest_ts", current_timestamp()) \
            .withColumn("_source_file", lit(f"dfp_cia_aberta_DRE_con_{ano}.csv"))

        # RECONCILIAÇÃO: Confirmar volume antes de gravar
        count_gravar = df_bronze.count()
        logger.info(f"[RECONCILIAÇÃO] Registros a gravar: {count_gravar:,}")
        assert count_gravar == count_validado, f"❌ Perda após metadados: {count_validado} → {count_gravar}"

        # CARGA APPEND-ONLY NA BRONZE
        df_bronze.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(f"{SCHEMA_BRONZE}.101_dre_dfp")

        logger.info(f"[GRAVAÇÃO] ✓ Ano {ano} gravado com sucesso (versão {versao_atual})")

        # Registrar ingestão na tabela de controle
        spark.sql(f"""
            INSERT INTO {SCHEMA_APOIO}.controle_ingestao
                (fonte, ano, arquivo, last_modified_cvm, versao_ingestao, ingest_ts, status, mensagem)
            VALUES (
                'dre',
                {ano},
                'dfp_cia_aberta_{ano}.zip',
                '{last_modified_cvm}',
                {versao_atual},
                current_timestamp(),
                'SUCCESS',
                NULL
            )
        """)

        duracao = time.time() - inicio
        anos_sucesso.append(ano)
        logger.info(f"[SUCESSO] ano={ano} | duração={duracao:.2f}s | registros={count_gravar:,}")
        
        # Registrar observabilidade (Grupo B)
        registrar_observabilidade_execucao(
            etapa='bronze',
            fonte='dre',
            ano=ano,
            inicio_epoch=inicio,
            duracao_segundos=duracao,
            status='SUCCESS',
            registros_processados=count_gravar,
            last_modified_cvm=str(last_modified_cvm) if last_modified_cvm else None
        )
        
    except Exception as e:
        duracao = time.time() - inicio
        anos_falha.append((ano, str(e)))
        logger.error(f"[FALHA] ano={ano} | duração={duracao:.2f}s | erro={str(e)}")
        
        # Registrar observabilidade (Grupo B)
        registrar_observabilidade_execucao(
            etapa='bronze',
            fonte='dre',
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
                    'dre',
                    {ano},
                    'dfp_cia_aberta_{ano}.zip',
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
# RELATÓRIO FINAL
logger.info(f"\n{'='*80}")
logger.info(f"BRONZE DRE - RELATÓRIO FINAL")
logger.info("="*80)
logger.info(f"✓ Sucesso: {anos_sucesso}")
if anos_falha:
    logger.warning(f"❌ Falhas: {[ano for ano, _ in anos_falha]}")
    for ano, erro in anos_falha:
        logger.warning(f"   • Ano {ano}: {erro}")
else:
    logger.info("✓ Nenhuma falha")
logger.info("="*80)

# Garantir falha de job quando há períodos não processados
if anos_falha:
    anos_falhados = [ano for ano, _ in anos_falha]
    raise RuntimeError(f"Falha ao processar {len(anos_falha)} ano(s): {anos_falhados}")