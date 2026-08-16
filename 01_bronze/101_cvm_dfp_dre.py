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
# Inicializar ANOS_PROCESSAR - capturar retorno explicitamente
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

# COMMAND ----------

# DBTITLE 1,CONFIGURAÇÃO E PARÂMETROS
# ANOS_PROCESSAR: Lista de anos definida pelo orquestrador
# Orquestrador detecta automaticamente quais anos processar (novos ou atualizados)

print("="*80)
print("BRONZE - DRE (101)")
print("="*80)
print(f"Anos a processar: {ANOS_PROCESSAR}")
print(f"Landing Zone: {VOLUME_LANDING_DFP}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,EXTRAÇÃO DE DADOS DA LANDING ZONE
# Loop: Processar cada ano da lista ANOS_PROCESSAR
# Lê arquivo ZIP da Landing Zone (já baixado por 002_download_cvm_para_landing)

# Rastreamento de sucesso/falha
anos_sucesso = []
anos_falha = []

for ano in ANOS_PROCESSAR:
    print(f"\n{'='*80}")
    print(f"Processando ano: {ano}")
    print("="*80)

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
            FROM proj_cvm_05_apoio.controle_ingestao
            WHERE fonte = 'dre'
              AND ano = {ano}
              AND last_modified_cvm = '{last_modified_cvm}'
              AND status = 'SUCCESS'
        """).collect()[0]['count']
        
        if ja_processado > 0:
            print(f"⏭️  Ano {ano} já processado com esta versão CVM - pulando")
            anos_sucesso.append(ano)  # Não é falha, apenas já processado
            continue
        
        # Buscar próxima versão de ingestão para este ano
        versao_atual = spark.sql(f"""
            SELECT COALESCE(MAX(_versao_ingestao), 0) + 1 as proxima_versao
            FROM proj_cvm_01_bronze.101_dre_dfp
            WHERE year(DT_REFER) = {ano}
        """).collect()[0]['proxima_versao']
        
        print(f"Versão de ingestão: {versao_atual}")
        print(f"Last-Modified CVM: {last_modified_cvm}")

        # Ler ZIP diretamente da Landing Zone (Spark Connect compatível)
        # Não usa /tmp - lê direto do /Volumes/ path com open()
        with open(zip_path, 'rb') as f:
            zip_bytes = io.BytesIO(f.read())

        with zipfile.ZipFile(zip_bytes) as z:
            with z.open(f"dfp_cia_aberta_DRE_con_{ano}.csv") as csv_file:
                df_pandas = pd.read_csv(csv_file, sep=";", encoding="ISO-8859-1")
        
        # RECONCILIAÇÃO: Contagem inicial
        count_extraido = len(df_pandas)
        print(f"Registros extraídos: {count_extraido:,}")
        
    except Exception as e:
        print(f"❌ ERRO ao processar ano {ano}: {e}")
        anos_falha.append((ano, str(e)))
        continue

# COMMAND ----------

# DBTITLE 1,TRANSFORMAÇÃO PARA SPARK COM VERSIONAMENTO
    # df_raw: Conversão do DataFrame pandas para Spark
    # Mantém a estrutura original completa extraída da CVM
    
    df_raw = spark.createDataFrame(df_pandas)
    
    # RECONCILIAÇÃO: Validar conversão pandas → Spark
    count_spark = df_raw.count()
    print(f"DataFrame Spark criado: {count_spark:,} registros")
    assert count_extraido == count_spark, f"❌ Perda na conversão: {count_extraido} → {count_spark}"

# COMMAND ----------

# DBTITLE 1,GUARDRAIL DE VALIDAÇÃO DE SCHEMA
    # df_validado: Validação de colunas essenciais e descarte de extras
    # Protege contra mudanças no formato dos arquivos publicados pela CVM
    
    df_validado = validar_e_projetar_schema(
        df_raw,
        COLUNAS_ESSENCIAIS_DRE,
        f"DRE {ano}"
    )
    
    # RECONCILIAÇÃO: Validar que schema não rejeitou registros
    count_validado = df_validado.count()
    print(f"Registros pós-validação: {count_validado:,}")
    if count_validado < count_spark:
        print(f"⚠️  {count_spark - count_validado} registros rejeitados na validação")

# COMMAND ----------

# DBTITLE 1,ADICIONAR METADADOS TÉCNICOS
    # df_bronze: Enriquecimento com metadados de rastreabilidade
    # Identificadores técnicos para auditoria e debug
    
    df_bronze = df_validado \
        .withColumn("_versao_ingestao", lit(versao_atual)) \
        .withColumn("_last_modified_cvm", lit(last_modified_cvm)) \
        .withColumn("_ingest_ts", current_timestamp()) \
        .withColumn("_source_file", lit(f"dfp_cia_aberta_DRE_con_{ano}.csv"))
    
    # RECONCILIAÇÃO: Confirmar volume antes de gravar
    count_gravar = df_bronze.count()
    print(f"✓ Confirmado: {count_gravar:,} registros a gravar")
    assert count_gravar == count_validado, f"❌ Perda após metadados: {count_validado} → {count_gravar}"

# COMMAND ----------

# DBTITLE 1,CARGA APPEND-ONLY NA BRONZE
    # Gravação: APPEND puro (preserva histórico completo)
    # Bronze nunca deleta dados - Silver filtra versão mais recente

    df_bronze.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("proj_cvm_01_bronze.101_dre_dfp")

    print(f"✓ Ano {ano} gravado com sucesso (versão {versao_atual})")
    anos_sucesso.append(ano)

    # Registrar ingestão na tabela de controle
    spark.sql(f"""
        INSERT INTO proj_cvm_05_apoio.controle_ingestao
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

    print(f"✓ Ingestão registrada na tabela de controle")

# COMMAND ----------

# DBTITLE 1,RELATÓRIO FINAL
# RELATÓRIO FINAL
print(f"\n{'='*80}")
print(f"BRONZE DRE - RELATÓRIO FINAL")
print("="*80)
print(f"✓ Sucesso: {anos_sucesso}")
if anos_falha:
    print(f"❌ Falhas: {[ano for ano, _ in anos_falha]}")
    for ano, erro in anos_falha:
        print(f"   • Ano {ano}: {erro}")
else:
    print("✓ Nenhuma falha")
print("="*80)

# Garantir falha de job quando há períodos não processados
if anos_falha:
    anos_falhados = [ano for ano, _ in anos_falha]
    raise RuntimeError(f"Falha ao processar {len(anos_falha)} ano(s): {anos_falhados}")