# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
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
# MAGIC * **Guardrail**: Bronze vazia para o ano → SKIP (não marca SUCCESS, evita estado irrecuperável)
# MAGIC * **Detecção**: anos pendentes via `inicializar_anos_processar()` (sem HTTP para CVM)
# MAGIC * **Bifurcação por CARGA**: `completa` (ignora idempotência, reprocessa) vs `incremental` (idempotência via `controle_ingestao`)
# MAGIC * **Idempotência**: verificação em `controle_ingestao` por `fonte` + `ano` + `last_modified_cvm` + `status=SUCCESS` (ler de `_metadata.json`)
# MAGIC * **Guardrails**: Bronze vazia → SKIP; unicidade da chave de negócio (`CNPJ_CIA`, `DT_REFER`, `VERSAO`, `CD_CONTA`, `GRUPO_DFP`, `ORDEM_EXERC`) → PARA
# MAGIC * **Observabilidade**: `registrar_observabilidade_execucao()` registra sucesso/falha por ano
# MAGIC * **Processamento resiliente**: try/except por ano (falha isolada não interrompe demais)
# MAGIC
# MAGIC ## Transformações Aplicadas
# MAGIC 1. **Filtro de versionamento**: Window Function (PARTITION BY `CNPJ_CIA`, `DT_REFER`, `CD_CONTA`, `ORDEM_EXERC`, ORDER BY `_versao_ingestao` DESC)
# MAGIC 2. **Conversão de tipos**: Datas (`to_date`), valores numéricos (`DoubleType`, `IntegerType`)
# MAGIC 3. **Deduplicação**: via Window Function (`row_number() == 1`) + guardrail de unicidade da chave de negócio
# MAGIC 4. **Tratamento de nulls**: Remoção de registros com `CNPJ_CIA`, `DT_REFER`, `CD_CONTA`, `VL_CONTA` nulos
# MAGIC 5. **Colunas calculadas**: `ANO`, `TRIMESTRE`, `MES` extraídos de `DT_REFER`; `DT_PROCESSAMENTO` (timestamp)
# MAGIC 6. **Normalização de escala**: `VL_CONTA` multiplicado por 1000 quando `ESCALA_MOEDA = MIL`
# MAGIC 7. **Enriquecimento hierárquico**: `ST_CONTA_FIXA`, `NIVEL_CONTA`, `CD_CONTA_PAI`, `CD_CONTA_RAIZ`, `TIPO_CONTA`
# MAGIC
# MAGIC ## Estratégia de Gravação
# MAGIC **REPLACE WHERE**: Substituição atômica por período - Delta Lake garante operação all-or-nothing, eliminando janela de vulnerabilidade
# MAGIC
# MAGIC ## Normalização de Escala Monetária
# MAGIC A CVM publica valores em duas escalas:
# MAGIC * `MIL` — valor em milhares de reais (necessita multiplicação por 1000)
# MAGIC * `UNIDADE` — valor já em reais
# MAGIC
# MAGIC A Silver normaliza `VL_CONTA` para **reais (unidade)** em todos os registros, preservando `ESCALA_MOEDA` para rastreabilidade.
# MAGIC
# MAGIC ## Enriquecimento Hierárquico de Contas
# MAGIC A CVM publica contas contábeis em estrutura hierárquica por notação de pontos (`CD_CONTA`): `2` → `2.01` → `2.01.01` → `2.01.01.01` → `2.01.01.01.01` (até 5 níveis). A Silver deriva 4 colunas dessa estrutura:
# MAGIC
# MAGIC * **`ST_CONTA_FIXA`**: Projetada da Bronze (S = conta fixa da estrutura CVM, N = detalhamento específico por empresa)
# MAGIC * **`NIVEL_CONTA`**: Nível hierárquico, derivado via `size(split(CD_CONTA, "[.]"))` (1 a 5)
# MAGIC * **`CD_CONTA_PAI`**: Conta pai na hierarquia — tudo antes do último `.` (NULL para nível 1)
# MAGIC * **`CD_CONTA_RAIZ`**: Conta raiz — primeiro segmento antes do primeiro `.` (ex: `2.01.01` → `2`)
# MAGIC * **`TIPO_CONTA`**: `TOTALIZADORA` (até 2 níveis) ou `ANALITICA` (3+ níveis)
# MAGIC
# MAGIC **Rollup**: o valor do pai é a soma dos filhos diretos. Dashboards devem filtrar por `NIVEL_CONTA` para evitar double-counting (somar todos os registros soma pais + filhos, inflando o total).
# MAGIC
# MAGIC ## Fontes
# MAGIC * **Origem**: `{SCHEMA_BRONZE}.103_bpp_dfp` — dados brutos ingeridos da CVM (notebook 103_cvm_dfp_bpp)
# MAGIC * **Metadados**: `_metadata.json` da Landing Zone (`{VOLUME_LANDING_DFP}/{ano}/`) — `last_modified_cvm` para idempotência
# MAGIC * **Destino**: `{SCHEMA_SILVER}.203_bpp_dfp` — dados limpos, padronizados e prontos para análise

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
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# COMMAND ----------

# DBTITLE 1,CONFIGURAÇÃO E PARÂMETROS
# Resumo de configuração e parâmetros do processamento
# Exibe anos detectados e tabela de origem para auditabilidade

logger.info("="*80)
logger.info("SILVER - BPP (203) - Balanço Patrimonial Passivo")
logger.info("="*80)
logger.info(f"Anos a processar: {ANOS_PROCESSAR}")
logger.info(f"Tabela origem (Bronze): {SCHEMA_BRONZE}.103_bpp_dfp")
logger.info(f"Tabela destino (Silver): {SCHEMA_SILVER}.203_bpp_dfp")
logger.info(f"Total de anos: {len(ANOS_PROCESSAR)}")
logger.info("="*80)

# COMMAND ----------

# DBTITLE 1,LOOP - PROCESSAMENTO POR ANO
# Processamento resiliente: try/except POR ano para isolamento de falhas
# Se um ano falhar, os demais continuam sendo processados

# ID unico da execucao (gerado no inicio do notebook, nao dentro de funcao)
# Passado como parametro para registrar_guardrail()
import uuid
id_execucao = str(uuid.uuid4())

anos_sucesso = []
anos_falha = []

for ano in ANOS_PROCESSAR:
    inicio = time.time()
    logger.info(f"\n{'='*80}")
    logger.info(f"[INÍCIO] Processando ano={ano}")
    logger.info("="*80)
    
    try:
        # Ler last_modified_cvm do _metadata.json para check de idempotência
        metadata_path = f"{VOLUME_LANDING_DFP}/{ano}/_metadata.json"
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        last_modified_cvm = metadata.get('last_modified_cvm')

        # IDEMPOTÊNCIA (CARGA=incremental): skip se Bronze não mudou
        if CARGA != "completa":
            ja_processado = spark.sql(f"""
                SELECT COUNT(*) as count
                FROM {SCHEMA_APOIO}.controle_ingestao
                WHERE fonte = 'bpp_silver'
                  AND ano = {ano}
                  AND last_modified_cvm = '{last_modified_cvm}'
                  AND status = 'SUCCESS'
            """).collect()[0]['count']
            
            if ja_processado > 0:
                logger.info(f"⏭️  [SKIP] Ano {ano} já processado para esta versão CVM")
                anos_sucesso.append(ano)
                continue

        # ETAPA 1: Filtro de versionamento (Window Function)
        # Particiona por chave natural e seleciona versão mais recente via Window Function
        logger.info("[1/4] Aplicando filtro de versionamento...")
        
        df_bronze = spark.table(f"{SCHEMA_BRONZE}.103_bpp_dfp") \
            .filter(year(col("DT_REFER")) == ano)
        
        # GUARDRAIL: Verificar se Bronze tem dados reais para este ano
        # Se Bronze está vazia, NÃO marca SUCCESS — evita estado irrecuperável
        count_bronze = df_bronze.count()
        if count_bronze == 0:
            registrar_guardrail(
                id_execucao=id_execucao,
                nome_guardrail='bronze_vazia',
                tipo_check='empty_table',
                resultado='SKIP',
                etapa='silver',
                fonte='bpp',
                ano=ano,
                esperado='>0',
                encontrado='0',
                detalhes=f'Bronze vazia para ano {ano}'
            )
            logger.warning(f"[SKIP] Bronze vazia para ano {ano} - pulando sem marcar SUCCESS")
            continue
        
        registrar_guardrail(
            id_execucao=id_execucao,
            nome_guardrail='bronze_vazia',
            tipo_check='empty_table',
            resultado='PASS',
            etapa='silver',
            fonte='bpp',
            ano=ano,
            esperado='>0',
            encontrado=str(count_bronze),
            detalhes=f'Bronze com {count_bronze} registros para ano {ano}'
        )
        
        window_spec = Window.partitionBy(
            "CNPJ_CIA", "DT_REFER", "CD_CONTA", "ORDEM_EXERC"
        ).orderBy(col("_versao_ingestao").desc())
        
        df_versao_atual = df_bronze.withColumn(
            "_row_num", row_number().over(window_spec)
        ).filter(col("_row_num") == 1).drop("_row_num")
        
        # GUARDRAIL: Unicidade da chave de negocio
        chave_negocio = ["CNPJ_CIA", "DT_REFER", "VERSAO", "CD_CONTA", "GRUPO_DFP", "ORDEM_EXERC"]
        df_duplicatas = df_versao_atual.groupBy(*chave_negocio).count().filter("count > 1")
        count_duplicatas = df_duplicatas.count()

        if count_duplicatas > 0:
            registrar_guardrail(
                id_execucao=id_execucao,
                nome_guardrail='unicidade_chave_negocio',
                tipo_check='uniqueness',
                resultado='FAIL',
                etapa='silver',
                fonte='bpp',
                ano=ano,
                registros_afetados=count_duplicatas,
                detalhes=f'{count_duplicatas} chaves duplicadas em ({", ".join(chave_negocio)})'
            )
            raise ValueError(f"Unicidade violada: {count_duplicatas} chaves duplicadas")
        else:
            registrar_guardrail(
                id_execucao=id_execucao,
                nome_guardrail='unicidade_chave_negocio',
                tipo_check='uniqueness',
                resultado='PASS',
                etapa='silver',
                fonte='bpp',
                ano=ano,
                registros_afetados=0,
                detalhes=f'Chave unica em ({", ".join(chave_negocio)})'
            )
        
        # ETAPA 2: Transformações (padronização, limpeza, enriquecimento)
        logger.info("[2/4] Aplicando transformações...")

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
        # "DT_INI_EXERC" removida: BPP não contém na fonte CVM
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
        "TIPO_CONTA"
        )
        
        count_registros = df_silver.count()
        logger.info(f"[TRANSFORMAÇÃO] DataFrame Silver criado: {count_registros:,} registros")
        
        # ETAPA 3: REPLACE WHERE (substituição atômica por período)
        logger.info("[3/4] Gravando na tabela Silver...")
        
        df_silver.write \
            .format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", f"ANO = {ano}") \
            .saveAsTable(f"{SCHEMA_SILVER}.203_bpp_dfp")
        
        logger.info(f"[GRAVAÇÃO] ✓ Ano {ano} gravado com sucesso")
        
        # ETAPA 4: Atualizar tabela de controle
        logger.info("[4/4] Registrando processamento...")
        
        spark.sql(f"""
            INSERT INTO {SCHEMA_APOIO}.controle_ingestao
                (fonte, ano, arquivo, last_modified_cvm, versao_ingestao, ingest_ts, status, mensagem)
            VALUES (
                'bpp_silver',
                {ano},
                '{SCHEMA_SILVER}.203_bpp_dfp',
                '{last_modified_cvm}',
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
            fonte='bpp_silver',
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
            fonte='bpp_silver',
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
                    'bpp_silver',
                    {ano},
                    '{SCHEMA_SILVER}.203_bpp_dfp',
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