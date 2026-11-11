# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Documentação
# MAGIC %md
# MAGIC # Transformação Silver - DFC DFP CVM
# MAGIC
# MAGIC ## Objetivo
# MAGIC Transformar dados brutos de **Demonstração do Fluxo de Caixa (DFC)** das **Demonstrações Financeiras Padronizadas (DFP)** da CVM, movendo-os da camada **bronze** para a camada **silver**.
# MAGIC
# MAGIC ## Camada Silver - Princípios
# MAGIC A camada silver aplica:
# MAGIC * **Versionamento**: Window Function seleciona versão mais recente da Bronze (ORDER BY _versao_ingestao DESC)
# MAGIC * **Limpeza**: remoção de nulls críticos, duplicados e inconsistências
# MAGIC * **Padronização**: tipos de dados corretos, formatos consistentes, **normalização de escala monetária**
# MAGIC * **Enriquecimento**: hierarquia de contas e **coluna derivada `METODO_DFC`**
# MAGIC * **Guardrail**: Bronze vazia para o ano → SKIP (não marca SUCCESS, evita estado irrecuperável)
# MAGIC * **Detecção**: anos pendentes via `inicializar_anos_processar()` (sem HTTP para CVM)
# MAGIC * **Bifurcação por CARGA**: `completa` (ignora idempotência, reprocessa) vs `incremental` (idempotência via `controle_ingestao`)
# MAGIC * **Idempotência**: verificação em `controle_ingestao` por `fonte` + `ano` + `last_modified_cvm` + `status=SUCCESS` (ler de `_metadata.json`)
# MAGIC * **Guardrails**: Bronze vazia → SKIP; unicidade da chave de negócio (`CNPJ_CIA`, `DT_REFER`, `CD_CONTA`, `ORDEM_EXERC`, `METODO_DFC` — alinhada com Window Function) → PARA
# MAGIC * **Observabilidade**: `registrar_observabilidade_execucao()` registra sucesso/falha por ano; `registrar_observabilidade_job()` atualiza o MERGE no nivel do run (contexto do job capturado via `dbutils.widgets.get()`)
# MAGIC * **Processamento resiliente**: try/except por ano (falha isolada não interrompe demais)
# MAGIC
# MAGIC ## Particularidade da DFC: METODO_DFC
# MAGIC A bronze `104_dfc_dfp` une os dois métodos de divulgação (MD e MI) publicados pela CVM. A coluna `METODO_DFC` é derivada de `GRUPO_DFP` e integra a chave de negócio da deduplicação, garantindo que registros de métodos distintos nunca colidam.
# MAGIC
# MAGIC ## Normalização de Escala Monetária
# MAGIC A CVM publica valores em duas escalas:
# MAGIC * `MIL` — valor em milhares de reais (multiplicado por 1000)
# MAGIC * `UNIDADE` — valor em reais (permanece inalterado)
# MAGIC A coluna `ESCALA_MOEDA` original é preservada para rastreabilidade; `VL_CONTA` fica normalizada em reais.
# MAGIC
# MAGIC ## Fontes
# MAGIC * **Origem**: `{SCHEMA_BRONZE}.104_dfc_dfp` — bronze APPEND-ONLY com versionamento
# MAGIC * **Destino**: `{SCHEMA_SILVER}.204_dfc_dfp` — dados limpos, tipados e enriquecidos, REPLACE WHERE por ano
# MAGIC
# MAGIC ## Função
# MAGIC Camada **Silver** - Limpeza técnica agnóstica de negócio: casting, deduplicação, padronização e enriquecimento hierárquico.

# COMMAND ----------

# DBTITLE 1,Carregar configurações
# MAGIC %run ../05_apoio/config_parametros

# COMMAND ----------

# DBTITLE 1,Carregar transformações Silver
import sys, os

_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_nb_path = _ctx.notebookPath().get()
_project_root = "/Workspace/" + "/".join(_nb_path.strip("/").split("/")[:-2])
sys.path.insert(0, os.path.join(_project_root, "05_apoio"))

from transformacoes_silver import normalizar_escala_monetaria, derivar_hierarquia_conta, deduplicar_por_versao

# COMMAND ----------

# DBTITLE 1,Inicializar Anos a Processar
# Captura explícita do retorno com guardrail de lista vazia
# CARGA=completa força todos os anos
ANOS_PROCESSAR = inicializar_anos_processar()

if not ANOS_PROCESSAR:
    raise ValueError("❌ ANOS_PROCESSAR vazio - nenhum ano para processar")

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import col, to_date, year, quarter, month, current_timestamp, when, lit
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
logger.info("SILVER - DFC (204) - Transformação DFC")
logger.info("="*80)
logger.info(f"Anos a processar: {ANOS_PROCESSAR}")
logger.info(f"Total de anos: {len(ANOS_PROCESSAR)}")
logger.info("="*80)

# COMMAND ----------

# DBTITLE 1,ORQUESTRAÇÃO RESILIENTE
# Orquestração resiliente: try/except POR ano para isolamento de falhas
# Se um ano falhar, os demais continuam sendo processados

# ID unico da execucao (gerado no inicio do notebook, nao dentro de funcao)
# Passado como parametro para registrar_guardrail()
import uuid
id_execucao = str(uuid.uuid4())

anos_sucesso = []
anos_falha = []
_inicio_notebook = datetime.now()

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
                WHERE fonte = 'dfc_silver'
                  AND ano = {ano}
                  AND last_modified_cvm = '{last_modified_cvm}'
                  AND status = 'SUCCESS'
            """).collect()[0]['count']

            if ja_processado > 0:
                logger.info(f"⏭️  [SKIP] Ano {ano} já processado para esta versão CVM")
                anos_sucesso.append(ano)
                continue

        # ETAPA 1: Filtro de versionamento (Window Function)
        logger.info("[1/4] Aplicando filtro de versionamento...")

        df_bronze = spark.table(f"{SCHEMA_BRONZE}.104_dfc_dfp") \
            .filter(year(col("DT_REFER")) == ano)

        # GUARDRAIL: Verificar se Bronze tem dados reais para este ano
        count_bronze = df_bronze.count()
        if count_bronze == 0:
            registrar_guardrail(
                id_execucao=id_execucao,
                nome_guardrail='bronze_vazia',
                tipo_check='empty_table',
                resultado='SKIP',
                etapa='silver',
                fonte='dfc',
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
            fonte='dfc',
            ano=ano,
            esperado='>0',
            encontrado=str(count_bronze),
            detalhes=f'Bronze com {count_bronze} registros para ano {ano}'
        )

        # Enriquecimento específico da DFC: método de divulgação derivado de GRUPO_DFP
        # A fonte já traz o método no texto ("Método Direto"/"Método Indireto");
        # a coluna derivada torna a chave de negócio explícita e impede que
        # registros de métodos distintos colidam na deduplicação
        df_bronze = df_bronze.withColumn(
            "METODO_DFC",
            when(col("GRUPO_DFP").contains("Método Direto"), lit("METODO_DIRETO"))
            .otherwise(lit("METODO_INDIRETO"))
        )

        df_versao_atual = deduplicar_por_versao(
            df_bronze,
            colunas_particao=["CNPJ_CIA", "DT_REFER", "CD_CONTA", "ORDEM_EXERC", "METODO_DFC"],
            coluna_versao="_versao_ingestao"
        )

        # GUARDRAIL: Unicidade da chave de negocio (alinhada com partitionBy da Window Function)
        chave_negocio = ["CNPJ_CIA", "DT_REFER", "CD_CONTA", "ORDEM_EXERC", "METODO_DFC"]
        df_duplicatas = df_versao_atual.groupBy(*chave_negocio).count().filter("count > 1")
        count_duplicatas = df_duplicatas.count()

        if count_duplicatas > 0:
            registrar_guardrail(
                id_execucao=id_execucao,
                nome_guardrail='unicidade_chave_negocio',
                tipo_check='uniqueness',
                resultado='FAIL',
                etapa='silver',
                fonte='dfc',
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
                fonte='dfc',
                ano=ano,
                registros_afetados=0,
                detalhes=f'Chave unica em ({", ".join(chave_negocio)})'
            )

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
            .withColumn("ANO", year(col("DT_REFER"))) \
            .withColumn("TRIMESTRE", quarter(col("DT_REFER"))) \
            .withColumn("MES", month(col("DT_REFER"))) \
            .withColumn("DT_PROCESSAMENTO", current_timestamp()) \
            .withColumn("ST_CONTA_FIXA", col("ST_CONTA_FIXA"))

        # Normalização de escala monetária (extraída para 05_apoio/transformacoes_silver)
        df_transformado = normalizar_escala_monetaria(df_transformado)

        # Enriquecimento hierárquico (extraída para 05_apoio/transformacoes_silver)
        df_transformado = derivar_hierarquia_conta(df_transformado)

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
            "METODO_DFC"
        )

        count_registros = df_silver.count()
        logger.info(f"[TRANSFORMAÇÃO] DataFrame Silver criado: {count_registros:,} registros")

        # ETAPA 3: REPLACE WHERE (substituição atômica por período)
        logger.info("[3/4] Gravando na tabela Silver...")

        df_silver.write \
            .format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", f"ANO = {ano}") \
            .saveAsTable(f"{SCHEMA_SILVER}.204_dfc_dfp")

        logger.info(f"[GRAVAÇÃO] ✓ Ano {ano} gravado com sucesso")

        # ETAPA 4: Atualizar tabela de controle
        logger.info("[4/4] Registrando processamento...")

        spark.sql(f"""
            INSERT INTO {SCHEMA_APOIO}.controle_ingestao
                (fonte, ano, arquivo, last_modified_cvm, versao_ingestao, ingest_ts, status, mensagem)
            VALUES (
                'dfc_silver',
                {ano},
                '{SCHEMA_SILVER}.204_dfc_dfp',
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
            id_execucao=id_execucao,
            etapa='silver',
            fonte='dfc_silver',
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
            id_execucao=id_execucao,
            etapa='silver',
            fonte='dfc_silver',
            ano=ano,
            inicio_epoch=inicio,
            duracao_segundos=duracao,
            status='ERROR',
            tipo_erro=type(e).__name__,
            mensagem_erro=str(e)
        )

# COMMAND ----------

# DBTITLE 1,RELATÓRIO FINAL
# RELATÓRIO FINAL
logger.info(f"\n{'='*80}")
logger.info(f"SILVER DFC - RELATÓRIO FINAL")
logger.info("="*80)
logger.info(f"✓ Sucesso: {anos_sucesso}")
if anos_falha:
    logger.warning(f"❌ Falhas: {[ano for ano, _ in anos_falha]}")
    for ano, erro in anos_falha:
        logger.warning(f"   • Ano {ano}: {erro}")
else:
    logger.info(f"✓ Nenhuma falha")
logger.info("="*80)

# Registrar job na tabela de observabilidade (MERGE idempotente)
registrar_observabilidade_job(
    inicio_ts=_inicio_notebook,
    fim_ts=datetime.now(),
    status='ERROR' if anos_falha else 'SUCCESS',
)

# Garantir falha de job quando há períodos não processados
if anos_falha:
    anos_falhados = [ano for ano, _ in anos_falha]
    raise RuntimeError(f"Falha ao processar {len(anos_falha)} ano(s): {anos_falhados}")