# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC

# COMMAND ----------

# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC

# COMMAND ----------

# ============================================================================
# Configuração Centralizada - Pipeline CVM
# ============================================================================
# Propósito: Parâmetros globais do pipeline - anos, URLs, schemas UC
# Uso: Importado por todos notebooks via %run ./config_parametros
# Vantagem: Mudança de configuração em um único lugar
# ============================================================================

import json
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# Fuso de referencia do projeto: os dados da CVM seguem o calendario
# fiscal brasileiro, entao "que ano e hoje" se responde em Brasilia.
FUSO_PROJETO = ZoneInfo("America/Sao_Paulo")

# ============================================================================
# PARÂMETROS DE PROCESSAMENTO
# ============================================================================

# Anos a processar - DEFINIDO DINAMICAMENTE ao final deste arquivo
# Executado automaticamente via detecção inteligente
# Pode ser sobrescrito via ANOS_OVERRIDE (veja final do arquivo)

# Ano inicial do projeto (janela começa em 2021, alinhado com a landing zone)
ANO_INICIAL_PROJETO = 2021

# Janela temporal relevante: quantos anos para trás processar a partir do ano atual
# Controla a inteligência temporal do orquestrador automático
# Ex: 5 anos = processa do ano (atual-5) até ano atual
JANELA_ANOS_RELEVANTE = 5

# ============================================================================
# URLs E FONTES DE DADOS
# ============================================================================

# Base URL da CVM - Dados Abertos DFP
CVM_BASE_URL = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"

# Padrão de nomenclatura dos arquivos CVM
TIPOS_DFP = {
    'dre': 'DRE',      # Demonstração do Resultado do Exercício
    'bpa': 'BPA',      # Balanço Patrimonial Ativo
    'bpp': 'BPP',      # Balanço Patrimonial Passivo
    'dfc': 'DFC',      # Demonstração do Fluxo de Caixa
    'dva': 'DVA',      # Demonstração do Valor Adicionado
    'dmpl': 'DMPL'     # Demonstração das Mutações do Patrimônio Líquido
}

# ============================================================================
# AMBIENTES.JSON — FONTE ÚNICA DE CONFIGURAÇÃO
# ============================================================================
# config_parametros.py lê ambientes.json em tempo de execução e deriva
# dele todos os nomes de catálogo, schema, tabela e volume.
# Nenhum outro notebook monta nome por conta própria.
# Eixos independentes: AMBIENTE (dev/test/prod) e CARGA (incremental/completa).


def _encontrar_ambientes_json() -> str:
    """Encontra ambientes.json relativo ao notebook em execução.

    O config é carregado via %run e roda em dois contextos:
    - Git folder: /Workspace/Users/.../projeto-cvm-dados-financeiros/05_apoio/
    - Bundle deploy: /Workspace/Users/.../.bundle/.../files/05_apoio/

    Em ambos, a estrutura de diretórios é a mesma: 05_apoio/ é subdiretório
    da raiz do projeto. A estratégia é obter o caminho do notebook chamador
    via dbutils.notebook.getContext() e subir até encontrar 05_apoio/ambientes.json.
    """
    import os

    # Estratégia 1: dbutils.notebook.getContext().notebookPath()
    # Disponível em notebook job tasks (não em Spark Connect puro).
    try:
        nb_path = str(dbutils.notebook.getContext().notebookPath())
        fs_path = f"/Workspace{nb_path}" if not nb_path.startswith("/Workspace") else nb_path
        nb_dir = os.path.dirname(fs_path)

        current = nb_dir
        for _ in range(6):
            for candidate in [
                os.path.join(current, "ambientes.json"),
                os.path.join(current, "05_apoio", "ambientes.json"),
            ]:
                if os.path.exists(candidate):
                    return candidate
            current = os.path.dirname(current)
    except (AttributeError, Exception):
        pass

    # Estratégia 2 (fallback): buscar em /Workspace/Users/*/projeto-cvm-dados-financeiros/
    users_dir = "/Workspace/Users"
    if os.path.exists(users_dir):
        for user_dir in os.listdir(users_dir):
            candidate = os.path.join(
                users_dir, user_dir,
                "projeto-cvm-dados-financeiros", "05_apoio", "ambientes.json",
            )
            if os.path.exists(candidate):
                return candidate
            # Também procurar em .bundle/<target>/files/
            bundle_base = os.path.join(
                users_dir, user_dir,
                ".bundle", "projeto-cvm-dados-financeiros",
            )
            if os.path.isdir(bundle_base):
                for target in os.listdir(bundle_base):
                    candidate = os.path.join(
                        bundle_base, target, "files", "05_apoio", "ambientes.json",
                    )
                    if os.path.exists(candidate):
                        return candidate

    raise FileNotFoundError(
        "ambientes.json não encontrado em 05_apoio/. "
        "Verifique se o arquivo existe no projeto e foi incluído no bundle deploy."
    )


def _resolver_parametro(nome: str, config_json: dict, secao: str) -> str:
    """Lê um parâmetro do JSON respeitando a precedência: widget > env > padrão."""
    precedencia = config_json[secao]["precedencia"]
    padrao = config_json[secao]["padrao"]

    for origem in precedencia:
        if origem.startswith("widget:"):
            widget_name = origem.split(":")[1]
            try:
                val = dbutils.widgets.get(widget_name).strip()
                if val:
                    return val
            except Exception:
                pass
        elif origem.startswith("variavel_ambiente:"):
            env_name = origem.split(":")[1]
            val = os.getenv(env_name, "").strip()
            if val:
                return val
        elif origem == "padrao":
            return padrao

    return padrao


def _carregar_config_ambiente() -> dict:
    """Carrega ambientes.json, resolve AMBIENTE e CARGA, deriva todas as variáveis.

    Returns:
        dict com catalogo, schemas, tabelas, volumes, ambiente, carga.

    Raises:
        ValueError: ambiente inválido ou declarado mas não instanciado.
    """
    json_path = _encontrar_ambientes_json()
    with open(json_path, "r", encoding="utf-8") as f:
        config_json = json.load(f)

    # Resolver eixos independentes
    ambiente = _resolver_parametro("AMBIENTE", config_json, "ambiente")
    carga = _resolver_parametro("CARGA", config_json, "carga")

    # Validar ambiente
    valores_aceitos = config_json["ambiente"]["valores_aceitos"]
    if ambiente not in valores_aceitos:
        raise ValueError(
            f"AMBIENTE='{ambiente}' inválido. Valores aceitos: {valores_aceitos}."
        )

    ambientes_def = config_json["ambientes"]
    if ambiente not in ambientes_def:
        raise ValueError(
            f"AMBIENTE='{ambiente}' não está declarado em ambientes.json."
        )

    amb_config = ambientes_def[ambiente]
    if not amb_config.get("existe", False):
        raise ValueError(
            f"AMBIENTE='{ambiente}' está declarado em ambientes.json mas não instanciado "
            f"(existe=false). {amb_config.get('_nota', '')}"
        )

    # Validar carga
    cargas_aceitas = config_json["carga"]["valores_aceitos"]
    if carga not in cargas_aceitas:
        raise ValueError(
            f"CARGA='{carga}' inválida. Valores aceitos: {cargas_aceitas}."
        )

    # Derivar nomes a partir do JSON
    catalogo = amb_config["catalogo"]
    prefixo = amb_config["prefixo"]
    camadas = config_json["camadas"]

    def _compor_schema(camada: str) -> str:
        return f"{prefixo}_{camadas[camada]}_{camada}"

    schemas = {camada: _compor_schema(camada) for camada in camadas}

    # Volume da Landing Zone (schema próprio, sem ordem nem camada)
    vol_config = config_json["volume"]
    volume_landing = vol_config["caminho"].format(
        catalogo=catalogo,
        schema=vol_config["schema"],
        nome=vol_config["nome"],
    )

    return {
        "catalogo": catalogo,
        "schemas": schemas,
        "volume_landing": volume_landing,
        "volume_schema": vol_config["schema"],
        "ambiente": ambiente,
        "carga": carga,
    }


# Carregar config e derivar variáveis globais
_config_amb = _carregar_config_ambiente()

CATALOG_NAME = _config_amb["catalogo"]
SCHEMA_BRONZE = _config_amb["schemas"]["bronze"]
SCHEMA_SILVER = _config_amb["schemas"]["silver"]
SCHEMA_GOLD = _config_amb["schemas"]["gold"]
SCHEMA_APOIO = _config_amb["schemas"]["apoio"]
TABELA_CONTROLE = f"{CATALOG_NAME}.{SCHEMA_APOIO}.controle_ingestao"
VOLUME_LANDING = _config_amb["volume_landing"]
VOLUME_LANDING_DFP = f"{VOLUME_LANDING}/dfp"
SCHEMA_VOLUME = _config_amb["volume_schema"]
AMBIENTE = _config_amb["ambiente"]
CARGA = _config_amb["carga"]

print(f"🏗️  AMBIENTE={AMBIENTE}, CARGA={CARGA}")
print(f"📋 Catalogo={CATALOG_NAME}")
print(f"📋 Bronze={SCHEMA_BRONZE}, Silver={SCHEMA_SILVER}, "
      f"Gold={SCHEMA_GOLD}, Apoio={SCHEMA_APOIO}")
print(f"📋 Volume Landing={VOLUME_LANDING}")

# Mapeamento fonte → tabela destino (para verificação de dados reais)
# Permite que a detecção verifique se dados existem fisicamente na tabela,
# não apenas confie no flag SUCCESS do controle
FONTE_TABELA_DESTINO = {
    'dre': f"{CATALOG_NAME}.{SCHEMA_BRONZE}.101_dre_dfp",
    'bpa': f"{CATALOG_NAME}.{SCHEMA_BRONZE}.102_bpa_dfp",
    'bpp': f"{CATALOG_NAME}.{SCHEMA_BRONZE}.103_bpp_dfp",
    'dre_silver': f"{CATALOG_NAME}.{SCHEMA_SILVER}.201_dre_dfp",
    'bpa_silver': f"{CATALOG_NAME}.{SCHEMA_SILVER}.202_bpa_dfp",
    'bpp_silver': f"{CATALOG_NAME}.{SCHEMA_SILVER}.203_bpp_dfp",
}

# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================

def get_url_arquivo_cvm(ano: int) -> str:
    """Constrói URL completa do arquivo ZIP DFP da CVM (contém todas demonstrações)."""
    return f"{CVM_BASE_URL}dfp_cia_aberta_{ano}.zip"


def get_anos_disponiveis_cvm() -> list:
    """Retorna anos disponíveis no projeto (2021 até ano corrente)."""
    ano_atual = datetime.now(FUSO_PROJETO).year
    return list(range(ANO_INICIAL_PROJETO, ano_atual + 1))


def get_novos_anos_para_processar(fonte: str, tabela_controle: str = TABELA_CONTROLE) -> list:
    """Identifica anos que precisam processamento.

    TRÊS ESTADOS DE DETECÇÃO (todos leitura local, sem HTTP):
    1. Nunca processado: ano sem registro SUCCESS no controle
    2. Fantasma: SUCCESS no controle mas sem dados na tabela destino
    3. Republicado: SUCCESS e dados existem, mas _metadata.json da landing
       tem last_modified_cvm diferente do registrado no controle

    O _metadata.json é atualizado pelo 003/004 quando a CVM republica o
    arquivo. Esta função compara esse valor contra o last_modified_cvm
    armazenado no controle_ingestao — leitura local, sem rede.

    Compatível com Spark Connect / Serverless Compute (não usa RDDs).
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.utils import AnalysisException
    spark = SparkSession.builder.getOrCreate()

    anos_disponiveis = get_anos_disponiveis_cvm()

    try:
        df_controle = spark.table(tabela_controle)
        anos_processados_rows = (
            df_controle
            .filter(f"fonte = '{fonte}' AND status = 'SUCCESS'")
            .select("ano", "last_modified_cvm")
            .distinct()
            .toPandas()
        )

        anos_processados = (
            sorted(anos_processados_rows['ano'].unique().tolist())
            if not anos_processados_rows.empty
            else []
        )

        # ESTADO 2: Fantasma — SUCCESS sem dados reais na tabela destino
        tabela_destino = FONTE_TABELA_DESTINO.get(fonte)
        anos_fantasma = []
        if tabela_destino:
            try:
                anos_com_dados_rows = (
                    spark.table(tabela_destino)
                    .selectExpr("year(DT_REFER) as ano")
                    .distinct()
                    .toPandas()
                )
                anos_com_dados = (
                    anos_com_dados_rows['ano'].tolist()
                    if not anos_com_dados_rows.empty
                    else []
                )
                anos_fantasma = [ano for ano in anos_processados if ano not in anos_com_dados]
                if anos_fantasma:
                    print(f"⚠️  [{fonte}] Anos fantasma (SUCCESS sem dados): {anos_fantasma}")
            except AnalysisException:
                pass  # Tabela ainda não existe (primeira execução)

        # ESTADO 3: Republicado — _metadata.json da landing tem last_modified_cvm
        # diferente do registrado no controle_ingestao (leitura local, sem rede)
        anos_republicados = []
        if anos_processados and not anos_processados_rows.empty:
            # Dict: ano -> lista de last_modified_cvm do controle
            controle_lm_por_ano = {}
            for _, row in anos_processados_rows.iterrows():
                ano_val = int(row['ano'])
                lm_val = row['last_modified_cvm']
                if ano_val not in controle_lm_por_ano:
                    controle_lm_por_ano[ano_val] = []
                if lm_val is not None:
                    controle_lm_por_ano[ano_val].append(lm_val)

            for ano in anos_processados:
                if ano in anos_fantasma:
                    continue  # Já marcado para reprocessamento

                metadata_path = f"{VOLUME_LANDING_DFP}/{ano}/_metadata.json"
                try:
                    with open(metadata_path, 'r') as f:
                        metadata = json.load(f)
                    metadata_lm_str = metadata.get('last_modified_cvm')
                    if not metadata_lm_str:
                        continue

                    # Normalizar para naive UTC datetime
                    metadata_lm = datetime.fromisoformat(metadata_lm_str)
                    if metadata_lm.tzinfo:
                        metadata_lm = metadata_lm.astimezone(timezone.utc).replace(tzinfo=None)

                    # Verificar se algum registro do controle tem este last_modified_cvm
                    tem_match = False
                    for controle_lm in controle_lm_por_ano.get(ano, []):
                        if controle_lm is None:
                            continue
                        # controle_lm pode ser pd.Timestamp — converter para datetime
                        if hasattr(controle_lm, 'to_pydatetime'):
                            controle_lm = controle_lm.to_pydatetime()
                        if controle_lm.tzinfo:
                            controle_lm = controle_lm.astimezone(timezone.utc).replace(tzinfo=None)
                        if metadata_lm == controle_lm:
                            tem_match = True
                            break

                    if not tem_match:
                        anos_republicados.append(ano)
                        print(f"🔄 [{fonte}] Ano {ano}: fonte republicada (Last-Modified mudou)")
                except Exception:
                    pass  # _metadata.json não existe ou não pode ser lido

            if anos_republicados:
                print(f"🔄 [{fonte}] Anos republicados pela CVM: {anos_republicados}")

        # Novos anos = nunca processados + fantasma + republicados
        novos_anos = [ano for ano in anos_disponiveis
                      if ano not in anos_processados
                      or ano in anos_fantasma
                      or ano in anos_republicados]
        return sorted(novos_anos)

    except Exception:
        return anos_disponiveis


def get_anos_para_processar_inteligente(fonte: str, tipo_demo: str,
                                       tabela_controle: str = TABELA_CONTROLE,
                                       force_anos: list = None) -> list:
    """Determina anos pendentes aplicando inteligência temporal.

    Detecta anos novos ou atualizados E aplica filtro de janela temporal,
    retornando apenas anos relevantes conforme JANELA_ANOS_RELEVANTE.

    Args:
        fonte: Identificador da fonte (ex: 'dre', 'bpa')
        tipo_demo: Tipo de demonstração (ex: 'dre', 'bpa')
        tabela_controle: Nome completo da tabela de controle
        force_anos: Lista de anos para forçar processamento (ignora toda lógica)

    Returns:
        Lista ordenada de anos pendentes dentro da janela temporal relevante
    """
    # Override manual: ignora toda lógica
    if force_anos is not None:
        return sorted(force_anos)

    # Detectar anos pendentes (leitura local, sem HTTP — republicação detectada via _metadata.json)
    novos_anos = get_novos_anos_para_processar(fonte, tabela_controle)
    todos_pendentes = set(novos_anos)

    # Aplicar janela temporal (política definida em JANELA_ANOS_RELEVANTE)
    ano_atual = datetime.now(FUSO_PROJETO).year
    ano_inicio_janela = ano_atual - JANELA_ANOS_RELEVANTE
    janela_relevante = set(range(ano_inicio_janela, ano_atual + 1))

    # Filtrar: manter apenas anos na janela
    anos_processar = sorted([ano for ano in todos_pendentes if ano in janela_relevante])
    return anos_processar


# ============================================================================
# GUARDRAILS - SCHEMA ESSENCIAL POR DEMONSTRAÇÃO
# ============================================================================

# Colunas obrigatórias que DEVEM existir na fonte CVM
# Qualquer coluna adicional é automaticamente descartada
# Estas definem o CONTRATO DE DADOS entre CVM e nosso pipeline

COLUNAS_ESSENCIAIS_DRE = [
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
    "ST_CONTA_FIXA"
]

COLUNAS_ESSENCIAIS_BPA = [
    "CNPJ_CIA",
    "DT_REFER",
    "VERSAO",
    "DENOM_CIA",
    "CD_CVM",
    "GRUPO_DFP",
    "MOEDA",
    "ESCALA_MOEDA",
    "ORDEM_EXERC",
    # "DT_INI_EXERC" removida: BPA (Balanço Patrimonial) não contém esta coluna na fonte CVM
    # BPA é snapshot de posição, não fluxo de período
    "DT_FIM_EXERC",
    "CD_CONTA",
    "DS_CONTA",
    "VL_CONTA",
    "ST_CONTA_FIXA"
]

COLUNAS_ESSENCIAIS_BPP = [
    "CNPJ_CIA",
    "DT_REFER",
    "VERSAO",
    "DENOM_CIA",
    "CD_CVM",
    "GRUPO_DFP",
    "MOEDA",
    "ESCALA_MOEDA",
    "ORDEM_EXERC",
    # "DT_INI_EXERC" removida: BPP (Balanço Patrimonial Passivo) não contém esta coluna na fonte CVM
    # BPP é snapshot de posição, não fluxo de período
    "DT_FIM_EXERC",
    "CD_CONTA",
    "DS_CONTA",
    "VL_CONTA",
    "ST_CONTA_FIXA"
]


def validar_e_projetar_schema(df, colunas_essenciais: list, fonte: str):
    """Valida que DataFrame contém todas as colunas essenciais e projeta apenas essas.

    Implementa guardrail de schema: garante robustez contra mudanças na fonte.
    - Se coluna essencial falta: FALHA (breaking change na fonte)
    - Se coluna extra existe: DESCARTA (mudança aditiva na fonte)

    Args:
        df: DataFrame Spark a validar
        colunas_essenciais: Lista de nomes de colunas obrigatórias
        fonte: Nome da fonte para mensagens de erro (ex: 'DRE Bronze 2023')

    Returns:
        DataFrame projetado apenas com colunas essenciais (na ordem da lista)

    Raises:
        ValueError: Se alguma coluna essencial estiver faltando
    """
    # Colunas presentes no DataFrame
    colunas_presentes = set(df.columns)
    colunas_requeridas = set(colunas_essenciais)

    # GUARDRAIL 1: Validar que todas as essenciais existem
    colunas_faltantes = colunas_requeridas - colunas_presentes
    if colunas_faltantes:
        raise ValueError(
            f"❌ ERRO DE SCHEMA - {fonte}\n"
            f"   Colunas obrigatórias faltando: {sorted(colunas_faltantes)}\n"
            f"   Colunas presentes na fonte: {sorted(colunas_presentes)}\n"
            f"   ⚠️  A fonte CVM pode ter mudado o schema. Verifique a documentação."
        )

    # GUARDRAIL 2: Identificar colunas extras (informativo, não bloqueia)
    colunas_extras = colunas_presentes - colunas_requeridas
    if colunas_extras:
        print(
            f"ℹ️  {fonte}: Colunas extras detectadas (serão descartadas): "
            f"{sorted(colunas_extras)}"
        )

    # Projetar apenas colunas essenciais na ordem definida
    return df.select(*colunas_essenciais)


# ============================================================================
# INICIALIZAÇÃO DE ANOS_PROCESSAR
# ============================================================================
# ANOS_PROCESSAR é definido via função inicializar_anos_processar()
# que deve ser chamada explicitamente pelos notebooks após carregar este arquivo

# Valor padrão (será sobrescrito pela função de inicialização)
ANOS_PROCESSAR = None

def inicializar_anos_processar(force_anos: list = None, silent: bool = False) -> list:
    """Inicializa ANOS_PROCESSAR com detecção inteligente.

    Deve ser chamada explicitamente pelos notebooks após carregar config_parametros.

    Args:
        force_anos: Lista de anos para forçar (ignora detecção)
        silent: Se True, não imprime mensagens de log

    Returns:
        Lista de anos a processar
    """
    global ANOS_PROCESSAR

    # PRIORIDADE 1: Widget ANOS_OVERRIDE (para jobs/testes)
    try:
        anos_override_widget = dbutils.widgets.get('ANOS_OVERRIDE').strip()
    except Exception:
        anos_override_widget = ''

    # PRIORIDADE 2: Variável de ambiente
    anos_override_env = os.getenv('ANOS_OVERRIDE', '').strip()

    # Consolidar ANOS_OVERRIDE (widget tem prioridade sobre env)
    anos_override = anos_override_widget or anos_override_env

    # CARGA=completa: força todos os anos disponíveis
    # Só aplica se não houver override explícito via force_anos ou ANOS_OVERRIDE
    if CARGA == "completa" and force_anos is None and not anos_override:
        force_anos = get_anos_disponiveis_cvm()

    if force_anos:
        # Override via parâmetro
        ANOS_PROCESSAR = sorted(force_anos)
        if not silent:
            print(f"🔧 ANOS_PROCESSAR (override manual): {ANOS_PROCESSAR}")
    elif anos_override:
        # Override via widget ou variável de ambiente
        ANOS_PROCESSAR = sorted([int(ano.strip()) for ano in anos_override.split(',')])
        if not silent:
            print(f"🔧 ANOS_PROCESSAR (variável ambiente): {ANOS_PROCESSAR}")
    else:
        # Detecção inteligente automática
        try:
            # Consolidar anos de múltiplas fontes DFP
            # Inclui fontes Bronze E Silver para que a detecção identifique
            # anos pendentes em qualquer camada, não apenas Bronze
            fontes_config = [
                ('dre', 'dre'),
                ('bpa', 'bpa'),
                ('bpp', 'bpp'),
                ('dre_silver', 'dre'),
                ('bpa_silver', 'bpa'),
                ('bpp_silver', 'bpp')
            ]

            anos_consolidados = set()
            for fonte, tipo_demo in fontes_config:
                anos_detectados = get_anos_para_processar_inteligente(
                    fonte=fonte,
                    tipo_demo=tipo_demo,
                    tabela_controle=TABELA_CONTROLE
                )
                anos_consolidados.update(anos_detectados)

            ANOS_PROCESSAR = sorted(list(anos_consolidados))

            if ANOS_PROCESSAR:
                if not silent:
                    print(f"🤖 ANOS_PROCESSAR (detecção inteligente): {ANOS_PROCESSAR}")
            else:
                # Fallback: se nenhum ano detectado, usar apenas ano atual
                ANOS_PROCESSAR = [datetime.now(FUSO_PROJETO).year]
                if not silent:
                    print(f"⚠️  ANOS_PROCESSAR (fallback - nenhum pendente): {ANOS_PROCESSAR}")

        except Exception as e:
            # Fallback em caso de erro (primeira execução, tabela não existe, etc)
            ANOS_PROCESSAR = [datetime.now(FUSO_PROJETO).year]
            if not silent:
                print(f"⚠️  ANOS_PROCESSAR (fallback - erro na detecção): {ANOS_PROCESSAR}")
                print(f"    Erro: {type(e).__name__}: {e}")

    return ANOS_PROCESSAR