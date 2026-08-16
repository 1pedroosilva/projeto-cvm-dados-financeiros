# Databricks notebook source
# ============================================================================
# Configuração Centralizada - Pipeline CVM
# ============================================================================
# Propósito: Parâmetros globais do pipeline - anos, URLs, schemas UC
# Uso: Importado por todos notebooks via %run ./config_parametros
# Vantagem: Mudança de configuração em um único lugar
# ============================================================================

import urllib.request
from datetime import datetime

# ============================================================================
# PARÂMETROS DE PROCESSAMENTO
# ============================================================================

# Anos a processar - DEFINIDO DINAMICAMENTE ao final deste arquivo
# Executado automaticamente via detecção inteligente
# Pode ser sobrescrito via ANOS_OVERRIDE (veja final do arquivo)

# Ano inicial disponível na CVM (histórico completo desde 2010)
ANO_INICIAL_CVM = 2010

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
# UNITY CATALOG - SCHEMAS E VOLUMES
# ============================================================================

SCHEMA_BRONZE = "proj_cvm_01_bronze"
SCHEMA_SILVER = "proj_cvm_02_silver"
SCHEMA_GOLD = "proj_cvm_03_gold"
CATALOG_NAME = "workspace"
VOLUME_LANDING = f"/Volumes/{CATALOG_NAME}/proj_cvm/landing"
VOLUME_LANDING_DFP = f"{VOLUME_LANDING}/dfp"
SCHEMA_APOIO = "proj_cvm_05_apoio"
TABELA_CONTROLE = f"{SCHEMA_APOIO}.controle_ingestao"

# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================

def get_url_arquivo_cvm(ano: int) -> str:
    """Constrói URL completa do arquivo ZIP DFP da CVM (contém todas demonstrações)."""
    return f"{CVM_BASE_URL}dfp_cia_aberta_{ano}.zip"


def get_anos_disponiveis_cvm() -> list:
    """Retorna anos disponíveis na CVM (2010 até ano corrente)."""
    ano_atual = datetime.now().year
    return list(range(ANO_INICIAL_CVM, ano_atual + 1))


def verificar_arquivo_existe_cvm(url: str) -> tuple:
    """Verifica se arquivo existe na CVM e retorna metadados HTTP.

    Returns:
        Tupla (existe: bool, last_modified: datetime, tamanho_bytes: int)
    """
    try:
        req = urllib.request.Request(url, method='HEAD')
        with urllib.request.urlopen(req, timeout=10) as response:
            last_modified_str = response.headers.get('Last-Modified')
            last_modified_dt = datetime.strptime(
                last_modified_str, '%a, %d %b %Y %H:%M:%S %Z'
            ) if last_modified_str else None

            content_length = response.headers.get('Content-Length')
            tamanho_bytes = int(content_length) if content_length else None

            return (True, last_modified_dt, tamanho_bytes)
    except Exception:
        return (False, None, None)


def get_novos_anos_para_processar(fonte: str, tabela_controle: str = TABELA_CONTROLE) -> list:
    """Identifica anos novos que ainda não foram processados.

    Compatível com Spark Connect / Serverless Compute (não usa RDDs).
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    anos_disponiveis = get_anos_disponiveis_cvm()

    try:
        df_controle = spark.table(tabela_controle)
        # Usar toPandas() ao invés de rdd (compatível com Spark Connect)
        anos_processados_rows = (
            df_controle
            .filter(f"fonte = '{fonte}' AND status = 'SUCCESS'")
            .select("ano")
            .distinct()
            .toPandas()
        )

        anos_processados = anos_processados_rows['ano'].tolist() if not anos_processados_rows.empty else []
        novos_anos = [ano for ano in anos_disponiveis if ano not in anos_processados]
        return sorted(novos_anos)

    except Exception:
        return anos_disponiveis


def get_anos_com_atualizacao_cvm(fonte: str, tipo_demo: str, tabela_controle: str = TABELA_CONTROLE) -> list:
    """Detecta anos cujo arquivo foi atualizado na CVM (Last-Modified mais recente).

    Nota: last_modified_cvm na tabela de controle é TIMESTAMP, permitindo comparação
    direta com datetime retornado por verificar_arquivo_existe_cvm.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.utils import AnalysisException

    spark = SparkSession.builder.getOrCreate()

    anos_atualizados = []

    try:
        df_controle = spark.table(tabela_controle)
        registros = (
            df_controle
            .filter(f"fonte = '{fonte}' AND status = 'SUCCESS'")
            .select("ano", "last_modified_cvm")
            .collect()
        )

        for row in registros:
            ano = row['ano']
            last_modified_local = row['last_modified_cvm']  # Já vem como datetime do TIMESTAMP

            url = get_url_arquivo_cvm(ano)
            existe, last_modified_cvm, _ = verificar_arquivo_existe_cvm(url)

            if existe and last_modified_cvm:
                # Comparação entre datetime objects (segura após DDL corrigido para TIMESTAMP)
                if last_modified_local is None or last_modified_cvm > last_modified_local:
                    anos_atualizados.append(ano)

        return sorted(anos_atualizados)

    except AnalysisException as e:
        # Tabela de controle não existe ainda (primeira execução)
        print(f"ℹ️  Tabela de controle não encontrada - primeira execução: {e}")
        return []
    except Exception as e:
        # Erros genéricos (rede, parsing, etc)
        print(f"⚠️  Erro ao detectar atualizações: {type(e).__name__}: {e}")
        return []


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

    # Detectar todos os anos pendentes
    novos_anos = get_novos_anos_para_processar(fonte, tabela_controle)
    anos_atualizados = get_anos_com_atualizacao_cvm(fonte, tipo_demo, tabela_controle)
    todos_pendentes = set(novos_anos + anos_atualizados)

    # Aplicar janela temporal (política definida em JANELA_ANOS_RELEVANTE)
    ano_atual = datetime.now().year
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
        print(f"ℹ️  {fonte}: Colunas extras detectadas (serão descartadas): {sorted(colunas_extras)}")

    # Projetar apenas colunas essenciais na ordem definida
    return df.select(*colunas_essenciais)


# ============================================================================
# INICIALIZAÇÃO DE ANOS_PROCESSAR
# ============================================================================
# ANOS_PROCESSAR é definido via função inicializar_anos_processar()
# que deve ser chamada explicitamente pelos notebooks após carregar este arquivo

import os

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

    # Verificar override via variável de ambiente
    anos_override_env = os.getenv('ANOS_OVERRIDE', '').strip()

    if force_anos:
        # Override via parâmetro
        ANOS_PROCESSAR = sorted(force_anos)
        if not silent:
            print(f"🔧 ANOS_PROCESSAR (override manual): {ANOS_PROCESSAR}")
    elif anos_override_env:
        # Override via variável de ambiente
        ANOS_PROCESSAR = sorted([int(ano.strip()) for ano in anos_override_env.split(',')])
        if not silent:
            print(f"🔧 ANOS_PROCESSAR (variável ambiente): {ANOS_PROCESSAR}")
    else:
        # Detecção inteligente automática
        try:
            # Consolidar anos de múltiplas fontes DFP
            fontes_config = [
                ('dre', 'dre'),
                ('bpa', 'bpa')
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
                ANOS_PROCESSAR = [datetime.now().year]
                if not silent:
                    print(f"⚠️  ANOS_PROCESSAR (fallback - nenhum pendente): {ANOS_PROCESSAR}")

        except Exception as e:
            # Fallback em caso de erro (primeira execução, tabela não existe, etc)
            ANOS_PROCESSAR = [datetime.now().year]
            if not silent:
                print(f"⚠️  ANOS_PROCESSAR (fallback - erro na detecção): {ANOS_PROCESSAR}")
                print(f"    Erro: {type(e).__name__}: {e}")

    return ANOS_PROCESSAR