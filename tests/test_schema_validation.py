"""Testes da função validar_e_projetar_schema — guardrail de schema do pipeline.

Cobre os quatro cenários críticos:
1. DataFrame com exatamente as colunas obrigatórias → passa sem erro
2. DataFrame com colunas obrigatórias + colunas extras → passa e descarta as extras
3. DataFrame faltando uma coluna obrigatória → levanta ValueError
4. DataFrame vazio (zero linhas, schema correto) → passa sem erro

Os testes rodam no CI do GitHub Actions sem Databricks, usando PySpark
em modo local (SparkSession.builder.master("local")).
"""

import importlib.util
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# ---------------------------------------------------------------------------
# Carregar config_parametros (notebook exportado como .py no Git folder)
# Mesma técnica do test_config_parametros.py existente
# ---------------------------------------------------------------------------

RAIZ = Path(__file__).resolve().parent.parent
CAMINHO_CONFIG = RAIZ / "05_apoio" / "config_parametros.py"

spec = importlib.util.spec_from_file_location("config_parametros", CAMINHO_CONFIG)
config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config)

# ---------------------------------------------------------------------------
# SparkSession local para os testes (sem Databricks)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark():
    """SparkSession para testes.

    No CI do GitHub Actions (sem Databricks): cria sessão local com
    .master("local"). No Databricks, SPARK_REMOTE já está definido e
    o PySpark opera em modo Spark Connect — usamos a sessão existente.
    """
    import os

    if "SPARK_REMOTE" in os.environ:
        # Databricks: Spark Connect já configurado
        spark = SparkSession.builder.getOrCreate()
        yield spark
    else:
        # CI / local: Spark em modo local
        spark = (
            SparkSession.builder
            .master("local")
            .appName("test_schema_validation")
            .getOrCreate()
        )
        yield spark
        spark.stop()


# Colunas obrigatórias para os testes — usa as reais do DRE como exemplo
COLUNAS_TESTE = config.COLUNAS_ESSENCIAIS_DRE


# ---------------------------------------------------------------------------
# Cenário 1: DataFrame com exatamente as colunas obrigatórias
# ---------------------------------------------------------------------------

def test_schema_exato_passa_sem_erro(spark):
    """DataFrame com exatamente as colunas obrigatórias deve passar e
    retornar o DataFrame projetado, sem erro."""
    linha = tuple("valor" for _ in COLUNAS_TESTE)
    df = spark.createDataFrame([linha], COLUNAS_TESTE)

    resultado = config.validar_e_projetar_schema(df, COLUNAS_TESTE, "DRE Teste")

    assert resultado is not None
    assert resultado.columns == COLUNAS_TESTE
    assert resultado.count() == 1


# ---------------------------------------------------------------------------
# Cenário 2: DataFrame com colunas obrigatórias + colunas extras
# ---------------------------------------------------------------------------

def test_colunas_extras_sao_descartadas(spark):
    """Colunas extras devem ser descartadas — o resultado contém apenas as
    colunas obrigatórias, na ordem definida."""
    colunas_com_extras = COLUNAS_TESTE + ["COLUNA_NOVA_1", "COLUNA_NOVA_2"]
    linha = tuple("valor" for _ in colunas_com_extras)
    df = spark.createDataFrame([linha], colunas_com_extras)

    resultado = config.validar_e_projetar_schema(df, COLUNAS_TESTE, "DRE Teste")

    assert resultado.columns == COLUNAS_TESTE
    assert "COLUNA_NOVA_1" not in resultado.columns
    assert "COLUNA_NOVA_2" not in resultado.columns
    assert resultado.count() == 1


# ---------------------------------------------------------------------------
# Cenário 3: DataFrame faltando uma coluna obrigatória
# ---------------------------------------------------------------------------

def test_coluna_faltante_levanta_value_error(spark):
    """Falta de coluna obrigatória deve levantar ValueError com mensagem
    indicando quais colunas faltam."""
    colunas_incompletas = COLUNAS_TESTE[:-1]  # remove a última coluna
    linha = tuple("valor" for _ in colunas_incompletas)
    df = spark.createDataFrame([linha], colunas_incompletas)

    with pytest.raises(ValueError, match="ERRO DE SCHEMA"):
        config.validar_e_projetar_schema(df, COLUNAS_TESTE, "DRE Teste")


# ---------------------------------------------------------------------------
# Cenário 4: DataFrame vazio (zero linhas, schema correto)
# ---------------------------------------------------------------------------

def test_dataframe_vazio_com_schema_correto_passa(spark):
    """DataFrame com zero linhas mas schema correto deve passar sem erro.

    O guardrail valida a presença das colunas (metadata), não a presença
    de dados — um DataFrame vazio com schema correto é válido.
    """
    schema = StructType([StructField(c, StringType(), True) for c in COLUNAS_TESTE])
    df = spark.createDataFrame([], schema=schema)

    resultado = config.validar_e_projetar_schema(df, COLUNAS_TESTE, "DRE Teste")

    assert resultado.columns == COLUNAS_TESTE
    assert resultado.count() == 0
