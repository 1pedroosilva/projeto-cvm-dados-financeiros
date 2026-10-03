"""Testes das funções de transformação da camada Silver.

Cobre as três lógicas críticas extraídas em 05_apoio/transformacoes_silver:
1. Normalização monetária — valor MIL multiplicado por 1000, valor não-MIL inalterado
2. Derivação hierárquica — NIVEL_CONTA, CD_CONTA_PAI, CD_CONTA_RAIZ, TIPO_CONTA
3. Deduplicação por versão — mantém apenas o registro de versão mais alta

Os testes rodam no CI do GitHub Actions sem Databricks, usando PySpark
em modo local (SparkSession.builder.master("local")).
"""

import importlib.util
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# ---------------------------------------------------------------------------
# Carregar transformacoes_silver (notebook exportado como .py no Git folder)
# ---------------------------------------------------------------------------

RAIZ = Path(__file__).resolve().parent.parent
CAMINHO_MOD = RAIZ / "05_apoio" / "transformacoes_silver.py"

spec = importlib.util.spec_from_file_location("transformacoes_silver", CAMINHO_MOD)
transformacoes = importlib.util.module_from_spec(spec)
spec.loader.exec_module(transformacoes)

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
        spark = SparkSession.builder.getOrCreate()
        yield spark
    else:
        spark = (
            SparkSession.builder
            .master("local")
            .appName("test_transformacoes_silver")
            .getOrCreate()
        )
        yield spark
        spark.stop()


# ===========================================================================
# 1. Normalização de escala monetária
# ===========================================================================


def test_normalizacao_mil_multiplica_por_1000(spark):
    """Valor com ESCALA_MOEDA='MIL' deve ser multiplicado por 1000."""
    df = spark.createDataFrame(
        [("MIL", 1500.0)],
        StructType([
            StructField("ESCALA_MOEDA", StringType(), True),
            StructField("VL_CONTA", DoubleType(), True),
        ]),
    )

    resultado = transformacoes.normalizar_escala_monetaria(df)
    linha = resultado.collect()[0]

    assert linha["VL_CONTA"] == 1500000.0


def test_normalizacao_unidade_nao_altera(spark):
    """Valor com ESCALA_MOEDA='UNIDADE' deve permanecer inalterado."""
    df = spark.createDataFrame(
        [("UNIDADE", 25000.50)],
        StructType([
            StructField("ESCALA_MOEDA", StringType(), True),
            StructField("VL_CONTA", DoubleType(), True),
        ]),
    )

    resultado = transformacoes.normalizar_escala_monetaria(df)
    linha = resultado.collect()[0]

    assert linha["VL_CONTA"] == 25000.50


def test_normalizacao_mista_mil_e_unidade(spark):
    """DataFrame com registros MIL e UNIDADE mistos — cada um tratado separadamente."""
    df = spark.createDataFrame(
        [("MIL", 10.0), ("UNIDADE", 10.0)],
        StructType([
            StructField("ESCALA_MOEDA", StringType(), True),
            StructField("VL_CONTA", DoubleType(), True),
        ]),
    )

    resultado = transformacoes.normalizar_escala_monetaria(df).orderBy("ESCALA_MOEDA")
    linhas = resultado.collect()

    mil = [r for r in linhas if r["ESCALA_MOEDA"] == "MIL"][0]
    unidade = [r for r in linhas if r["ESCALA_MOEDA"] == "UNIDADE"][0]

    assert mil["VL_CONTA"] == 10000.0
    assert unidade["VL_CONTA"] == 10.0


# ===========================================================================
# 2. Derivação hierárquica a partir de CD_CONTA
# ===========================================================================


def test_hierarquia_3_niveis(spark):
    """Conta de 3 níveis (ex: '3.01.02') deve derivar corretamente todas as colunas."""
    df = spark.createDataFrame(
        [("3.01.02",)],
        StructType([StructField("CD_CONTA", StringType(), True)]),
    )

    resultado = transformacoes.derivar_hierarquia_conta(df)
    linha = resultado.collect()[0]

    assert linha["NIVEL_CONTA"] == 3
    assert linha["CD_CONTA_PAI"] == "3.01"
    assert linha["CD_CONTA_RAIZ"] == "3"
    assert linha["TIPO_CONTA"] == "ANALITICA"


def test_hierarquia_1_nivel(spark):
    """Conta raiz de 1 nível (ex: '3') deve ter pai NULL e tipo TOTALIZADORA."""
    df = spark.createDataFrame(
        [("3",)],
        StructType([StructField("CD_CONTA", StringType(), True)]),
    )

    resultado = transformacoes.derivar_hierarquia_conta(df)
    linha = resultado.collect()[0]

    assert linha["NIVEL_CONTA"] == 1
    assert linha["CD_CONTA_PAI"] is None
    assert linha["CD_CONTA_RAIZ"] == "3"
    assert linha["TIPO_CONTA"] == "TOTALIZADORA"


def test_hierarquia_2_niveis(spark):
    """Conta de 2 níveis (ex: '3.01') deve ter tipo TOTALIZADORA (nível <= 2)."""
    df = spark.createDataFrame(
        [("3.01",)],
        StructType([StructField("CD_CONTA", StringType(), True)]),
    )

    resultado = transformacoes.derivar_hierarquia_conta(df)
    linha = resultado.collect()[0]

    assert linha["NIVEL_CONTA"] == 2
    assert linha["CD_CONTA_PAI"] == "3"
    assert linha["CD_CONTA_RAIZ"] == "3"
    assert linha["TIPO_CONTA"] == "TOTALIZADORA"


def test_hierarquia_5_niveis(spark):
    """Conta de 5 níveis (ex: '2.01.01.01.01') deve ter tipo ANALITICA."""
    df = spark.createDataFrame(
        [("2.01.01.01.01",)],
        StructType([StructField("CD_CONTA", StringType(), True)]),
    )

    resultado = transformacoes.derivar_hierarquia_conta(df)
    linha = resultado.collect()[0]

    assert linha["NIVEL_CONTA"] == 5
    assert linha["CD_CONTA_PAI"] == "2.01.01.01"
    assert linha["CD_CONTA_RAIZ"] == "2"
    assert linha["TIPO_CONTA"] == "ANALITICA"


# ===========================================================================
# 3. Deduplicação por versão
# ===========================================================================


def test_deduplicacao_mantem_versao_mais_alta(spark):
    """Quando há múltiplas versões da mesma chave, mantém apenas a mais alta."""
    schema = StructType([
        StructField("CNPJ_CIA", StringType(), True),
        StructField("CD_CONTA", StringType(), True),
        StructField("_versao_ingestao", IntegerType(), True),
        StructField("VL_CONTA", DoubleType(), True),
    ])

    df = spark.createDataFrame(
        [
            ("123456", "3.01", 1, 100.0),
            ("123456", "3.01", 2, 200.0),
            ("123456", "3.01", 3, 300.0),
        ],
        schema,
    )

    resultado = transformacoes.deduplicar_por_versao(
        df,
        colunas_particao=["CNPJ_CIA", "CD_CONTA"],
        coluna_versao="_versao_ingestao",
    )

    assert resultado.count() == 1
    linha = resultado.collect()[0]
    assert linha["_versao_ingestao"] == 3
    assert linha["VL_CONTA"] == 300.0


def test_deduplicacao_chaves_diferentes_mantidas(spark):
    """Registros com chaves de negócio diferentes não são eliminados."""
    schema = StructType([
        StructField("CNPJ_CIA", StringType(), True),
        StructField("CD_CONTA", StringType(), True),
        StructField("_versao_ingestao", IntegerType(), True),
    ])

    df = spark.createDataFrame(
        [
            ("123456", "3.01", 1),
            ("123456", "3.01", 2),
            ("789012", "3.02", 1),
        ],
        schema,
    )

    resultado = transformacoes.deduplicar_por_versao(
        df,
        colunas_particao=["CNPJ_CIA", "CD_CONTA"],
        coluna_versao="_versao_ingestao",
    )

    assert resultado.count() == 2
    versoes = {r["_versao_ingestao"] for r in resultado.collect()}
    assert versoes == {2, 1}


def test_deduplicacao_sem_duplicata_mantem_tudo(spark):
    """Quando não há duplicatas, todos os registros são mantidos."""
    schema = StructType([
        StructField("CNPJ_CIA", StringType(), True),
        StructField("CD_CONTA", StringType(), True),
        StructField("_versao_ingestao", IntegerType(), True),
    ])

    df = spark.createDataFrame(
        [
            ("111", "1.01", 5),
            ("222", "1.02", 3),
            ("333", "1.03", 1),
        ],
        schema,
    )

    resultado = transformacoes.deduplicar_por_versao(
        df,
        colunas_particao=["CNPJ_CIA", "CD_CONTA"],
        coluna_versao="_versao_ingestao",
    )

    assert resultado.count() == 3