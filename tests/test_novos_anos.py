"""Testes dos tres estados de deteccao em get_novos_anos_para_processar().

Estados cobertos:
1. Nunca processado -- ano no range do projeto mas sem registro SUCCESS no controle
2. Fantasma -- SUCCESS no controle mas sem dados reais na tabela bronze
3. Republicado -- CVM atualizou o arquivo (last_modified_cvm mudou no _metadata.json)

Todas as dependencias Spark/Delta sao mockadas -- roda no CI sem Databricks.
"""

import importlib.util
import json
import sys
from datetime import datetime
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Carregar config_parametros (mesmo padrao do test_config_parametros.py)
# ---------------------------------------------------------------------------

RAIZ = Path(__file__).resolve().parent.parent
CAMINHO_CONFIG = RAIZ / "05_apoio" / "config_parametros.py"

spec = importlib.util.spec_from_file_location("config_parametros", CAMINHO_CONFIG)
config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config)


# ---------------------------------------------------------------------------
# Helper: simula um Spark DataFrame encadeavel
# ---------------------------------------------------------------------------

class _FakeSparkTable:
    """Simula um Spark DataFrame cuja cadeia .filter().select().distinct().toPandas()
    retorna um DataFrame pandas pre-construido.

    Os metodos filter/select/selectExpr/distinct sao no-ops (retornam self)
    porque o DataFrame ja vem com as linhas e colunas corretas para o teste.
    """

    def __init__(self, pandas_df: pd.DataFrame):
        self._df = pandas_df

    def filter(self, *args, **kwargs):
        return self

    def select(self, *args, **kwargs):
        return self

    def selectExpr(self, *args, **kwargs):
        return self

    def distinct(self):
        return self

    def toPandas(self):
        return self._df


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def spark_mock():
    """Injeta mocks de pyspark em sys.modules e retorna o mock do SparkSession.

    Permite que os imports internos de get_novos_anos_para_processar
    (from pyspark.sql import SparkSession / AnalysisException) funcionem
    sem ter Spark instalado no CI.
    """
    _spark = MagicMock()
    with patch.dict(sys.modules, {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.utils": MagicMock(),
    }):
        sys.modules["pyspark.sql"].SparkSession = MagicMock()
        sys.modules["pyspark.sql"].SparkSession.builder.getOrCreate.return_value = _spark
        sys.modules["pyspark.sql.utils"].AnalysisException = type(
            "AnalysisException", (Exception,), {}
        )
        yield _spark


@pytest.fixture
def anos_fixos():
    """Mocka get_anos_disponiveis_cvm para retornar [2023, 2024] (deterministico)."""
    with patch.object(config, "get_anos_disponiveis_cvm", return_value=[2023, 2024]):
        yield


def _configurar_spark_table(spark_mock, controle_df, bronze_df):
    """Faz spark.table() devolver _FakeSparkTable conforme o nome da tabela."""
    spark_mock.table.side_effect = lambda name: (
        _FakeSparkTable(controle_df) if name == config.TABELA_CONTROLE
        else _FakeSparkTable(bronze_df)
    )


# ---------------------------------------------------------------------------
# Testes -- um por estado de deteccao
# ---------------------------------------------------------------------------

def test_estado_nunca_processado(spark_mock, anos_fixos):
    """Estado 1: ano no range do projeto mas sem registro SUCCESS no controle.

    Cenario: tabela de controle vazia (nenhuma fonte com SUCCESS).
    Esperado: todos os anos disponiveis aparecem no resultado.
    """
    controle_df = pd.DataFrame({
        "ano": pd.Series([], dtype="int64"),
        "last_modified_cvm": pd.Series([], dtype="object"),
    })
    bronze_df = pd.DataFrame({"ano": pd.Series([], dtype="int64")})

    _configurar_spark_table(spark_mock, controle_df, bronze_df)

    resultado = config.get_novos_anos_para_processar("dre")

    assert resultado == [2023, 2024], (
        f"Esperado [2023, 2024] (todos nunca processados), obtido {resultado}"
    )


def test_estado_fantasma(spark_mock, anos_fixos):
    """Estado 2: SUCCESS no controle mas sem dados reais na tabela bronze.

    Cenario: controle marca 2023 e 2024 como SUCCESS; bronze tem dados
    apenas para 2024. O ano 2023 e fantasma -- aparece no controle como
    sucesso mas nao tem dados fisicos na tabela bronze.
    Esperado: apenas 2023 (fantasma) aparece para reprocessamento.
    """
    lm = datetime(2024, 1, 1, 0, 0, 0)

    controle_df = pd.DataFrame({
        "ano": [2023, 2024],
        "last_modified_cvm": [lm, lm],
    })
    # Bronze tem dados apenas para 2024 -> 2023 e fantasma
    bronze_df = pd.DataFrame({"ano": [2024]})

    _configurar_spark_table(spark_mock, controle_df, bronze_df)

    # Mock open: _metadata.json nao existe no CI -> FileNotFoundError
    # e capturado pelo except Exception: pass da funcao
    with patch("builtins.open", side_effect=FileNotFoundError):
        resultado = config.get_novos_anos_para_processar("dre")

    assert 2023 in resultado, "Ano fantasma (2023) deve aparecer para reprocessamento"
    assert 2024 not in resultado, "Ano com dados (2024) nao deve aparecer"
    assert resultado == [2023], f"Esperado [2023], obtido {resultado}"


def test_estado_republicado(spark_mock, anos_fixos):
    """Estado 3: CVM atualizou o arquivo (last_modified_cvm mudou).

    Cenario: controle e bronze tem 2023 e 2024 como SUCCESS e com dados.
    O _metadata.json de 2023 tem last_modified_cvm diferente do registrado
    no controle (CVM republicou). O _metadata.json de 2024 coincide com
    o controle (nao houve republicacao).
    Esperado: apenas 2023 (republicado) aparece para reprocessamento.
    """
    lm_antigo = datetime(2024, 1, 1, 0, 0, 0)
    lm_novo = datetime(2024, 6, 1, 12, 0, 0)

    controle_df = pd.DataFrame({
        "ano": [2023, 2024],
        "last_modified_cvm": [lm_antigo, lm_antigo],
    })
    # Bronze tem dados para ambos (sem fantasmas)
    bronze_df = pd.DataFrame({"ano": [2023, 2024]})

    _configurar_spark_table(spark_mock, controle_df, bronze_df)

    # Mock open: 2023 tem metadata com last_modified_cvm novo (republicado);
    # 2024 tem metadata com last_modified_cvm igual ao controle (nao republicado)
    metadata_por_ano = {
        "2023": json.dumps({"last_modified_cvm": lm_novo.isoformat()}),
        "2024": json.dumps({"last_modified_cvm": lm_antigo.isoformat()}),
    }

    def _open_side_effect(path, *args, **kwargs):
        path_str = str(path)
        for ano_str, content in metadata_por_ano.items():
            if f"/{ano_str}/_metadata.json" in path_str:
                return StringIO(content)
        raise FileNotFoundError(path)

    with patch("builtins.open", side_effect=_open_side_effect):
        resultado = config.get_novos_anos_para_processar("dre")

    assert 2023 in resultado, "Ano republicado (2023) deve aparecer para reprocessamento"
    assert 2024 not in resultado, "Ano nao republicado (2024) nao deve aparecer"
    assert resultado == [2023], f"Esperado [2023], obtido {resultado}"