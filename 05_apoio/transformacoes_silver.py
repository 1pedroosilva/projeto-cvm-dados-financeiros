# Databricks notebook source
# -*- coding: utf-8 -*-
"""Funções puras de transformação da camada Silver.

Contém as três lógicas críticas extraídas dos notebooks 201/202/203:
1. Normalização monetária (MIL → reais)
2. Derivação hierárquica a partir de CD_CONTA
3. Deduplicação por versão (ROW_NUMBER por _versao_ingestao DESC)
"""

from pyspark.sql import Window
from pyspark.sql.functions import col, when, split, size, regexp_extract, lit, row_number


# ---------------------------------------------------------------------------
# 1. Normalização de escala monetária
# ---------------------------------------------------------------------------

def normalizar_escala_monetaria(df):
    """Multiplica VL_CONTA por 1000 quando ESCALA_MOEDA = 'MIL'.

    Preserva ESCALA_MOEDA original para rastreabilidade.
    Registros com escala 'UNIDADE' (ou qualquer outro valor) permanecem inalterados.
    """
    return df.withColumn(
        "VL_CONTA",
        when(col("ESCALA_MOEDA") == "MIL", col("VL_CONTA") * 1000)
        .otherwise(col("VL_CONTA")),
    )


# ---------------------------------------------------------------------------
# 2. Derivação hierárquica a partir de CD_CONTA
# ---------------------------------------------------------------------------

def derivar_hierarquia_conta(df):
    """Deriva colunas hierárquicas a partir de CD_CONTA.

    Adiciona:
    - NIVEL_CONTA: profundidade na hierarquia (nº de segmentos separados por ponto)
    - CD_CONTA_PAI: código da conta pai (remove o último segmento)
    - CD_CONTA_RAIZ: primeiro segmento da conta
    - TIPO_CONTA: 'TOTALIZADORA' (nível <= 2) ou 'ANALITICA' (nível > 2)
    """
    return (
        df
        .withColumn("NIVEL_CONTA", size(split(col("CD_CONTA"), "[.]")))
        .withColumn(
            "CD_CONTA_PAI",
            when(
                size(split(col("CD_CONTA"), "[.]")) > 1,
                regexp_extract(col("CD_CONTA"), r"^(.+)\.[^.]+$", 1),
            ).otherwise(lit(None).cast("string")),
        )
        .withColumn("CD_CONTA_RAIZ", split(col("CD_CONTA"), "[.]")[0])
        .withColumn(
            "TIPO_CONTA",
            when(size(split(col("CD_CONTA"), "[.]")) <= 2, lit("TOTALIZADORA")).otherwise(lit("ANALITICA")),
        )
    )


# ---------------------------------------------------------------------------
# 3. Deduplicação por versão
# ---------------------------------------------------------------------------

def deduplicar_por_versao(df, colunas_particao, coluna_versao="_versao_ingestao"):
    """Mantém apenas o registro de versão mais alta por chave de negócio.

    Usa ROW_NUMBER() ordenado por coluna_versao DESC dentro de cada partição
    definida por colunas_particao, e filtra apenas a primeira linha (rn == 1).

    Args:
        df: DataFrame de entrada (ex: bronze filtrado por ano).
        colunas_particao: lista de colunas que formam a chave de negócio
            (ex: ["CNPJ_CIA", "DT_REFER", "CD_CONTA", "ORDEM_EXERC"]).
        coluna_versao: nome da coluna de versionamento (default: _versao_ingestao).

    Returns:
        DataFrame com apenas o registro mais recente por partição.
    """
    window_spec = Window.partitionBy(*colunas_particao).orderBy(col(coluna_versao).desc())
    return (
        df.withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )