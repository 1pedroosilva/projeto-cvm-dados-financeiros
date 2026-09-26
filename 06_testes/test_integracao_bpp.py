# Databricks notebook source
# DBTITLE 1,DOCUMENTAÇÃO
# MAGIC %md
# MAGIC # Teste de Integração E2E - Pipeline BPP (Bronze → Silver)
# MAGIC
# MAGIC **Propósito**: Validar execução ponta a ponta do pipeline BPP  
# MAGIC **Escopo**: Bronze ingestão + Silver transformação + Validações de qualidade  
# MAGIC **Dados**: Ano 2021 (volume mínimo, execução rápida)  
# MAGIC **Ambientes**: Schemas isolados via AMBIENTE (produção vs teste)

# COMMAND ----------

# DBTITLE 1,CARREGAR CONFIGURAÇÕES
# MAGIC %run ../05_apoio/config_parametros

# COMMAND ----------

# DBTITLE 1,IMPORTS
# IMPORTS
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import row_number, col

spark = SparkSession.builder.getOrCreate()

# Estrutura de resultados — cada validacao registra um dict
# Candidata a virar tabela de observabilidade de qualidade
resultados = []

# Confirmar que schemas de teste estao configurados
print(f"\u2139\ufe0f  Schemas configurados:")
print(f"   Bronze: {SCHEMA_BRONZE}")
print(f"   Silver: {SCHEMA_SILVER}")
print(f"   Apoio: {SCHEMA_APOIO}")
print(f"   Ambiente: '{AMBIENTE}'")

# COMMAND ----------

# DBTITLE 1,VALIDAÇÃO 1: Bronze tem dados
# ============================================================================
# VALIDAÇÃO 1: Tabela Bronze existe e tem dados
# ============================================================================
# Objetivo: Confirmar que ingestão Bronze funcionou
# Critério: COUNT(*) > 0 na tabela Bronze BPP
# ============================================================================

print("✅ Validando: Tabela Bronze existe e tem dados...")

try:
    tabela_bronze = f"{SCHEMA_BRONZE}.103_bpp_dfp"
    df_bronze = spark.table(tabela_bronze)
    count_bronze = df_bronze.count()

    if count_bronze > 0:
        resultados.append({"nome": "Validação 1: Bronze tem dados",
                          "status": "PASS", "valor_esperado": "> 0",
                          "valor_obtido": str(count_bronze),
                          "mensagem": f"Bronze tem {count_bronze:,} registros"})
        print(f"   ✅ Bronze tem {count_bronze:,} registros")
    else:
        resultados.append({"nome": "Validação 1: Bronze tem dados",
                          "status": "FAIL", "valor_esperado": "> 0",
                          "valor_obtido": "0",
                          "mensagem": f"Tabela Bronze vazia ({tabela_bronze})"})
        print(f"   ❌ Tabela Bronze vazia ({tabela_bronze})")

except Exception as e:
    import traceback
    resultados.append({"nome": "Validação 1: Bronze tem dados",
                      "status": "ERROR", "valor_esperado": "> 0",
                      "valor_obtido": "exceção",
                      "mensagem": traceback.format_exc()})
    print(f"   ❌ ERRO: {e}")

# COMMAND ----------

# DBTITLE 1,VALIDAÇÃO 2: Silver tem dados
# ============================================================================
# VALIDAÇÃO 2: Tabela Silver existe e tem dados
# ============================================================================
# Objetivo: Confirmar que transformação Silver funcionou
# Critério: COUNT(*) > 0 na tabela Silver BPP
# ============================================================================

print("✅ Validando: Tabela Silver existe e tem dados...")

try:
    tabela_silver = f"{SCHEMA_SILVER}.203_bpp_dfp"
    df_silver = spark.table(tabela_silver)
    count_silver = df_silver.count()

    if count_silver > 0:
        resultados.append({"nome": "Validação 2: Silver tem dados",
                          "status": "PASS", "valor_esperado": "> 0",
                          "valor_obtido": str(count_silver),
                          "mensagem": f"Silver tem {count_silver:,} registros"})
        print(f"   ✅ Silver tem {count_silver:,} registros")
    else:
        resultados.append({"nome": "Validação 2: Silver tem dados",
                          "status": "FAIL", "valor_esperado": "> 0",
                          "valor_obtido": "0",
                          "mensagem": f"Tabela Silver vazia ({tabela_silver})"})
        print(f"   ❌ Tabela Silver vazia ({tabela_silver})")

except Exception as e:
    import traceback
    resultados.append({"nome": "Validação 2: Silver tem dados",
                      "status": "ERROR", "valor_esperado": "> 0",
                      "valor_obtido": "exceção",
                      "mensagem": traceback.format_exc()})
    print(f"   ❌ ERRO: {e}")

# COMMAND ----------

# DBTITLE 1,VALIDAÇÃO 3: Bronze - duplicatas = Silver
# ============================================================================
# VALIDAÇÃO 3: Bronze - duplicatas = Silver (sem perda não explicada)
# ============================================================================
# Objetivo: Confirmar que a transformação não perdeu registros além da
#           deduplicação esperada pela Window Function
# Critério: count_silver == count_bronze - duplicatas_bronze
# Nota: A Silver aplica ROW_NUMBER=1 sobre (CNPJ_CIA, DT_REFER, CD_CONTA,
#       ORDEM_EXERC) ORDER BY _versao_ingestao DESC. Duplicatas exatas na
#       Bronze (mesma chave, mesmo _versao_ingestao) são colapsadas como
#       efeito colateral dessa Window Function.
# ============================================================================

print("✅ Validando: Bronze - duplicatas = Silver...")

try:
    # a) Calcular excedentes na Bronze pela mesma chave da Window Function do 203
    window_dedup = Window.partitionBy(
        "CNPJ_CIA", "DT_REFER", "CD_CONTA", "ORDEM_EXERC"
    ).orderBy(col("_versao_ingestao").desc())

    duplicatas_bronze = (
        df_bronze
        .withColumn("_row_num", row_number().over(window_dedup))
        .filter(col("_row_num") > 1)
        .count()
    )

    # b) Silver deve ter exatamente Bronze - duplicatas
    esperado_silver = count_bronze - duplicatas_bronze

    if count_silver == esperado_silver:
        resultados.append({"nome": "Validação 3: Bronze - duplicatas = Silver",
                          "status": "PASS",
                          "valor_esperado": str(esperado_silver),
                          "valor_obtido": str(count_silver),
                          "mensagem": f"Bronze: {count_bronze:,} | Duplicatas: {duplicatas_bronze:,} | Silver: {count_silver:,}"})
        print(f"   ✅ Bronze: {count_bronze:,} | Duplicatas: {duplicatas_bronze:,} | Silver: {count_silver:,}")
    else:
        msg = (f"Perda não explicada: Bronze={count_bronze:,}, Duplicatas={duplicatas_bronze:,}, "
               f"Esperado={esperado_silver:,}, Atual={count_silver:,}, "
               f"Diferença={esperado_silver - count_silver:,}")
        resultados.append({"nome": "Validação 3: Bronze - duplicatas = Silver",
                          "status": "FAIL",
                          "valor_esperado": str(esperado_silver),
                          "valor_obtido": str(count_silver),
                          "mensagem": msg})
        print(f"   ❌ {msg}")

except Exception as e:
    import traceback
    resultados.append({"nome": "Validação 3: Bronze - duplicatas = Silver",
                      "status": "ERROR",
                      "valor_esperado": "count_bronze - duplicatas_bronze",
                      "valor_obtido": "exceção",
                      "mensagem": traceback.format_exc()})
    print(f"   ❌ ERRO: {e}")

# COMMAND ----------

# DBTITLE 1,VALIDAÇÃO 4: PKs únicas (4 colunas)
# ============================================================================
# VALIDAÇÃO 4: PKs únicas (CNPJ_CIA + DT_REFER + CD_CONTA + ORDEM_EXERC)
# ============================================================================
# Objetivo: Confirmar que não há duplicação de chaves primárias
# Critério: Cada combinação (CNPJ_CIA, DT_REFER, CD_CONTA, ORDEM_EXERC) aparece 1x
# Nota: ORDEM_EXERC é parte da PK porque a BPP traz exercício corrente (ÚLTIMO)
#       e anterior (PENÚLTIMO) para a mesma conta e data de referência.
#       A Window Function do 203 particiona por essas 4 colunas.
# ============================================================================

print("✅ Validando: PKs únicas em Silver (4 colunas)...")

try:
    df_duplicatas = (
        df_silver
        .groupBy("cnpj_cia", "dt_refer", "cd_conta", "ordem_exerc")
        .count()
        .filter("count > 1")
    )

    count_duplicatas = df_duplicatas.count()

    if count_duplicatas == 0:
        resultados.append({"nome": "Validação 4: PKs únicas",
                          "status": "PASS", "valor_esperado": "0",
                          "valor_obtido": "0",
                          "mensagem": "Nenhuma PK duplicada (4 colunas)"})
        print(f"   ✅ Nenhuma PK duplicada (4 colunas)")
    else:
        resultados.append({"nome": "Validação 4: PKs únicas",
                          "status": "FAIL", "valor_esperado": "0",
                          "valor_obtido": str(count_duplicatas),
                          "mensagem": f"{count_duplicatas} combinações duplicadas (4 colunas)"})
        print(f"   ❌ {count_duplicatas} PKs duplicadas (4 colunas)")

except Exception as e:
    import traceback
    resultados.append({"nome": "Validação 4: PKs únicas",
                      "status": "ERROR", "valor_esperado": "0",
                      "valor_obtido": "exceção",
                      "mensagem": traceback.format_exc()})
    print(f"   ❌ ERRO: {e}")

# COMMAND ----------

# DBTITLE 1,VALIDAÇÃO 5: Metadados populados
# ============================================================================
# VALIDAÇÃO 5: Colunas de metadados populadas (Bronze)
# ============================================================================
# Objetivo: Confirmar que colunas de auditoria estão preenchidas
# Critério: _versao_ingestao, _ingest_ts, _last_modified_cvm não-nulos
# Nota: Estas colunas existem na Bronze (adicionadas na ingestão).
#       A Silver não as projeta — a verificação é sobre a Bronze.
# ============================================================================

print("✅ Validando: Colunas de metadados populadas em Bronze...")

try:
    colunas_meta = ["_versao_ingestao", "_ingest_ts", "_last_modified_cvm"]
    nulls = {}
    for c in colunas_meta:
        nulls[c] = df_bronze.filter(f"{c} IS NULL").count()

    total_nulls = sum(nulls.values())

    if total_nulls == 0:
        resultados.append({"nome": "Validação 5: Metadados populados",
                          "status": "PASS", "valor_esperado": "0 nulls",
                          "valor_obtido": "0 nulls",
                          "mensagem": f"Todas as 3 colunas de metadados estão populadas"})
        print(f"   ✅ Todas as colunas de metadados estão populadas")
    else:
        detalhe = ", ".join(f"{c}={v}" for c, v in nulls.items() if v > 0)
        resultados.append({"nome": "Validação 5: Metadados populados",
                          "status": "FAIL", "valor_esperado": "0 nulls",
                          "valor_obtido": f"{total_nulls} nulls",
                          "mensagem": f"Nulls: {detalhe}"})
        print(f"   ❌ {total_nulls} nulls: {detalhe}")

except Exception as e:
    import traceback
    resultados.append({"nome": "Validação 5: Metadados populados",
                      "status": "ERROR", "valor_esperado": "0 nulls",
                      "valor_obtido": "exceção",
                      "mensagem": traceback.format_exc()})
    print(f"   ❌ ERRO: {e}")

# COMMAND ----------

# DBTITLE 1,RESUMO DOS TESTES
# ============================================================================
# RESUMO DOS TESTES
# ============================================================================

print("\n" + "="*80)
print("\U0001f4cb RESULTADOS DAS VALIDA\u00c7\u00d5ES")
print("="*80)
print(f"{'#':<4} {'Valida\u00e7\u00e3o':<45} {'Status':<8} {'Esperado':<15} {'Obtido':<15}")
print("-"*87)
for i, r in enumerate(resultados, 1):
    icon = "\u2705" if r["status"] == "PASS" else "\u274c"
    print(f"{i:<4} {r['nome']:<45} {icon} {r['status']:<5} {r['valor_esperado']:<15} {r['valor_obtido']:<15}")

print()
falhas = [r for r in resultados if r["status"] in ("FAIL", "ERROR")]

if not falhas:
    print("="*80)
    print("\u2705 TODOS OS TESTES PASSARAM")
    print("="*80)
    print(f"\n\U0001f4c8 Estat\u00edsticas:")
    print(f"   \u2022 Registros Bronze: {count_bronze:,}")
    print(f"   \u2022 Duplicatas Bronze (descartadas pela Silver): {duplicatas_bronze:,}")
    print(f"   \u2022 Registros Silver: {count_silver:,}")
    print(f"   \u2022 Schemas testados: Bronze, Silver")
    print(f"   \u2022 Ambiente: '{AMBIENTE}'")
    print(f"\n\u2713 Pipeline BPP funcionando corretamente (Bronze \u2192 Silver)\n")
else:
    print("="*80)
    print(f"\u274c {len(falhas)} VALIDA\u00c7\u00c3O(\u00d5ES) FALHARAM")
    print("="*80)
    for r in falhas:
        print(f"\n   {r['nome']}")
        print(f"   Esperado: {r['valor_esperado']}")
        print(f"   Obtido:   {r['valor_obtido']}")
        print(f"   Mensagem: {r['mensagem'][:300]}")

    resumo = "; ".join(
        f"{r['nome']} (esperado={r['valor_esperado']}, obtido={r['valor_obtido']})"
        for r in falhas
    )
    raise AssertionError(f"{len(falhas)} valida\u00e7\u00e3o(\u00f5es) falharam: {resumo}")

# COMMAND ----------

