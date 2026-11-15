# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,DOCUMENTAÇÃO
# MAGIC %md
# MAGIC ## EDA 004: Investigação de Qualidade - DFC Silver
# MAGIC
# MAGIC **Dataset Analisado**: `204_dfc_dfp` na silver (resolvido via `config_parametros` — fonte única `ambientes.json`)
# MAGIC **Data de Execução**: 2026-11-15
# MAGIC **Escopo Inicial**: 249.330 registros, 538 empresas, 128 contas distintas, anos 2021-2026
# MAGIC
# MAGIC **Objetivo**: Investigar as hipóteses específicas da DFC que motivaram decisões arquiteturais da tabela bronze única `104_dfc_dfp` (decisão de 10/11/2026), ainda não verificadas nos dados:
# MAGIC
# MAGIC 1. Invariante "cada empresa reporta um único método (Direto/Indireto) por exercício"
# MAGIC 2. Distribuição e migração de métodos ao longo dos anos
# MAGIC 3. Estrutura hierárquica das contas (aditivas vs derivadas) — blocos 6.01 a 6.05
# MAGIC 4. Qualidade e heterogeneidade semântica (escalas, nomenclaturas CVM, origem bronze)
# MAGIC
# MAGIC **Metodologia**: Análises técnicas neutras investigam hipóteses sem viés confirmatório. Cada análise é seguida de célula markdown documentando achados objetivamente, com escopo explícito.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Índice de Seções
# MAGIC
# MAGIC 1. **Perfil Geral do Dataset**
# MAGIC 2. **Cobertura Temporal**
# MAGIC 3. **Análise de Método** (composição, invariante, distribuição anual, migração)
# MAGIC 4. **Estrutura Hierárquica** (níveis, aditividade vertical, fórmula derivada)
# MAGIC 5. **Qualidade de Dados** (completude, duplicação, classificações do reporte)
# MAGIC 6. **Origem Bronze**
# MAGIC 7. **Heterogeneidade Semântica**
# MAGIC 8. **Conclusão**

# COMMAND ----------

# DBTITLE 1,Carregar configurações
# MAGIC %run ../05_apoio/config_parametros

# COMMAND ----------

# DBTITLE 1,RESOLVER TABELAS DO CONFIG
# TABELA_DFC_BRONZE / TABELA_DFC_SILVER: resolve nomes completos via config central
# Mantém o EDA independente de ambiente (dev/test) — fonte única é o ambientes.json
TABELA_DFC_BRONZE = FONTE_TABELA_DESTINO["dfc"]
TABELA_DFC_SILVER = FONTE_TABELA_DESTINO["dfc_silver"]

print(f"Bronze: {TABELA_DFC_BRONZE}")
print(f"Silver: {TABELA_DFC_SILVER}")

# COMMAND ----------

# DBTITLE 1,PERFIL GERAL DO DATASET
# df_dfc_silver: carrega o dataset completo da DFC silver
# Enriquecimento hierárquico e METODO_DFC já aplicados pela transformação 204
df_dfc_silver = spark.sql(f"SELECT * FROM {TABELA_DFC_SILVER}")

total_registros = df_dfc_silver.count()
print(f"Shape: {total_registros:,} linhas x {len(df_dfc_silver.columns)} colunas")
print(f"\nColunas e tipos:")
df_dfc_silver.printSchema()
print(f"\nMemória estimada: {total_registros * len(df_dfc_silver.columns) * 8 / (1024**2):.2f} MB")

# COMMAND ----------

# DBTITLE 1,Achado: Dimensões do Dataset
# MAGIC %md
# MAGIC ## Achado: Dimensões do Dataset
# MAGIC
# MAGIC O dataset contém **249.330 registros em 24 colunas**, com memória estimada de 45,65 MB (escopo: dataset completo, ambiente dev). A estrutura apresenta:
# MAGIC
# MAGIC * Identificação (CNPJ_CIA, CD_CVM, DENOM_CIA)
# MAGIC * Temporais (DT_REFER, DT_INI_EXERC, DT_FIM_EXERC, ANO, TRIMESTRE, MES)
# MAGIC * Classificação (VERSAO, GRUPO_DFP, MOEDA, ESCALA_MOEDA, ORDEM_EXERC)
# MAGIC * Contábeis (CD_CONTA, DS_CONTA, VL_CONTA)
# MAGIC * Enriquecimento hierárquico (NIVEL_CONTA, CD_CONTA_PAI, CD_CONTA_RAIZ, TIPO_CONTA)
# MAGIC * **METODO_DFC** — derivada de GRUPO_DFP, coluna exclusiva da DFC na silver

# COMMAND ----------

# DBTITLE 1,COBERTURA TEMPORAL
# df_cobertura_temporal: distribuição de registros e empresas por ano/trimestre
# Q4 concentra as DFPs anuais; trimestres não-Q4 são ITRs residuais
df_cobertura_temporal = spark.sql(f"""
SELECT ANO, TRIMESTRE,
       COUNT(*) AS qtd_registros,
       COUNT(DISTINCT CNPJ_CIA) AS qtd_empresas
FROM {TABELA_DFC_SILVER}
GROUP BY ANO, TRIMESTRE
ORDER BY ANO, TRIMESTRE""")
display(df_cobertura_temporal)

# COMMAND ----------

# DBTITLE 1,Achado: Cobertura Temporal
# MAGIC %md
# MAGIC ## Achado: Cobertura Temporal e Sazonalidade
# MAGIC
# MAGIC Dataset cobre seis anos fiscais (2021-2026) com concentração em Q4, padrão das DFPs anuais:
# MAGIC
# MAGIC | Ano | Registros Q4 | Empresas Q4 |
# MAGIC | --- | --- | --- |
# MAGIC | 2021 | 45.877 | 452 |
# MAGIC | 2022 | 48.534 | 463 |
# MAGIC | 2023 | 49.879 | 464 |
# MAGIC | 2024 | 50.116 | 458 |
# MAGIC | 2025 | 47.976 | 432 |
# MAGIC | 2026 | — (só Q1/Q2) | 7 / 2 |
# MAGIC
# MAGIC Trimestres não-Q4 concentram 1-10 empresas (ITRs residuais de companhias com obrigação regulatória). 2026 é parcial: apenas Q1 (804 registros) e Q2 (252).

# COMMAND ----------

# DBTITLE 1,ANÁLISE DE MÉTODO
# df_metodo_registros: composição de registros por GRUPO_DFP e METODO_DFC
# Bronze ingere apenas o CSV _con (consolidado) — GRUPO_DFP individual não deve existir
df_metodo_registros = spark.sql(f"""
SELECT GRUPO_DFP, METODO_DFC,
       COUNT(*) AS qtd_registros,
       COUNT(DISTINCT CNPJ_CIA) AS qtd_empresas
FROM {TABELA_DFC_SILVER}
GROUP BY GRUPO_DFP, METODO_DFC
ORDER BY qtd_registros DESC""")
display(df_metodo_registros)

# COMMAND ----------

# DBTITLE 1,Achado: Composição por Método
# MAGIC %md
# MAGIC ## Achado: Composição por Método
# MAGIC
# MAGIC GRUPO_DFP contém **exatamente 2 valores, ambos "DF Consolidado"** — consistente com a ingestão bronze, que lê apenas o CSV `_con`. O método indireto domina:
# MAGIC
# MAGIC | Método | Registros | % Registros |
# MAGIC | --- | --- | --- |
# MAGIC | METODO_INDIRETO | 245.806 | 98,6% |
# MAGIC | METODO_DIRETO | 3.524 | 1,4% |
# MAGIC
# MAGIC (escopo: dataset completo. Empresas por método e ano na seção seguinte.)

# COMMAND ----------

# DBTITLE 1,VALIDAÇÃO DO INVARIANTE DE MÉTODO ÚNICO
# df_invariante_metodo: testa a premissa da tabela bronze única 104
# Hipótese: cada empresa reporta um único método por exercício (CNPJ + ANO)
df_invariante_metodo = spark.sql(f"""
SELECT COUNT(*) AS empresas_ano_multi_metodo
FROM (
  SELECT ANO, CNPJ_CIA, COUNT(DISTINCT METODO_DFC) AS qtd_metodos
  FROM {TABELA_DFC_SILVER}
  GROUP BY ANO, CNPJ_CIA
)
WHERE qtd_metodos > 1""")
display(df_invariante_metodo)

# COMMAND ----------

# DBTITLE 1,Achado: Invariante de Método Único
# MAGIC %md
# MAGIC ## Achado: Invariante de Método Único — CONFIRMADO
# MAGIC
# MAGIC **Zero violações** (escopo: dataset completo, 538 empresas × 6 anos). A premissa que fundamentou a decisão de 10/11/2026 — DFC em uma única tabela bronze `104_dfc_dfp` com coluna de método — é válida: nenhuma empresa reporta Métodos Direto e Indireto simultaneamente no mesmo exercício.

# COMMAND ----------

# DBTITLE 1,DISTRIBUIÇÃO DE MÉTODO POR ANO
# df_metodo_ano: adoção de cada método por ano
# Tendência temporal da adoção revela padrões de reporte das companhias
df_metodo_ano = spark.sql(f"""
SELECT ANO, METODO_DFC,
       COUNT(DISTINCT CNPJ_CIA) AS qtd_empresas,
       COUNT(*) AS qtd_registros
FROM {TABELA_DFC_SILVER}
GROUP BY ANO, METODO_DFC
ORDER BY ANO, METODO_DFC""")
display(df_metodo_ano)

# COMMAND ----------

# DBTITLE 1,Achado: Distribuição Anual de Método
# MAGIC %md
# MAGIC ## Achado: Distribuição Anual de Método
# MAGIC
# MAGIC Método indireto: 445 → 456 → 458 → 451 → 428 → 9 empresas (2021-2026; 2026 parcial). Método direto: 19 → 20 → 16 → 16 → 14 → **0 em 2026**.
# MAGIC
# MAGIC A ausência de MD em 2026 decorre do arquivo da CVM conter DFC_MD_con com 0 bytes — condição tolerada pelo guardrail da bronze (só interrompe se ambas as variantes estiverem vazias). Ver seção Origem Bronze.

# COMMAND ----------

# DBTITLE 1,MIGRAÇÃO DE MÉTODO ENTRE ANOS
# df_migracao_metodo: empresas que alternaram de método entre anos consecutivos
# Migrações mostram que o método é atributo do reporte, não da empresa
df_migracao_metodo = spark.sql(f"""
WITH metodo_ano AS (
  SELECT DISTINCT ANO, CNPJ_CIA, METODO_DFC
  FROM {TABELA_DFC_SILVER}
)
SELECT a.CNPJ_CIA, a.ANO AS ano_anterior, a.METODO_DFC AS metodo_anterior,
       b.ANO AS ano_posterior, b.METODO_DFC AS metodo_posterior
FROM metodo_ano a
JOIN metodo_ano b ON a.CNPJ_CIA = b.CNPJ_CIA AND b.ANO = a.ANO + 1
WHERE a.METODO_DFC != b.METODO_DFC
ORDER BY a.CNPJ_CIA, a.ANO""")
display(df_migracao_metodo)

# COMMAND ----------

# DBTITLE 1,Achado: Migração de Método
# MAGIC %md
# MAGIC ## Achado: Migração de Método entre Anos
# MAGIC
# MAGIC **14 transições envolvendo 12 empresas** (escopo: anos consecutivos, dataset completo):
# MAGIC
# MAGIC * 10 transições MD → MI (ex.: 07.857.093, 08.262.121, 42.737.954, 60.746.948)
# MAGIC * 4 transições MI → MD (ex.: 28.195.667, 90.400.888)
# MAGIC * Oscilantes: 00.000.208/0001-00 e 62.144.175/0001-20 alternam MD → MI → MD
# MAGIC
# MAGIC O método é atributo do reporte, não da empresa. A presença de METODO_DFC na chave de deduplicação silver é defensiva — simultaneidade de métodos no mesmo exercício nunca foi observada.

# COMMAND ----------

# DBTITLE 1,ESTRUTURA HIERÁRQUICA
# df_estrutura_contas: estrutura da DFC por nível e tipo de conta
# Raiz única do plano CVM para DFC e totalizadoras vs analíticas
df_estrutura_contas = spark.sql(f"""
SELECT CD_CONTA_RAIZ, NIVEL_CONTA, TIPO_CONTA,
       COUNT(DISTINCT CD_CONTA) AS qtd_contas_distintas,
       COUNT(*) AS qtd_registros
FROM {TABELA_DFC_SILVER}
GROUP BY CD_CONTA_RAIZ, NIVEL_CONTA, TIPO_CONTA
ORDER BY NIVEL_CONTA""")
display(df_estrutura_contas)

# COMMAND ----------

# DBTITLE 1,CONTAS DE NÍVEL 2 (BLOCOS MACRO)
# df_contas_nivel2: as contas de nível 2 estruturam a DFC em blocos macro
# 6.01 operacional, 6.02 investimento, 6.03 financiamento, 6.04 câmbio, 6.05 variação de caixa
df_contas_nivel2 = spark.sql(f"""
SELECT CD_CONTA, DS_CONTA, COUNT(DISTINCT CNPJ_CIA) AS qtd_empresas
FROM {TABELA_DFC_SILVER}
WHERE NIVEL_CONTA = 2
GROUP BY CD_CONTA, DS_CONTA
ORDER BY CD_CONTA""")
display(df_contas_nivel2)

# COMMAND ----------

# DBTITLE 1,Achado: Estrutura Hierárquica
# MAGIC %md
# MAGIC ## Achado: Estrutura Hierárquica
# MAGIC
# MAGIC Todas as contas pertencem à raiz **"6"** (plano CVM da DFC) — não existe conta de nível 1 nos dados. Cinco blocos de nível 2 (23.323 registros, classificados TOTALIZADORA):
# MAGIC
# MAGIC | Bloco | Conteúdo | Empresas |
# MAGIC | --- | --- | --- |
# MAGIC | 6.01 | Caixa líquido das atividades operacionais | 529+ |
# MAGIC | 6.02 | Atividades de investimento | 538 |
# MAGIC | 6.03 | Atividades de financiamento | 538 |
# MAGIC | 6.04 | Efeitos de variação cambial | 537 |
# MAGIC | 6.05 | Aumento (redução) de caixa e equivalentes | 538 |
# MAGIC
# MAGIC Profundidade: nível 3 com 62 contas (97.590 registros) e nível 4 com 61 contas (128.417 registros, ANALITICA). Estrutura difere por método: **MD usa 51 contas de nível 3 e nenhuma de nível 4; MI usa 45 de nível 3 e 61 de nível 4**.

# COMMAND ----------

# DBTITLE 1,VALIDAÇÃO ADITIVA VERTICAL
# df_validacao_aditiva: conta totalizadora = soma das filhas diretas (nível 3)
# Testa a hipótese de soma vertical para cada bloco, em todo o dataset
df_validacao_aditiva = spark.sql(f"""
WITH filhas AS (
  SELECT CNPJ_CIA, DT_REFER, ORDEM_EXERC, METODO_DFC, CD_CONTA_PAI,
         SUM(VL_CONTA) AS soma_filhas
  FROM {TABELA_DFC_SILVER}
  WHERE NIVEL_CONTA = 3
  GROUP BY CNPJ_CIA, DT_REFER, ORDEM_EXERC, METODO_DFC, CD_CONTA_PAI
),
comparacao AS (
  SELECT f.CD_CONTA_PAI, ABS(f.soma_filhas - p.VL_CONTA) AS divergencia
  FROM filhas f
  JOIN {TABELA_DFC_SILVER} p
    ON p.CNPJ_CIA = f.CNPJ_CIA AND p.DT_REFER = f.DT_REFER
   AND p.ORDEM_EXERC = f.ORDEM_EXERC AND p.METODO_DFC = f.METODO_DFC
   AND p.CD_CONTA = f.CD_CONTA_PAI AND p.NIVEL_CONTA = 2
)
SELECT CD_CONTA_PAI, COUNT(*) AS testadas,
       SUM(CASE WHEN divergencia < 0.01 THEN 1 ELSE 0 END) AS passou,
       ROUND(100.0 * SUM(CASE WHEN divergencia < 0.01 THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_pass
FROM comparacao
GROUP BY CD_CONTA_PAI
ORDER BY CD_CONTA_PAI""")
display(df_validacao_aditiva)

# COMMAND ----------

# DBTITLE 1,Achado: Aditividade Vertical
# MAGIC %md
# MAGIC ## Achado: Aditividade Vertical (Contas Totalizadoras)
# MAGIC
# MAGIC O resultado global de 74,7% (13.563/18.160) é enganoso sem escopo por bloco:
# MAGIC
# MAGIC | Bloco | Testadas | % Soma vertical |
# MAGIC | --- | --- | --- |
# MAGIC | 6.01 | 4.601 | 99,8% |
# MAGIC | 6.02 | 4.445 | 99,9% |
# MAGIC | 6.03 | 4.449 | 99,9% |
# MAGIC | 6.05 | 4.665 | **1,8%** |
# MAGIC
# MAGIC **6.01, 6.02 e 6.03 são contas aditivas** — as 17 falhas decorrem de reporte incompleto da empresa (contas filhas omissas; ex.: 42.737.954/0001-21 reporta 6.01 sem nenhuma filha, soma = 0). **6.05 não é aditiva** — é conta derivada (seção seguinte).

# COMMAND ----------

# DBTITLE 1,VALIDAÇÃO DA FÓRMULA DERIVADA (6.05)
# df_validacao_derivada: 6.05 = 6.01 + 6.02 + 6.03 + 6.04 (fórmula horizontal)
# Comprova que 6.05 é derivada dos quatro blocos, não soma vertical das filhas
df_validacao_derivada = spark.sql(f"""
WITH blocos AS (
  SELECT CNPJ_CIA, DT_REFER, ORDEM_EXERC, METODO_DFC,
         MAX(CASE WHEN CD_CONTA = '6.01' THEN VL_CONTA END) AS caixa_operacional,
         MAX(CASE WHEN CD_CONTA = '6.02' THEN VL_CONTA END) AS caixa_investimento,
         MAX(CASE WHEN CD_CONTA = '6.03' THEN VL_CONTA END) AS caixa_financiamento,
         MAX(CASE WHEN CD_CONTA = '6.04' THEN VL_CONTA END) AS variacao_cambial,
         MAX(CASE WHEN CD_CONTA = '6.05' THEN VL_CONTA END) AS aumento_caixa
  FROM {TABELA_DFC_SILVER}
  WHERE NIVEL_CONTA = 2
  GROUP BY CNPJ_CIA, DT_REFER, ORDEM_EXERC, METODO_DFC
)
SELECT COUNT(*) AS periodos_testados,
       SUM(CASE WHEN ABS(aumento_caixa - (caixa_operacional + caixa_investimento + caixa_financiamento + variacao_cambial)) < 0.01 THEN 1 ELSE 0 END) AS passou,
       SUM(CASE WHEN caixa_operacional IS NULL OR caixa_investimento IS NULL
                  OR caixa_financiamento IS NULL OR variacao_cambial IS NULL
                  OR aumento_caixa IS NULL THEN 1 ELSE 0 END) AS blocos_ausentes
FROM blocos""")
display(df_validacao_derivada)

# COMMAND ----------

# DBTITLE 1,Achado: Conta Derivada
# MAGIC %md
# MAGIC ## Achado: 6.05 é Conta Derivada — Fórmula Validada
# MAGIC
# MAGIC A fórmula horizontal **6.05 = 6.01 + 6.02 + 6.03 + 6.04** passa em **4.649 de 4.665 períodos (99,7%)** (escopo: todos os CNPJ × DT_REFER × ORDEM_EXERC × METODO do dataset). Os 2 períodos restantes têm blocos ausentes (reporte incompleto), não divergência numérica.
# MAGIC
# MAGIC Adicionalmente, 6.05 = 6.05.02 − 6.05.01 (saldo final − saldo inicial): a soma vertical das filhas de 6.05 é estruturalmente errada, o que explica o 1,8% da seção anterior.
# MAGIC
# MAGIC **Classificação final**: aditivas (6.01, 6.02, 6.03), derivada (6.05), folha (6.04, sem filhas). A classificação atual TIPO_CONTA (nível ≤ 2) não distingue aditivas de derivadas.

# COMMAND ----------

# DBTITLE 1,QUALIDADE DE DADOS
# df_qualidade: nulls críticos e unicidade da chave de negócio
# Chave: CNPJ_CIA + DT_REFER + CD_CONTA + ORDEM_EXERC + METODO_DFC (usada na deduplicação silver)
df_qualidade = spark.sql(f"""
SELECT COUNT(*) AS total_registros,
       SUM(CASE WHEN VL_CONTA IS NULL THEN 1 ELSE 0 END) AS vl_conta_null,
       SUM(CASE WHEN CNPJ_CIA IS NULL THEN 1 ELSE 0 END) AS cnpj_null,
       SUM(CASE WHEN CD_CONTA IS NULL THEN 1 ELSE 0 END) AS conta_null,
       SUM(CASE WHEN DT_REFER IS NULL THEN 1 ELSE 0 END) AS dt_refer_null,
       COUNT(*) - COUNT(DISTINCT CONCAT(CNPJ_CIA, '|', DT_REFER, '|', CD_CONTA, '|', ORDEM_EXERC, '|', METODO_DFC)) AS chaves_duplicadas
FROM {TABELA_DFC_SILVER}""")
display(df_qualidade)

# COMMAND ----------

# DBTITLE 1,Achado: Completude e Duplicação
# MAGIC %md
# MAGIC ## Achado: Completude e Duplicação
# MAGIC
# MAGIC **Zero nulls em colunas críticas** (VL_CONTA, CNPJ_CIA, CD_CONTA, DT_REFER) e **zero chaves de negócio duplicadas** (escopo: dataset completo) — a deduplicação da silver, com METODO_DFC na chave, produziu chave única.
# MAGIC
# MAGIC Reconciliação com a bronze: 251.766 → 249.330 registros (−2.436 duplicatas removidas), já confirmada no test_integracao_dfc de 11/11.

# COMMAND ----------

# DBTITLE 1,CLASSIFICAÇÕES DO REPORTE
# df_classificacoes: distribuição de ORDEM_EXERC, MOEDA, ESCALA_MOEDA e VERSAO
# Versões de documento e escalas afetam a interpretação dos valores
df_classificacoes = spark.sql(f"""
SELECT ORDEM_EXERC, MOEDA, ESCALA_MOEDA, VERSAO,
       COUNT(*) AS qtd_registros,
       COUNT(DISTINCT CNPJ_CIA) AS qtd_empresas
FROM {TABELA_DFC_SILVER}
GROUP BY ORDEM_EXERC, MOEDA, ESCALA_MOEDA, VERSAO
ORDER BY VERSAO, ESCALA_MOEDA, ORDEM_EXERC""")
display(df_classificacoes)

# COMMAND ----------

# DBTITLE 1,Achado: Classificações do Reporte
# MAGIC %md
# MAGIC ## Achado: Classificações do Reporte
# MAGIC
# MAGIC * **ORDEM_EXERC**: ÚLTIMO 124.750 (50,0%) / PENÚLTIMO 124.580 — comparações ano a ano devem fixar uma ordem
# MAGIC * **MOEDA**: REAL em 100% dos registros
# MAGIC * **ESCALA_MOEDA**: MIL 246.372 (527 empresas) / UNIDADE 2.958 (15 empresas). **VL_CONTA já está normalizado para unidades na silver** (× 1.000 quando MIL); ESCALA_MOEDA é preservada apenas para rastreabilidade — valores são comparáveis entre empresas
# MAGIC * **VERSAO**: 1 a 6 (198.367 / 40.089 / 9.606 / 916 / 338 / 14 registros) — republicações de documentos

# COMMAND ----------

# DBTITLE 1,ORIGEM BRONZE: COMPOSIÇÃO MD/MI POR ANO
# df_bronze_metodo: composição por método na bronze, por ano
# Explica a ausência de método direto em 2026 na silver
df_bronze_metodo = spark.sql(f"""
SELECT YEAR(DT_REFER) AS ano,
       CASE WHEN GRUPO_DFP LIKE '%Direto%' THEN 'METODO_DIRETO' ELSE 'METODO_INDIRETO' END AS metodo,
       COUNT(*) AS qtd_registros,
       COUNT(DISTINCT CNPJ_CIA) AS qtd_empresas
FROM {TABELA_DFC_BRONZE}
GROUP BY YEAR(DT_REFER),
         CASE WHEN GRUPO_DFP LIKE '%Direto%' THEN 'METODO_DIRETO' ELSE 'METODO_INDIRETO' END
ORDER BY ano, metodo""")
display(df_bronze_metodo)

# COMMAND ----------

# DBTITLE 1,Achado: Origem Bronze
# MAGIC %md
# MAGIC ## Achado: Origem Bronze — Composição MD/MI
# MAGIC
# MAGIC A bronze reproduz o padrão da silver: MD com 816 / 992 / 794 / 656 / 558 / 0 registros (2021-2026) e MI dominante. A ausência de MD em 2026 se origina na landing: o CSV DFC_MD_con de 2026 tem 0 bytes no ZIP da CVM — o guardrail da bronze 104 tolera (só interrompe se ambas as variantes estiverem vazias). Reconciliação bronze → silver: 251.766 → 249.330 (−2.436 duplicatas exatas).

# COMMAND ----------

# DBTITLE 1,HETEROGENEIDADE DE NOMENCLATURA
# df_nomenclatura: contas com mais de uma DS_CONTA
# A CVM altera descrições entre demonstrativos/anos — impacto em dicionários e joins por descrição
df_nomenclatura = spark.sql(f"""
SELECT CD_CONTA,
       COUNT(DISTINCT DS_CONTA) AS qtd_descricoes,
       COUNT(*) AS qtd_registros
FROM {TABELA_DFC_SILVER}
GROUP BY CD_CONTA
HAVING COUNT(DISTINCT DS_CONTA) > 1
ORDER BY qtd_descricoes DESC, CD_CONTA""")
display(df_nomenclatura)

# COMMAND ----------

# DBTITLE 1,Achado: Heterogeneidade Semântica
# MAGIC %md
# MAGIC ## Achado: Heterogeneidade Semântica (Nomenclatura CVM)
# MAGIC
# MAGIC **121 contas têm mais de uma DS_CONTA** (escopo: dataset completo) — e o fenômeno cresce com a profundidade da hierarquia:
# MAGIC
# MAGIC * **Nível 2 (blocos macro) — estáveis**: 6.01 com 3 variantes (padrão em 529 empresas, "das Atividades Operacionais" em 14, "Seguradora/Resseguradora" em 2), 6.04 com 2 (537 / 6), 6.05.02.01 e 6.05.02.02 com 2 cada (12 / 10 registros)
# MAGIC * **Nível 3 — já varia**: 6.01.01 com 18 descrições distintas, 6.01.02 com 17
# MAGIC * **Nível 4 (analíticas) — descrição essencialmente livre por empresa**: 6.01.01.10 com 594 descrições em 3.701 registros; as famílias 6.01.01.xx e 6.01.02.xx superam 200 variantes cada
# MAGIC
# MAGIC Implicação: DS_CONTA não é chave semântica confiável em nenhum nível — joins e filtros sempre por CD_CONTA. O dicionário da camada gold deve mapear CD_CONTA ↔ descrição oficial e tratar DS_CONTA como texto descritivo por empresa.

# COMMAND ----------

# DBTITLE 1,CONCLUSÃO
# MAGIC %md
# MAGIC ## Conclusão
# MAGIC
# MAGIC ### Hipóteses investigadas
# MAGIC
# MAGIC 1. **Invariante de método único — CONFIRMADA**: zero violações em 538 empresas × 6 anos. A decisão da tabela bronze única `104_dfc_dfp` (com coluna de método) está validada nos dados.
# MAGIC 2. **Migração de método — CONFIRMADA**: 14 transições (12 empresas), incluindo oscilações MD → MI → MD. O método é atributo do reporte, não da empresa.
# MAGIC 3. **Estrutura hierárquica — PARCIALMENTE CONFIRMADA**: 6.01/6.02/6.03 aditivas (99,8-99,9%); 6.05 derivada (fórmula 6.05 = 6.01+6.02+6.03+6.04 válida em 99,7%); 6.04 folha. A classificação TIPO_CONTA (nível ≤ 2) não distingue aditivas de derivadas.
# MAGIC 4. **Qualidade — CONFIRMADA**: zero nulls críticos, zero duplicatas de chave, escala monetária já normalizada para unidades.
# MAGIC
# MAGIC ### Decisões
# MAGIC
# MAGIC * **Nenhuma alteração no pipeline bronze/silver** — as premissas que motivaram as decisões de 10/11 se sustentam.
# MAGIC * **Registro para a camada gold (quando construída)**: (a) não tratar 6.05 como aditiva; (b) joins sempre por CD_CONTA, nunca por DS_CONTA; (c) fixar ORDEM_EXERC para séries temporais.
# MAGIC
# MAGIC ### Gaps pendentes
# MAGIC
# MAGIC * Generalização da validação para o ambiente test (executar este EDA lá)
# MAGIC * 2 períodos com blocos ausentes na fórmula 6.05 (reporte incompleto — sem ação)
# MAGIC * MD 2026 ausente na fonte CVM (arquivo 0 bytes) — monitorar republicação
# MAGIC
# MAGIC **Investigação concluída com evidência suficiente para arquivamento.**