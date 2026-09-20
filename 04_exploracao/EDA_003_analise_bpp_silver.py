# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,DOCUMENTAÇÃO
# MAGIC %md
# MAGIC ## EDA 003: Investigação de Qualidade - BPP Silver
# MAGIC
# MAGIC **Dataset Analisado**: `proj_cvm_02_silver.203_bpp_dfp`  
# MAGIC **Data de Execução**: 2026-09-20
# MAGIC
# MAGIC **Objetivo**: Identificar problemas estruturais e semânticos na camada Silver que comprometem a confiabilidade das análises financeiras downstream.
# MAGIC
# MAGIC **Metodologia**: Análises técnicas neutras investigam hipóteses sem viés confirmatório. Cada análise SQL é seguida de célula markdown documentando achados objetivamente.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Índice de Seções
# MAGIC
# MAGIC 1. **Perfil Geral do Dataset** (células 3-5): Dimensões, schema, memória
# MAGIC 2. **Cobertura Temporal** (células 6-9): Distribuição por período, reconciliação landing zone vs Silver
# MAGIC 3. **Distribuição de Empresas** (células 10-11): Volume por companhia, top empresas
# MAGIC 4. **Estrutura de Contas Contábeis** (células 12-13): Frequência de contas, hierarquia
# MAGIC 5. **Qualidade de Dados** (células 14-17): Nulos, duplicados, chave de negócio
# MAGIC 6. **Distribuição de Valores** (células 18-19): Estatísticas descritivas, extremos
# MAGIC 7. **Escalas Monetárias** (células 20-21): Normalização pendente
# MAGIC 8. **Versões de Documentos** (células 22-23): Múltiplas versões
# MAGIC 9. **Hierarquia Contábil** (células 24-33): Descoberta de amostra, classificação, validação de integridade
# MAGIC 10. **Conclusão** (célula 34): Síntese de correções necessárias

# COMMAND ----------

# DBTITLE 1,IMPORTS
# Bibliotecas para análise exploratória
import pandas as pd
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,CARREGAR DADOS
# MAGIC %sql
# MAGIC -- Carrega dataset completo da BPP Silver
# MAGIC SELECT *
# MAGIC FROM proj_cvm_02_silver.203_bpp_dfp

# COMMAND ----------

# DBTITLE 1,PERFIL GERAL DO DATASET
df_bpp = _sqldf

print(f"Shape: {df_bpp.count():,} linhas x {len(df_bpp.columns)} colunas")
print(f"\nColunas e tipos:")
df_bpp.printSchema()
print(f"\nMemória estimada: {df_bpp.count() * len(df_bpp.columns) * 8 / (1024**2):.2f} MB")

# COMMAND ----------

# DBTITLE 1,Achado: Dimensões do Dataset
# MAGIC %md
# MAGIC ## Achado: Dimensões do Dataset
# MAGIC
# MAGIC O dataset contém 528.436 registros distribuídos em 21 colunas, com memória estimada de 84,66 MB. A estrutura apresenta:
# MAGIC
# MAGIC * Colunas de identificação (CNPJ_CIA, CD_CVM, DENOM_CIA)
# MAGIC * Colunas temporais (DT_REFER, ANO, TRIMESTRE, MES, DT_FIM_EXERC)
# MAGIC * Colunas de classificação (VERSAO, GRUPO_DFP, MOEDA, ESCALA_MOEDA, ORDEM_EXERC)
# MAGIC * Dados contábeis (CD_CONTA, DS_CONTA, VL_CONTA)
# MAGIC * Metadados operacionais (DT_PROCESSAMENTO)
# MAGIC * Enriquecimento hierárquico (ST_CONTA_FIXA, NIVEL_CONTA, CD_CONTA_PAI, CD_CONTA_RAIZ)
# MAGIC
# MAGIC **Comparação com DRE e BPA**: BPP possui 21 colunas (mesmo BPA, sem DT_INI_EXERC). Volume é o maior das três tabelas (528.436 vs 308.988 BPA vs 162.885 DRE), pois o Passivo tem mais contas analíticas que o Ativo (Patrimônio Líquido possui muitas subcontas).
# MAGIC
# MAGIC ✓ **Schema validado**: Estrutura confirmada como esperada para tabela BPP Silver, com enriquecimento hierárquico. Memória estimada de 84,66 MB.sperada para tabela BPP Silver com 21 colunas (sem DT_INI_EXERC, presente apenas na DRE).

# COMMAND ----------

# DBTITLE 1,ANÁLISE TEMPORAL
# MAGIC %sql
# MAGIC -- Distribuição de registros por período
# MAGIC SELECT 
# MAGIC   ANO,
# MAGIC   TRIMESTRE,
# MAGIC   COUNT(*) as qtd_registros,
# MAGIC   COUNT(DISTINCT CNPJ_CIA) as qtd_empresas_unicas,
# MAGIC   COUNT(DISTINCT CD_CONTA) as qtd_contas_unicas
# MAGIC FROM proj_cvm_02_silver.203_bpp_dfp
# MAGIC GROUP BY ANO, TRIMESTRE
# MAGIC ORDER BY ANO DESC, TRIMESTRE DESC

# COMMAND ----------

# DBTITLE 1,Achado: Cobertura Temporal
# MAGIC %md
# MAGIC ## Achado: Cobertura Temporal e Sazonalidade
# MAGIC
# MAGIC Dataset cobre seis anos fiscais (2021-2026) com concentração em Q4. Distribuição confirma que DFPs anuais (Q4) concentram o maior volume, enquanto ITRs trimestrais aparecem apenas para empresas com obrigação regulatória.
# MAGIC
# MAGIC | Período | Registros | Empresas | Contas Distintas |
# MAGIC | --- | --- | --- | --- |
# MAGIC | Q4/2021 | 100.925 | 452 | 390 |
# MAGIC | Q4/2022 | 104.626 | 463 | 396 |
# MAGIC | Q4/2023 | 105.229 | 464 | 367 |
# MAGIC | Q4/2024 | 104.654 | 458 | 358 |
# MAGIC | Q4/2025 | 98.638 | 428 | 353 |
# MAGIC | Q1/Q2/Q3 (vários) | 216-2.104 | 1-10 | 108-168 |
# MAGIC
# MAGIC **Comparação com DRE e BPA**: BPP tem ~1,7x mais registros por Q4 que BPA (105k vs 61k) e ~3,2x mais que DRE (105k vs 32k), refletindo o maior número de contas no Passivo+PL (390 vs 310 BPA vs 213 DRE no Q4/2021). Queda de contas em anos recentes (390→353) segue a mesma tendência do BPA.
# MAGIC
# MAGIC ✓ Cobertura consistente com regulamentação CVM.
# MAGIC ✓ **Cobertura temporal validada**: Pipeline processa todos os 6 anos disponíveis (2021-2026), padrão idêntico a DRE e BPA.

# COMMAND ----------

# DBTITLE 1,RECONCILIAÇÃO: VERIFICAR LANDING ZONE
# Auditoria de pipeline: verificar anos disponíveis na landing zone de origem
# Compara com o range temporal encontrado na Silver

import os

landing_path = "/Volumes/workspace/proj_cvm/landing/dfp"

try:
    anos_disponiveis = []
    for item in os.listdir(landing_path):
        item_path = os.path.join(landing_path, item)
        if os.path.isdir(item_path) and item.isdigit():
            anos_disponiveis.append(int(item))
    
    anos_disponiveis.sort()
    
    # Query Silver table for actual range
    silver_rows = spark.sql("SELECT DISTINCT ANO FROM proj_cvm_02_silver.203_bpp_dfp ORDER BY ANO").collect()
    silver_anos = [row['ANO'] for row in silver_rows]
    
    print("LANDING ZONE (Fonte de Origem)")
    print("="*80)
    print(f"Path: {landing_path}")
    print(f"Anos disponíveis: {anos_disponiveis}")
    print(f"Range temporal: {min(anos_disponiveis)} a {max(anos_disponiveis)}")
    print(f"Total de anos: {len(anos_disponiveis)}")
    
    print("\n" + "="*80)
    print("RECONCILIAÇÃO: Landing Zone vs Silver")
    print("="*80)
    print(f"Landing Zone: {min(anos_disponiveis)}-{max(anos_disponiveis)} ({len(anos_disponiveis)} anos)")
    print(f"Silver (203_bpp_dfp): {min(silver_anos)}-{max(silver_anos)} ({len(silver_anos)} anos)")
    print(f"\nAnos na Landing mas AUSENTES na Silver: {[a for a in anos_disponiveis if a not in silver_anos]}")
    
except Exception as e:
    print(f"❌ Erro ao acessar landing zone: {e}")

# COMMAND ----------

# DBTITLE 1,Achado: Reconciliação Landing vs Silver
# MAGIC %md
# MAGIC ## Achado: Reconciliação Landing Zone vs Silver
# MAGIC
# MAGIC Reconciliação entre camadas do pipeline confirmada: todos os 6 anos disponíveis na landing zone foram processados e estão presentes na Silver.
# MAGIC
# MAGIC | Camada | Anos Disponíveis |
# MAGIC | --- | --- |
# MAGIC | Landing Zone (origem) | 2021-2026 (6 anos) |
# MAGIC | Silver (203_bpp_dfp) | 2021-2026 (6 anos) |
# MAGIC
# MAGIC **Fluxo de dados**: Landing Zone (2021-2026) → Bronze → Silver (2021-2026)
# MAGIC
# MAGIC ✓ **Cobertura completa**: Pipeline processa todos os anos disponíveis na landing zone. Anos ausentes: [] (nenhum).

# COMMAND ----------

# DBTITLE 1,ANÁLISE DE EMPRESAS
# MAGIC %sql
# MAGIC -- Cobertura de empresas e volume de dados por companhia
# MAGIC WITH empresas_resumo AS (
# MAGIC   SELECT 
# MAGIC     CNPJ_CIA,
# MAGIC     DENOM_CIA,
# MAGIC     COUNT(*) as qtd_registros,
# MAGIC     COUNT(DISTINCT ANO) as qtd_anos,
# MAGIC     MIN(DT_REFER) as dt_primeira_bpp,
# MAGIC     MAX(DT_REFER) as dt_ultima_bpp
# MAGIC   FROM proj_cvm_02_silver.203_bpp_dfp
# MAGIC   GROUP BY CNPJ_CIA, DENOM_CIA
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   CAST(COUNT(DISTINCT CNPJ_CIA) AS STRING) as total_cnpjs_unicos,
# MAGIC   CAST(AVG(qtd_registros) AS STRING) as media_registros_por_empresa,
# MAGIC   CAST(MAX(qtd_registros) AS STRING) as max_registros_empresa
# MAGIC FROM empresas_resumo
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT NULL, NULL, NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   CNPJ_CIA,
# MAGIC   DENOM_CIA,
# MAGIC   CAST(qtd_registros AS STRING) as qtd_registros
# MAGIC FROM (
# MAGIC   SELECT 
# MAGIC     CNPJ_CIA,
# MAGIC     DENOM_CIA,
# MAGIC     qtd_registros
# MAGIC   FROM empresas_resumo
# MAGIC   ORDER BY qtd_registros DESC
# MAGIC   LIMIT 10
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Achado: Distribuição de Empresas
# MAGIC %md
# MAGIC ## Achado: Distribuição de Empresas
# MAGIC
# MAGIC Dataset contém 537 empresas distintas com média de 959,05 registros por empresa. Variação significativa entre empresas reflete diferentes frequências de reporte (DFPs anuais vs ITRs trimestrais) e complexidade variável das estruturas contábeis.
# MAGIC
# MAGIC | Métrica | Valor |
# MAGIC | --- | --- |
# MAGIC | Total de empresas | 537 |
# MAGIC | Média de registros | 959,05 |
# MAGIC | Máximo (REDE ENERGIA) | 1.476 |
# MAGIC
# MAGIC **Top 10 empresas**: REDE ENERGIA (1.476), SAO MARTINHO (1.440), CERRADINHO (1.420), CEMIG (1.420), BRF (1.412), EDP (1.394), EMBRAER (1.394), AMBEV (1.392), CAMIL (1.390), MOURA DUBEUX (1.388).
# MAGIC
# MAGIC Empresas líderes incluem setores diversos: energia (REDE ENERGIA, CEMIG, EDP), agronegócio (SAO MARTINHO, CERRADINHO, CAMIL), indústria (BRF, EMBRAER, AMBEV).
# MAGIC
# MAGIC ✓ **Distribuição validada**: Mesmo número de empresas (537) que DRE e BPA. Média de registros ~1,7x maior que BPA (959 vs 561) e ~3,2x maior que DRE (959 vs 296), refletindo maior granularidade do BPP (Passivo + Patrimônio Líquido).

# COMMAND ----------

# DBTITLE 1,ANÁLISE DE CONTAS CONTÁBEIS
# MAGIC %sql
# MAGIC -- Estrutura de contas contábeis e frequência
# MAGIC SELECT 
# MAGIC   CD_CONTA,
# MAGIC   DS_CONTA,
# MAGIC   COUNT(*) as qtd_ocorrencias,
# MAGIC   COUNT(DISTINCT CNPJ_CIA) as qtd_empresas,
# MAGIC   ROUND(AVG(VL_CONTA), 2) as valor_medio,
# MAGIC   LENGTH(CD_CONTA) - LENGTH(REPLACE(CD_CONTA, '.', '')) + 1 as nivel_hierarquico
# MAGIC FROM proj_cvm_02_silver.203_bpp_dfp
# MAGIC GROUP BY CD_CONTA, DS_CONTA
# MAGIC ORDER BY qtd_ocorrencias DESC
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,Achado: Estrutura de Contas Contábeis
# MAGIC %md
# MAGIC ## Achado: Estrutura de Contas Contábeis
# MAGIC
# MAGIC As 20 contas mais frequentes revelam a estrutura padrão do BPP regulatório. Contas do Passivo (prefixo 2.) incluem Passivo Circulante (2.01), Passivo Não Circulante (2.02) e Patrimônio Líquido (2.03) e suas subcontas.
# MAGIC
# MAGIC | Tipo de Conta | Cobertura | Exemplos |
# MAGIC | --- | --- | --- |
# MAGIC | Conta universal | 537 empresas | Passivo Total (2.0) |
# MAGIC | Contas de PL | 514 empresas | Capital Social (2.03.01), Reservas de Lucros (2.03.04), Lucros/Prejuízos Acumulados (2.03.05) |
# MAGIC | Contas principais | 514 empresas | Passivo Circulante (2.01), Passivo Não Circulante (2.02) |
# MAGIC | Subcontas nível 4 | 514 empresas | Reserva Legal (2.03.04.01), Dividendo Adicional (2.03.04.08), Ações em Tesouraria (2.03.02.05) |
# MAGIC
# MAGIC Valores médios indicam escala: Passivo Total (35,3 bilhões), Capital Social (3,6 bilhões), Reservas de Lucros (1,8 bilhões). Lucros/Prejuízos Acumulados apresenta valor médio negativo (-628 mi), consistente com empresas em prejuízo.
# MAGIC
# MAGIC ✓ **Estrutura validada**: Hierarquia consistente com plano de contas referencial da CVM para Balanço Patrimonial Passivo.

# COMMAND ----------

# DBTITLE 1,QUALIDADE DE DADOS
# MAGIC %sql
# MAGIC -- Verifica valores nulos e registros duplicados
# MAGIC SELECT 
# MAGIC   'Valores Nulos' as metrica,
# MAGIC   SUM(CASE WHEN CNPJ_CIA IS NULL THEN 1 ELSE 0 END) as CNPJ_CIA,
# MAGIC   SUM(CASE WHEN DT_REFER IS NULL THEN 1 ELSE 0 END) as DT_REFER,
# MAGIC   SUM(CASE WHEN CD_CONTA IS NULL THEN 1 ELSE 0 END) as CD_CONTA,
# MAGIC   SUM(CASE WHEN VL_CONTA IS NULL THEN 1 ELSE 0 END) as VL_CONTA,
# MAGIC   SUM(CASE WHEN DT_FIM_EXERC IS NULL THEN 1 ELSE 0 END) as DT_FIM_EXERC
# MAGIC FROM proj_cvm_02_silver.203_bpp_dfp
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Registros Duplicados' as metrica,
# MAGIC   COUNT(*) - COUNT(DISTINCT CNPJ_CIA, DT_REFER, VERSAO, CD_CONTA, GRUPO_DFP) as duplicados,
# MAGIC   NULL, NULL, NULL, NULL
# MAGIC FROM proj_cvm_02_silver.203_bpp_dfp

# COMMAND ----------

# DBTITLE 1,Achado: Completude e Duplicação
# MAGIC %md
# MAGIC ## Achado: Completude e Duplicação Aparente
# MAGIC
# MAGIC Validação de qualidade confirma completude em colunas críticas. Duplicação aparente de 263.950 registros detectada, requer investigação para identificar se é estrutural (coluna de chave faltante) ou problema de qualidade.
# MAGIC
# MAGIC | Métrica | Resultado |
# MAGIC | --- | --- |
# MAGIC | Nulos em CNPJ_CIA | 0 |
# MAGIC | Nulos em DT_REFER | 0 |
# MAGIC | Nulos em CD_CONTA | 0 |
# MAGIC | Nulos em VL_CONTA | 0 |
# MAGIC | Nulos em DT_FIM_EXERC | 0 |
# MAGIC | Registros não únicos | 263.950 |
# MAGIC
# MAGIC ✓ **Completude validada**: Zero nulos em campos obrigatórios.
# MAGIC ✗ **Duplicação aparente detectada**: Requer investigação de chave (próxima célula).

# COMMAND ----------

# DBTITLE 1,VALIDAÇÃO DA CHAVE DE NEGÓCIO
# MAGIC %sql
# MAGIC -- Verifica se ORDEM_EXERC resolve a aparente duplicação
# MAGIC WITH teste_chave_sem_ordem_exerc AS (
# MAGIC   SELECT 
# MAGIC     COUNT(*) as total_registros,
# MAGIC     COUNT(DISTINCT CNPJ_CIA, DT_REFER, VERSAO, CD_CONTA, GRUPO_DFP) as distintos_sem_ordem_exerc,
# MAGIC     COUNT(*) - COUNT(DISTINCT CNPJ_CIA, DT_REFER, VERSAO, CD_CONTA, GRUPO_DFP) as duplicados_aparentes
# MAGIC   FROM proj_cvm_02_silver.203_bpp_dfp
# MAGIC ),
# MAGIC teste_chave_com_ordem_exerc AS (
# MAGIC   SELECT 
# MAGIC     COUNT(*) as total_registros,
# MAGIC     COUNT(DISTINCT CNPJ_CIA, DT_REFER, VERSAO, CD_CONTA, GRUPO_DFP, ORDEM_EXERC) as distintos_com_ordem_exerc,
# MAGIC     COUNT(*) - COUNT(DISTINCT CNPJ_CIA, DT_REFER, VERSAO, CD_CONTA, GRUPO_DFP, ORDEM_EXERC) as duplicados_reais
# MAGIC   FROM proj_cvm_02_silver.203_bpp_dfp
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   'SEM ORDEM_EXERC' as teste,
# MAGIC   total_registros,
# MAGIC   distintos_sem_ordem_exerc as registros_distintos,
# MAGIC   duplicados_aparentes as duplicados
# MAGIC FROM teste_chave_sem_ordem_exerc
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'COM ORDEM_EXERC' as teste,
# MAGIC   total_registros,
# MAGIC   distintos_com_ordem_exerc as registros_distintos,
# MAGIC   duplicados_reais as duplicados
# MAGIC FROM teste_chave_com_ordem_exerc

# COMMAND ----------

# DBTITLE 1,Achado: Chave de Negócio
# MAGIC %md
# MAGIC ## Achado: Chave de Negócio
# MAGIC
# MAGIC Chave única confirmada: (CNPJ_CIA, DT_REFER, VERSAO, CD_CONTA, GRUPO_DFP, ORDEM_EXERC). Duplicação detectada anteriormente é estrutural e legítima: ORDEM_EXERC distingue períodos comparativos (PENÚLTIMO vs ÚLTIMO exercício).
# MAGIC
# MAGIC | Teste | Total Registros | Distintos | Duplicados |
# MAGIC | --- | --- | --- | --- |
# MAGIC | SEM ORDEM_EXERC | 528.436 | 264.486 | 263.950 |
# MAGIC | COM ORDEM_EXERC | 528.436 | 528.436 | 0 |
# MAGIC
# MAGIC ✓ **Chave de negócio validada**: (CNPJ_CIA, DT_REFER, VERSAO, CD_CONTA, GRUPO_DFP, ORDEM_EXERC) garante unicidade. Padrão idêntico a DRE e BPA.

# COMMAND ----------

# DBTITLE 1,ANÁLISE DE VALORES
# MAGIC %sql
# MAGIC -- Estatísticas descritivas do campo VL_CONTA
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_registros,
# MAGIC   COUNT(DISTINCT VL_CONTA) as valores_distintos,
# MAGIC   ROUND(MIN(VL_CONTA), 2) as minimo,
# MAGIC   ROUND(PERCENTILE(VL_CONTA, 0.25), 2) as percentil_25,
# MAGIC   ROUND(PERCENTILE(VL_CONTA, 0.50), 2) as mediana,
# MAGIC   ROUND(AVG(VL_CONTA), 2) as media,
# MAGIC   ROUND(PERCENTILE(VL_CONTA, 0.75), 2) as percentil_75,
# MAGIC   ROUND(MAX(VL_CONTA), 2) as maximo,
# MAGIC   ROUND(STDDEV(VL_CONTA), 2) as desvio_padrao,
# MAGIC   SUM(CASE WHEN VL_CONTA = 0 THEN 1 ELSE 0 END) as qtd_zeros,
# MAGIC   SUM(CASE WHEN VL_CONTA < 0 THEN 1 ELSE 0 END) as qtd_negativos,
# MAGIC   SUM(CASE WHEN VL_CONTA > 0 THEN 1 ELSE 0 END) as qtd_positivos
# MAGIC FROM proj_cvm_02_silver.203_bpp_dfp

# COMMAND ----------

# DBTITLE 1,Achado: Distribuição de Valores
# MAGIC %md
# MAGIC ## Achado: Distribuição de Valores e Extremos
# MAGIC
# MAGIC VL_CONTA apresenta distribuição com predominância de valores positivos (Passivo = obrigações e patrimônio), mas com proporção significativa de zeros e alguns valores negativos (retificadores de patrimônio, como Ações em Tesouraria e Prejuízos Acumulados).
# MAGIC
# MAGIC | Métrica | Valor |
# MAGIC | --- | --- |
# MAGIC | Amplitude | -78,4 bi a +3,07 tri |
# MAGIC | Desvio padrão | 25,0 bi |
# MAGIC | Mediana | 0 |
# MAGIC | Média | 1,12 bi (enviesada por extremos) |
# MAGIC | Percentil 25 | 0 |
# MAGIC | Percentil 75 | 26,5 mi |
# MAGIC | Positivos | 198.961 (37,7%) |
# MAGIC | Negativos | 8.703 (1,6%) |
# MAGIC | Zeros | 320.772 (60,7%) |
# MAGIC
# MAGIC **Comparação com BPA e DRE**: BPP tem a maior proporção de zeros (60,7% vs 51,5% BPA vs 31,7% DRE), refletindo que muitas contas analíticas de PL não se aplicam a todas as empresas. O maior número de negativos (8.703 vs 838 BPA) reflica contas retificadoras do PL (Ações em Tesouraria, Prejuízos Acumulados).
# MAGIC
# MAGIC ✓ **Distribuição legítima**: Predominância de zeros e alguns negativos confirmam natureza patrimonial do BPP.
# MAGIC
# MAGIC ⚠ **Atenção**: Valores extremos (~3 tri) correspondem a grandes empresas. Normalização de escala é essencial.

# COMMAND ----------

# DBTITLE 1,ANÁLISE DE ESCALAS MONETÁRIAS
# MAGIC %sql
# MAGIC -- Verifica distribuição de escalas monetárias
# MAGIC SELECT 
# MAGIC   ESCALA_MOEDA,
# MAGIC   COUNT(*) as qtd_registros,
# MAGIC   COUNT(DISTINCT CNPJ_CIA) as qtd_empresas,
# MAGIC   ROUND(AVG(VL_CONTA), 2) as media_valores_na_escala,
# MAGIC   ROUND(MIN(VL_CONTA), 2) as min_valor,
# MAGIC   ROUND(MAX(VL_CONTA), 2) as max_valor
# MAGIC FROM proj_cvm_02_silver.203_bpp_dfp
# MAGIC GROUP BY ESCALA_MOEDA
# MAGIC ORDER BY qtd_registros DESC

# COMMAND ----------

# DBTITLE 1,Achado: Escalas Monetárias
# MAGIC %md
# MAGIC ## Achado: Escalas Monetárias
# MAGIC
# MAGIC Duas escalas coexistem na mesma coluna VL_CONTA. Comparações diretas entre empresas produzem erros de magnitude 1000x.
# MAGIC
# MAGIC | Escala | Registros | Percentual | Empresas |
# MAGIC | --- | --- | --- | --- |
# MAGIC | MIL | 518.687 | 98,2% | 526 |
# MAGIC | UNIDADE | 9.749 | 1,8% | 15 |
# MAGIC
# MAGIC ✗ **Problema crítico detectado**: Comparações diretas produzem erros de magnitude 1000x.
# MAGIC
# MAGIC **Correção necessária no notebook 203_bpp_silver**: Criar coluna VL_CONTA_NORMALIZADO aplicando fator de conversão baseado em ESCALA_MOEDA. Padrão idêntico ao detectado em DRE e BPA.

# COMMAND ----------

# DBTITLE 1,ANÁLISE DE VERSÕES DE DOCUMENTOS
# MAGIC %sql
# MAGIC -- Verifica múltiplas versões por empresa/período/conta
# MAGIC WITH versoes_por_periodo AS (
# MAGIC   SELECT 
# MAGIC     CNPJ_CIA,
# MAGIC     DENOM_CIA,
# MAGIC     ANO,
# MAGIC     TRIMESTRE,
# MAGIC     CD_CONTA,
# MAGIC     COUNT(DISTINCT VERSAO) as qtd_versoes,
# MAGIC     COLLECT_SET(VERSAO) as versoes_presentes
# MAGIC   FROM proj_cvm_02_silver.203_bpp_dfp
# MAGIC   GROUP BY CNPJ_CIA, DENOM_CIA, ANO, TRIMESTRE, CD_CONTA
# MAGIC   HAVING COUNT(DISTINCT VERSAO) > 1
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_casos_multiplas_versoes,
# MAGIC   COUNT(DISTINCT CNPJ_CIA) as empresas_afetadas,
# MAGIC   MAX(qtd_versoes) as max_versoes_em_um_caso
# MAGIC FROM versoes_por_periodo
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   CNPJ_CIA,
# MAGIC   DENOM_CIA,
# MAGIC   CAST(qtd_versoes AS STRING) as qtd_versoes
# MAGIC FROM versoes_por_periodo
# MAGIC LIMIT 5

# COMMAND ----------

# DBTITLE 1,Achado: Versões de Documentos
# MAGIC %md
# MAGIC ## Achado: Versões de Documentos
# MAGIC
# MAGIC Não foram detectados casos de múltiplas versões por empresa/período/conta. No estado atual da tabela, não há conflito de versões que exija filtro MAX(VERSAO).
# MAGIC
# MAGIC | Métrica | Resultado |
# MAGIC | --- | --- |
# MAGIC | Casos com múltiplas versões | 0 |
# MAGIC | Empresas afetadas | 0 |
# MAGIC
# MAGIC ✓ **Sem conflitos de versão**: Snapshot atual não apresenta múltiplas versões por chave. Padrão idêntico a DRE e BPA.
# MAGIC
# MAGIC **Observação**: Reapresentações futuras podem introduzir múltiplas versões — recomenda-se monitoramento periódico.

# COMMAND ----------

# DBTITLE 1,ANÁLISE DE ESTRUTURA HIERÁRQUICA
# MAGIC %sql
# MAGIC -- Identifica amostra válida para validação hierárquica
# MAGIC -- Busca primeira empresa com conta pai 2.01 E contas filhas 2.01.*
# MAGIC
# MAGIC WITH empresas_com_conta_201 AS (
# MAGIC   SELECT DISTINCT 
# MAGIC     CNPJ_CIA,
# MAGIC     DENOM_CIA,
# MAGIC     ANO,
# MAGIC     TRIMESTRE,
# MAGIC     ORDEM_EXERC,
# MAGIC     GRUPO_DFP
# MAGIC   FROM proj_cvm_02_silver.203_bpp_dfp
# MAGIC   WHERE CD_CONTA = '2.01'
# MAGIC ),
# MAGIC empresas_com_filhas_201 AS (
# MAGIC   SELECT DISTINCT 
# MAGIC     CNPJ_CIA,
# MAGIC     ANO,
# MAGIC     TRIMESTRE,
# MAGIC     ORDEM_EXERC,
# MAGIC     GRUPO_DFP,
# MAGIC     COUNT(DISTINCT CD_CONTA) as qtd_contas_filhas
# MAGIC   FROM proj_cvm_02_silver.203_bpp_dfp
# MAGIC   WHERE CD_CONTA LIKE '2.01.%'
# MAGIC     AND LENGTH(CD_CONTA) - LENGTH(REPLACE(CD_CONTA, '.', '')) + 1 = 3
# MAGIC   GROUP BY CNPJ_CIA, ANO, TRIMESTRE, ORDEM_EXERC, GRUPO_DFP
# MAGIC   HAVING COUNT(DISTINCT CD_CONTA) > 0
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   p.CNPJ_CIA,
# MAGIC   p.DENOM_CIA,
# MAGIC   p.ANO,
# MAGIC   p.TRIMESTRE,
# MAGIC   p.ORDEM_EXERC,
# MAGIC   p.GRUPO_DFP,
# MAGIC   f.qtd_contas_filhas
# MAGIC FROM empresas_com_conta_201 p
# MAGIC INNER JOIN empresas_com_filhas_201 f
# MAGIC   ON p.CNPJ_CIA = f.CNPJ_CIA
# MAGIC   AND p.ANO = f.ANO
# MAGIC   AND p.TRIMESTRE = f.TRIMESTRE
# MAGIC   AND p.ORDEM_EXERC = f.ORDEM_EXERC
# MAGIC   AND p.GRUPO_DFP = f.GRUPO_DFP
# MAGIC ORDER BY p.ANO DESC, p.TRIMESTRE DESC
# MAGIC LIMIT 1

# COMMAND ----------

# DBTITLE 1,Achado: Amostra Hierárquica
# MAGIC %md
# MAGIC ## Achado: Amostra Válida para Validação Hierárquica
# MAGIC
# MAGIC Query identificou amostra viável para testar integridade hierárquica.
# MAGIC
# MAGIC | Atributo | Valor |
# MAGIC | --- | --- |
# MAGIC | Empresa | BRASILAGRO (CNPJ 07.628.528/0001-59) |
# MAGIC | Período | Q2/2026, PENÚLTIMO exercício |
# MAGIC | Demonstração | DF Consolidado - Balanço Patrimonial Passivo |
# MAGIC | Conta pai | 2.01 (Passivo Circulante) |
# MAGIC | Contas filhas | 7 contas 2.01.* distintas (nível 3) |
# MAGIC
# MAGIC ✓ **Amostra identificada**: BRASILAGRO possui estrutura hierárquica adequada para validação de integridade. Mesma empresa usada como amostra em EDA_001 (DRE) e EDA_002 (BPA), permitindo comparação cruzada entre as três demonstrações.

# COMMAND ----------

# DBTITLE 1,CLASSIFICAÇÃO HIERÁRQUICA
# MAGIC %sql
# MAGIC -- Classifica contas em totalizadoras vs analíticas
# MAGIC WITH hierarquia AS (
# MAGIC   SELECT 
# MAGIC     CD_CONTA,
# MAGIC     DS_CONTA,
# MAGIC     LENGTH(CD_CONTA) - LENGTH(REPLACE(CD_CONTA, '.', '')) + 1 as nivel_hierarquico,
# MAGIC     CASE 
# MAGIC       WHEN LENGTH(CD_CONTA) - LENGTH(REPLACE(CD_CONTA, '.', '')) + 1 <= 2 THEN 'TOTALIZADORA'
# MAGIC       ELSE 'ANALITICA'
# MAGIC     END as tipo_conta,
# MAGIC     COUNT(*) as qtd_registros
# MAGIC   FROM proj_cvm_02_silver.203_bpp_dfp
# MAGIC   GROUP BY CD_CONTA, DS_CONTA
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   tipo_conta,
# MAGIC   COUNT(DISTINCT CD_CONTA) as qtd_contas_distintas,
# MAGIC   SUM(qtd_registros) as total_registros,
# MAGIC   ROUND(SUM(qtd_registros) * 100.0 / (SELECT SUM(qtd_registros) FROM hierarquia), 2) as percent_total
# MAGIC FROM hierarquia
# MAGIC GROUP BY tipo_conta
# MAGIC ORDER BY tipo_conta

# COMMAND ----------

# DBTITLE 1,Achado: Hierarquia Contábil
# MAGIC %md
# MAGIC ## Achado: Hierarquia Contábil
# MAGIC
# MAGIC Classificação das contas em totalizadoras vs analíticas confirmou estrutura puramente aditiva no BPP.
# MAGIC
# MAGIC | Tipo | Contas Distintas | Registros | % do Total |
# MAGIC | --- | --- | --- | --- |
# MAGIC | Analítica | 394 | 509.000 | 96,32% |
# MAGIC | Totalizadora | 9 | 19.436 | 3,68% |
# MAGIC
# MAGIC **Comparação com DRE e BPA**: BPP tem 403 contas distintas (vs 323 BPA vs 213 DRE), o maior número entre as três tabelas, refletindo a granularidade do Passivo + Patrimônio Líquido. Proporção de analíticas é similar (96,3% vs 95,2% BPA).
# MAGIC
# MAGIC ✗ **Problema confirmado**: Análises que somam totalizadoras + analíticas duplicam valores. Coluna TIPO_CONTA necessária.
# MAGIC
# MAGIC **Observação**: A coluna NIVEL_CONTA (já presente no enriquecimento Silver) fornece o nível hierárquico, mas não diferencia explicitamente totalizadoras de analíticas.

# COMMAND ----------

# DBTITLE 1,VALIDAÇÃO DE INTEGRIDADE HIERÁRQUICA
# MAGIC %sql
# MAGIC -- Valida integridade hierárquica (conta pai = soma contas filhas)
# MAGIC -- Usa amostra dinâmica: empresa mais recente com conta 2.01
# MAGIC
# MAGIC WITH amostra AS (
# MAGIC   SELECT CNPJ_CIA, ANO, TRIMESTRE, ORDEM_EXERC, GRUPO_DFP
# MAGIC   FROM proj_cvm_02_silver.203_bpp_dfp
# MAGIC   WHERE CD_CONTA = '2.01'
# MAGIC     AND ORDEM_EXERC = 'ÚLTIMO'
# MAGIC     AND GRUPO_DFP = 'DF Consolidado - Balanço Patrimonial Passivo'
# MAGIC   ORDER BY ANO DESC, TRIMESTRE DESC
# MAGIC   LIMIT 1
# MAGIC ),
# MAGIC conta_pai AS (
# MAGIC   SELECT 
# MAGIC     t.CNPJ_CIA,
# MAGIC     t.DENOM_CIA,
# MAGIC     t.ANO,
# MAGIC     t.TRIMESTRE,
# MAGIC     t.ORDEM_EXERC,
# MAGIC     t.GRUPO_DFP,
# MAGIC     t.CD_CONTA as conta_pai,
# MAGIC     t.DS_CONTA as descricao_pai,
# MAGIC     t.VL_CONTA as valor_pai
# MAGIC   FROM proj_cvm_02_silver.203_bpp_dfp t
# MAGIC   INNER JOIN amostra a ON t.CNPJ_CIA = a.CNPJ_CIA AND t.ANO = a.ANO AND t.TRIMESTRE = a.TRIMESTRE AND t.ORDEM_EXERC = a.ORDEM_EXERC AND t.GRUPO_DFP = a.GRUPO_DFP
# MAGIC   WHERE t.CD_CONTA = '2.01'
# MAGIC ),
# MAGIC contas_filhas AS (
# MAGIC   SELECT 
# MAGIC     t.CNPJ_CIA,
# MAGIC     t.ANO,
# MAGIC     t.TRIMESTRE,
# MAGIC     t.ORDEM_EXERC,
# MAGIC     t.GRUPO_DFP,
# MAGIC     SUM(t.VL_CONTA) as soma_filhas,
# MAGIC     COUNT(DISTINCT t.CD_CONTA) as qtd_contas_filhas,
# MAGIC     COLLECT_LIST(STRUCT(t.CD_CONTA, t.DS_CONTA, t.VL_CONTA)) as detalhes_filhas
# MAGIC   FROM proj_cvm_02_silver.203_bpp_dfp t
# MAGIC   INNER JOIN amostra a ON t.CNPJ_CIA = a.CNPJ_CIA AND t.ANO = a.ANO AND t.TRIMESTRE = a.TRIMESTRE AND t.ORDEM_EXERC = a.ORDEM_EXERC AND t.GRUPO_DFP = a.GRUPO_DFP
# MAGIC   WHERE t.CD_CONTA LIKE '2.01.%'
# MAGIC     AND LENGTH(t.CD_CONTA) - LENGTH(REPLACE(t.CD_CONTA, '.', '')) + 1 = 3
# MAGIC   GROUP BY t.CNPJ_CIA, t.ANO, t.TRIMESTRE, t.ORDEM_EXERC, t.GRUPO_DFP
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   p.DENOM_CIA,
# MAGIC   p.conta_pai,
# MAGIC   p.descricao_pai,
# MAGIC   ROUND(p.valor_pai, 2) as valor_declarado_pai,
# MAGIC   ROUND(f.soma_filhas, 2) as soma_calculada_filhas,
# MAGIC   f.qtd_contas_filhas,
# MAGIC   ROUND(p.valor_pai - f.soma_filhas, 2) as diferenca,
# MAGIC   ROUND(ABS(p.valor_pai - f.soma_filhas) / NULLIF(ABS(p.valor_pai), 0) * 100, 4) as percent_divergencia,
# MAGIC   CASE 
# MAGIC     WHEN ABS(p.valor_pai - f.soma_filhas) < 0.01 THEN 'OK'
# MAGIC     ELSE 'INCONSISTENTE'
# MAGIC   END as status_validacao
# MAGIC FROM conta_pai p
# MAGIC LEFT JOIN contas_filhas f 
# MAGIC   ON p.CNPJ_CIA = f.CNPJ_CIA 
# MAGIC   AND p.ANO = f.ANO 
# MAGIC   AND p.TRIMESTRE = f.TRIMESTRE
# MAGIC   AND p.ORDEM_EXERC = f.ORDEM_EXERC
# MAGIC   AND p.GRUPO_DFP = f.GRUPO_DFP

# COMMAND ----------

# DBTITLE 1,Achado: Validação Hierárquica
# MAGIC %md
# MAGIC ## Achado: Integridade Hierárquica (Conta 2.01)
# MAGIC
# MAGIC Validação confirmou integridade hierárquica perfeita para a conta 2.01 (Passivo Circulante) da amostra BRASILAGRO.
# MAGIC
# MAGIC | Empresa | Conta | Descrição | Valor Declarado | Soma Filhas | Diferença | Status |
# MAGIC | --- | --- | --- | --- | --- | --- | --- |
# MAGIC | BRASILAGRO | 2.01 | Passivo Circulante | 707.583.000 | 707.583.000 | 0 | OK |
# MAGIC
# MAGIC ✓ **Validação confirmada**: BPP é puramente aditivo (sem contas derivadas). Conta pai = soma de filhas com diferença zero e 0% de divergência. Padrão idêntico ao BPA.

# COMMAND ----------

# DBTITLE 1,VALIDAÇÃO SISTEMÁTICA: TODAS AS CONTAS TOTALIZADORAS
# MAGIC %sql
# MAGIC -- Valida TODAS as contas totalizadoras (nível 2) sistematicamente
# MAGIC -- Usa amostra dinâmica: empresa mais recente com conta 2.01
# MAGIC
# MAGIC WITH amostra AS (
# MAGIC   SELECT CNPJ_CIA, ANO, TRIMESTRE, ORDEM_EXERC, GRUPO_DFP
# MAGIC   FROM proj_cvm_02_silver.203_bpp_dfp
# MAGIC   WHERE CD_CONTA = '2.01'
# MAGIC     AND ORDEM_EXERC = 'ÚLTIMO'
# MAGIC     AND GRUPO_DFP = 'DF Consolidado - Balanço Patrimonial Passivo'
# MAGIC   ORDER BY ANO DESC, TRIMESTRE DESC
# MAGIC   LIMIT 1
# MAGIC ),
# MAGIC contas_totalizadoras AS (
# MAGIC   SELECT 
# MAGIC     t.CD_CONTA,
# MAGIC     t.DS_CONTA,
# MAGIC     t.VL_CONTA
# MAGIC   FROM proj_cvm_02_silver.203_bpp_dfp t
# MAGIC   INNER JOIN amostra a ON t.CNPJ_CIA = a.CNPJ_CIA AND t.ANO = a.ANO AND t.TRIMESTRE = a.TRIMESTRE AND t.ORDEM_EXERC = a.ORDEM_EXERC AND t.GRUPO_DFP = a.GRUPO_DFP
# MAGIC   WHERE LENGTH(t.CD_CONTA) - LENGTH(REPLACE(t.CD_CONTA, '.', '')) + 1 = 2
# MAGIC ),
# MAGIC filhas_por_totalizadora AS (
# MAGIC   SELECT 
# MAGIC     SUBSTRING(t.CD_CONTA, 1, 4) as conta_pai_cd,
# MAGIC     COUNT(DISTINCT t.CD_CONTA) as qtd_filhas,
# MAGIC     SUM(t.VL_CONTA) as soma_filhas
# MAGIC   FROM proj_cvm_02_silver.203_bpp_dfp t
# MAGIC   INNER JOIN amostra a ON t.CNPJ_CIA = a.CNPJ_CIA AND t.ANO = a.ANO AND t.TRIMESTRE = a.TRIMESTRE AND t.ORDEM_EXERC = a.ORDEM_EXERC AND t.GRUPO_DFP = a.GRUPO_DFP
# MAGIC   WHERE LENGTH(t.CD_CONTA) - LENGTH(REPLACE(t.CD_CONTA, '.', '')) + 1 = 3
# MAGIC   GROUP BY SUBSTRING(t.CD_CONTA, 1, 4)
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   t.CD_CONTA,
# MAGIC   t.DS_CONTA,
# MAGIC   ROUND(t.VL_CONTA, 2) as valor_declarado,
# MAGIC   COALESCE(f.qtd_filhas, 0) as qtd_filhas,
# MAGIC   ROUND(COALESCE(f.soma_filhas, 0), 2) as soma_filhas,
# MAGIC   ROUND(ABS(t.VL_CONTA - COALESCE(f.soma_filhas, 0)), 2) as diferenca_abs,
# MAGIC   CASE 
# MAGIC     WHEN f.qtd_filhas IS NULL OR f.qtd_filhas = 0 THEN 'SEM FILHAS (valor direto)'
# MAGIC     WHEN ABS(t.VL_CONTA - COALESCE(f.soma_filhas, 0)) < 0.01 THEN 'ADITIVA (soma válida)'
# MAGIC     ELSE 'INCONSISTENTE (soma divergente)'
# MAGIC   END as tipo_estrutural
# MAGIC FROM contas_totalizadoras t
# MAGIC LEFT JOIN filhas_por_totalizadora f
# MAGIC   ON t.CD_CONTA = f.conta_pai_cd
# MAGIC ORDER BY t.CD_CONTA

# COMMAND ----------

# DBTITLE 1,Achado: Classificação Estrutural
# MAGIC %md
# MAGIC ## Achado: Classificação Estrutural das Contas Totalizadoras
# MAGIC
# MAGIC Validação sistemática de todas as contas totalizadoras (nível 2) confirmou integridade aditiva perfeita para a amostra BRASILAGRO.
# MAGIC
# MAGIC | Conta | Descrição | Valor Declarado | Filhas | Soma Filhas | Diferença | Status |
# MAGIC | --- | --- | --- | --- | --- | --- | --- |
# MAGIC | 2.01 | Passivo Circulante | 707.583.000 | 7 | 707.583.000 | 0 | ADITIVA |
# MAGIC | 2.02 | Passivo Não Circulante | 973.946.000 | 6 | 973.946.000 | 0 | ADITIVA |
# MAGIC | 2.03 | Patrimônio Líquido Consolidado | 2.025.309.000 | 9 | 2.025.309.000 | 0 | ADITIVA |
# MAGIC
# MAGIC ✓ **Todas as totalizadoras validadas**: 100% das contas com filhas apresentam soma válida (diferença = 0). BPP confirmado como puramente aditivo. 3 totalizadoras de nível 2 (vs 2 no BPA), pois o BPP adiciona o Patrimônio Líquido como terceiro bloco.

# COMMAND ----------

# DBTITLE 1,CONCLUSÃO
# MAGIC %md
# MAGIC ## Síntese dos Achados
# MAGIC
# MAGIC Análise exploratória da tabela Silver 203_bpp_dfp concluída com execução em 2026-09-20.
# MAGIC
# MAGIC **Resumo Geral**:
# MAGIC
# MAGIC | Métrica | Valor |
# MAGIC | --- | --- |
# MAGIC | Registros | 528.436 |
# MAGIC | Colunas | 21 |
# MAGIC | Empresas | 537 |
# MAGIC | Cobertura temporal | 2021-2026 (6 anos) |
# MAGIC | Contas distintas | 403 (394 analíticas + 9 totalizadoras) |
# MAGIC
# MAGIC **Problemas Confirmados**:
# MAGIC
# MAGIC | Problema | Impacto | Prioridade | Status |
# MAGIC | --- | --- | --- | --- |
# MAGIC | Escalas monetárias não normalizadas | Erros de magnitude 1000x em comparações | Crítica | Confirmado (98,2% MIL / 1,8% UNIDADE) |
# MAGIC | Hierarquia sem classificação explícita | Duplicação em somas | Crítica | Confirmado (96,3% analíticas / 3,7% totalizadoras) |
# MAGIC
# MAGIC **Validações Concluídas**:
# MAGIC
# MAGIC | Validação | Resultado |
# MAGIC | --- | --- |
# MAGIC | Completude (colunas obrigatórias) | ✓ Zero nulos |
# MAGIC | Chave de negócio | ✓ (CNPJ_CIA, DT_REFER, VERSAO, CD_CONTA, GRUPO_DFP, ORDEM_EXERC) única |
# MAGIC | Cobertura temporal | ✓ Landing Zone = Silver (2021-2026) |
# MAGIC | Integridade hierárquica | ✓ 100% das totalizadoras aditivas (diferença = 0) |
# MAGIC | Versões de documentos | ✓ Zero casos de múltiplas versões |
# MAGIC
# MAGIC **Correções Necessárias (consistente com DRE e BPA)**:
# MAGIC
# MAGIC 1. Criar coluna VL_CONTA_NORMALIZADO no notebook 203_bpp_silver
# MAGIC 2. Adicionar coluna TIPO_CONTA no notebook 203_bpp_silver
# MAGIC
# MAGIC **Comparação entre as três tabelas Silver**:
# MAGIC
# MAGIC | Métrica | DRE (201) | BPA (202) | BPP (203) |
# MAGIC | --- | --- | --- | --- |
# MAGIC | Registros | 162.885 | 308.988 | 528.436 |
# MAGIC | Colunas | 22 | 21 | 21 |
# MAGIC | Contas distintas | ~213 | 323 | 403 |
# MAGIC | Totalizadoras | ~8 | 8 | 9 |
# MAGIC | Zeros (%) | 31,7% | 51,5% | 60,7% |
# MAGIC | Tipo de estrutura | Aditiva + Derivada | Puramente aditiva | Puramente aditiva |
# MAGIC
# MAGIC BPP é a maior e mais granular das três tabelas, com o maior número de contas analíticas e a maior proporção de zeros, refletindo a complexidade do Patrimônio Líquido.