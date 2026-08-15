# Databricks notebook source
# DBTITLE 1,DOCUMENTAÇÃO
# MAGIC %md
# MAGIC # Documentação de Metadados - COMMENT ON TABLE/COLUMN
# MAGIC
# MAGIC ## Objetivo
# MAGIC Documentar tabelas e colunas do projeto no catálogo Unity Catalog via comandos `COMMENT ON`. Preenche metadados que aparecem na interface do Databricks, facilitando descoberta de dados, governança e onboarding de novos usuários.
# MAGIC
# MAGIC ## Abordagem
# MAGIC * **Metadados de tabela**: Descrição de propósito, origem, frequência, estratégia de processamento
# MAGIC * **Metadados de coluna**: Descrição de cada campo (significado de negócio, formato, origem)
# MAGIC * **Cobertura**: Todas as tabelas Bronze e Silver (DRE e BPA)
# MAGIC
# MAGIC ## Estrutura
# MAGIC Para cada tabela:
# MAGIC 1. `COMMENT ON TABLE` com:
# MAGIC    - Demonstração financeira (DRE/BPA)
# MAGIC    - Origem dos dados (portal CVM, estrutura original)
# MAGIC    - Transformações aplicadas (para Silver)
# MAGIC    - Frequência e estratégia de gravação
# MAGIC 2. Loop de `COMMENT ON COLUMN` para cada campo:
# MAGIC    - Colunas de negócio (CNPJ_CIA, DT_REFER, VL_CONTA, etc.)
# MAGIC    - Colunas técnicas (_versao_ingestao, _last_modified_cvm, etc.)
# MAGIC
# MAGIC ## Benefícios
# MAGIC * **Self-service**: Analistas entendem tabelas sem precisar consultar documentação externa
# MAGIC * **Governança**: Metadados visíveis no Unity Catalog Explorer para auditoria
# MAGIC * **Descoberta**: Busca por descrições no catálogo ajuda a encontrar dados relevantes
# MAGIC
# MAGIC ## Função
# MAGIC Script de apoio - Governança e documentação de dados

# COMMAND ----------

# DBTITLE 1,DOCUMENTAÇÃO DA TABELA BRONZE - 101_dre_dfp
# Documentação da tabela bronze
spark.sql("""
COMMENT ON TABLE proj_cvm_01_bronze.101_dre_dfp IS 
'Demonstração do Resultado do Exercício (DRE) - Dados brutos extraídos do portal de Dados Abertos da CVM. 
Mantém a estrutura original dos arquivos DFP (Demonstrações Financeiras Padronizadas) publicados pelas companhias abertas brasileiras.
Frequência: Anual | Fonte: Portal CVM | Processamento: APPEND incremental (com versionamento) | Metadados: _versao_ingestao, _last_modified_cvm, _ingest_ts, _source_file'
""")

# Documentação das colunas bronze
for col, desc in [
    ('CNPJ_CIA', 'CNPJ da companhia (formato: XX.XXX.XXX/XXXX-XX)'),
    ('DT_REFER', 'Data de referência do documento (formato: YYYY-MM-DD)'),
    ('VERSAO', 'Versão do documento (ex: Original, Reapresentação)'),
    ('DENOM_CIA', 'Denominação social da companhia (razão social)'),
    ('CD_CVM', 'Código CVM da companhia (identificador único na CVM)'),
    ('GRUPO_DFP', 'Grupo da demonstração (ex: DF Consolidadas, DF Individuais)'),
    ('MOEDA', 'Moeda utilizada na demonstração (ex: REAL)'),
    ('ESCALA_MOEDA', 'Escala de valores (ex: UNIDADE, MIL, MILHAO)'),
    ('ORDEM_EXERC', 'Ordem do exercício (ex: ÚNICO, PENULTIMO)'),
    ('DT_INI_EXERC', 'Data de início do exercício fiscal (formato: YYYY-MM-DD)'),
    ('DT_FIM_EXERC', 'Data de fim do exercício fiscal (formato: YYYY-MM-DD)'),
    ('CD_CONTA', 'Código da conta contábil (estrutura hierárquica com pontos)'),
    ('DS_CONTA', 'Descrição da conta contábil (ex: Receita de Venda de Bens e/ou Serviços)'),
    ('VL_CONTA', 'Valor da conta contábil (string conforme extraído da CVM)'),
    ('ST_CONTA_FIXA', 'Indica se a conta é fixa (S/N) - controla se a conta aparece sempre na estrutura padrão'),
    ('_versao_ingestao', 'Versão da ingestão (incrementa a cada nova carga do mesmo arquivo)'),
    ('_last_modified_cvm', 'Last-Modified header do arquivo na CVM (usado para detectar atualizações)'),
    ('_ingest_ts', 'Timestamp de ingestão dos dados (metadado técnico)'),
    ('_source_file', 'Nome do arquivo de origem da CVM (metadado técnico)')
]:
    spark.sql(f"COMMENT ON COLUMN proj_cvm_01_bronze.101_dre_dfp.{col} IS '{desc}'")

print("✅ Tabela bronze documentada")

# COMMAND ----------

# DBTITLE 1,DOCUMENTAÇÃO DA TABELA SILVER - 201_dre_dfp
# Documentação da tabela silver
spark.sql("""
COMMENT ON TABLE proj_cvm_02_silver.201_dre_dfp IS 
'Demonstração do Resultado do Exercício (DRE) - Dados transformados e enriquecidos. 
Transformações aplicadas: (1) Conversão de tipos de dados, (2) Padronização de CNPJ, (3) Remoção de duplicados, 
(4) Tratamento de nulls críticos, (5) Enriquecimento com colunas temporais (ANO, TRIMESTRE, MES). 
Qualidade: Dados limpos e prontos para análise | Origem: proj_cvm_01_bronze.101_dre_dfp | Processamento: MERGE incremental | Particionamento: ANO'
""")

# Documentação das colunas silver
for col, desc in [
    ('CNPJ_CIA', 'CNPJ da companhia (apenas dígitos, sem formatação)'),
    ('DT_REFER', 'Data de referência do documento (tipo DATE)'),
    ('VERSAO', 'Versão do documento (ex: Original, Reapresentação)'),
    ('DENOM_CIA', 'Denominação social da companhia (razão social)'),
    ('CD_CVM', 'Código CVM da companhia (STRING)'),
    ('GRUPO_DFP', 'Grupo da demonstração (ex: DF Consolidadas, DF Individuais)'),
    ('MOEDA', 'Moeda utilizada na demonstração (ex: REAL)'),
    ('ESCALA_MOEDA', 'Escala de valores (ex: UNIDADE, MIL, MILHAO)'),
    ('ORDEM_EXERC', 'Ordem do exercício (ex: ÚNICO, PENULTIMO)'),
    ('DT_INI_EXERC', 'Data de início do exercício fiscal (tipo DATE)'),
    ('DT_FIM_EXERC', 'Data de fim do exercício fiscal (tipo DATE)'),
    ('CD_CONTA', 'Código da conta contábil (estrutura hierárquica com pontos)'),
    ('DS_CONTA', 'Descrição da conta contábil (ex: Receita de Venda de Bens e/ou Serviços)'),
    ('VL_CONTA', 'Valor da conta contábil (tipo DOUBLE)'),
    ('ANO', 'Ano extraído de DT_REFER (tipo INT) - Coluna de particionamento para MERGE incremental eficiente'),
    ('TRIMESTRE', 'Trimestre extraído de DT_REFER (tipo INT, valores: 1-4) - Facilita análises trimestrais'),
    ('MES', 'Mês extraído de DT_REFER (tipo INT, valores: 1-12) - Facilita análises mensais'),
    ('DT_PROCESSAMENTO', 'Timestamp de processamento da transformação (tipo TIMESTAMP) - Rastreabilidade de quando o dado foi processado')
]:
    spark.sql(f"COMMENT ON COLUMN proj_cvm_02_silver.201_dre_dfp.{col} IS '{desc}'")

print("✅ Tabela silver documentada")

# COMMAND ----------

# DBTITLE 1,DOCUMENTAÇÃO DA TABELA BRONZE - 102_bpa_dfp
# Documentação da tabela bronze BPA
spark.sql("""
COMMENT ON TABLE proj_cvm_01_bronze.102_bpa_dfp IS 
'Balanço Patrimonial Ativo (BPA) - Dados brutos extraídos do portal de Dados Abertos da CVM. 
Mantém a estrutura original dos arquivos DFP (Demonstrações Financeiras Padronizadas) publicados pelas companhias abertas brasileiras.
Frequência: Anual | Fonte: Portal CVM | Processamento: APPEND incremental (com versionamento) | Metadados: _versao_ingestao, _last_modified_cvm, _ingest_ts, _source_file'
""")

# Documentação das colunas bronze BPA
for col, desc in [
    ('CNPJ_CIA', 'CNPJ da companhia (formato: XX.XXX.XXX/XXXX-XX)'),
    ('DT_REFER', 'Data de referência do documento (formato: YYYY-MM-DD)'),
    ('VERSAO', 'Versão do documento (ex: Original, Reapresentação)'),
    ('DENOM_CIA', 'Denominação social da companhia (razão social)'),
    ('CD_CVM', 'Código CVM da companhia (identificador único na CVM)'),
    ('GRUPO_DFP', 'Grupo da demonstração (ex: DF Consolidadas, DF Individuais)'),
    ('MOEDA', 'Moeda utilizada na demonstração (ex: REAL)'),
    ('ESCALA_MOEDA', 'Escala de valores (ex: UNIDADE, MIL, MILHAO)'),
    ('ORDEM_EXERC', 'Ordem do exercício (ex: ÚNICO, PENULTIMO)'),
    ('DT_FIM_EXERC', 'Data de fim do exercício fiscal (formato: YYYY-MM-DD)'),
    ('CD_CONTA', 'Código da conta contábil (estrutura hierárquica com pontos)'),
    ('DS_CONTA', 'Descrição da conta contábil (ex: Ativo Total, Ativo Circulante)'),
    ('VL_CONTA', 'Valor da conta contábil (string conforme extraído da CVM)'),
    ('ST_CONTA_FIXA', 'Indica se a conta é fixa (S/N) - controla se a conta aparece sempre na estrutura padrão'),
    ('_versao_ingestao', 'Versão da ingestão (incrementa a cada nova carga do mesmo arquivo)'),
    ('_last_modified_cvm', 'Last-Modified header do arquivo na CVM (usado para detectar atualizações)'),
    ('_ingest_ts', 'Timestamp de ingestão dos dados (metadado técnico)'),
    ('_source_file', 'Nome do arquivo de origem da CVM (metadado técnico)')
]:
    spark.sql(f"COMMENT ON COLUMN proj_cvm_01_bronze.102_bpa_dfp.{col} IS '{desc}'")

print("✅ Tabela bronze 102_bpa_dfp documentada")

# COMMAND ----------

# DBTITLE 1,DOCUMENTAÇÃO DA TABELA SILVER - 202_bpa_dfp
# Documentação da tabela silver BPA
spark.sql("""
COMMENT ON TABLE proj_cvm_02_silver.202_bpa_dfp IS 
'Balanço Patrimonial Ativo (BPA) - Dados transformados e enriquecidos. 
Transformações aplicadas: (1) Conversão de tipos de dados, (2) Padronização de CNPJ, (3) Remoção de duplicados, 
(4) Tratamento de nulls críticos, (5) Enriquecimento com colunas temporais (ANO, TRIMESTRE, MES). 
Qualidade: Dados limpos e prontos para análise | Origem: proj_cvm_01_bronze.102_bpa_dfp | Processamento: DELETE+APPEND incremental | Particionamento: ANO'
""")

# Documentação das colunas silver BPA
for col, desc in [
    ('CNPJ_CIA', 'CNPJ da companhia (apenas dígitos, sem formatação)'),
    ('DT_REFER', 'Data de referência do documento (tipo DATE)'),
    ('VERSAO', 'Versão do documento (tipo INT)'),
    ('DENOM_CIA', 'Denominação social da companhia (razão social)'),
    ('CD_CVM', 'Código CVM da companhia (tipo INT)'),
    ('GRUPO_DFP', 'Grupo da demonstração (ex: DF Consolidadas, DF Individuais)'),
    ('MOEDA', 'Moeda utilizada na demonstração (ex: REAL)'),
    ('ESCALA_MOEDA', 'Escala de valores (ex: UNIDADE, MIL, MILHAO)'),
    ('ORDEM_EXERC', 'Ordem do exercício (ex: ÚNICO, PENULTIMO)'),
    ('DT_FIM_EXERC', 'Data de fim do exercício fiscal (tipo DATE)'),
    ('CD_CONTA', 'Código da conta contábil (estrutura hierárquica com pontos)'),
    ('DS_CONTA', 'Descrição da conta contábil (ex: Ativo Total, Ativo Circulante)'),
    ('VL_CONTA', 'Valor da conta contábil (tipo DOUBLE)'),
    ('ANO', 'Ano extraído de DT_REFER (tipo INT) - Coluna de particionamento'),
    ('TRIMESTRE', 'Trimestre extraído de DT_REFER (tipo INT, valores: 1-4)'),
    ('MES', 'Mês extraído de DT_REFER (tipo INT, valores: 1-12)'),
    ('DT_PROCESSAMENTO', 'Timestamp de processamento da transformação (tipo TIMESTAMP)')
]:
    spark.sql(f"COMMENT ON COLUMN proj_cvm_02_silver.202_bpa_dfp.{col} IS '{desc}'")

print("✅ Tabela silver 202_bpa_dfp documentada")

# COMMAND ----------

# DBTITLE 1,DOCUMENTAÇÃO DA TABELA BRONZE - 103_bpp_dfp
# Documentação da tabela bronze BPP
spark.sql("""
COMMENT ON TABLE proj_cvm_01_bronze.103_bpp_dfp IS 
'Balanço Patrimonial Passivo (BPP) - Dados brutos extraídos do portal de Dados Abertos da CVM. 
Mantém a estrutura original dos arquivos DFP (Demonstrações Financeiras Padronizadas) publicados pelas companhias abertas brasileiras.
Frequência: Anual | Fonte: Portal CVM | Processamento: APPEND incremental (com versionamento) | Metadados: _versao_ingestao, _last_modified_cvm, _ingest_ts, _source_file'
""")

# Documentação das colunas bronze BPP
for col, desc in [
    ('CNPJ_CIA', 'CNPJ da companhia (formato: XX.XXX.XXX/XXXX-XX)'),
    ('DT_REFER', 'Data de referência do documento (formato: YYYY-MM-DD)'),
    ('VERSAO', 'Versão do documento (ex: Original, Reapresentação)'),
    ('DENOM_CIA', 'Denominação social da companhia (razão social)'),
    ('CD_CVM', 'Código CVM da companhia (identificador único na CVM)'),
    ('GRUPO_DFP', 'Grupo da demonstração (ex: DF Consolidadas, DF Individuais)'),
    ('MOEDA', 'Moeda utilizada na demonstração (ex: REAL)'),
    ('ESCALA_MOEDA', 'Escala de valores (ex: UNIDADE, MIL, MILHAO)'),
    ('ORDEM_EXERC', 'Ordem do exercício (ex: ÚNICO, PENULTIMO)'),
    ('DT_FIM_EXERC', 'Data de fim do exercício fiscal (formato: YYYY-MM-DD)'),
    ('CD_CONTA', 'Código da conta contábil (estrutura hierárquica com pontos)'),
    ('DS_CONTA', 'Descrição da conta contábil (ex: Passivo Total, Passivo Circulante, Patrimônio Líquido)'),
    ('VL_CONTA', 'Valor da conta contábil (string conforme extraído da CVM)'),
    ('ST_CONTA_FIXA', 'Indica se a conta é fixa (S/N) - controla se a conta aparece sempre na estrutura padrão'),
    ('_versao_ingestao', 'Versão da ingestão (incrementa a cada nova carga do mesmo arquivo)'),
    ('_last_modified_cvm', 'Last-Modified header do arquivo na CVM (usado para detectar atualizações)'),
    ('_ingest_ts', 'Timestamp de ingestão dos dados (metadado técnico)'),
    ('_source_file', 'Nome do arquivo de origem da CVM (metadado técnico)')
]:
    spark.sql(f"COMMENT ON COLUMN proj_cvm_01_bronze.103_bpp_dfp.{col} IS '{desc}'")

print("✅ Tabela bronze 103_bpp_dfp documentada")

# COMMAND ----------

# DBTITLE 1,DOCUMENTAÇÃO DA TABELA SILVER - 203_bpp_dfp
# Documentação da tabela silver BPP
spark.sql("""
COMMENT ON TABLE proj_cvm_02_silver.203_bpp_dfp IS 
'Balanço Patrimonial Passivo (BPP) - Dados transformados e enriquecidos. 
Transformações aplicadas: (1) Conversão de tipos de dados, (2) Padronização de CNPJ, (3) Remoção de duplicados, 
(4) Tratamento de nulls críticos, (5) Enriquecimento com colunas temporais (ANO, TRIMESTRE, MES). 
Qualidade: Dados limpos e prontos para análise | Origem: proj_cvm_01_bronze.103_bpp_dfp | Processamento: DELETE+APPEND incremental | Particionamento: ANO'
""")

# Documentação das colunas silver BPP
for col, desc in [
    ('CNPJ_CIA', 'CNPJ da companhia (apenas dígitos, sem formatação)'),
    ('DT_REFER', 'Data de referência do documento (tipo DATE)'),
    ('VERSAO', 'Versão do documento (tipo INT)'),
    ('DENOM_CIA', 'Denominação social da companhia (razão social)'),
    ('CD_CVM', 'Código CVM da companhia (tipo INT)'),
    ('GRUPO_DFP', 'Grupo da demonstração (ex: DF Consolidadas, DF Individuais)'),
    ('MOEDA', 'Moeda utilizada na demonstração (ex: REAL)'),
    ('ESCALA_MOEDA', 'Escala de valores (ex: UNIDADE, MIL, MILHAO)'),
    ('ORDEM_EXERC', 'Ordem do exercício (ex: ÚNICO, PENULTIMO)'),
    ('DT_FIM_EXERC', 'Data de fim do exercício fiscal (tipo DATE)'),
    ('CD_CONTA', 'Código da conta contábil (estrutura hierárquica com pontos)'),
    ('DS_CONTA', 'Descrição da conta contábil (ex: Passivo Total, Passivo Circulante, Patrimônio Líquido)'),
    ('VL_CONTA', 'Valor da conta contábil (tipo DOUBLE)'),
    ('ANO', 'Ano extraído de DT_REFER (tipo INT) - Coluna de particionamento'),
    ('TRIMESTRE', 'Trimestre extraído de DT_REFER (tipo INT, valores: 1-4)'),
    ('MES', 'Mês extraído de DT_REFER (tipo INT, valores: 1-12)'),
    ('DT_PROCESSAMENTO', 'Timestamp de processamento da transformação (tipo TIMESTAMP)')
]:
    spark.sql(f"COMMENT ON COLUMN proj_cvm_02_silver.203_bpp_dfp.{col} IS '{desc}'")

print("✅ Tabela silver 203_bpp_dfp documentada")