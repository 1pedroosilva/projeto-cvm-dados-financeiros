# Databricks notebook source
# DBTITLE 1,DOCUMENTAÇÃO
# MAGIC %md
# MAGIC # Orquestrador de Pipeline - Detecção Inteligente de Períodos
# MAGIC
# MAGIC ## Objetivo
# MAGIC Coordenar a execução do pipeline identificando automaticamente quais anos precisam ser processados (novos ou atualizados). Detecta mudanças na fonte CVM comparando metadados HTTP (`Last-Modified`) com a tabela de controle de ingestão.
# MAGIC
# MAGIC ## Estratégia de Detecção
# MAGIC * **Modo Automático**: Consulta tabela de controle (`proj_cvm_04_apoio.controle_ingestao`) e compara com metadados da Landing Zone para identificar:
# MAGIC   - Novos anos disponíveis na CVM
# MAGIC   - Arquivos atualizados (via `last_modified` do header HTTP)
# MAGIC   - Anos nunca processados
# MAGIC * **Modo Manual (Override)**: Widget permite forçar processamento de anos específicos para reprocessamento ou correções
# MAGIC
# MAGIC ## Algoritmo
# MAGIC 1. Ler widget `anos_override` (se vazio, usar detecção automática)
# MAGIC 2. Para cada fonte DFP (DRE, BPA):
# MAGIC    - Chamar `get_anos_para_processar_inteligente()`
# MAGIC    - Consolidar anos pendentes de todas as fontes
# MAGIC 3. Definir `ANOS_PROCESSAR` (variável usada por notebooks downstream)
# MAGIC 4. Exibir resumo e próximos passos do pipeline
# MAGIC
# MAGIC ## Função
# MAGIC Script de apoio - Orquestração e coordenação do pipeline

# COMMAND ----------

# DBTITLE 1,INICIALIZAÇÃO E IMPORTS
# Carregar configurações centralizadas do pipeline
%run ./config_parametros

# Inicializar ANOS_PROCESSAR com detecção inteligente
inicializar_anos_processar()

# COMMAND ----------

# DBTITLE 1,WIDGET DE OVERRIDE MANUAL
# Permite forçar processamento de anos específicos (separados por vírgula)
# Deixar vazio para detecção automática
dbutils.widgets.text("anos_override", "", "Anos para Processar (override)")

anos_override_str = dbutils.widgets.get("anos_override").strip()

# COMMAND ----------

# DBTITLE 1,DETECÇÃO INTELIGENTE DE ANOS
# O config_parametros já definiu ANOS_PROCESSAR automaticamente
# Aqui apenas validamos e permitimos override via widget se necessário

print("="*80)
print("ORQUESTRADOR - VALIDAÇÃO DE PERÍODOS")
print("="*80)

print(f"\n📋 ANOS_PROCESSAR do config_parametros: {ANOS_PROCESSAR}")
print(f"   Janela temporal: últimos {JANELA_ANOS_RELEVANTE} anos")

# Se há override manual via widget, sobrescrever
if anos_override_str:
    ANOS_PROCESSAR = [int(ano.strip()) for ano in anos_override_str.split(",")]
    print(f"\n🔧 OVERRIDE MANUAL ATIVO (widget)")
    print(f"   Anos sobrescritos: {ANOS_PROCESSAR}")

# Resultado final
print(f"\n{'='*80}")
print(f"ANOS FINAIS PARA PIPELINE: {ANOS_PROCESSAR}")
print(f"{'='*80}")

if not ANOS_PROCESSAR:
    print("\n⚠️  ATENÇÃO: Nenhum ano para processar")
    print("   - Todos os anos já estão atualizados")
    print("   - Use widget 'anos_override' para forçar reprocessamento")

# COMMAND ----------

# DBTITLE 1,RESUMO DA EXECUÇÃO
# Apresenta decisões do orquestrador
from datetime import datetime

print("="*80)
print(f"RESUMO - ORQUESTRADOR PIPELINE CVM")
print(f"Execução: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)
print(f"\nModo: {'OVERRIDE MANUAL' if anos_override_str else 'DETECÇÃO AUTOMÁTICA'}")
print(f"Anos a processar: {ANOS_PROCESSAR if ANOS_PROCESSAR else 'Nenhum'}")
print(f"Quantidade: {len(ANOS_PROCESSAR) if ANOS_PROCESSAR else 0} ano(s)")
print(f"\nPróximos passos:")
if ANOS_PROCESSAR:
    print(f"  1. Executar notebook: 002_download_cvm_para_landing")
    print(f"  2. Executar notebooks Bronze: 101_cvm_dfp_dre, 102_cvm_dfp_bpa")
    print(f"  3. Executar notebooks Silver: 201_cvm_dfp_dre, 202_cvm_dfp_bpa")
else:
    print(f"  • Nenhuma ação necessária - dados já atualizados")
print("="*80)