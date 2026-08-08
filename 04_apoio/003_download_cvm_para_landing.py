# Databricks notebook source
# DBTITLE 1,DOCUMENTAÇÃO
# MAGIC %md
# MAGIC # Download de Arquivos CVM para Landing Zone
# MAGIC
# MAGIC ## Objetivo
# MAGIC Fazer download dos arquivos ZIP de Demonstrações Financeiras Padronizadas (DFP) do portal CVM e armazená-los na Landing Zone em Unity Catalog Volume. Preserva arquivos originais e metadados HTTP para rastreabilidade e detecção de atualizações.
# MAGIC
# MAGIC ## Landing Zone
# MAGIC * **Localização**: `/Volumes/main/proj_cvm/landing/dfp/`
# MAGIC * **Estrutura**: Um subdiretório por ano (`/2023/`, `/2024/`, etc.)
# MAGIC * **Conteúdo por ano**:
# MAGIC   - `dfp_cia_aberta_YYYY.zip` - Arquivo original da CVM
# MAGIC   - `_metadata.json` - Metadados HTTP (Last-Modified, tamanho, URL, timestamp de download)
# MAGIC
# MAGIC ## Estratégia de Download
# MAGIC 1. Para cada ano em `ANOS_PROCESSAR` (definido pelo orquestrador):
# MAGIC    - Fazer requisição HEAD para obter metadados HTTP (Last-Modified)
# MAGIC    - Comparar com metadados locais (se existirem)
# MAGIC    - Baixar apenas se arquivo for novo ou atualizado
# MAGIC 2. Gravar arquivo ZIP + metadados JSON na Landing Zone
# MAGIC 3. Validar estrutura criada
# MAGIC
# MAGIC ## Benefícios
# MAGIC * **Evita downloads desnecessários**: Comparação de timestamps economiza banda e tempo
# MAGIC * **Preservação**: Arquivos originais inalterados permitem reprocessamento idêntico
# MAGIC * **Rastreabilidade**: Metadados HTTP documentam origem e versão exata dos dados
# MAGIC
# MAGIC ## Função
# MAGIC Script de apoio - Ingestão de dados brutos da fonte externa

# COMMAND ----------

# DBTITLE 1,INICIALIZAÇÃO E IMPORTS
# Carregar configurações centralizadas do pipeline
%run ./config_parametros

from datetime import datetime

print("="*80)
print("INICIALIZAÇÃO - DETECÇÃO DE ANOS")
print("="*80)

# Inicializar ANOS_PROCESSAR com logs detalhados
if ANOS_PROCESSAR is None:
    print("⚠️  ANOS_PROCESSAR está None - inicializando com detecção inteligente...")
    try:
        anos_detectados = inicializar_anos_processar(silent=False)
        print(f"\n✅ Detecção retornou: {anos_detectados}")
    except Exception as e:
        print(f"\n❌ ERRO na detecção inteligente: {type(e).__name__}: {e}")
        print(f"   Traceback: {e}")
        anos_detectados = None
else:
    print(f"✓ ANOS_PROCESSAR já inicializado: {ANOS_PROCESSAR}")
    anos_detectados = ANOS_PROCESSAR

# FALLBACK ROBUSTO: Se detecção falhou ou retornou vazio, usar anos recentes
if not anos_detectados:
    ano_atual = datetime.now().year
    anos_fallback = list(range(ano_atual - 5, ano_atual + 1))  # Últimos 5 anos + atual
    print(f"\n⚠️  FALLBACK ATIVADO: Usando últimos 5 anos = {anos_fallback}")
    ANOS_PROCESSAR = anos_fallback
else:
    ANOS_PROCESSAR = anos_detectados

print(f"\n🎯 Anos finais a processar: {ANOS_PROCESSAR}")
print("="*80)

import urllib.request
import json
import io
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,CRIAR ESTRUTURA DE LANDING ZONE
# Criar schema e volume UC se não existirem
print(f"Verificando estrutura Unity Catalog...")

# Criar schema proj_cvm
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.proj_cvm")
print(f"✅ Schema: {CATALOG_NAME}.proj_cvm")

# Criar volume landing
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.proj_cvm.landing")
print(f"✅ Volume: {CATALOG_NAME}.proj_cvm.landing")

# Criar diretório base do Landing Zone se não existir
print(f"\nCriando estrutura de Landing Zone...")
dbutils.fs.mkdirs(VOLUME_LANDING_DFP)
print(f"✅ Landing Zone: {VOLUME_LANDING_DFP}")

# COMMAND ----------

# DBTITLE 1,DOWNLOAD PARA LANDING ZONE
# Para cada ano, baixar arquivo ZIP da CVM para Volume UC
import os

print("="*80)
print("DOWNLOAD DE ARQUIVOS CVM PARA LANDING ZONE")
print("="*80)

for ano in ANOS_PROCESSAR:
    print(f"\n{'='*80}")
    print(f"ANO: {ano}")
    print("="*80)
    
    # URL do arquivo
    url = get_url_arquivo_cvm(ano)
    ano_path = f"{VOLUME_LANDING_DFP}/{ano}"
    
    # Verificar metadados HTTP
    print(f"  🔍 Verificando arquivo na CVM...")
    try:
        req = urllib.request.Request(url, method='HEAD')
        with urllib.request.urlopen(req, timeout=10) as response:
            last_modified_cvm = response.headers.get('Last-Modified')
            last_modified_dt = datetime.strptime(last_modified_cvm, '%a, %d %b %Y %H:%M:%S %Z')
            content_length = int(response.headers.get('Content-Length', 0))
    except urllib.error.HTTPError as e:
        print(f"  ⚠️  Arquivo não encontrado na CVM (HTTP {e.code}) - pulando ano {ano}")
        continue
    except Exception as e:
        print(f"  ⚠️  Erro ao verificar arquivo (Ano {ano}): {e} - pulando")
        continue
    
    print(f"  ℹ️  Last-Modified: {last_modified_cvm}")
    print(f"  ℹ️  Tamanho: {content_length / (1024*1024):.2f} MB")
    
    # Ler metadados locais (se existir)
    metadata_path = f"{ano_path}/_metadata.json"
    precisa_download = True
    
    try:
        # Tentar ler metadados existentes do Volume UC via Python open()
        with open(metadata_path, 'r') as f:
            metadata_local = json.load(f)
        last_modified_local = datetime.fromisoformat(metadata_local['last_modified_cvm'])
        
        if last_modified_dt <= last_modified_local:
            print(f"  ✓ Arquivo já está atualizado")
            precisa_download = False
    except Exception:
        print(f"  • Primeira vez - arquivo será baixado")
    
    if precisa_download:
        # Download do arquivo
        print(f"  ⬇️  Baixando de {url}")
        with urllib.request.urlopen(url, timeout=300) as response:
            zip_bytes = response.read()
        
        # Nome do arquivo (sempre sobrescreve se já existir)
        arquivo_nome = f"dfp_cia_aberta_{ano}.zip"
        arquivo_path = f"{ano_path}/{arquivo_nome}"
        
        # Criar diretório do ano (se não existir)
        os.makedirs(ano_path, exist_ok=True)
        
        # Gravar diretamente no Volume UC usando Python open()
        # Compatível com Spark Connect / Serverless (não usa /tmp)
        with open(arquivo_path, 'wb') as f:
            f.write(zip_bytes)
        
        print(f"  ✓ Arquivo salvo: {arquivo_path}")
        print(f"  • Tamanho: {len(zip_bytes) / (1024*1024):.2f} MB")
        
        # Gravar metadados
        metadata = {
            'ano': ano,
            'url': url,
            'last_modified_cvm': last_modified_dt.isoformat(),
            'download_ts': datetime.now().isoformat(),
            'tamanho_bytes': len(zip_bytes),
            'arquivo_nome': arquivo_nome
        }
        
        # Gravar metadata diretamente no Volume UC
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"  ✓ Metadados salvos")

print(f"\n{'='*80}")
print("DOWNLOAD CONCLUÍDO")
print("="*80)

# COMMAND ----------

# DBTITLE 1,VALIDAÇÃO DA LANDING ZONE
# Listar estrutura da Landing Zone criada
print("\n📂 Estrutura da Landing Zone:")
print("="*80)

# Usar os API Python padrão (compatível com Spark Connect)
import os

try:
    # Listar anos (diretórios)
    for ano in sorted(os.listdir(VOLUME_LANDING_DFP)):
        ano_path = f"{VOLUME_LANDING_DFP}/{ano}"
        if os.path.isdir(ano_path):
            print(f"\n📁 Ano: {ano}")
            
            # Listar arquivos do ano
            for arquivo in os.listdir(ano_path):
                arquivo_path = f"{ano_path}/{arquivo}"
                if os.path.isfile(arquivo_path):
                    tamanho_bytes = os.path.getsize(arquivo_path)
                    tamanho_mb = tamanho_bytes / (1024*1024)
                    print(f"   • {arquivo} ({tamanho_mb:.2f} MB)")
except Exception as e:
    print(f"⚠️  Landing Zone ainda vazia: {e}")