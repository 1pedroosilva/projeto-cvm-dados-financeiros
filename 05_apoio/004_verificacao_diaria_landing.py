# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,DOCUMENTAÇÃO
# MAGIC %md
# MAGIC # Verificação Diária - Landing Zone CVM
# MAGIC
# MAGIC ## Objetivo
# MAGIC Verificar se há novas versões dos arquivos ZIP de DFP da CVM e baixá-las para a Landing Zone, arquivando versões anteriores antes de sobrescrever.
# MAGIC
# MAGIC ## Landing Zone
# MAGIC * **Localização**: `/Volumes/workspace/proj_cvm/landing/dfp/`
# MAGIC * **Estrutura**: Um subdiretório por ano (`/2023/`, `/2024/`, etc.)
# MAGIC * **Conteúdo por ano**:
# MAGIC   - `dfp_cia_aberta_YYYY.zip` - Versão mais recente do arquivo original da CVM
# MAGIC   - `_metadata.json` - Metadados HTTP da versão atual
# MAGIC   - `archive/` - Versões anteriores arquivadas com timestamp
# MAGIC
# MAGIC ## Estratégia de Versionamento
# MAGIC 1. Para cada ano em `ANOS_PROCESSAR`:
# MAGIC    - Verificar `Last-Modified` da CVM via requisição HEAD
# MAGIC    - Comparar com metadados locais (se existirem)
# MAGIC    - Se atualizado: arquivar versão atual → baixar nova versão
# MAGIC 2. Arquivos arquivados em `archive/` com sufixo de timestamp (YYYYMMDD_HHMMSS)
# MAGIC 3. Landing Zone sempre contém a versão mais recente no caminho padrão
# MAGIC
# MAGIC ## Função
# MAGIC Script de apoio - Verificação diária e manutenção da Landing Zone

# COMMAND ----------

# DBTITLE 1,CARREGAR CONFIGURAÇÕES
# MAGIC %run ./config_parametros

# COMMAND ----------

# DBTITLE 1,INICIALIZAR ANOS A PROCESSAR
# Capturar timestamp de início para cálculo de duração na observabilidade
_inicio_execucao = datetime.now()

# Capturar explicitamente o retorno de inicializar_anos_processar()
ANOS_PROCESSAR = inicializar_anos_processar()

if not ANOS_PROCESSAR:
    raise ValueError("❌ ANOS_PROCESSAR vazio - nenhum ano para processar")

print(f"🎯 Anos a verificar: {ANOS_PROCESSAR}")

# COMMAND ----------

# DBTITLE 1,IMPORTS
import os
import json
import shutil
import urllib.request
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,VERIFICAÇÃO E DOWNLOAD COM ARQUIVAMENTO
# Loop principal: verificar CVM, arquivar versão atual e baixar novas versões
# Arquiva em {ano}/archive/ antes de sobrescrever, preservando histórico de versões

print("=" * 80)
print("VERIFICAÇÃO DIÁRIA - LANDING ZONE CVM")
print("=" * 80)

arquivos_baixados = []
arquivos_arquivados = []
arquivos_ignorados = []
erros = []

for ano in ANOS_PROCESSAR:
    print(f"\n{'=' * 80}")
    print(f"ANO: {ano}")
    print("=" * 80)

    # URL do arquivo na CVM
    url = get_url_arquivo_cvm(ano)
    ano_path = f"{VOLUME_LANDING_DFP}/{ano}"
    zip_path = f"{ano_path}/dfp_cia_aberta_{ano}.zip"
    metadata_path = f"{ano_path}/_metadata.json"

    # 1. Verificar arquivo na CVM via HEAD
    print(f"  🔍 Verificando arquivo na CVM...")
    try:
        req = urllib.request.Request(url, method='HEAD')
        with urllib.request.urlopen(req, timeout=10) as response:
            last_modified_cvm = response.headers.get('Last-Modified')
            last_modified_dt = datetime.strptime(
                last_modified_cvm, '%a, %d %b %Y %H:%M:%S %Z'
            )
            content_length = int(response.headers.get('Content-Length', 0))
    except urllib.error.HTTPError as e:
        print(f"  ⚠️  Arquivo não encontrado na CVM (HTTP {e.code}) - pulando ano {ano}")
        erros.append((ano, f"HTTP {e.code}"))
        continue
    except Exception as e:
        print(f"  ⚠️  Erro ao verificar arquivo (Ano {ano}): {e} - pulando")
        erros.append((ano, str(e)))
        continue

    print(f"  ℹ️  Last-Modified CVM: {last_modified_cvm}")
    print(f"  ℹ️  Tamanho: {content_length / (1024 * 1024):.2f} MB")

    # 2. Ler metadados locais e comparar
    precisa_download = True
    try:
        with open(metadata_path, 'r') as f:
            metadata_local = json.load(f)
        last_modified_local = datetime.fromisoformat(metadata_local['last_modified_cvm'])

        if last_modified_dt <= last_modified_local:
            print(f"  ✓ Arquivo já está atualizado")
            precisa_download = False
    except Exception:
        print(f"  • Primeira vez - arquivo será baixado")

    # 3. Se atualizado, arquivar versão atual e baixar nova
    if precisa_download:
        # Arquivar versão atual (se existe)
        if os.path.exists(zip_path):
            archive_dir = f"{ano_path}/archive"
            os.makedirs(archive_dir, exist_ok=True)

            timestamp_arquivo = datetime.now().strftime('%Y%m%d_%H%M%S')

            # Arquivar ZIP
            zip_arquivado = f"{archive_dir}/dfp_cia_aberta_{ano}_{timestamp_arquivo}.zip"
            shutil.copy2(zip_path, zip_arquivado)
            print(f"  📦 Versão anterior arquivada: {zip_arquivado}")
            arquivos_arquivados.append((ano, zip_arquivado))

            # Arquivar metadados
            if os.path.exists(metadata_path):
                metadata_arquivado = f"{archive_dir}/_metadata_{timestamp_arquivo}.json"
                shutil.copy2(metadata_path, metadata_arquivado)

        # Download da nova versão
        print(f"  ⬇️  Baixando de {url}")
        with urllib.request.urlopen(url, timeout=300) as response:
            zip_bytes = response.read()

        # Criar diretório do ano (se não existir)
        os.makedirs(ano_path, exist_ok=True)

        # Gravar ZIP no caminho padrão
        with open(zip_path, 'wb') as f:
            f.write(zip_bytes)

        print(f"  ✓ Arquivo salvo: {zip_path}")
        print(f"  • Tamanho: {len(zip_bytes) / (1024 * 1024):.2f} MB")

        # Gravar metadados atualizados
        metadata = {
            'ano': ano,
            'url': url,
            'last_modified_cvm': last_modified_dt.isoformat(),
            'download_ts': datetime.now().isoformat(),
            'tamanho_bytes': len(zip_bytes),
            'arquivo_nome': f"dfp_cia_aberta_{ano}.zip"
        }

        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)

        print(f"  ✓ Metadados salvos")
        arquivos_baixados.append((ano, len(zip_bytes)))
    else:
        arquivos_ignorados.append(ano)

print(f"\n{'=' * 80}")
print("VERIFICAÇÃO CONCLUÍDA")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,VALIDAÇÃO DA LANDING ZONE
# Listar estrutura completa da Landing Zone, incluindo arquivos arquivados
# Valida que arquivos e metadados estão no local esperado

print("\n📂 Estrutura da Landing Zone:")
print("=" * 80)

for ano in sorted(os.listdir(VOLUME_LANDING_DFP)):
    ano_path = f"{VOLUME_LANDING_DFP}/{ano}"
    if os.path.isdir(ano_path):
        print(f"\n📁 Ano: {ano}")

        # Listar arquivos do ano (não recursivo)
        for arquivo in sorted(os.listdir(ano_path)):
            arquivo_path = f"{ano_path}/{arquivo}"
            if os.path.isfile(arquivo_path):
                tamanho_bytes = os.path.getsize(arquivo_path)
                tamanho_mb = tamanho_bytes / (1024 * 1024)
                print(f"   • {arquivo} ({tamanho_mb:.2f} MB)")
            elif os.path.isdir(arquivo_path):
                print(f"   📦 {arquivo}/")
                # Listar arquivos arquivados
                for arq_arquivo in sorted(os.listdir(arquivo_path)):
                    arq_path = f"{arquivo_path}/{arq_arquivo}"
                    if os.path.isfile(arq_path):
                        tamanho_bytes = os.path.getsize(arq_path)
                        tamanho_mb = tamanho_bytes / (1024 * 1024)
                        print(f"      • {arq_arquivo} ({tamanho_mb:.2f} MB)")

print("=" * 80)

# COMMAND ----------

# DBTITLE 1,RESUMO DA EXECUÇÃO
# Resumo final: contagem de operações e tamanho total
# Permite verificar rapidamente se houve atualizações e arquivamentos

print("\n" + "=" * 80)
print("RESUMO - VERIFICAÇÃO DIÁRIA LANDING ZONE")
print(f"Execução: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)

print(f"\n📊 Estatísticas:")
print(f"  • Anos verificados: {len(ANOS_PROCESSAR)}")
print(f"  • Arquivos baixados: {len(arquivos_baixados)}")
print(f"  • Arquivos arquivados: {len(arquivos_arquivados)}")
print(f"  • Arquivos ignorados (já atualizados): {len(arquivos_ignorados)}")
if erros:
    print(f"  • Erros: {len(erros)}")

if arquivos_baixados:
    total_baixado = sum(size for _, size in arquivos_baixados)
    print(f"\n⬇️  Arquivos baixados:")
    for ano, size in arquivos_baixados:
        print(f"   • {ano}: {size / (1024 * 1024):.2f} MB")
    print(f"   Total: {total_baixado / (1024 * 1024):.2f} MB")

if arquivos_arquivados:
    print(f"\n📦 Arquivos arquivados:")
    for ano, path in arquivos_arquivados:
        size = os.path.getsize(path)
        print(f"   • {ano}: {os.path.basename(path)} ({size / (1024 * 1024):.2f} MB)")

if arquivos_ignorados:
    print(f"\n✓ Arquivos já atualizados: {arquivos_ignorados}")

if erros:
    print(f"\n⚠️  Erros:")
    for ano, msg in erros:
        print(f"   • Ano {ano}: {msg}")

print("=" * 80)

# COMMAND ----------

# DBTITLE 1,REGISTRO DE OBSERVABILIDADE
# Registrar execução na tabela de observabilidade
# Captura métricas da execução atual e insere na tabela observabilidade_execucoes

import uuid

# Capturar contexto do job (se disponível)
job_id = None
run_id = None
task_key = None
try:
    _jid = os.getenv('DATABRICKS_JOB_ID')
    if _jid:
        job_id = int(_jid)
    _rid = os.getenv('DATABRICKS_JOB_RUN_ID')
    if _rid:
        run_id = int(_rid)
    task_key = os.getenv('DATABRICKS_JOB_TASK_KEY')
except Exception:
    pass

# Determinar status final
if erros and not arquivos_baixados:
    status_final = 'ERROR'
elif erros and arquivos_baixados:
    status_final = 'PARTIAL'
else:
    status_final = 'SUCCESS'

# Calcular métricas agregadas
total_bytes_baixados = sum(size for _, size in arquivos_baixados) if arquivos_baixados else 0
total_bytes_arquivados = 0
for _, path in arquivos_arquivados:
    try:
        total_bytes_arquivados += os.path.getsize(path)
    except Exception:
        pass

# Construir mensagem de erro (até 2000 chars)
if erros:
    mensagem_erro = '; '.join(f"Ano {ano}: {msg}" for ano, msg in erros)[:2000]
else:
    mensagem_erro = None

# Calcular duração
_fim_execucao = datetime.now()
_duracao = (_fim_execucao - _inicio_execucao).total_seconds()

# Helper para formatar strings SQL
def _sql_str(val):
    if val is None:
        return 'NULL'
    return "'" + str(val).replace("'", "''") + "'"

# Inserir registro
id_exec = str(uuid.uuid4())
notebook_path = '/Workspace/Users/1pedro.osilva@gmail.com/projeto-cvm-dados-financeiros/05_apoio/004_verificacao_diaria_landing'

spark.sql(f"""
    INSERT INTO workspace.proj_cvm_05_apoio.observabilidade_execucoes
        (id_execucao, job_id, run_id, task_key, notebook_path,
         etapa, fonte, status,
         arquivos_verificados, arquivos_baixados, arquivos_arquivados, arquivos_ignorados,
         bytes_baixados, bytes_arquivados,
         mensagem_erro, inicio_ts, fim_ts, duracao_segundos, created_at)
    VALUES (
        {_sql_str(id_exec)},
        {job_id or 'NULL'},
        {run_id or 'NULL'},
        {_sql_str(task_key)},
        {_sql_str(notebook_path)},
        {_sql_str('verificacao')},
        {_sql_str('landing')},
        {_sql_str(status_final)},
        {len(ANOS_PROCESSAR)},
        {len(arquivos_baixados)},
        {len(arquivos_arquivados)},
        {len(arquivos_ignorados)},
        {total_bytes_baixados},
        {total_bytes_arquivados},
        {_sql_str(mensagem_erro)},
        {_sql_str(_inicio_execucao.strftime('%Y-%m-%d %H:%M:%S'))},
        {_sql_str(_fim_execucao.strftime('%Y-%m-%d %H:%M:%S'))},
        {_duracao},
        current_timestamp()
    )
""")

print(f"\n✅ Registro de observabilidade criado: {id_exec}")
print(f"   Status: {status_final}")
print(f"   Duração: {_duracao:.1f}s")
print(f"   Arquivos: {len(arquivos_baixados)} baixados, {len(arquivos_arquivados)} arquivados, {len(arquivos_ignorados)} ignorados")
if erros:
    print(f"   Erros: {len(erros)}")