# Resiliência Operacional em Pipelines de Dados

## Relação com Arquitetura Medalhão

Este documento COMPLEMENTA `arquitetura_medalhao.md` (seções 5.1 Idempotência, 5.3 Qualidade, 5.4 Observabilidade). Enquanto o medalhão define **o que cada camada faz**, este documento define **como garantir que falhas não destruam a execução**.

**Princípio fundador**: Em pipelines de dados, falhas são inevitáveis (rede instável, APIs com rate limit, dados mal-formados, timeouts). Resiliência não é "tratar erros quando acontecem" — é **projetar para que falhas parciais sejam recuperáveis sem perda de trabalho**.

---

## 1. Retry Logic (Falhas Transitórias)

### 1.1 Quando Aplicar Retry

**Aplicar retry em**:
* Chamadas HTTP/APIs (timeout, 5xx, rate limit)
* Leitura de arquivos remotos (rede instável)
* Escrita em storage (contention temporário)
* Operações Delta (conflitos de transação)

**NÃO aplicar retry em**:
* Erros de schema (dado mal-formado não vai "melhorar" tentando de novo)
* Erros de lógica (bug no código)
* 4xx HTTP (Bad Request, Unauthorized — problema permanente)

### 1.2 Padrão: Exponential Backoff com Jitter

```python
import time
import random

def retry_com_backoff(funcao, max_tentativas=3, backoff_inicial=1):
    """
    Retry com exponential backoff + jitter.
    
    Args:
        funcao: Função a executar
        max_tentativas: Máximo de tentativas
        backoff_inicial: Tempo inicial de espera (segundos)
    """
    for tentativa in range(1, max_tentativas + 1):
        try:
            return funcao()
        except Exception as e:
            if tentativa == max_tentativas:
                raise  # Última tentativa, propaga erro
            
            # Exponential backoff: 1s, 2s, 4s, 8s...
            espera = backoff_inicial * (2 ** (tentativa - 1))
            # Jitter: randomiza ±20% para evitar thundering herd
            espera = espera * (0.8 + 0.4 * random.random())
            
            print(f"[RETRY] Tentativa {tentativa}/{max_tentativas} falhou: {e}")
            print(f"[RETRY] Aguardando {espera:.1f}s antes de tentar novamente")
            time.sleep(espera)
```

**Por que jitter?** Se 100 jobs falharem simultaneamente e todos esperarem exatamente 2s, todos vão bater na API ao mesmo tempo de novo — criando um *thundering herd*. Jitter espalha as tentativas.

### 1.3 Rate Limit Específico

Quando a API retorna `429 Too Many Requests` ou `Retry-After` header:

```python
import requests
import time

def request_com_rate_limit(url, max_tentativas=5):
    for tentativa in range(1, max_tentativas + 1):
        response = requests.get(url)
        
        if response.status_code == 200:
            return response
        
        if response.status_code == 429:
            # Respeitar Retry-After se disponível
            retry_after = int(response.headers.get('Retry-After', 60))
            print(f"[RATE LIMIT] Aguardando {retry_after}s")
            time.sleep(retry_after)
            continue
        
        # Outros erros: backoff padrão
        response.raise_for_status()
```

---

## 2. Tratamento Granular de Erros

### 2.1 Princípio: Falha Parcial ≠ Falha Total

**Antipadrão**:
```python
# ❌ Processar 5 anos em um único bloco try/except
try:
    for ano in [2021, 2022, 2023, 2024, 2025]:
        processar_ano(ano)  # Se 2022 falhar, TUDO falha
except Exception as e:
    print(f"Erro: {e}")
    # Perdeu todo o trabalho de 2021
```

**Padrão robusto**:
```python
# ✅ Try/except POR ano — falha isolada
anos_sucesso = []
anos_falha = []

for ano in [2021, 2022, 2023, 2024, 2025]:
    try:
        processar_ano(ano)
        anos_sucesso.append(ano)
        print(f"✅ Ano {ano} processado com sucesso")
    except Exception as e:
        anos_falha.append((ano, str(e)))
        print(f"❌ Ano {ano} falhou: {e}")
        # Continua processando próximos anos

# Relatório final
print(f"\n📊 Resumo: {len(anos_sucesso)} sucessos, {len(anos_falha)} falhas")
if anos_falha:
    print("\n⚠️ Anos que falharam:")
    for ano, erro in anos_falha:
        print(f"  - {ano}: {erro}")
```

**Ganho**: Se 2022 falha (arquivo corrompido), 2021/2023/2024/2025 são processados. Reprocessamento só precisa corrigir 2022.

### 2.2 Logging Estruturado

Cada etapa deve logar:
* Timestamp
* Ação (o que está tentando fazer)
* Contexto (ano, arquivo, tabela)
* Status (início, sucesso, falha)
* Duração

```python
import logging
import time
from datetime import datetime

# Configurar logger estruturado
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def processar_ano_com_log(ano):
    inicio = time.time()
    logger.info(f"[INÍCIO] Processando ano={ano}")
    
    try:
        # Lógica de processamento
        resultado = processar_ano(ano)
        
        duracao = time.time() - inicio
        logger.info(f"[SUCESSO] ano={ano} | duração={duracao:.2f}s | registros={resultado['count']}")
        return resultado
        
    except Exception as e:
        duracao = time.time() - inicio
        logger.error(f"[FALHA] ano={ano} | duração={duracao:.2f}s | erro={str(e)}")
        raise
```

**Benefício**: Log estruturado permite rastrear exatamente onde e por que falhou, com contexto suficiente para diagnosticar.

---

## 3. Checkpointing e Retomada

### 3.1 Tabela de Controle (State Management)

Para pipelines que processam múltiplos períodos/arquivos, manter tabela de controle:

```sql
CREATE TABLE IF NOT EXISTS bronze.controle_ingestao (
    fonte          STRING,
    ano            INT,
    status         STRING,  -- 'pendente', 'processando', 'sucesso', 'falha'
    data_inicio    TIMESTAMP,
    data_fim       TIMESTAMP,
    tentativas     INT,
    erro           STRING,
    registros      BIGINT,
    CONSTRAINT pk PRIMARY KEY (fonte, ano)
)
```

### 3.2 Padrão: Checkpoint antes de iniciar, commit após sucesso

```python
from pyspark.sql.functions import current_timestamp, lit

def processar_com_checkpoint(fonte, ano):
    # 1. Marcar como 'processando'
    spark.sql(f"""
        MERGE INTO bronze.controle_ingestao t
        USING (SELECT '{fonte}' as fonte, {ano} as ano) s
        ON t.fonte = s.fonte AND t.ano = s.ano
        WHEN MATCHED THEN UPDATE SET
            status = 'processando',
            data_inicio = current_timestamp(),
            tentativas = t.tentativas + 1
        WHEN NOT MATCHED THEN INSERT
            (fonte, ano, status, data_inicio, tentativas)
            VALUES ('{fonte}', {ano}, 'processando', current_timestamp(), 1)
    """)
    
    try:
        # 2. Processar
        resultado = processar_ano(ano)
        
        # 3. Marcar como 'sucesso'
        spark.sql(f"""
            UPDATE bronze.controle_ingestao
            SET status = 'sucesso',
                data_fim = current_timestamp(),
                registros = {resultado['count']},
                erro = NULL
            WHERE fonte = '{fonte}' AND ano = {ano}
        """)
        
    except Exception as e:
        # 4. Marcar como 'falha'
        spark.sql(f"""
            UPDATE bronze.controle_ingestao
            SET status = 'falha',
                data_fim = current_timestamp(),
                erro = '{str(e).replace("'", "''")}'  -- Escape SQL
            WHERE fonte = '{fonte}' AND ano = {ano}
        """)
        raise
```

### 3.3 Detecção Inteligente de Períodos Pendentes

```python
def get_anos_pendentes(fonte, janela_anos=5):
    """
    Retorna anos pendentes ou com falha, dentro da janela temporal.
    """
    ano_atual = datetime.now().year
    ano_inicio = ano_atual - janela_anos + 1
    
    df_controle = spark.sql(f"""
        SELECT ano, status, tentativas
        FROM bronze.controle_ingestao
        WHERE fonte = '{fonte}'
          AND ano BETWEEN {ano_inicio} AND {ano_atual}
    """)
    
    # Anos dentro da janela
    anos_janela = set(range(ano_inicio, ano_atual + 1))
    
    # Anos já processados com sucesso
    anos_sucesso = set(
        row.ano for row in df_controle.filter("status = 'sucesso'").collect()
    )
    
    # Anos pendentes = janela - sucesso
    anos_pendentes = sorted(anos_janela - anos_sucesso)
    
    return anos_pendentes
```

**Ganho**: Pipeline detecta automaticamente quais períodos faltam processar, sem hardcoding de listas.

---

## 4. Validação de Pré-requisitos (Guardrails)

### 4.1 Validar Antes de Processar

**Antipadrão**: Começar processamento e falhar no meio.

**Padrão robusto**: Validar tudo ANTES de iniciar.

```python
def validar_prerequisitos(fonte, ano):
    """
    Valida que todos os pré-requisitos existem antes de processar.
    Retorna (bool, mensagem_erro).
    """
    # 1. Arquivo na Landing Zone existe?
    arquivo_path = f"/Volumes/cvm/landing/{fonte}/{ano}/arquivo_{ano}.zip"
    try:
        dbutils.fs.ls(arquivo_path)
    except:
        return False, f"Arquivo não encontrado: {arquivo_path}"
    
    # 2. Tabela de destino existe?
    if not spark.catalog.tableExists(f"bronze.{fonte}"):
        return False, f"Tabela bronze.{fonte} não existe"
    
    # 3. Tabela de controle existe?
    if not spark.catalog.tableExists("bronze.controle_ingestao"):
        return False, "Tabela de controle não existe"
    
    # 4. Não está sendo processado por outro job?
    em_processamento = spark.sql(f"""
        SELECT COUNT(*) as cnt
        FROM bronze.controle_ingestao
        WHERE fonte = '{fonte}' AND ano = {ano}
          AND status = 'processando'
          AND data_inicio > current_timestamp() - INTERVAL 2 HOURS
    """).first().cnt
    
    if em_processamento > 0:
        return False, f"Ano {ano} já está sendo processado por outro job"
    
    return True, None

# Uso
valido, erro = validar_prerequisitos('dfp_dre', 2023)
if not valido:
    raise RuntimeError(f"Pré-requisito não atendido: {erro}")
```

**Ganho**: Falha RÁPIDA e CLARA antes de gastar tempo/recursos.

---

## 5. Auto-ajuste de Períodos (Dinamismo)

### 5.1 Detectar Janela Temporal Automaticamente

```python
from datetime import datetime

def get_janela_anos(janela_anos_relevante=5):
    """
    Retorna (ano_inicio, ano_fim) baseado na data atual.
    """
    ano_atual = datetime.now().year
    ano_inicio = ano_atual - janela_anos_relevante + 1
    return ano_inicio, ano_atual

# Uso
ano_inicio, ano_fim = get_janela_anos(janela_anos_relevante=5)
print(f"Janela temporal: {ano_inicio} a {ano_fim}")
# Em 2026: janela = [2022, 2023, 2024, 2025, 2026]
```

### 5.2 Parametrização Externa

**Princípio**: Parâmetros críticos (janela temporal, fonte, tabela) devem vir de configuração EXTERNA, não hardcoded.

```python
# 04_apoio/config_parametros.py
PARAMETROS = {
    "janela_anos_relevante": 5,
    "max_tentativas_retry": 3,
    "backoff_inicial_segundos": 2,
    "timeout_http_segundos": 30,
}

# Uso no notebook
from config_parametros import PARAMETROS

janela = PARAMETROS["janela_anos_relevante"]
anos_processar = get_anos_pendentes(fonte='dfp_dre', janela_anos=janela)
```

**Ganho**: Ajustar janela temporal não requer editar código do notebook — apenas atualizar config.

---

## 6. Antipadrões (O Que NÃO Fazer)

| Antipadrão | Consequência | Solução |
|------------|--------------|----------|
| Try/except global sem granularidade | Falha em 1 ano destrói todo o trabalho | Try/except POR período |
| Retry sem backoff | Thundering herd, piora rate limit | Exponential backoff + jitter |
| Processar sem validar pré-requisitos | Falha no meio, desperdício de recursos | Validar ANTES de iniciar |
| Hardcoding de listas de períodos | Pipeline não detecta pendentes | Tabela de controle + detecção inteligente |
| Sem logging estruturado | Impossível diagnosticar falhas | Logger com timestamp, contexto, status |
| Sem tabela de controle | Não sabe o que foi processado | State management em tabela Delta |
| Reprocessar tudo sempre | Ineficiente, desperdício | Processar apenas pendentes/falhados |

---

## 7. Checklist de Resiliência (Validação de Código)

Antes de considerar um pipeline "production-grade", verificar:

- [ ] **Retry logic**: Implementado para chamadas HTTP/APIs com backoff exponencial?
- [ ] **Tratamento granular**: Try/except por unidade de trabalho (não global)?
- [ ] **Checkpointing**: Tabela de controle registra estado de cada período?
- [ ] **Validação de pré-requisitos**: Valida tudo ANTES de processar?
- [ ] **Logging estruturado**: Cada ação loga timestamp, contexto, status?
- [ ] **Idempotência**: Rodar N vezes o mesmo período produz o mesmo resultado? (ver `arquitetura_medalhao.md` seção 5.1)
- [ ] **Auto-ajuste**: Detecta automaticamente períodos pendentes (não hardcoded)?
- [ ] **Parametrização**: Janela temporal vem de config externa?
- [ ] **Relatório de falhas**: No final, lista explicitamente o que falhou?
- [ ] **Documentação**: Falhas esperadas e recovery documentados?

---

## Referências

* **Idempotência**: Ver `arquitetura_medalhao.md` seção 5.1
* **Guardrails por camada**: Ver `arquitetura_medalhao.md` seção 5.3
* **Observabilidade**: Ver `arquitetura_medalhao.md` seção 5.4
* **Estratégias de gravação**: Ver `arquitetura_medalhao.md` seção 5.5
