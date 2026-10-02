# Guardrails - Validações e Proteções de Dados

## Propósito

Guardrails são validações executadas **ANTES** de modificar dados em tabelas Delta. Protegem contra:
* Perda acidental de dados (DELETE sem dados para substituir)
* Corrupção de schema (colunas críticas faltando)
* Propagação de erros (dados ruins da Bronze para Silver)

**Princípio**: Validar → Falhou? PARA (preserva dados). Passou? Procede.

---

## Bronze - Guardrails de Ingestão

### Contexto

Bronze usa estratégia **APPEND-ONLY**. Guardrails garantem que apenas dados válidos são inseridos (sem DELETE prévio).

### Guardrails Implementados

| Guardrail | Condição | Ação se Falha | Razão |
| --- | --- | --- | --- |
| **Arquivo vazio** | `len(df_pandas) == 0` | PARA (Bronze preservada) | Evita ingerir arquivo CSV sem dados |
| **Schema inválido** | Colunas essenciais faltando | PARA (Bronze preservada) | Via `validar_e_projetar_schema()` - valida presença de colunas críticas definidas em `config_parametros.py` |
| **Reconciliação de contagem** | `count_tabela != count_gravar` | PARA (Bronze preservada) | Verifica que contagem gravada na tabela Delta equivale à contagem do DataFrame gravado |

> **Status (02/10/2026)**: Os três guardrails Bronze estão **implementados** nos notebooks 101 (DRE), 102 (BPA) e 103 (BPP). Verificação feita pela leitura integral do código de cada notebook.

### Fluxo de Erro

```python
for ano in ANOS_PROCESSAR:
    try:
        # [1/4] Ler arquivo da Landing Zone
        df_pandas = read_csv_from_zip(...)
        
        # [2/4] GUARDRAILS
        if len(df_pandas) == 0:
            raise ValueError("Arquivo vazio")
        
        df_raw = spark.createDataFrame(df_pandas)
        df_validado = validar_e_projetar_schema(df_raw, COLUNAS_ESSENCIAIS_DRE, "DRE")
        # ↑ Levanta exception se colunas críticas faltam
        
        # [3/4] APPEND (só executa se guardrails passaram)
        df_bronze.write.mode("append").saveAsTable("{SCHEMA_BRONZE}.101_dre_dfp")

        # [4/4] GUARDRAIL: Reconciliação de contagem (pós-gravação)
        # Lê tabela Delta, compara count_tabela vs count_gravar
        # Se divergir: registrar_guardrail(FAIL) + raise ValueError → PARA
        
    except Exception as e:
        # Bronze NÃO modificada (APPEND não foi executado)
        logger.error(f"[FALHA] ano={ano} | erro={e}")
        # Registra erro em controle_ingestao
        # Continua para próximo ano
        continue
```

### Notebooks que Implementam

* **101_cvm_dfp_dre** (célula 6 - ORQUESTRAÇÃO RESILIENTE) - DRE
* **102_cvm_dfp_bpa** (célula 6 - ORQUESTRAÇÃO RESILIENTE) - BPA
* **103_cvm_dfp_bpp** (células 7-8 e 11 - EXTRAÇÃO, TRANSFORMAÇÃO e ORQUESTRAÇÃO) - BPP

### Função de Validação

```python
# Definida em config_parametros.py

def validar_e_projetar_schema(df: DataFrame, colunas_essenciais: List[str], contexto: str) -> DataFrame:
    """
    Valida que DataFrame contém todas as colunas essenciais.
    Projeta apenas essas colunas (descarta extras).
    
    Raises:
        ValueError: Se alguma coluna essencial está faltando
    """
    colunas_faltando = set(colunas_essenciais) - set(df.columns)
    
    if colunas_faltando:
        raise ValueError(
            f"[{contexto}] Schema inválido - colunas faltando: {colunas_faltando}"
        )
    
    return df.select(*colunas_essenciais)
```

### Colunas Essenciais Validadas

**DRE** (`COLUNAS_ESSENCIAIS_DRE`):
```python
[
    "CNPJ_CIA", "DT_REFER", "VERSAO", "DENOM_CIA", "CD_CVM",
    "GRUPO_DFP", "MOEDA", "ESCALA_MOEDA", "ORDEM_EXERC",
    "DT_INI_EXERC", "DT_FIM_EXERC", "CD_CONTA", "DS_CONTA",
    "VL_CONTA", "ST_CONTA_FIXA"
]
```

**BPA** (`COLUNAS_ESSENCIAIS_BPA`):
```python
[
    "CNPJ_CIA", "DT_REFER", "VERSAO", "DENOM_CIA", "CD_CVM",
    "GRUPO_DFP", "MOEDA", "ESCALA_MOEDA", "ORDEM_EXERC",
    # "DT_INI_EXERC" removida: BPA não contém esta coluna na fonte CVM
    "DT_FIM_EXERC", "CD_CONTA", "DS_CONTA",
    "VL_CONTA", "ST_CONTA_FIXA"
]
```

### Implementação: Arquivo Vazio

**Mecanismo**: Após ler o CSV do ZIP da Landing Zone para `df_pandas` (pandas), verifica `len(df_pandas) == 0`.

**Comportamento em falha**:

1. Chama `registrar_guardrail()` com `resultado='FAIL'`, `tipo_check='empty_file'`, registrando o nome do arquivo CSV vazio.
2. Levanta `ValueError` com o nome do arquivo CSV.
3. A exceção é capturada pelo `try/except` do loop de orquestração: o ano é adicionado a `anos_falha`, o erro é logado, e um registro `FAILED` é inserido em `controle_ingestao`.
4. O processamento continua para o próximo ano (falha granular).

**Uniformidade**: Implementado de forma idêntica nos três notebooks. A única variação é estrutural:

* DRE e BPA: verificação inline na célula de orquestração.
* BPP: verificação dentro da função `extrair_dados()`, que levanta a exceção para a célula de orquestração capturar.

### Implementação: Reconciliação de Contagem

**Mecanismo**: Quatro pontos de reconciliação rastreiam a contagem de registros em cada transição do pipeline:

| Ponto | Transição | Variáveis | Ação em divergência |
| --- | --- | --- | --- |
| 1 | Extração → Spark | `count_extraido` vs `count_spark` | `assert` → PARA |
| 2 | Pós-validação de schema | `count_spark` vs `count_validado` | `logger.warning` (não PARA — `validar_e_projetar_schema` projeta colunas, não filtra linhas) |
| 3 | Pós-metadados técnicos | `count_validado` vs `count_gravar` | `assert` → PARA |
| 4 | Pós-gravação na tabela Delta | `count_gravar` vs `count_tabela` | `registrar_guardrail()` + `raise ValueError` → PARA |

O ponto 4 é o guardrail formal: após o `APPEND` na tabela Delta, lê a tabela filtrando por `ano` e `_versao_ingestao`, compara com a contagem do DataFrame gravado. Em caso de divergência, chama `registrar_guardrail()` com `resultado='FAIL'` (registrando `esperado`, `encontrado` e `registros_afetados`) e levanta `ValueError`.

**Comportamento em falha** (ponto 4):

1. Chama `registrar_guardrail()` com `resultado='FAIL'`, `tipo_check='row_count'`, `esperado`, `encontrado` e `registros_afetados`.
2. Levanta `ValueError`.
3. A exceção é capturada pelo `try/except` do loop de orquestração: o ano é adicionado a `anos_falha`, o erro é logado, e um registro `FAILED` é inserido em `controle_ingestao`.
4. O processamento continua para o próximo ano (falha granular).

**Uniformidade**: A lógica dos 4 pontos é idêntica nos três notebooks. A variação é estrutural e de nomenclatura:

* DRE e BPA: os 4 pontos estão inline na célula de orquestração. A variável do ponto 3 chama-se `count_gravar`.
* BPP: os pontos 1-3 estão na função `transformar_bronze()` (que retorna `df_bronze` e `count_registros`); o ponto 4 está na célula de orquestração. A variável do ponto 3 chama-se `count_registros`.

Em todos os notebooks, o ponto 2 (pós-validação de schema) apenas emite `logger.warning` e não interrompe o processamento, pois `validar_e_projetar_schema()` seleciona colunas (não filtra linhas), então a contagem deve permanecer igual — o warning é uma rede de segurança.

---

## Silver - Guardrails de Transformação

### Contexto

Silver usa **REPLACE WHERE** (substituição atômica por período). Guardrail garante que não processa Silver se Bronze não tem dados para o ano.

### Guardrails Implementados

| Guardrail | Condição | Ação se Falha | Razão |
| --- | --- | --- | --- |
| **Bronze vazia** | `count_bronze == 0` | SKIP (Silver preservada) | Evita DELETE de Silver quando Bronze não tem dados para o ano |
| **Unicidade da chave de negócio** | Duplicatas após Window Function em `groupBy(chave).count() > 1` | PARA (Silver preservada) | Garante que cada combinação da chave aparece 1x após deduplicação |

### Fluxo

```python
for ano in ANOS_PROCESSAR:
    # GUARDRAIL: Bronze tem dados?
    count_bronze = spark.table("{SCHEMA_BRONZE}.101_dre_dfp") \
        .filter(year(col("DT_REFER")) == ano) \
        .count()
    
    if count_bronze == 0:
        print(f"Bronze vazia para ano {ano} - SKIP")
        continue  # Silver preservada, não executa DELETE
    
    # Processar
    df_bronze = spark.table("bronze").filter(...)
    df_silver = transform(df_bronze)
    
    # REPLACE WHERE (substituição atômica)
    df_silver.write.format("delta").mode("overwrite") \
        .option("replaceWhere", f"ANO = {ano}") \
        .saveAsTable("{SCHEMA_SILVER}.201_dre_dfp")
```

### Chave de Unicidade da Silver

A chave de negócio da Silver é composta por **4 colunas**:

```
(CNPJ_CIA, DT_REFER, CD_CONTA, ORDEM_EXERC)
```

Esta chave é usada em três pontos, sempre alinhada:

1. **Window Function** (filtro de versão): `PARTITION BY (CNPJ_CIA, DT_REFER, CD_CONTA, ORDEM_EXERC) ORDER BY _versao_ingestao DESC` -> `row_number() == 1`
2. **Guardrail de unicidade**: `groupBy(chave).count().filter("count > 1")` -> se > 0, PARA
3. **Teste de integração** (VALIDACAO 4): verifica `groupBy(chave).count()` na Silver finalizada

#### Por que VERSAO e GRUPO_DFP não estão na chave

**VERSAO** — A CVM publica demonstrações em versões. A Bronze preserva todas (APPEND-ONLY). A Silver seleciona apenas a **versão mais recente** via `ORDER BY _versao_ingestao DESC, row_number() == 1`. Incluir `VERSAO` na chave de unicidade significaria manter múltiplas versões do mesmo registro, contradizendo o propósito da Silver de ter um único registro por chave de negócio.

**GRUPO_DFP** — Cada notebook Silver processa um tipo de demonstração específico a partir de uma tabela Bronze dedicada (DRE -> `101_dre_dfp`, BPA -> `102_bpa_dfp`, BPP -> `103_bpp_dfp`). A separação por `GRUPO_DFP` já é garantida pela divisão de tabelas; a coluna é preservada na Silver para rastreabilidade, mas não participa da deduplicação.

### Notebooks que Implementam

* **201_cvm_dfp_dre** (célula 5) - DRE Silver
* **202_cvm_dfp_bpa** (célula 5) - BPA Silver
* **203_cvm_dfp_bpp** (célula 5) - BPP Silver

### Testes que Validam

* **test_integracao_dre** (VALIDACAO 4) - PKs únicas em 4 colunas
* **test_integracao_bpa** (VALIDACAO 4) - PKs únicas em 4 colunas
* **test_integracao_bpp** (VALIDACAO 4) - PKs únicas em 4 colunas

---

## Princípios de Design

### 1. Fail-Safe

Se validação falha, **dados originais são preservados**. DELETE só executa após todas as validações passarem.

### 2. Early Fail

Validações ocorrem o mais cedo possível no pipeline. Não desperdiça processamento em dados ruins.

### 3. Explícito

Erros são logados claramente:
* Console: mensagem descritiva
* Tabela de controle: registro permanente com status ERROR

### 4. Granular

Erro em um ano não impede processamento de outros anos (loop continua).

---

## Rastreamento de Erros

Todos os erros são registrados em `{SCHEMA_APOIO}.controle_ingestao`:

```sql
INSERT INTO {SCHEMA_APOIO}.controle_ingestao
    (fonte, ano, arquivo, last_modified_cvm, versao_ingestao, ingest_ts, status, mensagem)
VALUES (
    'dre',                      -- fonte
    2023,                       -- ano que falhou
    'dfp_cia_aberta_2023.zip',  -- arquivo
    NULL,                       -- last_modified (NULL em erro)
    NULL,                       -- versão (NULL em erro)
    current_timestamp(),        -- timestamp do erro
    'FAILED',                   -- status
    'Arquivo vazio! Abortando.' -- mensagem de erro (truncada em 500 chars)
)
```

Para investigar falhas:

```sql
SELECT ano, fonte, mensagem, ingest_ts
FROM {SCHEMA_APOIO}.controle_ingestao
WHERE status = 'FAILED'
ORDER BY ingest_ts DESC;
```

---

## Quando NÃO Usar Guardrails

**Não validar**:
* Valores de negócio (e.g., "receita deve ser positiva") → isso é responsabilidade da camada Gold/análise
* Formato de datas específico → transformações devem ser tolerantes
* Cardinalidade ("deve ter exatamente X empresas") → fonte externa pode mudar

**Validar apenas**:
* Presença de colunas críticas (schema)
* Arquivo não-vazio (sanity check)
* Dependências upstream existem (Bronze tem dados para Silver processar)

---

## Evolução Futura

Guardrails potenciais para considerar:

* **Landing Zone**: Validar arquivo ZIP não-corrompido (checksum?)
* **Bronze**: Detectar mudanças drásticas de schema (e.g., 50% das colunas mudaram)
* **Silver**: Detectar perda anormal de registros (e.g., Bronze tinha 30k, Silver gerou 300 - possível filtro errado)

**Critério**: Adicionar guardrail apenas se erro **já ocorreu** ou risco é **demonstravelmente alto**. Não adicionar preventivamente "por via das dúvidas".