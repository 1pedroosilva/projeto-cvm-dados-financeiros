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

Bronze usa estratégia **DELETE WHERE ano + APPEND**. Guardrails garantem que DELETE só executa se há dados válidos para substituir.

### Guardrails Implementados

| Guardrail | Condição | Ação se Falha | Razão |
| --- | --- | --- | --- |
| **Arquivo vazio** | `len(df_pandas) == 0` | PARA (Bronze preservada) | Evita DELETE de dados bons seguido de APPEND vazio |
| **Schema inválido** | Colunas essenciais faltando | PARA (Bronze preservada) | Via `validar_e_projetar_schema()` - valida presença de colunas críticas definidas em `config_parametros.py` |

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
        
        # [3/4] DELETE + APPEND (só executa se guardrails passaram)
        spark.sql(f"DELETE FROM bronze WHERE year(DT_REFER) = {ano}")
        df_bronze.write.append()
        
    except Exception as e:
        # Bronze NÃO modificada (DELETE não foi executado)
        print(f"ERRO: {e}")
        # Registra erro em controle_ingestao
        # Continua para próximo ano
        continue
```

### Notebooks que Implementam

* **101_cvm_dfp_dre** (célula 5) - DRE
* **102_cvm_dfp_bpa** (célula 5) - BPA

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
    "DT_INI_EXERC", "DT_FIM_EXERC", "CD_CONTA", "DS_CONTA",
    "VL_CONTA", "ST_CONTA_FIXA"
]
```

---

## Silver - Guardrails de Transformação

### Contexto

Silver também usa **DELETE WHERE ano + APPEND**. Guardrail garante que não apaga Silver se Bronze não tem dados para processar.

### Guardrails Implementados

| Guardrail | Condição | Ação se Falha | Razão |
| --- | --- | --- | --- |
| **Bronze vazia** | `count_bronze == 0` | SKIP (Silver preservada) | Evita DELETE de Silver quando Bronze não tem dados para o ano |

### Fluxo

```python
for ano in ANOS_PROCESSAR:
    # GUARDRAIL: Bronze tem dados?
    count_bronze = spark.table("proj_cvm_01_bronze.101_dre_dfp") \
        .filter(year(col("DT_REFER")) == ano) \
        .count()
    
    if count_bronze == 0:
        print(f"Bronze vazia para ano {ano} - SKIP")
        continue  # Silver preservada, não executa DELETE
    
    # Processar
    df_bronze = spark.table("bronze").filter(...)
    df_silver = transform(df_bronze)
    
    # DELETE + APPEND
    spark.sql(f"DELETE FROM silver WHERE ANO = {ano}")
    df_silver.write.append()
```

### Notebooks que Implementam

* **201_cvm_dfp_dre** (célula 4) - DRE Silver
* **202_cvm_dfp_bpa** (célula 4) - BPA Silver

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

Todos os erros são registrados em `proj_cvm_04_apoio.controle_ingestao`:

```sql
INSERT INTO proj_cvm_04_apoio.controle_ingestao
    (fonte, ano, arquivo, last_modified_cvm, versao_ingestao, ingest_ts, status, mensagem)
VALUES (
    'dre',                      -- fonte
    2023,                       -- ano que falhou
    'dfp_cia_aberta_2023.zip',  -- arquivo
    NULL,                       -- last_modified (NULL em erro)
    NULL,                       -- versão (NULL em erro)
    current_timestamp(),        -- timestamp do erro
    'ERROR',                    -- status
    'Arquivo vazio! Abortando.' -- mensagem de erro (truncada em 500 chars)
)
```

Para investigar falhas:

```sql
SELECT ano, fonte, mensagem, ingest_ts
FROM proj_cvm_04_apoio.controle_ingestao
WHERE status = 'ERROR'
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