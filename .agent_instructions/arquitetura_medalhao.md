# Arquitetura Medalhão — Análise Arquitetural

## 1. O problema que a arquitetura resolve

A arquitetura medalhão não é sobre "ter três camadas". É uma resposta a uma
tensão estrutural do processamento de dados: **o dado bruto é confiável quanto à
origem mas inútil para consumo; o dado refinado é útil mas frágil quanto à
auditabilidade.** Um único estágio força a escolher entre os dois. O medalhão
desacopla essa tensão em estágios, cada um com um contrato próprio.

O ganho central é a **reprocessabilidade**: como cada camada é derivável da
anterior por transformações determinísticas, qualquer erro de lógica é corrigível
reprocessando a jusante, sem perda de origem. Isso só é verdade se a fronteira
entre camadas for respeitada com rigor — é aí que a maioria das implementações
falha.


## 2. Princípios fundamentais (o "porquê" antes do "como")

1. **Progressão monotônica de qualidade e semântica.** Cada camada só adiciona
   estrutura, nunca a remove arbitrariamente. Bronze → Silver → Gold é um fluxo
   de refinamento crescente.

2. **Imutabilidade da origem.** Bronze é a fronteira de verdade. Tudo a jusante
   é reconstruível; Bronze, não. Se você não consegue recriar Silver e Gold só a
   partir de Bronze, a arquitetura está quebrada.

3. **Determinismo das transformações.** Reprocessar o mesmo Bronze deve produzir
   o mesmo Silver/Gold — dada a mesma versão de regras. Não-determinismo
   (ex.: `current_timestamp()` como chave, ordenação instável em dedupe) destrói
   a reprodutibilidade e é uma fonte silenciosa de divergência.

4. **Separação de responsabilidades por camada.** Cada camada resolve UMA classe
   de problema. Misturar limpeza técnica (Silver) com regra de negócio (Gold) na
   mesma etapa é o antipadrão mais comum e o mais caro de desfazer.


## 3. Anatomia das camadas

### 3.1 Bronze — camada de captura (raw)

**Contrato:** representar fielmente o que a fonte entregou, com rastreabilidade
de ingestão.

| Faz                                          | NÃO faz                              |
|----------------------------------------------|--------------------------------------|
| Ingestão as-is do dado bruto                 | Casting de tipos de negócio          |
| Adiciona metadados técnicos de ingestão      | Deduplicação                         |
| Preserva duplicatas, nulos, inconsistências  | Aplicação de regra de negócio        |
| Valida forma (arquivo/coluna existe?)        | Transformação de conteúdo            |
| Append-only (histórico de tudo que chegou)   | Filtro semântico de linhas           |

Metadados técnicos típicos (não descaracterizam o "raw" porque não alteram
conteúdo):

```python
df_bronze = (
    df_raw
    .withColumn("_ingest_ts",     current_timestamp())
    .withColumn("_source_file",   input_file_name())
    .withColumn("_run_id",        lit(run_id))
    .withColumn("_ingest_date",   current_date())
)
```

**Distinção crítica — validar ≠ transformar.** Validação de schema/arquivo no
Bronze verifica e rejeita/alerta, mas não muda o conteúdo. É por isso que
guardrails de validação de forma pertencem ao Bronze sem violar o "raw".
Transformação de conteúdo é o que marca a passagem para Silver.

**Formato:** Delta, append-only. O padrão maduro usa ingestão incremental
(Auto Loader) com checkpoint, garantindo *exactly-once* na captura de arquivos
novos.


### 3.1.1 Landing Zone e Preservação de Arquivos Originais

Algumas arquiteturas separam **Landing Zone** (arquivos originais preservados) de
**Bronze** (dados ingeridos em Delta). Esse padrão adiciona uma camada de
auditabilidade e resiliência:

**Landing Zone:**
- Arquivos originais preservados *exatamente* como baixados da fonte
- Organizados por período/fonte em Unity Catalog Volumes
- Metadados HTTP preservados (Last-Modified, tamanho, checksum)
- Versionamento de arquivos: se a fonte atualiza arquivo histórico, nova versão
  é preservada (ex.: `arquivo_2020.zip.v1`, `arquivo_2020.zip.v2`)

**Vantagens:**
- **Reprocessamento offline**: Bronze pode ser reconstruído mesmo se a fonte
  externa ficar indisponível
- **Auditoria forense**: arquivo original disponível para validação/comparação
- **Detecção de correções**: rastrear quando a fonte atualizou dados históricos
- **Cache local**: reduz dependência de rede/APIs externas em reprocessamentos

**Trade-off:** duplicação de storage (arquivo + Delta). Vale quando:
- Fonte externa não é confiável (APIs instáveis, SLAs fracos)
- Requisitos de compliance exigem preservar arquivo original
- Fonte faz correções retroativas (arquivos históricos mudam)

```python
# Estrutura típica de Landing Zone em UC Volume
/Volumes/catalog/schema/landing/
  ├── fonte_a/
  │   ├── 2020/
  │   │   ├── arquivo_2020.zip
  │   │   ├── arquivo_2020.zip.v2  # Versão atualizada pela fonte
  │   │   └── _metadata.json       # Last-Modified, URL, timestamp
  │   └── 2021/
  │       └── arquivo_2021.zip
  └── fonte_b/
      └── ...
```

**Fluxo com Landing Zone:**
```
Fonte externa → Landing Zone (Volume) → Bronze (Delta) → Silver → Gold
```

**Detecção de atualizações:** comparar `Last-Modified` HTTP da fonte com metadado
local. Se mais recente, re-baixar e versionar. Bronze detecta novas versões via
tabela de controle.


### 3.2 Silver — camada de conformação (limpeza técnica)

**Contrato:** produzir um dado limpo, tipado e conformado, fiel ao grão da
origem, mas ainda **sem regra de negócio**.

Transformações que pertencem aqui:

- **Casting** para tipos corretos (string → date/decimal/int).
- **Deduplicação** (definir a chave natural e a política de desempate —
  ordenação determinística obrigatória).
- **Tratamento de nulos** (rejeição, default, ou quarentena).
- **Padronização** (normalização de encoding, trim, casing, unidades).
- **Conformação de schema** entre múltiplas fontes que descrevem a mesma entidade.
- **Validações de integridade** (unicidade de chave, faixas válidas, invariantes
  de soma).

```python
w = Window.partitionBy("chave_natural").orderBy(col("_ingest_ts").desc())

df_silver = (
    df_bronze
    .withColumn("valor", col("valor").cast("decimal(18,2)"))
    .withColumn("_rn", row_number().over(w))
    .filter(col("_rn") == 1)          # dedupe determinístico
    .drop("_rn")
    .filter(col("chave_natural").isNotNull())
)
```

**A linha que não se cruza:** se uma transformação depende de conhecimento de
negócio ("custo do sistema X vai para a área Y"), ela **não é Silver**. Silver é
limpeza *agnóstica de negócio*. O teste prático: um engenheiro sem contexto de
domínio conseguiria escrever a regra? Se sim, é Silver. Se precisa de alguém do
negócio para definir, é Gold.


### 3.3 Gold — camada de consumo (semântica de negócio)

**Contrato:** entregar dado modelado para consumo — agregações, regras de
negócio aplicadas, modelagem dimensional, métricas prontas.

- **Agregação** ao grão de consumo (não mais o grão da origem).
- **Regras de negócio** (alocações, rateios, classificações, derivações).
- **Modelagem** (fatos e dimensões, ou tabelas *wide* orientadas a consumo).
- **Invariantes de negócio** (ex.: conservação — a soma alocada deve igualar a
  soma de entrada).
- **Rastreabilidade de regra** (qual regra produziu cada registro).

**Ponto que a maioria ignora — versionamento de regra.** Gold é onde a regra vive,
e regra muda no tempo. Rastrear *qual versão da regra* produziu cada registro
(ex.: coluna de rule-code concatenado por registro) é o que permite auditar e
reprocessar corretamente. E leva à armadilha abaixo.


## 4. A fronteira mais difícil: reprocessamento histórico × versão de regra

Reprocessar Gold a partir de Bronze é trivial *se a regra for a mesma*. O problema
aparece quando você reprocessa um período antigo **hoje**, com o código de regra
de **hoje**:

- Se o objetivo é **corrigir um bug** → reprocessar com a regra atual é o certo.
- Se o objetivo é **reconstruir o que foi reportado à época** → você precisa
  aplicar a *versão da regra vigente naquele momento*, não a atual.

Esses dois objetivos são incompatíveis num pipeline que trata regra como código
mutável. Reproduzir fielmente o passado exige que a regra seja **versionada como
dado** (tabela de regras com vigência temporal) e não apenas como código:

```sql
-- regra como dado versionado, não como lógica hardcoded
SELECT f.*, r.rule_code
FROM   fato f
JOIN   dim_regra r
  ON   f.chave = r.chave
 AND   f.data_competencia BETWEEN r.vigencia_ini AND r.vigencia_fim
```

Sem isso, "reprocessar o histórico" e "preservar a versão de regra da época" são
mutuamente exclusivos. Decidir *qual* dos dois você quer garantir é uma decisão
de arquitetura, não de implementação — e precisa ser explícita.


## 5. Preocupações transversais

### 5.1 Idempotência
Toda escrita a jusante deve ser idempotente: rodar duas vezes o mesmo período não
pode duplicar nem divergir. Padrões: `MERGE` por chave+competência, ou
overwrite particionado (`replaceWhere`). Append cego em camadas refinadas é
antipadrão.

### 5.2 Governança (Unity Catalog)
As camadas mapeiam naturalmente para a hierarquia `catálogo → schema → tabela`.
Padrão comum: um schema (ou catálogo) por camada (`bronze`, `silver`, `gold`),
com permissões progressivamente mais amplas — Bronze restrito a engenharia, Gold
liberado para consumo analítico. Linhagem e controle de acesso vivem aqui, não no
código.

### 5.3 Qualidade
Guardrails posicionados por camada, cada um coerente com o contrato:
- Bronze: validação de forma (arquivo/coluna).
- Silver: dedupe, nulos, invariantes técnicas (ex.: variância de soma).
- Gold: regras de negócio, invariante de conservação, variância histórica.

Uma tabela de auditoria central + log de execução transforma os guardrails de
"checagens dispersas" em observabilidade rastreável.

### 5.4 Observabilidade
Log de execução por camada + audit table + métricas de volume/rejeição. Sem isso,
uma divergência em Gold é impossível de localizar entre 30 tabelas.

### 5.5 Estratégias de Gravação por Cenário

A escolha entre `DELETE WHERE + APPEND`, `MERGE`, e `replaceWhere` depende do padrão
de atualização dos dados e da camada:

#### Bronze: Sempre APPEND
```python
# Bronze é append-only por definição
df_bronze.write.mode("append").saveAsTable("bronze.tabela")
```

**Nunca:** DELETE WHERE em Bronze. Versionamento via coluna `_versao_ingestao`.

#### Silver/Gold: Cenários

**Cenário 1: Ingestão batch periódica (ex: dados anuais/trimestrais)**

```python
# Estratégia: DELETE WHERE + APPEND
# Remove período sendo reprocessado, depois insere nova versão

# 1. Deletar período a ser reprocessado
spark.sql(f"""
  DELETE FROM silver.tabela
  WHERE ano IN ({','.join(map(str, anos_processar))})
""")

# 2. Inserir nova versão
df_silver.write.mode("append").saveAsTable("silver.tabela")
```

**Quando usar:**
- Dados organizados em períodos fixos (ano, trimestre, mês)
- Reprocessamento por período completo (não por registro individual)
- Simplicidade: 2 comandos SQL sequenciais, fácil de debugar
- Não há risco de DELETE + APPEND parcial (ambos em mesma transação)

**Vantagens:**
- Idempotente: rodar N vezes produz mesmo resultado
- Simples de auditar (log explícito de DELETE + volume de INSERT)
- Performance previsível para volumes grandes

**Desvantagens:**
- Não funciona para streaming (precisa de período fechado)
- Window de DELETE + APPEND pode causar leituras inconsistentes se não transacionado

---

**Cenário 2: Streaming ou CDC incremental**

```python
# Estratégia: MERGE (upsert)
from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, "silver.tabela")

delta_table.alias("target").merge(
    df_novos_dados.alias("source"),
    "target.chave_natural = source.chave_natural"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

**Quando usar:**
- Streaming contínuo ou micro-batches
- CDC (Change Data Capture): inserts, updates, deletes por registro
- Não há "período fechado" — dados chegam continuamente

---

**Cenário 3: Sobrescrever partições específicas**

```python
# Estratégia: replaceWhere (Delta Lake)
df_silver.write \\
    .mode("overwrite") \\
    .option("replaceWhere", f"ano IN ({','.join(map(str, anos_processar))})") \\
    .saveAsTable("silver.tabela")
```

**Quando usar:**
- Tabela particionada por coluna de período
- Quer atomicidade: partição substituída em transação única
- Equivalente a DELETE WHERE + APPEND, mas atômico

**Vantagens:**
- Atômico: leitores não veem estado intermediário
- Otimizado para partições grandes

**Desvantagens:**
- Requer particionamento físico por coluna usada no `replaceWhere`
- Menos flexível que DELETE WHERE (precisa match exato com partição)

---

**Resumo - Escolha por Contexto:**

| Cenário | Bronze | Silver/Gold |
|---------|--------|-------------|
| Batch periódico (ano/trimestre) | APPEND | DELETE WHERE + APPEND ou replaceWhere |
| Streaming contínuo | APPEND | MERGE |
| CDC incremental | APPEND | MERGE |
| Versionamento histórico | APPEND | Filtro em query (via `_versao_ingestao`) |

**Antipadrão:** APPEND sem dedupe em Silver/Gold. Gera duplicatas em reprocessamento.


## 6. Antipadrões (o que quebra a arquitetura)

| Antipadrão                                          | Consequência                              |
|-----------------------------------------------------|-------------------------------------------|
| Regra de negócio no Silver                          | Perde reprodutibilidade; Silver vira Gold |
| Bronze com transformação de conteúdo                | Perde a fonte de verdade; sem rollback    |
| Pular Silver (Bronze → Gold direto)                 | Limpeza e regra acopladas; indebugável    |
| Regra como código mutável sem versão                | Reprocessamento histórico infiel          |
| Append não-idempotente em Silver/Gold               | Duplicação em reprocessamento             |
| Dedupe com ordenação não-determinística             | Resultado varia entre execuções           |
| Camadas sem contrato explícito                      | Fronteiras erodem; tudo vira "meio-Gold"  |


## 7. Trade-offs e variações

- **Raw/Landing antes do Bronze.** Algumas arquiteturas separam "arquivos
  originais intocados" (landing) de "Delta ingerido com metadados" (Bronze).
  Muda o vocabulário, não o princípio.
- **Camadas extras.** Projetos grandes às vezes inserem uma camada intermediária
  (ex.: "Silver conformado" × "Silver enriquecido") ou uma camada de *serving*
  separada do Gold. Legítimo quando o grão de consumo diverge muito do grão
  limpo.
- **Quando NÃO usar medalhão.** Dado que já chega limpo e governado, pipelines
  triviais, ou latência ultra-baixa onde três hops são caros demais. Medalhão
  cobra em I/O e storage o que paga em auditabilidade — nem sempre vale.


## 8. Mapeamento para Databricks

| Conceito medalhão            | Implementação Databricks                          |
|------------------------------|---------------------------------------------------|
| Tabelas de cada camada       | Delta tables (Unity Catalog managed)              |
| Ingestão incremental Bronze  | Auto Loader (`cloudFiles`) + checkpoint           |
| Transformação Silver/Gold    | PySpark ou SQL                                     |
| Qualidade declarativa        | Expectations (Lakeflow Declarative Pipelines)     |
| Orquestração procedural      | Lakeflow Jobs (tasks de notebook encadeadas)      |
| Orquestração declarativa     | Lakeflow Declarative Pipelines (DAG automático)   |
| Governança/linhagem          | Unity Catalog                                     |

**Job vs Pipeline no contexto medalhão:** um pipeline procedural (notebooks
orquestrados por um Job) dá controle total e guardrails customizados por camada,
ao custo de você manter a ordem e as validações na mão. Uma Declarative Pipeline
infere o DAG entre camadas e move os guardrails para *expectations* declarativas,
ao custo de menos controle fino. A escolha depende de quanto da sua lógica de
qualidade é expressável declarativamente — migrar guardrails imperativos ricos
para expectations nem sempre é 1:1.