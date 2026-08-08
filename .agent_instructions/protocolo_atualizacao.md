# Protocolo de Atualização de Documentação

## Como Usar Este Arquivo

**Comando padrão para o assistente**: "Atualize a documentação"

Ao final de qualquer sessão que implementou mudanças no projeto, o assistente deve:
1. Carregar este arquivo
2. Identificar quais tipos de mudanças foram feitas
3. Seguir a matriz de impactos abaixo
4. Atualizar todos os arquivos afetados

---

## ⚠️ ALERTA CRÍTICO PARA O ASSISTENTE

**REGRA INQUEBRAVEL**: Este arquivo DEVE ser lido ANTES de qualquer resposta sobre atualização de documentação.

### Processo Obrigatório:

1. **PARAR** - Não assumir nada, não confiar em memória
2. **LER** - Carregar este arquivo usando readAssetById (ID: 4053803626820843)
3. **MAPEAR** - Aplicar matriz de impactos para CADA mudança identificada
4. **LISTAR** - Criar checklist explícito com TODOS os arquivos mandatórios
5. **CONFIRMAR** - Apresentar checklist ao usuário antes de executar
6. **EXECUTAR** - Atualizar arquivos somente após confirmação

### Formato de Resposta Obrigatório:

```markdown
## 📋 CHECKLIST DE DOCUMENTAÇÃO

Protocolo carregado: ✅

### Mudanças Identificadas:
1. [mudança] - classificação: [tipo do protocolo]

### Arquivos Mandatórios (matriz aplicada):
- [ ] arquivo1.md → Seção X (razão)
- [ ] arquivo2.md → Seção Y (razão)

Confirma que posso prosseguir?
```

### PROIBIDO:
- ❌ Assumir quais arquivos atualizar sem consultar a matriz
- ❌ Confiar em memória de sessões anteriores
- ❌ Pular a leitura deste arquivo "porque já sei"
- ❌ Listar arquivos antes de ler o protocolo

### Por Que Esta Regra Existe:

Se o usuário precisa "ser enfático" para você seguir o protocolo, então:
- O protocolo é inútil
- A carga cognitiva está no lugar errado
- O sistema é frágil e não-confiável

O protocolo existe para TIRAR a carga do usuário e TORNAR o assistente sistemático.

---

## 🎯 Princípio DRY na Documentação

**Regra de Ouro**: Cada informação tem UMA ÚNICA localização autoritativa.

* **README.md**: Índice executivo - visão geral, estrutura, status alto nível, links
* **arquitetura.md**: Fonte única de verdade técnica - TODOS os detalhes (camadas, schemas, convenções, pipeline, estratégias)
* **evolucao_projeto.md**: Histórico cronológico - por que decisões foram tomadas
* **dicionario_dados.md**: Metadados de negócio

**NUNCA duplicar**: Detalhes técnicos (características de camadas, estratégias de gravação, convenções) vão APENAS em arquitetura.md.

---

## 📋 Matriz de Impactos

### ➕ Quando CRIAR/MODIFICAR NOTEBOOK

**Arquivos a atualizar:**
- [ ] `README.md` → Seção "Estrutura do Projeto" (atualizar árvore de diretórios, se necessário)
- [ ] `README.md` → Seção "Status Atual" (adicionar linha simples com ✅)
- [ ] `00_documentacao/tecnica/arquitetura.md` → Seção "Pipeline Implementado" (adicionar à tabela da camada correspondente com detalhes técnicos)
- [ ] `evolucao_projeto.md` → Novo registro de sessão (se houver decisão arquitetural ou evolução significativa)

**Exemplo README.md (simples):**
```markdown
- ✅ **Silver** - DRE transformada (notebook `201_cvm_dfp_dre.py`)
  - Tabela: `proj_cvm_02_silver.201_dre_dfp`
```

**Exemplo arquitetura.md (detalhado):**
```markdown
| Notebook | Tabela UC | Demonstração |
| --- | --- | --- |
| `201_cvm_dfp_dre.py` | `proj_cvm_02_silver.201_dre_dfp` | DRE transformada |

**Características Técnicas:**
* Filtro de versionamento: Window Function para versão mais recente
* Transformações: Conversão de tipos, normalização, remoção duplicatas
* Estratégia: DELETE WHERE ano + APPEND
```

---

### 🗄️ Quando CRIAR SCHEMA Unity Catalog

**Arquivos a atualizar:**
- [ ] `README.md` → Seção "Status Atual" (marcar schema como criado, se necessário)
- [ ] `00_documentacao/tecnica/arquitetura.md` → Seção da camada correspondente (documentar características, propósito, estratégias)

**Exemplo README.md:**
```markdown
- ✅ Scripts DDL (000_ddl_create_tables.py + 001_ddl_controle_ingestao.py)
```

**Exemplo arquitetura.md:**
```markdown
#### Camada Bronze (01_bronze/)
* **Objetivo**: Captura de dados brutos da fonte
* **Schema**: `proj_cvm_01_bronze`
* **Estratégia de Gravação**: APPEND (nunca deleta dados históricos)
* **Versionamento**: Colunas `_versao_ingestao`, `_last_modified_cvm`, `_ingest_ts`
```

---

### 📊 Quando CRIAR TABELA Unity Catalog

**Arquivos a atualizar:**
- [ ] `README.md` → Seção "Status Atual" (adicionar linha simples)
- [ ] `00_documentacao/tecnica/arquitetura.md` → Seção "Pipeline Implementado" (adicionar à tabela da camada + características técnicas)

**Exemplo README.md:**
```markdown
- ✅ **Bronze** - DRE com versionamento (notebook `101_cvm_dfp_dre.py`)
  - Tabela: `proj_cvm_01_bronze.101_dre_dfp`
```

**Exemplo arquitetura.md:**
```markdown
**Características Técnicas:**
* **Origem**: Leitura de Landing Zone
* **Versionamento**: Colunas `_versao_ingestao`, `_last_modified_cvm`, `_ingest_ts`
* **Estratégia**: APPEND-ONLY (histórico completo preservado)
* **Particionamento**: Por `ano`
```

---

### 🏗️ Quando TOMAR DECISÃO ARQUITETURAL

**Arquivos a atualizar:**
- [ ] `evolucao_projeto.md` → Novo registro completo com:
  - Data da sessão
  - Contexto que motivou a decisão
  - Decisões técnicas tomadas
  - Alternativas consideradas
  - Justificativas
  - Implementações realizadas
  - Aprendizados (Key Insight)
- [ ] `00_documentacao/tecnica/arquitetura.md` → Atualizar seções técnicas relevantes com a decisão implementada

**Decisões arquiteturais incluem:**
* Escolha de tecnologia (PySpark vs SQL, Delta vs Parquet, etc)
* Padrões de nomenclatura
* Estrutura de pipelines
* Modelagem de dados
* Estratégias de particionamento
* Políticas de retenção
* Arquitetura de camadas
* Estratégias de gravação (APPEND, DELETE+APPEND, OVERWRITE)
* Versionamento e rastreabilidade

---

### 📁 Quando CRIAR/MODIFICAR PASTA

**Arquivos a atualizar:**
- [ ] `README.md` do projeto → Seção "Estrutura do Projeto" (atualizar árvore de diretórios)

**Exemplo:**
```
projeto_cvm_dados_financeiros/
├── 00_documentacao/
│   ├── evolucao_projeto.md
│   ├── tecnica/
│   └── negocio/
├── 01_bronze/
│   └── 101_cvm_dfp_dre.ipynb
```

---

### 📝 Quando MUDAR PADRÃO/CONVENÇÃO

**Arquivos a atualizar:**
- [ ] Arquivo de especificação relevante em `.agent_instructions/`
  - `nomenclaturas.md` - mudanças em naming
  - `estrutura_notebooks.md` - mudanças em estrutura de células
  - `unity_catalog.md` - mudanças em schemas/tabelas
  - `escolha_sql_pyspark.md` - mudanças em critérios de decisão
- [ ] `00_documentacao/tecnica/arquitetura.md` → Seção "Padrões de Desenvolvimento" (atualizar convenções, princípios, estruturas)
- [ ] `evolucao_projeto.md` → Justificativa da mudança de padrão (OBRIGATÓRIO - mudanças de padrão são decisões arquiteturais)

**NUNCA**: Documentar convenções técnicas no README.md - elas pertencem a arquitetura.md

---

### 📄 Quando CRIAR/MODIFICAR ARQUIVO DE DOCUMENTAÇÃO

**Arquivos a atualizar:**
- [ ] `README.md` do projeto → Seção "Documentação" (adicionar referência ao novo arquivo)

**Exemplo:**
```markdown
* **Decisões Técnicas e Evolução**: Ver [00_documentacao/evolucao_projeto.md](00_documentacao/evolucao_projeto.md)
```

---

### 🚀 Quando IMPLEMENTAR NOVA CAMADA (Bronze/Silver/Gold)

**Arquivos a atualizar:**
- [ ] `README.md` → Seção "Status Atual" (adicionar linha simples de status)
- [ ] `00_documentacao/tecnica/arquitetura.md` → Seção "Pipeline Implementado" (criar/atualizar subseção completa da camada com tabela de notebooks/tabelas e características técnicas)
- [ ] `evolucao_projeto.md` → Novo registro cronológico da implementação

**Exemplo README.md:**
```markdown
- ✅ **Silver** - DRE transformada (notebook `201_cvm_dfp_dre.py`)
  - Tabela: `proj_cvm_02_silver.201_dre_dfp`
```

**Exemplo arquitetura.md:**
```markdown
### Camada Silver

**Notebooks e Tabelas:**

| Notebook | Tabela UC | Demonstração |
| --- | --- | --- |
| `201_cvm_dfp_dre.py` | `proj_cvm_02_silver.201_dre_dfp` | DRE transformada |

**Características Técnicas:**
* Filtro de versionamento
* Transformações aplicadas
* Estratégia de gravação
* Particionamento
```

---

### 🔧 Quando EXPANDIR PARA NOVA FONTE DE DADOS

**Arquivos a atualizar:**
- [ ] `README.md` → Seção "Fontes de Dados" (adicionar nova demonstração à lista)
- [ ] `README.md` → Seção "Status Atual" (adicionar linhas de pipeline de dados)
- [ ] `00_documentacao/tecnica/arquitetura.md` → Seção "Pipeline Implementado" (adicionar notebooks/tabelas nas camadas com detalhes técnicos completos)
- [ ] `evolucao_projeto.md` → Novo registro cronológico com decisões de modelagem

**Exemplo README.md:**
```markdown
### CVM - Demonstrações Financeiras Padronizadas (DFP)
* **Demonstrações Implementadas**:
  - DRE (Demonstração do Resultado do Exercício)
  - BPA (Balanço Patrimonial Ativo)
  - BPP (Balanço Patrimonial Passivo)

**Pipeline de Dados:**
- ✅ **Bronze** - BPP com versionamento (notebook `103_cvm_dfp_bpp.py`)
  - Tabela: `proj_cvm_01_bronze.103_bpp_dfp`
```

**Exemplo arquitetura.md:**
```markdown
| `103_cvm_dfp_bpp.py` | `proj_cvm_01_bronze.103_bpp_dfp` | BPP (Balanço Patrimonial Passivo) |

**Características Técnicas:**
* (detalhes completos aqui)
```

---

## 🔄 Workflow Padrão de Fechamento de Sessão

Ao final de cada sessão relevante, seguir este checklist:

1. **Identificar mudanças**: O que foi criado/modificado nesta sessão?
2. **Carregar protocolo**: Ler este arquivo (`protocolo_atualizacao.md`)
3. **Mapear impactos**: Consultar matriz acima
4. **Aplicar DRY**: Garantir que detalhes técnicos vão APENAS para arquitetura.md
5. **Atualizar arquivos**: Aplicar todas as atualizações necessárias
6. **Registrar no evolucao_projeto.md**: Se houver evolução significativa
7. **Validar consistência**: Verificar se não há duplicações entre README e arquitetura.md

---

## 💡 Exemplo de Uso Prático

**Cenário**: Usuário criou notebook `301_cvm_dfp_dre` na camada Gold

**Assistente deve:**

1. Atualizar `README.md` (SIMPLES):
   - Seção "Estrutura do Projeto": adicionar `301_cvm_dfp_dre.py` em `03_gold/` (se necessário)
   - Seção "Status Atual" → "Pipeline de Dados":
     ```markdown
     - ✅ **Gold** - KPIs DRE (notebook `301_cvm_dfp_dre.py`)
       - Tabela: `proj_cvm_03_gold.301_kpis_dre`
     ```

2. Atualizar `00_documentacao/tecnica/arquitetura.md` (DETALHADO):
   - Seção "Pipeline Implementado" → Subseção "Camada Gold":
     ```markdown
     ### Camada Gold
     
     **Notebooks e Tabelas:**
     
     | Notebook | Tabela UC | Descrição |
     | --- | --- | --- |
     | `301_cvm_dfp_dre.py` | `proj_cvm_03_gold.301_kpis_dre` | KPIs financeiros DRE |
     
     **Características Técnicas:**
     * **Agregações**: Margem líquida, EBITDA, ROE por empresa/período
     * **Estratégia**: DELETE WHERE + APPEND (reprocessa períodos afetados)
     * **Particionamento**: Por ano
     * **Métricas calculadas**: [lista completa]
     ```

3. Atualizar `evolucao_projeto.md`:
   - Nova entrada cronológica com data da sessão
   - Contexto: por que implementar Gold agora?
   - Decisões: quais métricas/KPIs, justificar escolhas de agregação
   - Implementado: lista objetiva do que foi feito
   - Key Insight: aprendizado não-óbvio

---

## 📌 Notas Importantes

* **Princípio DRY é sagrado**: Cada informação técnica tem UMA ÚNICA localização - arquitetura.md
* **Não confiar em memória**: Sempre carregar este arquivo explicitamente ao atualizar documentação
* **Checklist completa**: Não pular etapas mesmo que pareçam óbvias
* **arquitetura.md é fonte única técnica**: TODOS os detalhes (camadas, schemas, convenções, estratégias, pipeline implementado)
* **README.md é índice executivo**: Visão geral, estrutura, status alto nível, links para arquitetura.md
* **evolucao_projeto.md é obrigatório**: Para qualquer evolução significativa ou decisão arquitetural
* **Consistência é crítica**: Documentação desatualizada quebra confiança em entrevistas técnicas
* **Nunca duplicar**: Se está em arquitetura.md, NÃO está em README.md (exceto referência/link)