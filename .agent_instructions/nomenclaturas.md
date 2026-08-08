# Nomenclatura e Convenções

## Princípios Gerais

* **Nomenclatura autocontida**: Todos os nomes (notebooks, células, dataframes, tabelas, variáveis) devem ser autoexplicativos - o nome deve descrever claramente o conteúdo ou propósito do objeto.

* **Convenção de caixa**: 
  - Preferência por letras minúsculas em toda nomenclatura (pastas, arquivos, variáveis, tabelas)
  - **EXCEÇÃO**: Títulos de células devem estar sempre em MAIÚSCULO

## Notebooks

* **Formato**: Sempre usar letras minúsculas, separar palavras com `_` (underscore) ou `-` (hífen). NUNCA usar espaços.

* **Numeração e Rastreabilidade (3 dígitos obrigatórios)**:
  - **Formato**: `[XXX]_[nome_base_idêntico_entre_camadas]` — SEMPRE 3 dígitos
  - **Primeiro dígito**: identifica a camada (1=bronze, 2=silver, 3=gold)
  - **Dois dígitos seguintes**: sequência dentro da camada (01-99)
  
  - **REGRA CRÍTICA DE RASTREABILIDADE**: 
    - O **nome base** (tudo após os 3 dígitos) deve ser **IDÊNTICO** nas três camadas
    - Apenas o **primeiro dígito** muda para indicar a camada
    - Isso cria rastreabilidade visual perfeita do fluxo de dados através do pipeline
  
  - **Exemplos corretos**:
    - `101_cvm_dfp_dre` → `201_cvm_dfp_dre` → `301_cvm_dfp_dre`
    - `102_dados_clientes` → `202_dados_clientes` → `302_dados_clientes`
  
  - **Exemplos INCORRETOS** (quebram rastreabilidade):
    - ❌ `101_ingestao_cvm` → `201_transformacao_cvm` → `301_agregacao_cvm` (nomes diferentes)
    - ❌ `101_cvm_dfp_dre` → `201_dre_dfp_silver` → `301_indicadores` (nomes diferentes)
  
  - **Benefícios**: 
    - Rastreabilidade imediata entre camadas (101→201→301)
    - Consistência visual perfeita
    - Ordenação lógica automática
    - Facilita identificar fluxos de dados relacionados
  
  - **CRÍTICO**: NUNCA usar apenas números sem nome descritivo — seria irracional e não autodescritivo

## DataFrames

* **Padrão**: `df_[descricao_autocontida_do_dado]`

* **NÃO incluir**:
  - Prefixo de camada (bronze/silver/gold) quando o notebook é dedicado a uma única camada - a camada já está explícita no nome do notebook e no schema do Unity Catalog
  - Sufixo de tecnologia (spark, pandas, etc) - o contexto tecnológico já está implícito no notebook

* **Foco**: Descrever o DADO, não a infraestrutura ou camada
* **Exemplo**: `df_dre_consolidada` (não `df_bronze_dre_consolidada_spark`)
* **Exceção**: Quando um notebook processa múltiplas camadas (ex: transformação bronze→silver), incluir a camada para clareza

## Princípio DRY (Don't Repeat Yourself)

* **Regra**: Evitar redundância com a hierarquia de pastas
* **O nome do arquivo NÃO deve repetir informações já explícitas na estrutura de diretórios**
* **Exemplos**: 
  - Se está em `/bronze/`, não adicionar sufixo `_bronze`
  - Se está em `/projeto_x/`, não adicionar prefixo `projeto_x_`
* **A camada e o projeto são definidos pela PASTA, não pelo NOME DO ARQUIVO**

## Pastas

* **Hierarquia de diretórios**: Organizar sempre por pastas com nomes autocontidos:
  1. Nível 1: Nome do projeto
  2. Nível 2: Camada (bronze/silver/gold ou equivalente)
  3. Nível 3: Notebooks por tema
  4. Pasta dedicada: `/documentacao` subdividida em `/tecnica` e `/negocio`

* **Numeração de pastas (BEST PRACTICE)**:
  - Sempre numerar pastas de camadas para forçar ordenação lógica (não alfabética)
  - Padrão: `01_bronze`, `02_silver`, `03_gold`
  - Convenção estabelecida em engenharia de dados, usada pela Databricks em exemplos oficiais
  - Facilita navegação e deixa clara a sequência do pipeline

## Versionamento

* **Git**: Usar Git apenas para código refinado e decisões consolidadas
* **Evitar**: Sufixos como `_v1`, `_v2` - isso é considerado desorganização

---

💡 **Ao implementar mudanças, consulte `.agent_instructions/protocolo_atualizacao.md` (ID: 4053803626820843) para atualizar documentações afetadas.**