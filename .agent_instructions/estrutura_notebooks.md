# Estrutura de Notebooks

## Formato de Arquivo

**REGRA OBRIGATÓRIA**: Notebooks devem ser SEMPRE criados no formato `.py` (Python Source / Databricks format).

**Justificativa - Versionamento Git Limpo**:
* **Diff legível**: Formato texto puro, linha por linha, ideal para code review em Pull Requests
* **Sem noise de metadata**: `.ipynb` (JSON) reescreve `execution_count` e outputs a cada save, poluindo diffs mesmo sem mudança lógica
* **Padrão de mercado**: Times que usam Databricks Repos + CI/CD geralmente padronizam em `.py` exatamente por isso
* **Histórico git como narrativa**: Para projetos de portfólio onde commits incrementais contam a história técnica, `.py` trabalha a favor
* **Menos conflitos de merge**: Metadados JSON são fonte clássica de conflitos chatos em colaboração

**Observação - Metadados do Workspace**:
* Data de criação, histórico de execução, execution_count: metadados internos do Databricks Workspace
* **Não aparecem no GitHub/portfólio** - audiência externa só vê o código commitado
* Para portfólio técnico, o que importa é o histórico git (commits, PRs, evolução do código)

**PROIBIDO**: Criar notebooks em formato `.ipynb` (Jupyter Notebook)
* Diffs poluídos com metadata JSON não-relevante
* Dificulta revisão de código (mudanças reais se perdem no noise)
* Conflitos de merge frequentes em outputs e execution_count

---

## Limitação Técnica Importante

**CÉLULAS MARKDOWN NÃO TÊM CAMPO DE TÍTULO**

* Apenas células de **código** (Python, SQL, Scala, R) possuem campo editável de título ao lado da numeração
* Células de **Markdown** não suportam títulos por limitação técnica da ferramenta Databricks
* Esta é uma característica da plataforma, não uma escolha de padrão
* Portanto: NUNCA cobrar ou esperar títulos em células markdown

## Células Iniciais

### Célula 1 - Documentação
* **Tipo**: Markdown
* **Conteúdo**: Explicação do objetivo, conteúdo e função do notebook
* **Título**: Não aplicável - células markdown não têm campo de título

### Célula 2 - Inicialização e Imports
* **Tipo**: Código (Python/Scala/R)
* **Título**: "INICIALIZAÇÃO E IMPORTS" (em MAIÚSCULO)
* **Regra**: Todos os imports devem estar concentrados aqui
* **NUNCA**: Espalhar imports pelo notebook

#### Carregamento de Módulos Compartilhados
* **Padrão obrigatório**: `%run ./nome_do_modulo` (caminho relativo)
* **PROIBIDO**: Usar caminho absoluto (`/Workspace/Users/...`) ou `open()` + `exec()`
* **Motivo**: Caminhos absolutos quebram portabilidade entre workspaces/contas e expõem e-mail/usuário no código
* **Exemplos**:
  - Notebook em `01_bronze/` carregando `04_apoio/config_parametros.py`: `%run ../04_apoio/config_parametros`
  - Notebook em `04_apoio/` carregando módulo na mesma pasta: `%run ./config_parametros`

## Demais Células

### Estrutura Padrão

Cada célula deve:

* **Ter título em MAIÚSCULO**

* **Seguir padrão de 2 linhas de cabeçalho**:
  - Linha 1: `# df_[nome]: [descrição do que a célula faz]`
  - Linha 2: `# [explicação técnica/de negócio do motivo/abordagem]`

* **Fluxo de dados**:
  - Ler de um ou mais dataframes/tabelas
  - Aplicar transformação
  - Gerar um novo dataframe de saída

* **Nomenclatura de saída**: `df_[descricao_autocontida]`

## Separação de Responsabilidades

**CRÍTICO**: Uma célula não deve fazer muitas coisas.

### Separar transformações em células distintas:

* **Células que leem dados** (de tabelas ou outros dataframes)
* **Células que transformam** (filtros, limpezas, conversões)
* **Células que consultam** (queries analíticas)
* **Células que fazem cálculos** (métricas, KPIs)
* **Células que agregam** (GROUP BY, sumarizações)
* **Células que gravam** (geralmente as últimas) - salvam dataframes resultantes em tabelas

## Comentários e Documentação

* **Comentários balanceados**: Comentários devem estar "na medida" - nem excessivos, nem escassos
* **Linguagem de negócio**: Preferir linguagem de negócio em vez de jargão técnico desnecessário
* **Foco no "porquê"**: Comentar o "porquê" mais do que o "como" quando o código é autoexplicativo

---

💡 **Ao implementar mudanças, consulte `.agent_instructions/protocolo_atualizacao.md` (ID: 4053803626820843) para atualizar documentações afetadas.**