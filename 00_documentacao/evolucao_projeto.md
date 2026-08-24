# Evolução do Projeto CVM

## Propósito deste Documento

Registro cronológico de evolução do projeto para defesa em entrevistas:
* Histórico de sessões de desenvolvimento
* Contexto e motivação de cada implementação
* Aprendizados técnicos não-óbvios acumulados

**Template de sessão** (máximo 25 linhas):
1. **Contexto**: 2-3 frases (o que motivou?)
2. **Decisão**: Bullet com escolha + 1 frase de justificativa
3. **Implementado**: Lista objetiva (3-5 itens)
4. **Key Insight**: 1 aprendizado realmente importante



## 📅 23/08/2026 - CI/CD e Testes Automatizados

### Contexto
Projetório de portfólio para transição de carreira exige demonstrar não apenas código funcional, mas maturidade profissional com garantias automatizadas de qualidade. Implementações locais (máquina pessoal) de GitHub Actions e testes unitários já validadas e commitadas, agora sincronizadas via pull para o workspace Databricks.

### Decisões
* **GitHub Actions para CI** → Workflow `.github/workflows/ci.yml` executa linting e testes automaticamente a cada push/PR, garantindo qualidade contínua sem intervenção manual
* **Ruff para linting e formatação** → Ferramenta moderna (escrita em Rust, extremamente rápida) substitui Flake8/Black/isort com configuração unificada em `ruff.toml`
* **pytest para testes unitários** → Cobertura inicial em `test_config_parametros.py` valida funções críticas de configuração (mapeamento de demonstrações, anos disponíveis, colunas de metadados)
* **Badge de status no README** → Indica visualmente saúde do CI na landing page do repositório

### Implementado
* Workflow CI (`.github/workflows/ci.yml`):
  - Job 1: Lint com Ruff (verificação de estilo, imports, complexidade)
  - Job 2: Testes com pytest (execução de suite unitária)
  - Trigger: push e pull requests para branch main
* Configuração Ruff (`ruff.toml`):
  - Line length 120 caracteres
  - Regras habilitadas: pycodestyle, pyflakes, isort, complexity
  - Exclusões: `.git`, `.databricks`, `__pycache__`, `.old`
* Testes unitários (`tests/test_config_parametros.py`):
  - 52 linhas, cobertura de 4 funções críticas do módulo de configuração
  - Valida mapeamento DRE/BPA/BPP, lista de anos 2021-2025, estrutura de colunas de controle
* Ajustes em `config_parametros.py`:
  - Refatoração para conformidade com regras de linting
  - Melhorias de legibilidade (45 linhas modificadas)
* Atualização `.gitignore`:
  - Exclusão de arquivos de cache Python (`__pycache__`, `.pytest_cache`)
  - Exclusão de diretórios Databricks (`.databricks`, `config`)

### Key Insight
CI/CD não é "extra" em projetos de portfólio profissionais — é demonstração de maturidade técnica. Recrutadores avaliam não apenas se o código funciona, mas se o candidato entende garantias de qualidade automatizadas, práticas de integração contínua e cultura de testes. GitHub Actions é gratuito para repositórios públicos (2000 minutos/mês), eliminando barreira de custo. Badge verde no README sinaliza "este projeto segue práticas profissionais" antes mesmo de abrir o código. Diferença entre "projeto de estudos" e "projeto de portfólio para entrevistas" está nestes detalhes de engenharia.

---

## 📅 19/08/2026 - Refactor Silver: DELETE+APPEND → REPLACE WHERE

### Contexto
Camada Silver usava DELETE WHERE ano + APPEND para idempotência por período. Problema: janela entre as duas operações não é atômica — falha após o DELETE deixa a partição vazia até o APPEND concluir. Em caso de interrupção (erro, timeout, reschedule), dados da partição ficam perdidos até reprocessamento manual.

### Decisões
* **Migrar para REPLACE WHERE** → Delta Lake garante substituição atômica (all-or-nothing) na partição — operação falha completamente ou sucede completamente, sem estado intermediário visível
* **Remover .distinct() redundante** → Bronze idempotente já garante 1 versão por ano; distinct() na Silver é redundante e adiciona custo desnecessário
* **Limpar comentários desatualizados** → Células markdown e documentação técnica contradiziam código (descreviam DELETE+APPEND, código usava REPLACE WHERE)

### Implementado
* Notebooks Silver (201_cvm_dfp_dre, 202_cvm_dfp_bpa, 203_cvm_dfp_bpp):
  - Código já usava REPLACE WHERE (refactor anterior)
  - Comentários markdown atualizados: "DELETE WHERE + APPEND" → "REPLACE WHERE (substituição atômica por período)"
  - 203: Removida chamada .distinct() redundante do pipeline de transformação
* Documentação de metadados (099_ddl_table_comments):
  - 3 tabelas Silver (201_dre_dfp, 202_bpa_dfp, 203_bpp_dfp): "Processamento: DELETE+APPEND incremental" → "Processamento: REPLACE WHERE (substituição atômica por período)"
* Documentação técnica (arquitetura.md):
  - Seção Silver atualizada: estratégia de gravação REPLACE WHERE documentada com explicação de atomicidade
  - 3 ocorrências corrigidas: pipeline, características técnicas, resumo da estratégia

### Key Insight
REPLACE WHERE é atomic replacement — Delta Lake torna a operação all-or-nothing na partição especificada, eliminando completamente a classe de falha do DELETE+APPEND (partição vazia entre DELETE e APPEND) sem adicionar complexidade. Operação crítica para pipelines de produção onde consistência de dados durante falhas é requisito não-negociável. DELETE+APPEND expõe janela de vulnerabilidade; REPLACE WHERE fecha essa janela por design.

---

## 📅 19/08/2026 - Migração de Deploy: Job Manual → Databricks Asset Bundle (DABs)

### Contexto
O Job `Pipeline CVM - DFP` era criado e mantido manualmente pela interface do Databricks, sem versionamento de sua configuração (tasks, schedule, notificações) no Git. Mudanças feitas na UI não deixavam rastro no repositório.

### Decisões
* **Adotar o job existente via bundle, não recriar** → `databricks bundle deployment bind pipeline_cvm_completo 661897477878521` vincula o bundle ao job já existente, preservando histórico de execuções e evitando duplicação
* **Compute serverless obrigatório** → o workspace não suporta cluster clássico; os 8 blocos `new_cluster` originais do YAML causavam falha de deploy (`Only serverless compute is supported`) e foram removidos
* **Schedule mantido pausado** → `pause_status: PAUSED`, ativação manual futura

### Implementado
* `databricks.yml` (raiz) e `resources/jobs/job_pipeline_cvm.yml` criados, definindo o job como código
* Job vinculado ao id `661897477878521` existente via bind, sem criar job duplicado
* Deploy validado (`databricks bundle deploy --target dev`) com as 8 tasks apontando para os notebooks reais do projeto

### Key Insight
Job gerenciado por bundle passa a executar cópias dos notebooks sincronizadas para `.bundle/<nome>/<target>/files/`, não os arquivos originais do projeto diretamente — editar um notebook sem rodar `bundle deploy` depois não afeta a próxima execução do job. Esse desacoplamento entre "arquivo que se edita" e "arquivo que executa" é a mudança operacional mais importante da migração.

---

## 📅 17/08/2026 - Correção EDA: Premissa Oculta em Validação Hierárquica

### Contexto
Análise exploratória EDA_001_analise_dre_silver continha erro sutil de premissas ocultas: célula 24 descobriu amostra válida (WLM, Q4/2025, PENÚLTIMO, DF Consolidado) com contas filhas de 3.01.*, mas célula 28 (validação hierárquica) reescreveu os filtros SQL manualmente — omitindo ORDEM_EXERC e GRUPO_DFP na CTE contas_filhas e no JOIN. Violação da frente "PREMISSAS OCULTAS" da skill revisao-codigo-quatro-frentes: validação assumiu estar testando a mesma amostra descoberta, mas filtros divergentes criaram risco de validar fatia diferente dos dados. Detectado por revisão externa.

### Decisões
* **Herdar filtros da descoberta** → Célula de validação deve replicar TODOS os filtros da célula de descoberta (CNPJ_CIA, ANO, TRIMESTRE, ORDEM_EXERC, GRUPO_DFP)
* **Corrigir CTE e JOIN** → Adicionar ORDEM_EXERC e GRUPO_DFP no SELECT da CTE conta_pai, na CTE contas_filhas (GROUP BY), e nas condições do JOIN
* **Documentar como exemplo de premissa oculta** → Caso clássico de validação que assume estar testando amostra X mas silenciosamente testa amostra Y

### Implementado
* Célula 28 (VALIDAÇÃO DE INTEGRIDADE HIERÁRQUICA) corrigida:
  - CTE conta_pai: SELECT adiciona ORDEM_EXERC, GRUPO_DFP
  - CTE contas_filhas: GROUP BY adiciona ORDEM_EXERC, GRUPO_DFP
  - JOIN: Condições adicionais ON p.ORDEM_EXERC = f.ORDEM_EXERC AND p.GRUPO_DFP = f.GRUPO_DFP
* Validação executada com sucesso — resultado permanece correto (2 contas, 0% divergência), mas código agora estruturalmente robusto

### Key Insight
Retranscrição manual de filtros entre células de descoberta e validação é antipadrão — cria superfície para divergência silenciosa ("dois números deveriam ser idênticos, mas divergiram"). Mesmo quando resultado numérico não diverge (neste caso ambos cenários retornaram 2 contas), erro estrutural permanece: em outro contexto (outra empresa/período), filtros incompletos poderiam agregar múltiplas combinações (PENÚLTIMO + ÚLTIMO, múltiplas demonstrações) e declarar "integridade validada" quando na verdade validou amostra diferente. Princípio: validações devem herdar valores descobertos via referências (variáveis, CTEs compartilhadas) ou garantir correspondência exata de filtros — nunca assumir que retranscrição manual preserva identidade da amostra.

---

## 📅 16/08/2026 - Guardrails Operacionais: Processamento Granular por Período

### Contexto
Notebooks Bronze processavam anos sequencialmente sem isolamento de falhas — erro em um período interrompia toda a execução, perdendo trabalho dos anos já processados. Faltava rastreabilidade: execuções não registravam quais períodos tiveram sucesso/falha.

### Decisões
* **Try-except granular por ano** → Falha isolada em um período não interrompe processamento dos demais
* **Rastreamento de execução** → Lista `anos_sucesso` registra períodos processados com sucesso
* **Relatório final consolidado** → Exibe resultado completo ao fim (sucessos, falhas, totais processados)

### Implementado
* Notebooks 101_cvm_dfp_dre, 102_cvm_dfp_bpa, 103_cvm_dfp_bpp:
  - Try-except envolvendo extração/transformação/carga por ano
  - `anos_sucesso.append(ano)` após carga bem-sucedida
  - Relatório final: contadores + listas de anos processados/falhados
* Pipeline executa até o fim mesmo com falhas parciais, permitindo diagnóstico granular

### Key Insight
Processamento granular com isolamento de falhas permite diagnosticar períodos específicos problemáticos sem perder trabalho dos períodos bem-sucedidos. Padrão essencial para pipelines batch que processam múltiplos períodos independentes — cada período é uma unidade isolada de trabalho.

---

## 📅 15/08/2026 - Separação Arquitetural: Skills como Projeto Independente

### Contexto
Projeto CVM cresceu com frameworks técnicos reutilizáveis (nomenclaturas, estrutura notebooks, revisão código 4 frentes, resiliência operacional, arquitetura medalhão, Unity Catalog, protocolo atualização) armazenados em `.agent_instructions/` local. Problema: **skills são padrões universais de mercado (Tipo 1 - Conceitual)**, não implementação específica do CVM (Tipo 2 - Projeto). Acoplamento viola princípio de separação de contexto: frameworks entre projetos não devem estar presos a um único projeto.

### Decisões
* **Criar projeto `databricks-genie-skills`** → Projeto de portfólio dedicado demonstrando investigação técnica (troubleshooting do Skill Registry, análise de causa raiz) + frameworks reutilizáveis
* **Mover skills para `/Users/<user>/.assistant/skills/`** → Source única, disponível globalmente para todos os projetos no workspace
* **Limpar CVM de skills locais** → `.agent_instructions/` movida para `_old/` (histórico, não versionada no Git)
* **Atualizar documentação CVM** → Remover referências a skills locais, adicionar ponteiro para databricks-genie-skills como fonte de padrões técnicos

### Implementado
* Projeto `databricks-genie-skills` criado:
  - README.md (ID: 4368132372133209) com visão geral, investigação técnica completa (INVESTIGATION_LOG.md)
  - 7 skills em `.assistant/skills/`: nomenclaturas, estrutura-notebooks, resiliencia-operacional, revisao-codigo-quatro-frentes, unity-catalog, protocolo-atualizacao, escolha-sql-pyspark
  - Decisões arquiteturais em `.project/decisoes.md` (ID: 1929315116616197, não versionado)
* Documentação CVM atualizada:
  - `evolucao_projeto.md`: Entrada cronológica registrando separação
  - `README.md`: Removidas referências a `.agent_instructions/` local
  - `arquitetura.md`: Atualizado para apontar databricks-genie-skills como fonte de padrões
* `.agent_instructions/` movida para `_old/.agent_instructions/` (já estava no .gitignore)

### Key Insight
Separação de contexto não é organização de pastas — é **arquitetura de conhecimento**. Skills (Tipo 1 - Conceitual) são frameworks universais, devem ser fonte única entre projetos; implementações específicas (Tipo 2 - Projeto) evoluem com cada projeto; histórico de decisões (Tipo 3 - Operacional) documenta o porquê. Acoplar skills ao CVM criava dependência artificial: próximo projeto precisaria duplicar skills ou importar pasta de outro projeto. Separação permite reutilização limpa e posiciona skills como deliverable independente de portfólio (demonstra capacidade de criar frameworks, não apenas consumir).

---

## 📅 10/08/2026 - Expansão de Demonstrações: BPP (Balanço Patrimonial Passivo)

### Contexto
Após implementação bem-sucedida de DRE (101/201) e BPA (102/202), expansão natural do pipeline para incluir BPP (Balanço Patrimonial Passivo). BPP é a terceira demonstração financeira essencial da CVM (junto com DRE e BPA), completando a visão do Balanço Patrimonial (Ativo + Passivo). Fonte CVM fornece arquivo único com múltiplas demonstrações - mesma estrutura de ingestão se aplica a BPP.

### Decisões
* **Criar notebooks 103/203 BPP** → Seguindo padrão de rastreabilidade estabelecido (103 Bronze, 203 Silver) e nome base idêntico entre camadas
* **Replicar estrutura BPA** → BPP tem estrutura análoga ao BPA (demonstração de posição patrimonial), adaptação direta do padrão já validado
* **Manter consistência de pipeline** → Mesmas validações, logging estruturado e tratamento de erros granular aplicados em DRE/BPA

### Implementado
* Notebooks criados:
  - `103_cvm_dfp_bpp` (Bronze, ID: 2152724235953208) → Tabela `proj_cvm_01_bronze.103_bpp_dfp`
  - `203_cvm_dfp_bpp` (Silver, ID: 2152724235953207) → Tabela `proj_cvm_02_silver.203_bpp_dfp`
* Estrutura técnica implementada:
  - Bronze: Ingestão idempotente (DELETE WHERE ano + APPEND), versionamento (_versao_ingestao, _last_modified_cvm, _ingest_ts)
  - Silver: Filtro de versão mais recente, transformações de tipo, DELETE WHERE ano + APPEND
  - Logging estruturado por ano, try/except granular (falha isolada), validação de pré-requisitos
* Documentação atualizada: README.md (estrutura + status), arquitetura.md (tabelas Bronze/Silver)

### Key Insight
Padrão de rastreabilidade (101→201, 102→202, 103→203) permite identificação visual imediata do fluxo entre camadas: mudança de primeiro dígito indica camada, nome base idêntico garante relacionamento. Expansão de demonstrações segue estrutura modular - cada nova demonstração replica padrão sem reinventar arquitetura.

---

## 📅 09/08/2026 - Reestruturação de Pastas: Ordem Lógica EDA antes de Apoio

### Contexto
Decisão de implementar BPP (Balanço Patrimonial Passivo) antes de EDA (Exploratory Data Analysis - Análise Exploratória de Dados) revelou necessidade de pasta dedicada para análises exploratórias. Estrutura original tinha `04_apoio/` logo após camadas medalhão, mas análises exploratórias fazem parte do **fluxo de dados** (dados → exploração → decisão), enquanto apoio é infraestrutura auxiliar. Numeração de pastas deve refletir ordem cronológica: análises acontecem depois dos dados (01/02/03) mas antes da infraestrutura (DDL, orquestrador, config).

### Decisões
* **Criar `04_exploracao/`** → Pasta para notebooks de EDA (um por fonte: eda_dre, eda_bpa, eda_bpp + notebook de análises cruzadas)
* **Renomear `04_apoio/` → `05_apoio/`** → Infraestrutura vem depois do fluxo de dados na ordenação lógica
* **Atualizar todas as referências** → Paths `%run` e tasks de Jobs orquestradores devem refletir nova numeração

### Implementado
* Pasta `04_exploracao/` criada
* Pasta `04_apoio/` renomeada para `05_apoio/`
* Notebooks 101, 102, 201, 202: Paths `%run ../04_apoio/config_parametros` → `../05_apoio/config_parametros` atualizados
* Job 661897477878521 (Pipeline CVM - DFP): 5 tasks atualizadas (orquestrador, DDL, download, table_comments apontando para `05_apoio/`)
* Documentação atualizada: `arquitetura.md` (seção Pastas + 11 referências), `README.md` (árvore de diretórios)

### Key Insight
Numeração de pastas não é cosmética — comunica visualmente a sequência lógica do pipeline. Colocar "apoio" (infraestrutura) antes de "análises exploratórias" (fluxo de dados) inverte a ordem conceitual. Reestruturação teve impacto cascata: 4 notebooks + 1 job + 2 docs. Auditar impactos ANTES de renomear (via `grep -r` + checklist de assets afetados) evita quebras silenciosas em execução.

---

## 📅 08/08/2026 - Correção de Parsing: Células language: run vs python

### Contexto
Após resolução do erro OSError no `%run ./config_parametros`, pipeline falhou novamente em runs 947286305660001 e 429215309554501. Novo erro diferente: `"Failed to parse %run command: string matching regex expected but '#' found"` nos notebooks Bronze (101_cvm_dfp_dre, 102_cvm_dfp_bpa). Causa raiz: células com `language: run` contendo código Python adicional após o comando `%run`. Databricks rejeita isso — células `run` aceitam APENAS o comando `%run`, nada mais (nem comentários). Progresso importante: task `download_cvm_landing` PASSOU pela primeira vez, confirmando que a correção anterior de path relativo estava correta.

### Decisões
* **Mudar células para `language: python`** → Quando célula tem `%run` + código Python adicional, tipo correto é `python` (não `run`)
* **Fallback robusto em ANOS_PROCESSAR** → Se detecção inteligente retornar lista vazia ou falhar, usar automaticamente últimos 5 anos (fallback de 2021-2026). Pipeline nunca falha por falta de anos.
* **Logs detalhados de inicialização** → Adicionar try/except + logs explícitos na detecção de anos para diagnosticar futuros problemas silenciosos

### Implementado
* Notebooks 101_cvm_dfp_dre e 102_cvm_dfp_bpa: Células de configuração mudadas de `language: run` para `language: python`
* Notebook 003_download_cvm_para_landing: Implementado fallback automático (últimos 5 anos) + logs detalhados de inicialização
* Primeira execução bem-sucedida de `download_cvm_landing` confirmada (run 429215309554501: DDL passou, download passou, Bronze falhou apenas por erro de parse)

### Key Insight
Células `language: run` são restritas — aceitam SOMENTE o comando `%run`, nenhum código adicional (nem comentários, nem imports, nem lógica). Para misturar `%run` com código Python, usar `language: python` que aceita magic commands. Databricks é estrito nisso porque células `run` são otimizadas para execução pura de notebook externo. Fallback robusto elimina classes de falha: pipeline sempre tem anos para processar, mesmo se detecção inteligente falhar silenciosamente (try/except que engole erro, tabela de controle inacessível, etc). Logs detalhados expõem problemas antes que virem falha de execução.

---

## 📅 08/08/2026 - Preparação Git: Documentação Portável e Anonimizada

### Contexto
Projeto pronto para versionamento público no GitHub após meses de desenvolvimento. Última etapa crítica antes do commit inicial: validação profunda de TODA a documentação para garantir portabilidade total e zero vazamento de informação pessoal. Documentação técnica polida é inútil se expõe caminhos do workspace ou identificação pessoal — recrutador vê isso como "código feito só pra screenshot", não pensado para reprodução.

### Decisões
* **Auditoria completa de referências** → Validar cada link, cada caminho, cada ID em todos os arquivos de documentação antes do commit irreversível
* **Anonimização de caminhos Databricks** → Substituir `/Workspace/Users/1pedro.osilva@gmail.com/...` por placeholders genéricos (`<user-email>`, `<caminho-absoluto>`)
* **Caminhos relativos nas especificações** → `/instrucoes/` (caminho absoluto workspace) → `.agent_instructions/` (caminho relativo Git)
* **Remoção de links internos Databricks** → Âncoras `#file-XXXXXX` não funcionam fora do workspace, substituir por referências textuais
* **Consistência de nomenclatura** → Nome da pasta mudou de underscore para hífen (`projeto_cvm_dados_financeiros` → `projeto-cvm-dados-financeiros`), corrigir em toda documentação

### Implementado
* **11 correções em 8 arquivos**:
  - 5 rodapés em `.agent_instructions/` (escolha_sql_pyspark, estrutura_notebooks, nomenclaturas, unity_catalog, protocolo_atualizacao)
  - 1 checklist interno (protocolo_atualizacao linha 197)
  - 4 caminhos absolutos anonimizados (evolucao_projeto × 2, arquitetura × 2)
  - 1 link morto removido (arquitetura linha 579: `#file-186477256358021`)
  - 2 nomes de pasta corrigidos (README, referencia_ids)
* **Validação final**: 0 referências a `/Users/1pedro`, 0 referências a `/instrucoes/`, 0 links internos Databricks
* **Commit inicial e push**: 25 arquivos limpos publicados no GitHub (4924 linhas), pasta `_old/` corretamente ignorada pelo `.gitignore`
* **Repositório público**: https://github.com/1pedroosilva/projeto-cvm-dados-financeiros

### Key Insight
Documentação para Git não é "documentação + versionamento" — é **documentação agnóstica de ambiente**. Cada caminho absoluto, cada âncora interna do workspace, cada referência pessoal quebra a promessa de portabilidade. Recrutador clonando repo deve conseguir executar código sem editar paths, sem saber de onde veio. Anonimização não é "segurança extra", é **profissionalismo básico** em código de portfólio. Validação pré-commit (git status, .gitignore, conferência manual) evitou exposição de `_old/` — 30 segundos extras de conferência salvaram de push irreversível com arquivos sensíveis.

---

## 📅 05/08/2026 - Padronização de Formato: Notebooks em .py para Git Limpo

### Contexto
Projeto tinha notebooks em formatos mistos: alguns `.py` (Python Source), um `.ipynb` (Jupyter). Formato misto viola princípio básico de consistência. Mais importante: para portfólio técnico onde histórico git conta narrativa, `.ipynb` (JSON) polui diffs com metadata não-relevante (execution_count, outputs), tornando code review ilegível. Times que usam Databricks + CI padronizam em `.py` exatamente por isso: diff linha-por-linha, sem noise.

### Decisões
* **Padronizar TUDO em `.py`** → Converter apenas 1 arquivo inconsistente (menos trabalho, menos risco) vs manter 8 arquivos já validados em produção
* **Justificativa git-first** → Metadados do workspace (data criação, execution_count) não aparecem no GitHub/portfólio; para audiência externa, só commits importam
* **Atualizar `estrutura_notebooks.md`** → Substituir instrução `.ipynb` por `.py` com justificativa de diff limpo e padrão de mercado
* **Documentar decisão** → Registrar contexto completo (o porquê de `.py` > `.ipynb` para portfólio) em vez de apenas "padronizar"

### Implementado
* Formato confirmado: 9 notebooks em `.py` (bronze: 101/102, silver: 201/202, apoio: 000/001/002/003/099)
* Especificação `estrutura_notebooks.md` → Nova seção "Formato de Arquivo" com justificativa git + proibição de `.ipynb`
* Documentação (README, arquitetura, evolucao) → Referências corrigidas para `.py` (estavam desatualizadas)

### Key Insight
Escolha de formato é decisão arquitetural, não detalhe técnico. `.ipynb` é superior para notebooks isolados (Jupyter, Colab), mas `.py` é superior para pipelines versionados. Razão: git não é ferramenta de backup, é ferramenta de narrativa — "quem lê esse diff entende o que mudou?". Metadata JSON responde errado. Para portfólio onde recrutador abre PRs pra avaliar evolução técnica, `.py` trabalha a favor; `.ipynb` trabalha contra. **Alerta pendente**: Histórico (27/07) registra falha `%run` + `.py` → antes de commitar, testar pipeline completo pra validar que `%run ./config_parametros` funciona ou confirmar se `exec(open())` é necessário.

---

## 📅 04/08/2026 - Padronização de Portabilidade: %run com Caminho Relativo

### Contexto
Todos os notebooks do pipeline carregavam `config_parametros.py` usando `open('/Workspace/Users/<user-email>/.../config_parametros.py') + exec()` com caminho absoluto hardcoded. Isso criava dois problemas críticos de portabilidade: (1) código quebra ao migrar para outro workspace/conta (novo e-mail = novo path), e (2) expõe identificação pessoal no código-fonte. Databricks fornece `%run` justamente para esse caso, com caminhos relativos que sobrevivem a mudanças de ambiente.

### Decisões
* **Substituir `open() + exec()` por `%run` com caminho relativo** → `%run ../04_apoio/config_parametros` (notebooks em bronze/silver) ou `%run ./config_parametros` (notebooks em apoio). Portabilidade total entre workspaces.
* **Atualizar especificação `estrutura_notebooks.md`** → Adicionar regra explícita proibindo caminhos absolutos/e-mail para carregamento de módulos compartilhados.
* **Corrigir `arquitetura.md`** → Seção "Importação de Módulos Python" reescrita com novo padrão e justificativa de portabilidade.

### Implementado
* 6 notebooks corrigidos:
  - `101_cvm_dfp_dre.ipynb`, `102_cvm_dfp_bpa.py` (bronze)
  - `201_cvm_dfp_dre.py`, `202_cvm_dfp_bpa.py` (silver)
  - `000_orquestrador_pipeline.py`, `003_download_cvm_para_landing.py` (apoio)
* Especificação `estrutura_notebooks.md` → Nova seção "Carregamento de Módulos Compartilhados" com padrão obrigatório e exemplos
* Documentação `arquitetura.md` → Seções "Configuração Centralizada" e "Padrões de Desenvolvimento" atualizadas

### Key Insight
Portabilidade não é "feature opcional" — é requisito de código profissional. Hardcoded paths com e-mail revelam código escrito "só para funcionar aqui e agora", não pensado para reprodução/migração. Em portfólio técnico, recrutador enxerga isso imediatamente: "Esse código consegue rodar em outro ambiente, ou foi feito só pra screenshot?" Databricks oferece `%run` (namespace compartilhado, caminho relativo) exatamente pra isso — usar a ferramenta correta demonstra conhecimento de plataforma.

---

## 📅 31/07/2026 - Auditoria Externa: Correção de Defasagem Spec-vs-Código

### Contexto
Validação técnica por agente externo (Gemini) revelou **defasagem crítica entre documentação (nível sênior) e implementação (gaps em pontos-chave)**. Diagnóstico: 8 achados técnicos, 4 críticos ou médios. Mais importante que bugs individuais foi o insight de que "documentação polida sobre código com bugs júnior pode soar como texto gerado por IA descolado da prática". Esse gap exato seria o primeiro achado de um recrutador técnico em code review de portfólio.

### Decisões
* **Correção técnica imediata** → Atacar 4 achados críticos/médios prioritários antes de melhorias arquiteturais (Achados #1, #4, #6, #7)
* **Confrontação técnica independente** → Validar fatos (o que está escrito no código) mas questionar severidades e interpretações do auditor
* **Documentação honesta** → Registrar gap encontrado e corrigido (não esconder), princípio de que "honestidade sobre lacunas impressiona mais que polimento que esconde"
* **Adiar melhorias da Fase 2** → Achados #2 (dedupe por VERSAO), #3 (idempotência fraca), #5 (Auto Loader) ficam para ponderação posterior

### Implementado
* **Achado #1** (conflito de tipo DDL × cast): DDL Silver alterado de `VERSAO STRING, CD_CVM STRING` para `VERSAO INT, CD_CVM INT` (notebooks 001, células 5-6). Alinhamento com casts aplicados na transformação Silver. Razão: consistência de schema, não policy ANSI (análise técnica refinada)
* **Achado #7** (guardrail incompleto): `ST_CONTA_FIXA` adicionada em `COLUNAS_ESSENCIAIS_DRE` e `COLUNAS_ESSENCIAIS_BPA` (config_parametros.py). Coluna existia no DDL Bronze mas faltava no contrato de dados — risco de append failure ou coluna sempre NULL
* **Achado #4** (detecção de atualizações morta): DDL da tabela de controle alterado de `last_modified_cvm STRING` para `TIMESTAMP` (notebook 002). Função `get_anos_com_atualizacao_cvm` corrigida com except mais específico (AnalysisException separado). Comparação `datetime > str` causava TypeError silencioso — recurso nunca funcionou desde implementação
* **Achado #6** (count remanescente): `.count()` removido do notebook Bronze 101 (célula 6). Antipadrão que força materialização prematura, inconsistente com remoção anterior do Silver

### Key Insight
Auditoria externa antecipou exatamente o que recrutador técnico enxergaria em code review de portfólio. **Confrontar tecnicamente** (não aceitar passivamente) foi crítico: Achado #1 tinha razão certa mas explicação errada (não é policy ANSI, é consistência de schema); Achado #2 pode ser over-engineering se fonte não traz duplicatas; Achado #5 recomendava Auto Loader quando `spark.read.csv` resolve 90% sem complexidade de checkpoint. Validar fatos no código, questionar interpretações de impacto — auditor pode estar certo nos bugs mas errado nas severidades.

---

## 📅 03/08/2026 - Correção Estrutural: Bronze Idempotente (DELETE+APPEND)

### Contexto
Investigação revelou que Bronze acumulava duplicatas técnicas: 10 execuções do job = 10 cópias dos mesmos 30k registros (307k total), causando "Silver < Bronze" (30k vs 307k). Root cause: estratégia APPEND-ONLY sem validação. Primeira correção (validar `last_modified_cvm` antes de APPEND) era **pontual, não escalável** - gambiarra que dependia de "se tudo der certo". Usuário enfatizou: "Não fazer correções fora dos notebooks que são gambiarras para ruídos" e "criar código robusto e independente".

### Decisões
* **Bronze idempotente (DELETE WHERE ano + APPEND)** → Sempre 1 versão por ano, executar 10x = mesmo resultado. Sem dependência de validações externas, sem acúmulo de versões.
* **Silver simplificada** → Guardrail único: Bronze tem dados? SIM → processa, NÃO → pula. Sem validar "perda de 95%" (problema da Bronze, não Silver).
* **Window Function removida da Silver** → Bronze idempotente = sempre 1 versão, deduplicação desnecessária.
* **Remover notebook separado de limpeza** → Gambiarra pontual. Bronze auto-corretiva elimina necessidade.
* **Documentação completa seguindo protocolo** → Registrar mudança arquitetural em `evolucao_projeto.md` + atualizar `arquitetura.md`.

### Implementado
* **101_cvm_dfp_dre** (célula 5): DELETE WHERE ano + APPEND. `_versao_ingestao` fixo em 1. Sem validação de `last_modified_cvm`.
* **102_cvm_dfp_bpa** (célula 5): Mesma lógica idempotente aplicada.
* **201_cvm_dfp_dre** (célula 4): Guardrail único (Bronze tem dados?). Window Function removida.
* **202_cvm_dfp_bpa** (célula 4): Mesma simplificação.
* **Notebook 999 deletado**: Limpeza one-time era gambiarra. Idempotência resolve na origem.
* **guardrails.md criado**: Documentação separada de validações Bronze/Silver (condições, fluxos, erros).
* **arquitetura.md atualizado**: Seções Bronze/Silver refletem estratégia idempotente, referência a guardrails.md.
* **README.md atualizado**: Árvore de diretórios + referência a guardrails.md.

### Key Insight
**Idempotência > Guardrails defensivos**. Validar "se arquivo mudou" é gambiarra - se bug introduzir duplicatas, ficam lá. DELETE WHERE ano + APPEND é **auto-corretivo**: bugs futuros não acumulam lixo, rodar 10x = rodar 1x. Simplicidade estrutural elimina necessidade de lógica defensiva. "Silver < Bronze" era sintoma de Bronze mal projetada, não problema da Silver.

---

## 📅 31/07/2026 - Limitação Delta Lake: ALTER COLUMN TYPE Não Suportado

### Contexto
Após corrigir DDL (INT/TIMESTAMP), tentamos aplicar schema migration via `ALTER TABLE ... ALTER COLUMN ... TYPE` para preservar metadados de criação (`created_time`, histórico Delta). Delta Lake rejeitou: `NOT_SUPPORTED_CHANGE_COLUMN`. Descobrimos que Delta Lake **não permite** mudar tipo de coluna existente (Parquet é imutável, requereria reescrever todos arquivos).

### Decisões
* **DROP + CREATE como única solução real** → Delta não suporta ALTER TYPE; CTAS (Create Table As Select) também cria nova tabela, perde metadados igualmente
* **Tradeoff consciente: schema correto > metadados** → Para portfólio, código executável do zero (funciona em ambiente novo) é mais importante que preservar `created_time` de tabelas antigas
* **Função de migration idempotente** → Adicionada `apply_schema_migration_if_needed()` no DDL que tenta ALTER (ambiente novo: falha silenciosamente, CREATE funciona; ambiente antigo: reporta limitação)
* **Documentar limitação em código** → Comentários no notebook explicam que Delta não suporta ALTER TYPE, alinhando expectativas

### Implementado
* Notebook 001: Célula de migration adicionada (tenta ALTER TABLE, detecta NOT_SUPPORTED_CHANGE_COLUMN)
* Drop manual: Tabelas Silver (201, 202) e Controle dropadas via SQL
* Job re-executado: Tabelas recriadas com schema correto (VERSAO/CD_CVM INT, last_modified_cvm TIMESTAMP)
* Dados preservados: Bronze intacta (276k DRE, 295k BPA), Silver reconstruída a partir de Bronze (30k DRE, 58k BPA)
* Run 39472409661291: SUCCESS, schema validado via DESCRIBE TABLE

### Key Insight
Delta Lake tem limitação arquitetural real: ALTER COLUMN TYPE não suportado (Parquet subjacente é imutável). Única solução é DROP+CREATE ou CTAS, ambas perdem metadados de criação. **Tradeoff de engenharia**: em portfólio, priorizamos código executável do zero (CREATE TABLE IF NOT EXISTS com schema correto funciona em clone do repo) sobre preservação de `created_time` (relevante apenas em ambiente existente). Em produção real, faria DROP+CREATE em maintenance window documentado. Descoberta dessa limitação **demonstra conhecimento profundo de Delta** — não é SQL tradicional, tem restrições de storage layer.

---

## 📅 31/07/2026 - Compatibilidade Spark Connect: Refatoração para Serverless

### Contexto
Pipeline falhando em execuções 213094734019502 e 1119125202764371. Três problemas raiz identificados:
1. Download para Landing Zone falhava com FileNotFoundError (Python open() não cria diretórios automaticamente)
2. Notebooks Bronze/Silver BPA falhavam com TypeError: 'NoneType' object is not iterable (ANOS_PROCESSAR não inicializado)
3. Restrições Spark Connect/Serverless bloqueiam acesso a filesystem local (/tmp)

### Decisões
* **Refatoração config_parametros.py** → ANOS_PROCESSAR não executa no import, função inicializar_anos_processar() com chamada explícita obrigatória
* **Download compatível com Spark Connect** → os.makedirs() antes de open(), gravação direta em Volume UC sem /tmp intermediário
* **Padrão de inicialização** → Todos notebooks devem chamar inicializar_anos_processar() após importar config

### Implementado
* config_parametros.py: Removida inicialização automática de ANOS_PROCESSAR, função inicializar_anos_processar() criada com lógica de override (env ou argumento explícito)
* Notebook 003_download_cvm_para_landing: Adicionado os.makedirs(ano_path, exist_ok=True) antes de gravar arquivo, download testado e validado (5 anos, total ~51 MB)
* Notebooks 102_cvm_dfp_bpa e 202_cvm_dfp_bpa: Adicionada chamada inicializar_anos_processar() após import de config
* Todos notebooks testados: ANOS_PROCESSAR corretamente inicializado como [2021, 2022, 2023, 2024, 2026]

### Key Insight
Spark Connect (Serverless Compute) tem restrições arquiteturais reais - bloqueia acesso a filesystem local (/tmp, paths fora de /Workspace) com LocalFilesystemAccessDeniedException. APIs Python padrão (open(), os.makedirs()) funcionam perfeitamente com Unity Catalog Volumes quando usadas diretamente, sem staging intermediário em /tmp. Diferencial técnico: conhecer essas limitações e projetar código que funciona nativamente no regime Serverless, não apenas "adaptar código antigo".

---

## 📅 27/07/2026 - Padronização de Numeração: Conformidade com Especificações

### Contexto
Auditoria da pasta `04_apoio/` revelou numeração inconsistente: dois notebooks iniciando com `000_` (viola unicidade), uso de `999_` em vez do padrão de 3 dígitos para utilitários (`099_`), e documentação mencionando arquivos `.sql` inexistentes. Segundo `/especificacoes/nomenclaturas.md`, notebooks devem usar SEMPRE 3 dígitos obrigatórios (`XXX_`) com numeração sequencial única.

### Decisões
* **Padronização em 3 dígitos** → Todos os notebooks seguem `XXX_[nome_base]`, eliminando duplicidades e garantindo ordem lógica clara
* **Exclusão de config_parametros.py da numeração** → Arquivo de configuração/biblioteca não é notebook sequencial, não recebe número (padrão Python: utils.py, config.py sem numeração)
* **Atualização em cascata** → 8 arquivos impactados: 4 docs (README, arquitetura, evolucao_projeto, referencia_ids), 1 job (4 tasks), 3 notebooks (orquestrador + 2 bronze com comentários)

### Implementado
* Notebooks renomeados:
  - `000_ddl_create_tables` → `001_ddl_create_tables`
  - `001_ddl_controle_ingestao` → `002_ddl_controle_ingestao`
  - `002_download_cvm_para_landing` → `003_download_cvm_para_landing`
  - `999_ddl_table_comments` → `099_ddl_table_comments`
* Documentação atualizada: README.md, arquitetura.md, referencia_ids.md (duplicidade resolvida)
* Job 661897477878521: 4 tasks atualizadas com novos paths
* Código atualizado: orquestrador (print de log), notebooks bronze 101/102 (comentários)
* Estrutura final: `000_orquestrador` (coordenador), `001/002/003` (setup/ingestão sequencial), `099` (utilitário docs), `config_parametros.py` (sem número)

### Key Insight
Padronização de nomenclatura não é cosmética — elimina ambiguidade operacional (qual `000_` executar primeiro?), força ordem lógica visível, e demonstra maturidade profissional em portfólio. Inconsistências se propagam: 1 renomeação impactou 8 arquivos (docs + job + código). Auditorias periódicas de conformidade com `/especificacoes/` previnem débito técnico documental.

---

## 📅 27/07/2026 - Guardrails de Qualidade: Pipeline Robusto a Mudanças de Schema

### Contexto
20 execuções consecutivas falharam por mismatches de schema entre fonte CVM e DDL Bronze/Silver. Bugs incluíam: colunas extras não declaradas, colunas esperadas ausentes (DT_INI_EXERC em BPA), metadados técnicos com nomes incorretos (_ingest_date vs _ingest_ts), e performance degradada por counts forçando full table scans (Silver DRE levava 316s).

### Decisões
* **Guardrails de schema via validação + projeção** → Bronze valida entrada (`validar_e_projetar_schema()`) e rejeita schemas incompatíveis; Silver projeta explicitamente colunas do DDL (`.select()`), descartando extras
* **Contrato de dados explícito** → Listas `COLUNAS_ESSENCIAIS_DRE` e `COLUNAS_ESSENCIAIS_BPA` em `config_parametros.py` definem schema mínimo esperado
* **Schema BPA ajustado** → BPA não contém `DT_INI_EXERC` (snapshot de posição, não período como DRE). DDL Bronze/Silver BPA atualizados, tabelas recriadas
* **Remoção de counts informativos** → Eliminados 3 `.count()` em Silver DRE/BPA (logs sem propósito funcional)
* **Correção de metadados** → Notebook de comentários documentava coluna inexistente `_ingest_date`; corrigido para metadados reais (`_versao_ingestao`, `_last_modified_cvm`, `_ingest_ts`, `_source_file`)

### Implementado
* Guardrails implementados (detalhes em [guardrails.md](00_documentacao/tecnica/guardrails.md)): Função `validar_e_projetar_schema()` em `config_parametros.py`, aplicada em Bronze 101/102, projeção explícita em Silver 201/202
* Schema BPA: Coluna `DT_INI_EXERC` removida de listas, DDL Bronze (cellId: 3a1d99ba), DDL Silver (cellId: 2ec17487), notebook 202
* Notebook 999: Corrigido `_ingest_date` → metadados corretos, descrição de processamento corrigida ("APPEND incremental" não "TRUNCATE + APPEND")
* Job: Path do notebook de comentários corrigido (99 → 999)

### Key Insight
Guardrails (ver [guardrails.md](00_documentacao/tecnica/guardrails.md)) detectam mudanças de fonte automaticamente. Counts desnecessários são antipadrão: Silver DRE de 316s → 15s (-95%, 21x) apenas removendo logs informativos sem propósito funcional.

---

## 📅 27/07/2026 - Correção: Compatibilidade de Namespaces em Databricks

### Contexto
Jobs falhavam com erros de parsing em notebooks bronze (101, 102) e erro de FileNotFoundError no notebook de download (002). Células Python com `%run` + imports não funcionam; tentativas de usar `os.makedirs()` em Volumes UC geraram "Operation not supported".

### Decisões
* **Migração de %run para exec(open())** → `%run` só funciona com `.ipynb`, não com `.py`. Solução: `exec(open('<caminho-absoluto>/config_parametros.py').read())`
* **Uso exclusivo de dbutils.fs para Volumes UC** → `os.makedirs()` não é suportado em `/dbfs/Volumes/`. Usar apenas `dbutils.fs.mkdirs()` para criar diretórios
* **Simplificação de versionamento** → Removida lógica complexa de versionamento de arquivos; arquivos sobrescritos quando necessário
* **Namespace dual em Volumes** → Databricks expõe Volumes em dois namespaces: `/Volumes/` (dbutils.fs) e `/dbfs/Volumes/` (Python I/O)

### Implementado
* Notebooks 101 e 102: Corrigidas células de importação (tipo `run` → `python`, `%run` → `exec(open())`)
* Notebook 002: Removido `os.makedirs()`, simplificado fluxo de criação de diretórios e versionamento
* Importação de config: 3 notebooks ajustados com padrão `exec(open())`

### Key Insight
Databricks tem dois namespaces para Unity Catalog Volumes: `/Volumes/` (usado por dbutils.fs) e `/dbfs/Volumes/` (usado por Python built-in como open()). Operações de sistema de arquivos (mkdir, ls) devem usar dbutils.fs; operações de I/O (read/write) usam Python com `/dbfs/` prefix. Misturar namespaces ou usar módulos OS padrão (os.makedirs, shutil) resulta em "Operation not supported".

---

## 📅 27/07/2026 - Refatoração: Aplicação Rigorosa de DRY na Documentação

### Contexto
README.md e arquitetura.md continham 5 seções duplicadas (camadas Medalhão, Landing Zone, convenções de numeração, princípio DRY, estrutura de notebooks). Violação clara do princípio DRY aplicado ao código mas não à documentação. Causa raiz identificada: protocolo_atualizacao.md instruía explicitamente a colocar detalhes técnicos no README.

### Decisões
* **README como índice executivo** → Visão geral (105 linhas), estrutura, status alto nível, links para arquitetura.md
* **arquitetura.md como fonte única técnica** → TODOS os detalhes (camadas, schemas, convenções, estratégias, pipeline)
* **Correção do protocolo** → protocolo_atualizacao.md agora exige separação clara e exemplifica README (simples) vs arquitetura.md (detalhado)
* **Regra absoluta** → Se está em arquitetura.md, NÃO está em README.md (exceto link/referência)

### Implementado
* README.md: Redução de 170 → 105 linhas (38%), remoção de 5 seções duplicadas, nova seção "Documentação Técnica" com links
* protocolo_atualizacao.md: Nova seção "Princípio DRY na Documentação", instruções corrigidas em 8 cenários, exemplos práticos README vs arquitetura.md
* Zero informação perdida (tudo duplicado já existia em arquitetura.md)

### Key Insight
Protocolo de documentação é código que gera documentação. Se o protocolo não aplica DRY rigorosamente, gerações futuras vão duplicar informações independente da boa intenção. Meta-documentação (protocolo) precisa de revisão tão crítica quanto código de produção.

---

## 📅 23/07/2026 - Implementação Final de Padrões Arquiteturais

### Contexto
Notebooks Bronze e Silver tinham gaps críticos vs. padrões definidos anteriormente: download direto da CVM (ignorando Landing Zone), ausência de filtro de versionamento em Silver, estratégias incorretas de gravação (TRUNCATE, DELETE sem critério), e configuração descentral izada.

### Decisões
* **Landing Zone como origem única** → Bronze lê de UC Volume, nunca baixa diretamente (separação ingestão/transformação)
* **Versionamento append-only em Bronze** → Colunas `_versao_ingestao`, `_last_modified_cvm`, `_ingest_ts` + APPEND puro (histórico completo)
* **Filtro de versionamento em Silver** → Window Function (PARTITION BY chave natural, ORDER BY _versao_ingestao DESC, ROW_NUMBER = 1)
* **DELETE WHERE + APPEND em Silver** → Idempotência por período (ano), preserva dados de outros anos
* **Config centralizada obrigatória** → `%run config_parametros` em TODOS notebooks (DRY global)
* **Tabela de controle** → Registro de todas ingestões (fonte, ano, versão, timestamp, status)

### Implementado
* Notebooks 101/102 (Bronze DRE/BPA): Loop ANOS_PROCESSAR, leitura landing zone, append-only, registro controle
* Notebook 201 (Silver DRE): Filtro versionamento + DELETE WHERE + APPEND, remove colunas técnicas Bronze
* Notebook 202 (Silver BPA): Criado com padrão idêntico ao 201
* config_parametros.py: Corrigido TABELA_CONTROLE, URLs, status='SUCCESS'

### Key Insight
Versionamento é decisão arquitetural binária: ou Bronze é append-only + Silver filtra versão mais recente, OU Bronze é idempotente + Silver confia. Misturar quebra rastreabilidade e audit trail. Não existe "meio-termo".

---

## 📅 14/07/2026 - Governança de Dados: DDL Explícito

### Contexto
`saveAsTable()` com inferência automática não demonstra controle sobre governança. Para portfólio bancário, é essencial demonstrar separação entre infraestrutura (DDL) e transformações (DML).

### Decisão
* **Pasta `04_apoio/`** para scripts de infraestrutura → Separa DDL (estruturas) de notebooks (transformações)
* **DDL explícito** (`00_ddl_create_tables.sql`) → Define schemas, tipos e particionamento antes da carga
* **Metadados** (`099_ddl_table_comments.py`) → COMMENT ON TABLE/COLUMN no catálogo (data discovery)
* **INSERT ao invés de saveAsTable** → Tabelas já existem, apenas populamos dados

### Implementado
* Pasta `04_apoio/` com 2 arquivos SQL (criação + documentação)
* DDL: 3 schemas + 2 tabelas (bronze 14 cols, silver 18 cols) + comentários
* Notebooks 101 e 201: `createOrReplaceTempView()` + `INSERT OVERWRITE TABLE`
* README atualizado com nova estrutura

### Key Insight
DDL explícito é padrão corporativo - demonstra que você projeta estruturas antes de popular dados, não deixa o Spark inferir e "torce para dar certo".

---

## 📅 14/07/2026 - Refatoração DRY: Nomenclatura de DataFrames

### Contexto
Notebook 201 violava princípio DRY com prefixos redundantes (`df_dre_bronze`, `df_dre_tipos_padronizados`). Nome do notebook já comunica camada e domínio.

### Decisão
* **Remover prefixos redundantes** → `df → df_padronizado → df_sem_duplicados → df_limpo → df_final`
* Nomenclatura descreve **transformação/estado**, não contexto já estabelecido pelo nome do notebook
* Comentários simplificados (de 4-5 linhas para 2 linhas por célula)

### Implementado
* Refatoradas 9 células do notebook 201
* Aplicado princípio DRY em todos DataFrames
* Comentários reduzidos mantendo clareza técnica

### Key Insight
DRY não é só código - aplica-se a nomenclatura e semântica. Nomenclatura autocontida ≠ nomenclatura redundante.

---

## 📅 14/07/2026 - Processamento Incremental: DELETE + APPEND (Bronze)

### Contexto
INSERT OVERWRITE deleta TODO o histórico a cada execução, inclusive anos anteriores. Ineficiente e arriscado para produção.

### Decisão
* **Bronze**: `DELETE WHERE ANO_REFER = X` + `APPEND` → Preserva histórico, reprocessa só o ano
* **Coluna ANO_REFER** extraída de DT_REFER → Usada para particionamento físico
* **Particionamento** `PARTITIONED BY (ANO_REFER)` → DELETE eficiente (só processa partição específica)
* Fonte CVM fornece ZIPs por ano → Alinhamento natural com estratégia incremental

### Implementado
* DDL: Adicionada coluna `ANO_REFER INT` + `PARTITIONED BY (ANO_REFER)` na bronze
* Notebook 101: `withColumn("ANO_REFER", year(...))` + DELETE condicional + append
* Silver também particionada por ANO (já tinha a coluna)

### Key Insight
Bronze usa DELETE+APPEND para snapshots completos por período. Se fonte fornece dados por partições naturais (ano/mês/dia), DELETE+APPEND é mais simples que MERGE e igualmente eficiente.

---

## 📅 14/07/2026 - MERGE Incremental (Silver)

### Contexto
Silver precisa capturar correções da fonte (CVM pode republicar DFPs corrigidos). DELETE+APPEND deleta toda partição; MERGE atualiza seletivamente.

### Decisão
* **MERGE com chave natural** `CNPJ_CIA + DT_REFER + CD_CONTA + ANO` → Atualiza existentes + insere novos
* **Incluir ANO no ON** → Otimiza MERGE (só processa partição específica)
* `UPDATE SET *` e `INSERT *` → Simplicidade (schema já validado no DDL)

### Implementado
* Notebook 201: Substituído INSERT OVERWRITE por MERGE INTO
* Chave composta identifica unicamente cada linha contábil da DRE
* Operação atômica (UPDATE + INSERT em uma transação)

### Key Insight
Escolha consciente por camada: Bronze snapshot (DELETE+APPEND), Silver transformado (MERGE). MERGE não é "sempre melhor" - cada padrão tem seu caso de uso ideal.

---

## 📅 22/07/2026 - Landing Zone e Preservação de Arquivos Originais

### Contexto
Bronze depende de fonte externa (CVM) estar sempre disponível para reprocessamentos. APIs instabilidades ou indisponibilidade impedem reconstruir pipeline. Compliance pode exigir arquivo original preservado.

### Decisão
* **Landing Zone em UC Volume** → `/Volumes/main/proj_cvm/landing/dfp/` preserva ZIPs originais
* **Metadados HTTP** → Arquivo `_metadata.json` por ano (Last-Modified, URL, tamanho)
* **Versionamento de arquivos** → Se CVM atualiza arquivo histórico, nova versão preservada (`.v<timestamp>`)
* **Bronze lê de Landing** → Não mais download direto da URL

### Implementado
* Notebook `003_download_cvm_para_landing.py` → Download + metadados + versionamento
* Estrutura: `/landing/dfp/2020/arquivo.zip` + `_metadata.json`
* Bronze ajustado para ler de Volume ao invés de URL
* Detecção automática de arquivos atualizados (compara Last-Modified)

### Key Insight
Landing Zone duplica storage mas elimina dependência de fonte externa em reprocessamentos. Crítico para ambientes regulatórios onde arquivo original é evidência.

---

## 📅 22/07/2026 - Versionamento em Bronze: Append-Only com Metadados

### Contexto
CVM pode republicar DFPs corrigidos anos depois. DELETE WHERE destrói auditoria ("quando a fonte corrigiu?"). Bronze precisa preservar histórico completo de ingestões.

### Decisão
* **Append-only em Bronze** → Nunca DELETE, sempre APPEND
* **Colunas de metadados**: `_versao_ingestao` (int crescente), `_last_modified_cvm` (timestamp fonte)
* **Silver filtra versão mais recente** → Window Function `row_number().over(Window.partitionBy("ano").orderBy(col("_versao_ingestao").desc())) == 1`
* **Tabela de controle** → `000_controle_ingestao` rastreia cada execução

### Implementado
* Bronze: colunas `_versao_ingestao`, `_last_modified_cvm`, `_ingest_ts` + APPEND
* Silver: Window Function filtra apenas versão mais recente por período
* Tabela `proj_cvm_01_bronze.000_controle_ingestao` (fonte, ano, URL, status)
* Notebook `002_ddl_controle_ingestao.py` cria estrutura de controle

### Key Insight
Append-only em Bronze é mais simples que MERGE e preserva auditoria completa. Silver resolve conflito de versões via query (filtro), não via DELETE. Permite comparar "versão publicada em 2020" vs "versão corrigida em 2024".

---

## 📅 22/07/2026 - Orquestrador Pipeline: Detecção Inteligente de Períodos

### Contexto
Pipeline processa anos fixos (hardcoded). Não detecta novos anos da CVM nem arquivos atualizados. Requer intervenção manual para adicionar anos.

### Decisão
* **Orquestrador (pre-flight check)** → Notebook `000_orquestrador_pipeline.py` define `ANOS_PROCESSAR` dinamicamente
* **Detecção automática**:
  - Consulta tabela de controle (o que já foi processado?)
  - Verifica metadados HTTP de cada ano (Last-Modified)
  - Compara: arquivo CVM mais recente que última ingestão?
* **Override manual** → Widget permite forçar anos específicos
* **Exporta variável** → `ANOS_PROCESSAR` disponível via `%run ./config_parametros`

### Implementado
* Notebook `000_orquestrador_pipeline.py` com lógica de detecção
* Função `get_anos_para_processar_inteligente()` em `config_parametros.py`
* Widget `anos_override` para forçar reprocessamento
* Notebooks downstream importam `ANOS_PROCESSAR` via `%run`

### Key Insight
Orquestrador é pre-flight check que elimina intervenção manual. Pipeline "acorda" sozinho quando CVM publica novo ano ou corrige arquivo histórico. Padrão maduro de observabilidade.

---

## 📅 22/07/2026 - Estratégias de Gravação por Cenário

### Contexto
Cada camada tem padrão de atualização diferente. Bronze append-only, Silver/Gold por período. DELETE+APPEND vs MERGE vs replaceWhere — qual usar?

### Decisão
* **Bronze**: Sempre APPEND (preserva histórico)
* **Silver/Gold batch periódico**: DELETE WHERE + APPEND (simples, idempotente)
* **Silver/Gold streaming**: MERGE (CDC, atualizações por registro)
* **replaceWhere**: Quando quer atomicidade (partição substituida em transação única)

### Implementado
* Bronze: `mode("append")` sempre
* Silver DRE: `DELETE FROM ... WHERE ano IN (...)` + `mode("append")`
* Documentado critérios de escolha por cenário
* Evita antipadrão: APPEND sem dedupe em Silver/Gold

### Key Insight
Não existe "estratégia sempre melhor". DELETE+APPEND é mais simples que MERGE para batch periódico. MERGE é essencial para streaming/CDC. Escolha consciente por camada demonstra maturidade.

