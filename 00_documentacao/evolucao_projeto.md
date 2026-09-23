# Evolução do Projeto CVM

## Propósito deste Documento

Registro cronológico de decisões arquiteturais e aprendizados técnicos do projeto:
* Histórico de sessões de desenvolvimento
* Contexto e motivação de cada implementação
* Aprendizados técnicos não-óbvios acumulados

**Template de sessão** (máximo 25 linhas):
1. **Contexto**: 2-3 frases (o que motivou?)
2. **Decisão**: Bullet com escolha + 1 frase de justificativa
3. **Implementado**: Lista objetiva (3-5 itens)
4. **Key Insight**: 1 aprendizado realmente importante



## 23/09/2026 - Correcao de %run, SCHEMA_SUFFIX e Refatoracao do Notebook de Testes

### Contexto
O job de testes de integracao falhava em `validacoes_integracao` com `NameError: SCHEMA_BRONZE is not defined`. Investigacao revelou tres bugs encadeados: o `%run` nao executava (metadado de magic ausente), `SCHEMA_SUFFIX` ainda era referenciado apos migracao para `AMBIENTE`, e a validacao 3 assumia `count_bronze == count_silver` sem descontar duplicatas.

### Decisoes
* **Corrigir %run manualmente pela UI** -> O `editAsset` escreve `# MAGIC %run` como texto sem marcar o metadado interno de magic; o runtime trata como comentario Python e ignora. A UI marca o metadado ao digitar `%run` na celula. Correcao manual e o workaround
* **Trocar SCHEMA_SUFFIX por AMBIENTE no notebook de testes** -> A migracao para `ambientes.json` descontinuou `SCHEMA_SUFFIX` mas 3 linhas no `test_integracao_dre.py` ainda o referenciavam
* **Validacao 3: `count_silver == count_bronze - duplicatas_bronze`** -> A Silver aplica `ROW_NUMBER=1` sobre `(CNPJ_CIA, DT_REFER, CD_CONTA, ORDEM_EXERC)` que colapsa duplicatas exatas da Bronze. Nao usar `Silver <= Bronze` pois passaria mesmo perdendo metade dos dados
* **Coletar resultados em vez de abortar na primeira falha** -> Cada validacao roda em try/except, registra dict em `resultados` (nome, status, esperado, obtido, mensagem). Celula final imprime tabela e levanta AssertionError unica listando TODAS as falhas
* **Validacao 4: incluir `ORDEM_EXERC` na chave** -> A DRE traz exercicio corrente (ULTIMO) e anterior (PENULTIMO) para a mesma conta; a PK e 4 colunas, nao 3

### Implementado
* `config_parametros.py`: cabecalho `%md` duplicado removido (causa raiz do `%run` misturado com markdown)
* 3 notebooks (000, 003, test_integracao_dre): `%run` isolado em celula propria (corrigido manualmente pela UI)
* `test_integracao_dre.py`: 3 referencias a `SCHEMA_SUFFIX` trocadas por `AMBIENTE` (linhas 9, 29, 168)
* `test_integracao_dre.py`: refatoracao completa -- 5 validacoes em try/except, lista `resultados`, tabela final, assert unico
* `test_integracao_dre.py`: validacao 4 com 4 colunas, validacao 5 confirmada (3 colunas de metadados existem na Bronze)
* Commits: `9d3fe44` (isolamento %run), `c4b2034` (limpeza), `dc11d22` (SCHEMA_SUFFIX->AMBIENTE), `4fbe66e` (validacao 3), `bf1583d` (refatoracao coletar-resultados)

### Key Insight
O `editAsset` escreve `# MAGIC %run` como texto sem marcar o metadado da celula -- o arquivo passa no git e falha na execucao. Celula de magic se corrige pela UI, nao por API. Separadamente, a fonte CVM traz linhas duplicadas (76 em 2021, todas de INTER & CO, CNPJ 42.737.954/0001-21). A Window Function do 201 as elimina como efeito colateral de um filtro desenhado para versionamento, nao para deduplicacao de origem. Comportamento conhecido: Bronze pode ter duplicatas; Silver as colapsa; testes devem descontar.

---

## 23/09/2026 - Alinhamento de Observabilidade (Grupo A)

### Contexto
Diagnostico de observabilidade do projeto revelou que 4 dos 6 notebooks (101, 102, 201, 202) nao seguiam o padrao de resiliencia ja estabelecido em 103 e 203. Usavam `print()` em vez de `logging` estruturado, nao registravam `FAILED` na tabela de controle, nao mediam duracao por ano, nao tinham `try/except` com isolamento de falhas, e nao geravam relatorio final de execucao.

### Decisoes
* **Alinhar 4 notebooks ao padrao 103/203, nao o contrario** -> 103 e 203 ja tinham o padrao correto (logger, try/except por ano, FAILED no controle, duracao, relatorio). Replicar o que funciona e mais seguro que redesenhar
* **Grupo A primeiro (alinhamento), Grupo B depois (novas metricas)** -> Alinhar o que existe e baixo risco e alto valor. Novas colunas/metricas exigem ALTER TABLE e reprocessamento -- medio risco, pode esperar
* **try/except envolve TODO o processamento do ano, nao so a extracao** -> Em 101/102 o try cobria apenas a extracao; transformacao e gravacao ficavam de fora. Um erro na gravacao derrubava o notebook inteiro

### Implementado
* **101 DRE Bronze**: imports (logging, time) + `logging.basicConfig` + logger substitui print + try/except envolve extracao->transformacao->gravacao->controle + `FAILED` no `controle_ingestao` + duracao por ano + logs `[SUCESSO]`/`[FALHA]`
* **102 BPA Bronze**: mesmas mudancas adaptadas para BPA
* **201 DRE Silver**: logger + try/except por ano (nao tinha) + `FAILED` no controle (nao tinha) + `time.time()` + `count_registros = df_silver.count()` + nova celula RELATORIO FINAL com `anos_sucesso`/`anos_falha` + `raise RuntimeError` se houve falha
* **202 BPA Silver**: mesmas mudancas adaptadas para BPA (sem `DT_INI_EXERC` e `TIPO_ESTRUTURAL`, que BPA nao tem)
* **Descoberta**: tabela `observabilidade_execucoes` (26 colunas) ja existe no DDL 001 mas NENHUM notebook a popula -- todos gravam em `controle_ingestao` (8 colunas). Grupo B precisa decidir se migra ou adiciona colunas

### Key Insight
O projeto ja tinha o padrao correto de observabilidade (em 103/203) mas nao o aplicava uniformemente. Quando um padrao funciona em parte do codigo, a divida tecnica nao esta em cria-lo -- esta em replica-lo. A `observabilidade_execucoes` orfa mostra que criar infraestrutura sem conectar quem a usa e trabalho pela metade.

---

## 22/09/2026 - Jobs Reais no Bundle, Target test e Criterio Assimetrico

### Contexto
O pytest quebrava no runner do GitHub porque config_parametros executava _carregar_config_ambiente em escopo de modulo e sem Databricks levantava FileNotFoundError. Separadamente, os jobs CVM reais rodavam soltos na UI, o YAML do bundle era ficcao (8 tasks) e o target ci nao existia no ambientes.json.

### Decisoes
* **Criterio assimetrico de degradacao no config** -> _encontrar_ambientes_json devolve None; sem dbutils degrada para dev/incremental (erro barato), com dbutils levanta FileNotFoundError (erro caro)
* **Partir dos jobs reais, nao consertar o YAML antigo** -> job_verificacao_diaria.yml e job_pipeline_semanal.yml espelhando tasks/schedules dos jobs reais. job_pipeline_cvm.yml apagado
* **Presets no dev para preservar nomes e schedules** -> mode: development prefixa e pausa; presets: { name_prefix: "", trigger_pause_status: UNPAUSED } reverte ambos
* **Target ci -> test** -> ambientes.json so conhece dev/test/prod. ANOS_OVERRIDE tem precedencia sobre CARGA=completa (linha 636): job de teste usa ANOS_OVERRIDE=2021 sem CARGA

### Implementado
* config_parametros: Estrategia 0 (__file__), -> str | None, criterio assimetrico, teste novo, ruff per-file-ignores
* job_verificacao_diaria.yml + job_pipeline_semanal.yml: AMBIENTE: ${bundle.target}. job_pipeline_cvm.yml + TEMPLATE_github_workflow.yml apagados
* databricks.yml: presets no dev; target ci -> test (proj_cvm_test)
* ambientes.json: test.existe=true; _nota_test atualizada
* job_testes_integracao.yml: SCHEMA_SUFFIX -> AMBIENTE; ANOS_OVERRIDE 2010 -> 2021; sem CARGA
* .github/workflows/testes_integracao.yml: ci/dev -> test/dev
* Bundle: 6 jobs-lixo apagados (UI), diretorios stale .bundle/ limpos

### Key Insight
Criterio assimetrico de degradacao: tolerante onde o erro e barato, intolerante onde o erro e caro. Fora do Databricks (pytest, clone), degradar para dev/incremental e inofensivo -- ninguem grava. Dentro do Databricks, degradar significaria um job com --target test escrevendo silenciosamente em proj_cvm_dev. A mesma funcao aplica duas politicas opostas porque o custo do erro e diferente em cada contexto.

---

## 21/09/2026 - Parametrizacao de Ambiente e Migracao de Schemas

### Contexto
O projeto tinha ~75 referencias hardcoded a `proj_cvm_*` em 11 arquivos .py. Qualquer mudanca de ambiente (dev->test->prod) exigia find-and-replace manual em todos os notebooks. O eixo AMBIENTE ja existia no `ambientes.json` mas os notebooks ignoravam o config e montavam nomes de schema por conta propria. O widget `MODO_DEV` nos 6 notebooks Bronze/Silver duplicava a logica de `CARGA` ja adicionada ao config. A tabela `controle_ingestao` era criada num notebook separado (002), o que quebrava a primeira execucao em schema novo.

### Decisoes
* **Substituir 75 referencias hardcoded por variaveis do config** -> Todo nome de schema, tabela e volume e derivado de `ambientes.json` via `config_parametros`. Nenhum notebook monta nome por conta propria. Principio: fonte unica, zero excoes
* **`MODO_DEV` removido, substituido por `CARGA` do config** -> O eixo CARGA (incremental/completa) ja controla a janela de anos via `inicializar_anos_processar()`. O widget era redundante e tinha default diferente entre Bronze (False) e Silver (True), causando comportamento inconsistente
* **`controle_ingestao` movida para o fluxo de DDL (`001_ddl_create_tables`)** -> Schema novo sem essa tabela quebra na primeira execucao do orquestrador (`get_novos_anos_para_processar` faz SELECT nela). O 002 permanece como validacao idempotente, mas a criacao primaria esta no 001
* **`ANO_INICIAL_CVM = 2010` -> `ANO_INICIAL_PROJETO = 2021`** -> A janela do projeto comeca em 2021, alinhada com a landing zone. `CARGA=completa` agora processa 2021-ano corrente. Antes processava 2010-ano corrente, mas nenhuma tabela tinha dados anteriores a 2021
* **`SCHEMA_VOLUME` derivado de `ambientes.json`** -> O schema do volume (`proj_cvm`) e compartilhado entre ambientes. Agora lido do JSON via `vol_config["schema"]`, nao hardcoded no 003_download
* **Migracao via CTAS (CREATE TABLE LIKE + INSERT INTO SELECT)** -> Mais rapido que reprocessamento. Silver comparada ano a ano (18/18 pares batem) antes do drop dos schemas antigos

### Implementado
* `config_parametros`: `ANO_INICIAL_CVM` -> `ANO_INICIAL_PROJETO = 2021`; `SCHEMA_VOLUME` adicionado (derivado de `ambientes.json`); linha longa do print quebrada para ruff
* `003_download`: fallback de anos alterado de `range(ano_atual - 5, ...)` para `range(ANO_INICIAL_PROJETO, ...)`; `proj_cvm` -> `{SCHEMA_VOLUME}` no CREATE SCHEMA/VOLUME
* `001_ddl_create_tables`: `%run config_parametros` adicionado; todas as DDL convertidas para f-strings com `{SCHEMA_BRONZE}`, `{SCHEMA_SILVER}`, `{SCHEMA_GOLD}`, `{SCHEMA_APOIO}`; `controle_ingestao` CREATE TABLE adicionada
* `002_ddl_controle_ingestao`: `%run config_parametros` adicionado; DDL convertidas para f-strings
* `099_ddl_table_comments`: `%run config_parametros` adicionado; COMMENT ON TABLE/COLUMN convertidos para f-strings
* `000_orquestrador_pipeline`: comentario sem schema cravado
* `004_verificacao_diaria_landing`: `workspace.proj_cvm_05_apoio` -> `{CATALOG_NAME}.{SCHEMA_APOIO}`; comentario da landing zone limpo
* `101-103 Bronze`: `MODO_DEV` removido; `SCHEMA_BRONZE`/`SCHEMA_APOIO` em f-strings; `.saveAsTable` convertido para f-string
* `201-203 Silver`: `MODO_DEV` removido; `SCHEMA_SILVER`/`SCHEMA_BRONZE`/`SCHEMA_APOIO` em f-strings; `spark.table` e `.saveAsTable` convertidos para f-strings
* `tests/test_config_parametros.py`: 3 assertes de 2010 atualizados para 2021
* Migracao: 4 schemas `proj_cvm_dev_*` criados; 8 tabelas copiadas via CTAS; Silver comparada ano a ano (18/18); 4 schemas antigos dropados com CASCADE
* Verificacao: `grep -rn "proj_cvm" --include=*.py .` (excluindo 04_exploracao, config, tests) vazio; `grep -rn "MODO_DEV" --include=*.py .` vazio; `ruff check .` verde; `pytest tests/` 8/8 verde

### Key Insight
Parametrizacao nao e so substituir strings -- e garantir que a fonte unica cobre todas as excoes. O `SCHEMA_VOLUME` era a excecao escondida: o volume mora num schema proprio (`proj_cvm`) que nao segue a regra de composicao de nomes. Sem declara-lo no `ambientes.json`, a "fonte unica" tinha um furo -- o 003_download era o unico arquivo que sabia o nome do schema do volume. Declarar `volume.schema` no JSON fechou a brecha: agora o config deriva 100% dos nomes, sem excoes.

---

## 20/09/2026 - Enriquecimento Hierarquico de Contas na Silver

### Contexto
Os dados CVM trazem contas contabeis em estrutura hierarquica por notacao de pontos (`CD_CONTA`), ate 5 niveis. Somar todos os registros de uma empresa soma pais + filhos, inflando o total (double-counting). A Silver nao tinha colunas para filtrar por nivel, e `ST_CONTA_FIXA` (que distingue contas fixas da estrutura CVM de detalhamentos por empresa) existia na Bronze mas foi descartada na projecao Silver.

### Decisoes
* **Enriquecimento hierarquico e Silver, nao Gold** → Derivar `NIVEL_CONTA`, `CD_CONTA_PAI`, `CD_CONTA_RAIZ` de `CD_CONTA` e transformacao tecnica agnostica de negocio (qualquer engenheiro sem contexto de dominio faria). Modelagem dimensional para consumo e Gold
* **Projetar `ST_CONTA_FIXA` da Bronze** → A coluna existia na fonte mas foi descartada na projecao Silver original. Agora preservada para distinguir contas fixas (S) de detalhamentos por empresa (N)
* **Descricao mais frequente para `dim_conta` futura** → A CVM nao publica lista oficial de contas com descricao canonica. Validacao empirica: 100% das contas fixas presentes em 2021 e 2025 tem a mesma descricao mais frequente. Abordagem aceita como estavel na pratica

### Implementado
* `ALTER TABLE` nas 3 tabelas Silver (201, 202, 203) adicionando `ST_CONTA_FIXA STRING`, `NIVEL_CONTA INT`, `CD_CONTA_PAI STRING`, `CD_CONTA_RAIZ STRING`
* Notebooks 201, 202, 203: imports (`split`, `size`, `regexp_extract`, `lit`), derivacao das 4 colunas na etapa de transformacao, adicao na projeção explicita
* DDL 001: `CREATE TABLE` atualizado para as 3 tabelas Silver (ambientes novos ja nascem com as colunas)
* Reprocessamento completo: DRE (162.885 registros), BPA (308.988), BPP (528.436) — 2021 a 2026
* Validacao: 962 contas unicas (236 DRE + 323 BPA + 403 BPP), zero sobreposicao entre demonstracoes, rollup confirmado (Petrobras BPA 2025: Ativo Total = 1.223 bi = 140 bi Circulante + 1.083 bi Nao Circulante)

### Key Insight
`ST_CONTA_FIXA` estava na Bronze mas foi descartada na projecao Silver — a projecao explicita que protege contra mudancas de schema na Bronze tambem descarta colunas uteis. Cada coluna descartada precisa ser justificada; colunas da fonte que carregam metadados estruturais (como `ST_CONTA_FIXA`) devem ser preservadas na Silver, mesmo que nao sejam usadas imediatamente, porque o custo de preservar e zero e o custo de redescobrir a omissao e um reprocessamento completo.

---

## 20/09/2026 - Invariante do Bronze e Cegueira a Republicacoes

### Contexto
Investigacao sobre por que a estrategia de gravacao do Bronze alternou entre APPEND-ONLY e DELETE+APPEND ao longo de dois meses revelou que a decisao nunca esteve ancorada em um criterio, apenas em uma estrategia. Em 03/08 a validacao de `last_modified_cvm` foi removida e, na sequencia, APPEND-ONLY foi condenado por nao ser idempotente: a premissa que o sustentava foi retirada e depois cobrada. O 103, criado em 10/08, nasceu com a verificacao de idempotencia, divergindo de 101/102 ate a convergencia posterior, que nao ficou registrada.

Diagnostico de 19/09 encontrou a mesma mecanica se repetindo. A funcao `get_anos_com_atualizacao_cvm` falhava com `TypeError: can't compare offset-naive and offset-aware datetimes`, e a chamada foi desconectada em vez de corrigida. A funcao ficou orfa. Resultado: a deteccao conhece apenas dois estados, nunca processado e fantasma (SUCCESS sem dados). Nao existe o estado "processado mas a fonte mudou".

### Invariante (criterio, nao estrategia)
O Bronze preserva toda versao publicada pela fonte. Idempotencia vem da verificacao previa de `Last-Modified`, nunca de apagar linha.

Decorrencias, que nao sao decisoes independentes:
* Bronze: APPEND puro + `_versao_ingestao` incremental + verificacao previa
* Silver: le Bronze filtrando a versao mais recente por Window Function
* Silver: `REPLACE WHERE` por periodo (substituicao atomica)
* HTTP a CVM existe apenas em notebooks que buscam dados (003, 004). Notebooks de execucao (101/102/103, 201/202/203) nunca falam com a fonte

Se duplicata reaparecer, o mecanismo de verificacao falhou e deve ser corrigido. A estrategia de gravacao nao esta em discussao.

### Decisoes
* **004 itera janela 2021-2026 direto, sem depender de ANOS_PROCESSAR** → O notebook cuja funcao e descobrir que a CVM mudou nao pode receber a lista de quem ja assumiu que nada mudou. Quebra a circularidade
* **Deteccao ganha terceiro estado, comparando o `Last-Modified` do `_metadata.json` da landing contra o `last_modified_cvm` do `controle_ingestao`** → Leitura local, sem rede. Cria o estado "processado mas a fonte mudou" que antes nao existia
* **`get_anos_com_atualizacao_cvm` removida de vez** → Fazia HTTP em contexto de execucao. `verificar_arquivo_existe_cvm` tambem removida (unica chamadora era a funcao acima)

### Implementado
* `config_parametros`: `get_novos_anos_para_processar` reescrita com 3 estados: (1) nunca processado, (2) fantasma (SUCCESS sem dados), (3) republicado (`_metadata.json` diverge do controle). Query do controle agora inclui `last_modified_cvm` no select. `anos_processados` usa `.unique()` para dedup
* `config_parametros`: funcoes `get_anos_com_atualizacao_cvm` e `verificar_arquivo_existe_cvm` removidas. Import `json` adicionado
* `config_parametros`: comentario de `get_anos_para_processar_inteligente` atualizado de "sem HTTP" para "leitura local, sem HTTP — republicacao detectada via _metadata.json"
* `004_verificacao_diaria_landing`: celula de inicializacao substitui `inicializar_anos_processar()` por `range(ano_atual - JANELA_ANOS_RELEVANTE, ano_atual + 1)` direto
* Validacao: 004 executou 2x (04:08 e 04:16 UTC) com SUCCESS, sem o TypeError anterior. Dados de 2021-2026 consistentes entre `_metadata.json` e `controle_ingestao`. Nenhum falso positivo

### Entrada retroativa
A volta de 101 e 102 para APPEND-ONLY ocorreu em data nao registrada, entre 10/08 e 19/09. A documentacao so foi alinhada ao codigo em 19/09. Registrado aqui para que o historico nao aparente salto de DELETE+APPEND direto para "doc corrigida".

### Key Insight
Decisao registrada como estrategia nao sobrevive ao primeiro bug, porque um bug e um argumento e "usamos X" nao e. Invariante sobrevive: ele diz o que nao pode ser sacrificado e transforma o bug em "o mecanismo falhou, conserte o mecanismo". Duas vezes o mesmo mecanismo de verificacao de `Last-Modified` foi removido por motivo pequeno, e nas duas a arquitetura perdeu capacidade sem que a perda ficasse registrada. Mecanismo que sustenta invariante precisa de teste, senao ele some em silencio e o desenho sem ele deixa de fazer sentido.

---

## 19/09/2026 - Normalização de Escala Monetária e Widget MODO_DEV

### Contexto
Avaliação da Silver revelou que `VL_CONTA` mantinha duas escalas sem normalização: ~540 empresas em MIL (milhares) e ~15 em UNIDADE (reais). Comparar ou somar valores entre empresas sem normalizar gera resultados errados por fator de 1000x. A mesma sessão corrigiu o padrão de inicialização de anos, que usava lista hardcoded.

### Decisões
* **Normalizar na Silver, não na Gold** → Silver é a camada de padronização; deixar para Gold exigiria que todo consumidor downstream verificasse ESCALA_MOEDA
* **Widget MODO_DEV em vez de hardcoded** → `dbutils.widgets.get('MODO_DEV')` permite reprocessar todos os anos em dev sem alterar código; default False é seguro para jobs
* **`get_anos_disponiveis_cvm()` em vez de lista fixa** → Função existente retorna 2010-ano corrente dinamicamente; lista hardcoded fica obsoleta ao adicionar novo ano
* **Preservar ESCALA_MOEDA** → Coluna mantida para rastreabilidade; Bronze preserva valor original

### Implementado
* Notebooks 201, 202, 203 (Silver): normalização `when(ESCALA_MOEDA == "MIL", VL_CONTA * 1000).otherwise(VL_CONTA)` após cast de tipos
* Notebooks 101-203 (6 notebooks): widget `MODO_DEV` na célula de inicialização — `False` por default (job/produção), `true` via widget UI para reprocessamento em dev
* Notebooks 201, 202, 203: separação de imports em célula dedicada, guardrail `raise` na captura de `ANOS_PROCESSAR`, títulos de células corrigidos
* Validação: Petrobras (MIL) 497.549.000 → 497.549.000.000 (x1000); CELPAR (UNIDADE) 80.854.699 mantido
* Reprocessamento completo: anos 2021-2026 normalizados nas três tabelas Silver

### Key Insight
Hardcoded parece inofensivo no momento da escrita mas é dívida técnica silenciosa — uma lista de anos funciona hoje e quebra silenciosamente quando um novo ano é adicionado. Widget com fallback dinâmico elimina a classe inteira de problema: o desenvolvedor não precisa saber quais anos existem, e o job não precisa receber parâmetro para funcionar corretamente.

---

## 📅 19/09/2026 - Correção de Loop Multi-célula, Guardrail Silver e Refatoração da Detecção

### Contexto
Pipeline Bronze apresentava lacunas irrecuperáveis: DRE Bronze só tinha 2024-2026 (faltava 2021-2023), Silver marcava SUCCESS com 0 linhas quando Bronze falhava, e o orquestrador fazia HTTP para a CVM dentro do job de execução — lento e responsabilidade do job diário. Análise revelou três bugs estruturais e discrepâncias entre documentação e código.

### Decisões
* **Consolidar loop multi-célula em célula única (101, 102)** → O Databricks executava o loop só na célula de extração; transformação/escrita rodavam uma vez com a última iteração. Padrão correto já existia no BPP (103)
* **Implementar guardrail Silver em 201, 202, 203** → Bronze vazia → skip sem SUCCESS → nunca cria estado irrecuperável. Guardrail já estava documentado em guardrails.md mas não implementado
* **Remover HTTP da CVM da detecção** → `get_anos_com_atualizacao_cvm` não é mais chamado por `get_anos_para_processar_inteligente`. Busca por arquivos novos é responsabilidade do job diário
* **Remover orquestrador do job semanal** → Cada notebook Bronze já chama `inicializar_anos_processar()` independentemente; orquestrador era redundante e fazia HTTP lento. Job agora: Bronze em paralelo → Silver respectivo
* **Adicionar fontes Silver na detecção** → `inicializar_anos_processar()` agora consulta 6 fontes (3 Bronze + 3 Silver). Antes só consultava Bronze; Silver nunca detectava anos faltantes
* **Dupla checagem na detecção** → `get_novos_anos_para_processar` verifica dados reais na tabela destino (não confia só no SUCCESS do controle). Anos "fantasma" (SUCCESS sem dados) são reprocessados

### Implementado
* Bronze 101/102: loop multi-célula consolidado em célula única (padrão BPP)
* Silver 201/202/203: guardrail `count_bronze == 0 → skip` implementado
* config_parametros: `get_anos_com_atualizacao_cvm` removido da detecção
* config_parametros: `fontes_config` expandido de 3 para 6 fontes (Bronze + Silver)
* config_parametros: `get_novos_anos_para_processar` verifica dados reais via `FONTE_TABELA_DESTINO`
* Job 890867014997453: task orquestrador removida; Bronze tasks sem dependência
* Validação final: Bronze e Silver com 2021-2026 completos em todas as 6 tabelas

### Key Insight
A "inteligência" do pipeline era frágil porque confiava em flags de controle sem verificar o estado real dos dados. Silver marcava SUCCESS com 0 linhas e o orquestrador nunca reprocessava — criando buracos irrecuperáveis. Princípio corrigido: verificação é sempre feita "na tabela", não só via flag do controle.

---

## 📅 19/09/2026 - Auditoria de Jobs, Versionamento e Bug Off-by-One

### Contexto
Após configuração e teste dos jobs diário (348419458655416) e semanal (890867014997453), foi necessário esclarecer responsabilidades dos notebooks de ingestão (003 vs 004), confirmar que o versionamento Bronze→Silver funciona corretamente, e investigar discrepância entre configuração de janela temporal (5 anos) e resultado observado (3 anos processados).

### Decisões
* **003_download permanece como notebook de teste/CI** → Usado apenas por jobs DAB ([dev], [CI], [proj_cvm_ci]); jobs de produção (diário/semanal) não o referenciam. Manter como está.
* **Não alterar mais os jobs** → Jobs diário e semanal estão configurados, testados e validados. Decisão de congelar configuração.
* **Bug off-by-one na janela temporal NÃO corrigido agora** → `JANELA_ANOS_RELEVANTE = 5` gera 6 anos na prática (`ano_atual - 5` inclui ano_atual, totalizando 6). Decisão do usuário: não corrigir neste momento.
* **Versionamento Bronze→Silver confirmado correto** → Bronze grava com APPEND (preserva histórico de versões); Silver aplica Window Function (`PARTITION BY chave_natural ORDER BY _versao_ingestao DESC`, `row_number() == 1`) para ler apenas a versão mais recente.

### Implementado
* Mapeamento completo de jobs ativos no workspace: 2 de produção (diário/semanal via UI) + 4 de teste/CI (via DAB)
* Confirmado que 003_download é referenciado apenas em `resources/jobs/job_pipeline_cvm.yml` (DAB), nunca nos jobs manuais
* Confirmado fluxo de versionamento: Bronze adiciona `_versao_ingestao`, `_last_modified_cvm`, `_ingest_ts` → Silver filtra versão mais recente → grava com REPLACE WHERE por ano
* Bug documentado mas não corrigido: `ano_inicio_janela = ano_atual - JANELA_ANOS_RELEVANTE` deveria ser `ano_atual - JANELA_ANOS_RELEVANTE + 1` para gerar exatamente 5 anos

### Key Insight
Separar jobs de teste (DAB/CI) de jobs de produção (manuais via UI) é uma estratégia válida, mas exige documentação clara de qual notebook serve qual propósito — sem isso, notebook 003 parece redundante quando na verdade atende um contexto diferente (validação de código vs execução de dados).

---

## 📅 23/08/2026 - CI/CD e Testes Automatizados

### Contexto
Pipelines de dados sem CI automático dependem de disciplina manual para manter qualidade. Implementações locais (máquina pessoal) de GitHub Actions e testes unitários já validadas e commitadas, agora sincronizadas via pull para o workspace Databricks.

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
CI garante que lint e testes rodem sem depender de disciplina manual a cada push. GitHub Actions é gratuito para repositórios públicos (2000 minutos/mês), eliminando barreira de custo. Badge verde no README sinaliza que o pipeline passa nos testes antes de qualquer execução. A diferença entre código com e sem CI não está na lógica de negócio — está na garantia automatizada de que regressões são detectadas antes de chegar à produção.

---

## 📅 24/08/2026 - Testes de Integração E2E

### Contexto
Testes unitários (pytest) validam módulos isolados, mas não garantem que o pipeline completo funciona ponta a ponta. Testes de integração que rodam no Databricks real — não apenas mocks locais — validam que o pipeline é executável em ambiente produtivo.

### Decisões
* **Pasta dedicada `06_testes/`** → Separação clara entre testes unitários (pytest local em `tests/`) e testes de integração E2E (notebooks Databricks executados no workspace)
* **Schemas isolados de teste** → Sufixo `_test` nos schemas (`bronze_cvm_dfp_test`, `silver_cvm_dfp_test`) evita poluir dados de produção durante testes
* **Workflow GitHub Actions manual** → Trigger `workflow_dispatch` (não automático) para controlar consumo de DBUs — testes E2E são executados sob demanda, não a cada push
* **Job dedicado no Databricks** → `[CI] Testes de Integração - Pipeline CVM` executa notebooks Bronze+Silver em ano específico (2010, volume mínimo) com 5 validações de qualidade
* **Badge de status no README** → Indicador visual de saúde dos testes de integração, complementando badge de CI/lint

### Implementado
* Notebooks de teste (`06_testes/`):
  - `criar_schemas_teste.py`: Cria schemas isolados com sufixo configurável
  - `test_integracao_dre.py`: Valida pipeline DRE E2E (Bronze existe, Silver existe, contagens batem, PKs únicas, metadados populados)
  - `TEMPLATE_github_workflow.yml`: Template para referência
* Workflow GitHub Actions (`.github/workflows/testes_integracao.yml`):
  - Deploy do bundle antes de rodar testes (garante código atualizado)
  - Disparo do job via Databricks CLI
  - Aguarda conclusão e reporta status (timeout 30min)
* Job Databricks criado: ID 176285066429591 (`[CI] Testes de Integração - Pipeline CVM`)
* Target `ci` configurado no `databricks.yml` com `schema_prefix: proj_cvm_ci`
* README atualizado com instruções de configuração de secrets (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`)

### Key Insight
Testes de integração E2E no Databricks real (não mocks) são o diferencial entre "código que parece funcionar" e "pipeline validado em produção". Schemas isolados com sufixo evitam contaminar dados reais — padrão essencial para ambientes corporativos onde teste e produção compartilham workspace. Workflow manual (não automático) equilibra validação rigorosa com controle de custo: DBUs são consumidos apenas quando necessário, não a cada commit.

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
* **Criar projeto `databricks-genie-skills`** → Projeto independente dedicado a investigação técnica (troubleshooting do Skill Registry, análise de causa raiz) + frameworks reutilizáveis
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
Separação de contexto não é organização de pastas — é **arquitetura de conhecimento**. Skills (Tipo 1 - Conceitual) são frameworks universais, devem ser fonte única entre projetos; implementações específicas (Tipo 2 - Projeto) evoluem com cada projeto; histórico de decisões (Tipo 3 - Operacional) documenta o porquê. Acoplar skills ao CVM criava dependência artificial: próximo projeto precisaria duplicar skills ou importar pasta de outro projeto. Separação permite reutilização limpa e evita acoplamento entre frameworks universais e implementação específica de um único projeto.

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
Projeto pronto para versionamento público no GitHub após meses de desenvolvimento. Última etapa crítica antes do commit inicial: validação profunda de TODA a documentação para garantir portabilidade total e zero vazamento de informação pessoal. Documentação técnica polida é inútil se expõe caminhos do workspace ou identificação pessoal — caminhos hardcoded quebram portabilidade e impedem reprodução em ambiente novo.

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
Documentação para Git não é "documentação + versionamento" — é **documentação agnóstica de ambiente**. Cada caminho absoluto, cada âncora interna do workspace, cada referência pessoal quebra a promessa de portabilidade. Um clone limpo deve executar sem editar paths nem conhecer o workspace de origem. Anonimização não é "segurança extra", é requisito de portabilidade. Validação pré-commit (git status, .gitignore, conferência manual) evitou exposição de `_old/` — 30 segundos extras de conferência salvaram de push irreversível com arquivos sensíveis.

---

## 📅 05/08/2026 - Padronização de Formato: Notebooks em .py para Git Limpo

### Contexto
Projeto tinha notebooks em formatos mistos: alguns `.py` (Python Source), um `.ipynb` (Jupyter). Formato misto viola princípio básico de consistência. Mais importante: para pipelines versionados onde histórico git conta narrativa, `.ipynb` (JSON) polui diffs com metadata não-relevante (execution_count, outputs), tornando code review ilegível. Times que usam Databricks + CI padronizam em `.py` exatamente por isso: diff linha-por-linha, sem noise.

### Decisões
* **Padronizar TUDO em `.py`** → Converter apenas 1 arquivo inconsistente (menos trabalho, menos risco) vs manter 8 arquivos já validados em produção
* **Justificativa git-first** → Metadados do workspace (data criação, execution_count) não aparecem no Git; o histórico relevante é o de commits, não de execução
* **Atualizar `estrutura_notebooks.md`** → Substituir instrução `.ipynb` por `.py` com justificativa de diff limpo e padrão de mercado
* **Documentar decisão** → Registrar contexto completo (o porquê de `.py` > `.ipynb` para pipelines versionados) em vez de apenas "padronizar"

### Implementado
* Formato confirmado: 9 notebooks em `.py` (bronze: 101/102, silver: 201/202, apoio: 000/001/002/003/099)
* Especificação `estrutura_notebooks.md` → Nova seção "Formato de Arquivo" com justificativa git + proibição de `.ipynb`
* Documentação (README, arquitetura, evolucao) → Referências corrigidas para `.py` (estavam desatualizadas)

### Key Insight
Escolha de formato é decisão arquitetural, não detalhe técnico. `.ipynb` é superior para notebooks isolados (Jupyter, Colab), mas `.py` é superior para pipelines versionados. Razão: git não é ferramenta de backup, é ferramenta de narrativa — o critério é se o diff sozinho reconstrói a mudança. Metadata JSON responde errado. Em pipelines versionados, `.py` produz diffs limpos; `.ipynb` polui o histórico com metadata de execução. **Alerta pendente**: Histórico (27/07) registra falha `%run` + `.py` → antes de commitar, testar pipeline completo pra validar que `%run ./config_parametros` funciona ou confirmar se `exec(open())` é necessário.

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
Portabilidade não é "feature opcional" — é requisito de código profissional. Hardcoded paths com e-mail revelam código escrito "só para funcionar aqui e agora", não pensado para reprodução/migração. Caminhos absolutos com e-mail quebram ao migrar para outro workspace e expõem identificação pessoal desnecessariamente. Databricks oferece `%run` (namespace compartilhado, caminho relativo) exatamente pra isso — usar a ferramenta correta garante portabilidade entre ambientes.

---

## 📅 31/07/2026 - Auditoria Externa: Correção de Defasagem Spec-vs-Código

### Contexto
Revisão externa do código revelou **defasagem crítica entre documentação e implementação (gaps em pontos-chave)**. Diagnóstico: 8 achados técnicos, 4 críticos ou médios. A defasagem entre o que a documentação descrevia e o que o código fazia indicava que decisões técnicas não estavam refletidas na implementação — o gap foi mapeado, priorizado e fechado nos 4 achados críticos/médios.

### Decisões
* **Correção técnica imediata** → Atacar 4 achados críticos/médios prioritários antes de melhorias arquiteturais (Achados #1, #4, #6, #7)
* **Confrontação técnica independente** → Validar fatos (o que está escrito no código) mas questionar severidades e interpretações do auditor
* **Documentação honesta** → Registrar gap encontrado e corrigido (não esconder), princípio de que documentar lacunas e correções é mais útil que polimento que as omite
* **Adiar melhorias da Fase 2** → Achados #2 (dedupe por VERSAO), #3 (idempotência fraca), #5 (Auto Loader) ficam para ponderação posterior

### Implementado
* **Achado #1** (conflito de tipo DDL × cast): DDL Silver alterado de `VERSAO STRING, CD_CVM STRING` para `VERSAO INT, CD_CVM INT` (notebooks 001, células 5-6). Alinhamento com casts aplicados na transformação Silver. Razão: consistência de schema, não policy ANSI (análise técnica refinada)
* **Achado #7** (guardrail incompleto): `ST_CONTA_FIXA` adicionada em `COLUNAS_ESSENCIAIS_DRE` e `COLUNAS_ESSENCIAIS_BPA` (config_parametros.py). Coluna existia no DDL Bronze mas faltava no contrato de dados — risco de append failure ou coluna sempre NULL
* **Achado #4** (detecção de atualizações morta): DDL da tabela de controle alterado de `last_modified_cvm STRING` para `TIMESTAMP` (notebook 002). Função `get_anos_com_atualizacao_cvm` corrigida com except mais específico (AnalysisException separado). Comparação `datetime > str` causava TypeError silencioso — recurso nunca funcionou desde implementação
* **Achado #6** (count remanescente): `.count()` removido do notebook Bronze 101 (célula 6). Antipadrão que força materialização prematura, inconsistente com remoção anterior do Silver

### Key Insight
Auditoria externa identificou defasagem entre documentação e implementação antes que evoluísse em ambiente de produção. **Confrontar tecnicamente** (não aceitar passivamente) foi crítico: Achado #1 tinha razão certa mas explicação errada (não é policy ANSI, é consistência de schema); Achado #2 pode ser over-engineering se fonte não traz duplicatas; Achado #5 recomendava Auto Loader quando `spark.read.csv` resolve 90% sem complexidade de checkpoint. Validar fatos no código, questionar interpretações de impacto — auditor pode estar certo nos bugs mas errado nas severidades.

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
* **Tradeoff consciente: schema correto > metadados** → Reprodutibilidade em ambiente novo pesa mais que preservar `created_time` de tabelas existentes
* **Função de migration idempotente** → Adicionada `apply_schema_migration_if_needed()` no DDL que tenta ALTER (ambiente novo: falha silenciosamente, CREATE funciona; ambiente antigo: reporta limitação)
* **Documentar limitação em código** → Comentários no notebook explicam que Delta não suporta ALTER TYPE, alinhando expectativas

### Implementado
* Notebook 001: Célula de migration adicionada (tenta ALTER TABLE, detecta NOT_SUPPORTED_CHANGE_COLUMN)
* Drop manual: Tabelas Silver (201, 202) e Controle dropadas via SQL
* Job re-executado: Tabelas recriadas com schema correto (VERSAO/CD_CVM INT, last_modified_cvm TIMESTAMP)
* Dados preservados: Bronze intacta (276k DRE, 295k BPA), Silver reconstruída a partir de Bronze (30k DRE, 58k BPA)
* Run 39472409661291: SUCCESS, schema validado via DESCRIBE TABLE

### Key Insight
Delta Lake tem limitação arquitetural real: ALTER COLUMN TYPE não suportado (Parquet subjacente é imutável). Única solução é DROP+CREATE ou CTAS, ambas perdem metadados de criação. **Tradeoff de engenharia**: reprodutibilidade em ambiente novo (CREATE TABLE IF NOT EXISTS com schema correto funciona em clone do repo) pesa mais que preservação de `created_time` (relevante apenas em ambiente existente). Em ambiente com tabelas existentes, DROP+CREATE é executado em janela de manutenção documentada.

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
Spark Connect (Serverless Compute) tem restrições arquiteturais reais - bloqueia acesso a filesystem local (/tmp, paths fora de /Workspace) com LocalFilesystemAccessDeniedException. APIs Python padrão (open(), os.makedirs()) funcionam perfeitamente com Unity Catalog Volumes quando usadas diretamente, sem staging intermediário em /tmp.

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
Padronização de nomenclatura não é cosmética — elimina ambiguidade operacional (qual `000_` executar primeiro?), força ordem lógica visível, e reduz débito técnico documental. Inconsistências se propagam: 1 renomeação impactou 8 arquivos (docs + job + código). Auditorias periódicas de conformidade com `/especificacoes/` previnem débito técnico documental.

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
`saveAsTable()` com inferência automática não garante controle sobre governança. Separar infraestrutura (DDL) de transformações (DML) torna o schema explícito e auditável.

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
O schema fica declarado e auditável antes da primeira carga, em vez de inferido pelo Spark a cada execução.

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
Orquestrador é pre-flight check que elimina intervenção manual. Pipeline "acorda" sozinho quando CVM publica novo ano ou corrige arquivo histórico.

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
Não existe "estratégia sempre melhor". DELETE+APPEND é mais simples que MERGE para batch periódico. MERGE é essencial para streaming/CDC. Escolha consciente por camada reflete entendimento dos tradeoffs de cada estratégia.

