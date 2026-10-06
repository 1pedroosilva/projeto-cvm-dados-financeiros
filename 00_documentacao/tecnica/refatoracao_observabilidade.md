# Refatoração da Observabilidade — Pipeline CVM

**Versão:** 1.0
**Data:** 03/10/2026

---

## Premissa fundamental: baseline 29/09/2026

> **Premissa:** Toda análise e decisão neste documento é válida exclusivamente com dados a partir de 29/09/2026 — data em que o pipeline CVM tornou-se operacional via jobs e as tabelas customizadas iniciaram captura consistente.
>
> Dados anteriores a 29/09/2026 são artefatos do período de desenvolvimento e **não devem ser usados** como base para nenhuma decisão arquitetural. Qualquer referência a dados pré-29/09 neste documento é estritamente contextual (preservação de histórico) e não influencia o design.

---

# Parte 1 — Design arquitetural

## Contexto

A observabilidade do pipeline CVM é capturada por logging manual em notebooks: `config_parametros` insere contexto do job (`job_id`, `run_id`, `status`, `trigger_type`, etc.) em `observabilidade_execucoes` e `observabilidade_jobs` via `dbutils.widgets.get()`. Esse padrão tem três problemas:

1. **Duplicação** — `system.lakeflow` já captura runs e tasks do workspace com metadados ricos (trigger_type, result_state, execution_duration). O logging manual reproduz esses mesmos dados
2. **Mistura de responsabilidades** — `observabilidade_execucoes` tem 8 colunas de infraestrutura (job_id, job_name, status, duracao_segundos, etc.) junto com métricas de negócio (registros_processados, arquivos_baixados). A tabela serve dois mestres
3. **Limitações do logging manual** — depende de `dbutils.widgets.get()` que falha em execução manual, exige `base_parameters` em todo job, e não captura runs que não passam pelos notebooks

O objetivo é separar infraestrutura (transposta do `system.lakeflow` para tabelas UC próprias) de negócio (métricas que apenas os notebooks conhecem), eliminando a redundância.

## Decisão central: transposição, não dependência

O design transpõe os dados do `system.lakeflow` para tabelas UC próprias via MERGE incremental diário. A transposição resolve três limitações:

| Limitação | system.lakeflow direto | Transposição UC |
|---|---|---|
| Retenção 365 dias | dados somem | você controla |
| Sem SLA de durabilidade | Databricks não garante | Delta ACID, `UNDROP` 7d |
| Acesso admin-only por padrão | grant explícito necessário | RBAC UC padrão |

### Fluxo de dados

**Antes:**

```
observabilidade_execucoes   (infra + negócio, escrita pelos notebooks)
        +
observabilidade_jobs        (infra, escrita pelos notebooks)
        +
system.lakeflow            (infra, leitura admin-only, retenção 365d)
```

Três fontes paralelas e redundantes para os mesmos dados de execução.

**Depois:**

```
system.lakeflow
      │
      ├── observabilidade_runs      (infra job-level)
      └── observabilidade_tasks      (infra task-level)
             │
             │  JOIN por run_id + task_key
             │
             └── observabilidade_execucoes   (métricas de negócio, escrita pelos notebooks)
```

`system.lakeflow` é a fonte única de infraestrutura. `observabilidade_execucoes` persiste apenas métricas de negócio que o system não conhece (`registros_processados`, `arquivos_baixados`, etc.). O JOIN entre as duas camadas é por chave (`run_id` + `task_key`), não por fluxo de dados.

---

## Tabelas finais

Todas as tabelas existem em **Dev** (`proj_cvm_dev_05_apoio`) e **Test** (`proj_cvm_test_05_apoio`), em `{SCHEMA_APOIO}` por-ambiente.

| tabela | origem | função |
|---|---|---|
| `observabilidade_runs` | espelho `job_run_timeline` | infra job-level |
| `observabilidade_tasks` | espelho `job_task_run_timeline` | infra task-level |
| `observabilidade_execucoes` | notebooks (DRY) — **evolui no lugar** | só métricas de negócio (ALTER TABLE remove 8 colunas de infra) |
| `observabilidade_execucoes_historico` | CTAS snapshot pré-29/09 | preservação de dados de desenvolvimento |
| `observabilidade_guardrails` | notebooks — **intacta** | quality checks |
| `controle_ingestao` | notebooks — **intacta** | controle landing zone |
| `jobs_metadata` | lookup manual (`{SCHEMA_APOIO}` por-ambiente) | `job_id → job_name + ambiente` |
| ~~`observabilidade_jobs`~~ | **ELIMINADA** (Dev + Test) | substituída por `observabilidade_runs` |

---

## Schemas

### `observabilidade_runs`
```sql
CREATE TABLE observabilidade_runs (
  run_id                    BIGINT,    -- CAST(system.run_id AS BIGINT)
  job_id                    BIGINT,    -- CAST(system.job_id AS BIGINT)
  workspace_id              STRING,
  trigger_type              STRING,
  result_state              STRING,
  run_type                  STRING,
  period_start_time         TIMESTAMP,
  period_end_time           TIMESTAMP,
  execution_duration_seconds BIGINT,
  run_duration_seconds      BIGINT,
  job_name                  STRING,   -- enriquecido via jobs_metadata (não existe em system.lakeflow)
  ambiente                  STRING,   -- derivado de workspace_id
  ingested_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
```

> **Nota:** `system.lakeflow` armazena `run_id` e `job_id` como **STRING**. O MERGE deve fazer `CAST(... AS BIGINT)` para compatibilidade com `observabilidade_execucoes` (que recebe `run_id` BIGINT do parâmetro `{{job.run_id}}`).

### `observabilidade_tasks`
```sql
CREATE TABLE observabilidade_tasks (
  run_id                     BIGINT,    -- CAST(system.run_id AS BIGINT) = TASK run ID
  job_run_id                 BIGINT,    -- CAST(system.job_run_id AS BIGINT) = PARENT JOB run ID
  task_key                   STRING,
  workspace_id               STRING,
  result_state               STRING,
  period_start_time          TIMESTAMP,
  period_end_time            TIMESTAMP,
  execution_duration_seconds BIGINT,
  setup_duration_seconds     BIGINT,
  cleanup_duration_seconds   BIGINT,
  termination_code           STRING,
  ingested_at                TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
```

> **Crítico — chaves de JOIN:** `run_id` nesta tabela é o **task run ID** (diferente do job run ID). `job_run_id` é que referencia o parent job run. Portanto:
> - `observabilidade_runs.run_id` JOIN com `observabilidade_tasks.job_run_id` (NÃO com `run_id`)
> - `observabilidade_execucoes.run_id` (que vem de `{{job.run_id}}` = job run ID) JOIN com `observabilidade_tasks.job_run_id`

### `observabilidade_execucoes` (DRY — evolução no lugar)

A tabela `observabilidade_execucoes` **continua existindo com o mesmo nome**. O schema evolui: 8 colunas de infraestrutura são removidas, mantendo apenas métricas de negócio.

**Schema final (após evolução):**
```sql
-- Colunas mantidas (métricas de negócio):
id_execucao, run_id, task_key, etapa, fonte, ano,
registros_processados, arquivos_verificados, arquivos_baixados,
arquivos_arquivados, arquivos_ignorados, bytes_baixados, bytes_arquivados,
last_modified_cvm, url_cvm, tipo_erro, mensagem_erro, parametros, created_at

-- Colunas removidas (8 — todas recuperáveis via system.lakeflow):
-- job_id, job_name, notebook_path, inicio_ts, fim_ts, duracao_segundos, status, trigger_type
```

> A tabela evolui no lugar. Os notebooks passam a escrever apenas as colunas de negócio no INSERT.

---

## Query padrão no dashboard
```sql
SELECT
  r.job_name,
  r.trigger_type,
  r.result_state          AS status,
  FROM_UTC_TIMESTAMP(r.period_start_time, 'America/Sao_Paulo') AS inicio,
  SUM(t.execution_duration_seconds) AS duracao_real,
  SUM(e.registros_processados)      AS volume,
  SUM(e.arquivos_baixados)          AS arquivos
FROM observabilidade_runs r
  INNER JOIN observabilidade_tasks t
    ON r.run_id = t.job_run_id
  INNER JOIN observabilidade_execucoes e
    ON t.job_run_id = e.run_id AND t.task_key = e.task_key
WHERE r.workspace_id = '7474657818873516'
GROUP BY ALL
```

Filtro por `workspace_id` dispensa `UNION ALL` entre ambientes.

---

## Otimização do dashboard (performance)

O dashboard atual tem 23 datasets, dos quais ~12 replicam a mesma CTE base (`base_jobs UNION ALL base_execs`). Mudou o JOIN? Corrige em 12 lugares.

**Otimização 1 — Eliminar duplicação de CTEs:** Todos os datasets consomem a mesma query base (JOIN `runs + tasks + execucoes`), seja como dataset compartilhado no dashboard ou como view SQL. Elimina a duplicação de lógica de manutenção.

**Otimização 2 — Eliminar datasets redundantes:** Filtros (`filtro_ambiente`, `filtro_tipo_exec`, `filtro_runs`) leem da base compartilhada em vez de replicar queries. 23 datasets → ~20.

**Otimização 3 (condicional) — Materializar a base:** Se após as otimizações 1 e 2 o tempo de refresh ou o número de scans justificar, criar `base_unificada_materializada` que pré-computa o JOIN. Decisão pendente de medição — não materializar sem evidência de que o ganho justifica a camada física adicional.

---

## Achados que embasam o design (todos verificados com dados ≥ 29/09/2026)

> **Todos os achados abaixo foram verificados com dados a partir de 29/09/2026.** Referências a dados pré-29/09 são apenas contextuais e não sustentam nenhuma decisão.

- `run_id → job_id` é estritamente 1:1 em `system.lakeflow` (verificado: 0 run_ids com múltiplos job_ids, dados desde 29/09)
- Desde 29/09, 100% das execuções têm `run_id` (verificado em 03/10/2026). Nenhuma execução manual de notebook após 29/09 (0 registros com `run_id` NULL pós-29/09).
- `system.lakeflow` não tem `job_name` — `run_name` retorna NULL; enriquecimento necessário via `jobs_metadata`
- `system.lakeflow.job_task_run_timeline.run_id` = **task run ID** (diferente do job run ID); `job_run_id` = parent job run ID — chaves de JOIN devem usar `job_run_id`
- `system.lakeflow` armazena `run_id` e `job_id` como STRING — CAST para BIGINT necessário no MERGE
- `observabilidade_jobs` — todos os campos são recuperáveis via `system.lakeflow.job_run_timeline`. O `system.lakeflow` captura todos os runs do workspace (Dev + Test + manuais), enquanto `observabilidade_jobs` captura apenas os runs logados pelos notebooks. Enriquecimento (`job_name`, `ambiente`) migra para `jobs_metadata` + `observabilidade_runs`

### Dados pré-29/09 (somente contexto, não influenciam o design)

- 192 registros (Dev) + 17 (Test) com `run_id = NULL` — artefatos de desenvolvimento, preservados em snapshot
- 360 guardrails órfãos — todos pré-29/09; desde 29/09: 0 órfãos. Problema não recorrente no design novo.

---

# Parte 2 — Plano de execução

## Job de transposição

**Notebook:** `05_apoio/005_transposicao_system.py`

**Lógica:**
1. Lê watermark implícito: `SELECT MAX(period_start_time) FROM observabilidade_runs`
2. MERGE incremental `system.lakeflow.job_run_timeline → observabilidade_runs` (com `CAST(run_id AS BIGINT)`, `CAST(job_id AS BIGINT)`)
3. MERGE incremental `system.lakeflow.job_task_run_timeline → observabilidade_tasks` (com `CAST(run_id AS BIGINT)`, `CAST(job_run_id AS BIGINT)`)
4. Enriquece `job_name` via JOIN em `jobs_metadata`

**Watermark:** `MAX(period_start_time)` da própria tabela — zero infra adicional. MERGE é idempotente.

**Job Databricks:** agendado diariamente às 07:00 BRT. Adicionado ao Bundle como novo job, sem modificar os jobs existentes.

**Timing:** `system.lakeflow` tem delay de ~15-30 min entre a execução do job e a disponibilidade dos dados. Agendamento às 07:00 BRT garante que:
- Verificação Diária (06:00 BRT) — dados disponíveis às 07:00 (garantido)
- Pipeline Semanal (07:00 BRT segunda) — dados entram na transposição de terça (T+1 aceitável)

---

## Preservação do histórico pré-29/09

Os 192 registros (Dev) + 17 (Test) com `run_id = NULL` são artefatos do período de desenvolvimento (pré-29/09). Antes da evolução do schema que remove as 8 colunas de infra, um snapshot congelado é preservado:

1. `CREATE TABLE observabilidade_execucoes_historico AS SELECT * FROM observabilidade_execucoes WHERE run_id IS NULL` (1 CTAS, Dev + Test)
2. Evoluir o schema de `observabilidade_execucoes` — remover as 8 colunas de infra
3. Os registros pré-29/09 perdem as colunas removidas (ficam NULL) — irrelevante, pois ninguém os consulta

> Snapshot congelado antes da evolução do schema. 1 CTAS por ambiente.

---

## Plano de execução

### Fase 0 — Preparação (concluída 03/10/2026)

**Criar `jobs_metadata`** (tabela por-ambiente, em `{SCHEMA_APOIO}`):
- Colunas: `job_id BIGINT, job_name STRING, ambiente STRING, ativo BOOLEAN, atualizado_em TIMESTAMP`
- Popular com `job_id` distintos do `system.lakeflow.job_run_timeline` pós-29/09 + `job_name` do Bundle YAML
- Automação futura: MERGE detecta `job_id` novo no `system.lakeflow` e insere com `job_name = NULL, ativo = false`

### Fase 1 — Transposição (concluída 06/10/2026)

**1.1 Criar tabelas** `observabilidade_runs` e `observabilidade_tasks` (Dev + Test, schemas conforme acima)

**1.2 Escrever notebook** `05_apoio/005_transposicao_system`:
1. Watermark: `SELECT MAX(period_start_time) FROM observabilidade_runs`
2. MERGE `system.lakeflow.job_run_timeline` → `observabilidade_runs` (CAST STRING → BIGINT)
3. MERGE `system.lakeflow.job_task_run_timeline` → `observabilidade_tasks` (CAST STRING → BIGINT)
4. Enriquecer `job_name` via JOIN em `jobs_metadata`

Idempotente. Self-healing (gap máximo = 1 dia).

**1.3 Criar job no DABs** (`resources/jobs/job_transposicao_system.yml`):
- Diário 07:00 BRT, UNPAUSED Dev / PAUSED Test
- Tasks: `ddl_create_tables` → `transposicao_system`

**1.4 Validar**: `observabilidade_runs` e `observabilidade_tasks` populadas, `job_name` não-NULL, MERGE idempotente, watermark avança

### Fase 2 — Dashboard rewrite (antes da evolução do schema)

> 5+ datasets leem colunas de `observabilidade_execucoes` que migrarão para `observabilidade_runs`/`tasks` (`status`, `duracao_segundos`). A evolução do schema só ocorre após o dashboard parar de referenciar essas colunas.

**2.1 Criar dataset compartilhado** `base_unificada` (JOIN `runs + tasks + execucoes`) — query base reutilizável no dashboard, sem materialização física. A materialização é avaliada na Otimização 3 após medição.

**2.2 Migrar 23 datasets:**
- **Categoria A** (8 datasets, lê `observabilidade_jobs` → lê `observabilidade_runs`): Status Hero, Histórico Duração, Últimas Execuções, Duração Média, Taxa Sucesso, Filtros (Ambiente, Tipo Exec, Runs)
- **Categoria B** (4 datasets, lê `execucoes JOIN jobs` → lê `base_unificada`): Breakdown Tempo, Heatmap Throughput, KPIs Execução, Scatter
- **Categoria C** (11 datasets, ajustar FK): guardrails (5 — `status` vem de `tasks.result_state`), `controle_ingestao` (3 — intactos), filtros `execucoes` (2), Cobertura (1 — `status` de `tasks`)

**2.3 Eliminar datasets redundantes** (filtros leem de `base_unificada`): 23 → ~20

**2.4 Validar**: dashboard carrega sem erro, nenhum dataset referencia `observabilidade_jobs`, widgets renderizam

### Fase 3 — Snapshot + evolução do schema + DRY

**3.1 Snapshot** (antes da evolução):
```sql
CREATE TABLE observabilidade_execucoes_historico AS
SELECT * FROM observabilidade_execucoes WHERE run_id IS NULL;
```
Dev (192 registros) + Test (17 registros). 1 CTAS por ambiente.

**3.2 Evoluir schema** de `observabilidade_execucoes` (Dev + Test) — as 8 colunas de infra migram para `observabilidade_runs`/`tasks`:
`job_id`, `job_name`, `notebook_path`, `inicio_ts`, `fim_ts`, `duracao_segundos`, `status`, `trigger_type`

**3.3 Atualizar 7 notebooks** — INSERT sem as 8 colunas migradas:
`001_ddl_create_tables`, `004_verificacao_diaria_landing`, `101_cvm_dfp_dre`, `102_cvm_dfp_bpa`, `103_cvm_dfp_bpp`, `201_cvm_dfp_dre`, `202_cvm_dfp_bpa`, `203_cvm_dfp_bpp`

**3.4 Validar**: `execucoes` sem as 8 colunas, `id_execucao` FK válida, `run_id` JOIN com `tasks.job_run_id`, INSERT não falha

### Fase 4 — Cleanup

**4.1** Aposentar `observabilidade_jobs` (Dev + Test) — após confirmar que nenhum dataset a referencia
**4.2** Avaliar remoção de `base_parameters` (`JOB_ID`, `JOB_NAME`, `TRIGGER_TYPE`) dos YAMLs de job
**4.3** Atualizar `099_ddl_table_comments` — comentários UC para novas tabelas, atualizar referências de `observabilidade_jobs`

### Dependências

```
Fase 0 (jobs_metadata) → Fase 1 (transposição) → Fase 2 (dashboard) → Fase 3 (snapshot + evolução + DRY) → Fase 4 (cleanup)
```

- Fase 1 precisa de Fase 0 (MERGE enriquece via `jobs_metadata`)
- Fase 2 precisa de Fase 1 (dashboard aponta para `runs`/`tasks` populadas)
- Fase 3 precisa de Fase 2 (evolução do schema só depois que dashboard não referencia as colunas)
- Fase 4 precisa de Fase 3 (aposentar `observabilidade_jobs` só após dashboard não a referenciar)

### Restrições do design

- `observabilidade_execucoes` evolui no lugar via `ALTER TABLE`
- `observabilidade_guardrails` permanece intacta
- `controle_ingestao` permanece intacta
- `base_parameters` dos YAMLs só são removidos após confirmar que os notebooks não os usam para lógica
- Cada fase é validável independentemente

### Documentação impactada

A implementação do plano requer atualização dos seguintes documentos do projeto:

- `README.md` — status do pipeline (novas tabelas, `observabilidade_jobs` aposentada)
- `00_documentacao/tecnica/arquitetura.md` — seção de observabilidade (novas tabelas, transposição, schemas)
- `00_documentacao/evolucao_projeto.md` — registro da decisão arquitetural
