# Decisões Arquiteturais

> Registro de decisões arquiteturais do projeto CVM Dados Financeiros.  
> Cada entrada: contexto, decisão, consequências.

---

## DA-01: Jobs agendados do target test pausados por override no bundle

**Data:** 23/09/2026  

### Contexto

O target `test` existe para o teste de integração disparado sob demanda pelo
GitHub Actions (`workflow_dispatch`). O bundle deploy cria três jobs para esse
target: verificação diária, pipeline semanal e testes de integração. Os dois
primeiros têm schedules (diário 6h, semanal segundas 7h) herdados do YAML de
job, que declara `pause_status: UNPAUSED` em cada `schedule`.

O `pause_status` não pode ser declarado no `presets` do target porque o DAB
recusa `UNPAUSED` em preset quando `mode: development` — força `PAUSED`.
Por isso o `pause_status` ficou no nível do job, onde a restrição não se aplica.
O efeito colateral: como o YAML de job é compartilhado entre targets, o test
herdava `UNPAUSED` junto com o dev.

Consequência concreta identificada: segunda 28/09 às 7h, o semanal de test
processaria todos os 6 anos e popularia 6 tabelas completas nos schemas de
teste — um gasto de compute sem consumidor real.

### Decisão

Pausar os jobs agendados do target test via **override de resources** no
`databricks.yml`, mantendo o padrão `UNPAUSED` nos YAMLs de job.

```yaml
targets:
  test:
    mode: development
    resources:
      jobs:
        verificacao_diaria:
          schedule:
            pause_status: PAUSED
        pipeline_semanal:
          schedule:
            pause_status: PAUSED
```

O merge do DAB sobrescreve apenas `schedule.pause_status` para o target test,
mantendo `quartz_cron_expression` e `timezone_id` intactos. O job de testes
de integração (sem schedule) não é afetado.

### Consequências

* **dev:** Verificação diária e pipeline semanal continuam `UNPAUSED` (nada muda).
* **test:** Verificação diária e pipeline semanal ficam `PAUSED`. O job de
  testes de integração permanece disparado manualmente pelo GitHub Actions.
* O padrão `UNPAUSED` nos YAMLs de job (`resources/jobs/*.yml`) é preservado —
  o override é visível apenas no `databricks.yml`, junto à definição do target.
* A pausa é versionada no Git e aplicada via `bundle deploy --target test`.
* Para reativar temporariamente um job de test, basta remover o override ou
  disparar manualmente via `jobs run-now`.
