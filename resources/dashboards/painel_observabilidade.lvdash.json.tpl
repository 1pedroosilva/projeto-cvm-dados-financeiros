{
  "datasets": [
    {
      "name": "obs_guardrails",
      "displayName": "Observabilidade Guardrails",
      "queryLines": [
        "SELECT\n",
        "  id_check,\n",
        "  id_execucao,\n",
        "  etapa,\n",
        "  fonte,\n",
        "  ano,\n",
        "  nome_guardrail,\n",
        "  tipo_check,\n",
        "  resultado,\n",
        "  esperado,\n",
        "  encontrado,\n",
        "  registros_afetados,\n",
        "  detalhes,\n",
        "  ts_check\n",
        "FROM\n",
        "  workspace.{{SCHEMA_PREFIX}}_05_apoio.observabilidade_guardrails\n",
        "ORDER BY\n",
        "  ts_check DESC"
      ]
    },
    {
      "name": "obs_execucoes",
      "displayName": "Observabilidade Execuções",
      "queryLines": [
        "SELECT *\n",
        "FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.observabilidade_execucoes\n",
        "ORDER BY created_at DESC"
      ]
    },
    {
      "name": "obs_ingestao",
      "displayName": "Controle Ingestão",
      "queryLines": [
        "SELECT\n",
        "  fonte,\n",
        "  ano,\n",
        "  arquivo,\n",
        "  last_modified_cvm,\n",
        "  versao_ingestao,\n",
        "  ingest_ts,\n",
        "  status,\n",
        "  mensagem,\n",
        "  bytes_arquivo\n",
        "FROM\n",
        "  workspace.{{SCHEMA_PREFIX}}_05_apoio.controle_ingestao\n",
        "ORDER BY\n",
        "  ingest_ts DESC"
      ]
    },
    {
      "name": "orq_ultima_run",
      "displayName": "Última Run",
      "queryLines": [
        "SELECT\n",
        "  job_name AS Pipeline,\n",
        "  DATE_FORMAT(period_end_time, 'dd/MM/yyyy HH:mm') AS `Concluido em`,\n",
        "  CASE WHEN DATEDIFF(SECOND, period_start_time, period_end_time) >= 60\n",
        "    THEN CONCAT(FLOOR(DATEDIFF(SECOND, period_start_time, period_end_time) / 60), 'min ', ROUND(DATEDIFF(SECOND, period_start_time, period_end_time) % 60), 's')\n",
        "    ELSE CONCAT(ROUND(DATEDIFF(SECOND, period_start_time, period_end_time)), ' seg')\n",
        "  END AS `Tempo de Execucao`,\n",
        "  CASE\n",
        "    WHEN result_state = 'SUCCEEDED' THEN '✅ Sucesso'\n",
        "    WHEN result_state = 'FAILED' THEN '❌ Falhou'\n",
        "    WHEN result_state = 'CANCELLED' THEN '⚠️ Cancelado'\n",
        "    WHEN result_state = 'ERROR' THEN '❌ Erro'\n",
        "    ELSE result_state\n",
        "  END AS Resultado\n",
        "FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.observabilidade_runs\n",
        "ORDER BY COALESCE(period_end_time, ingested_at) DESC\n",
        "LIMIT 1"
      ],
      "catalog": "workspace",
      "schema": "{{SCHEMA_PREFIX}}_05_apoio"
    },
    {
      "name": "orq_media_5runs",
      "displayName": "Duração Média Pipeline (7 dias)",
      "queryLines": [
        "SELECT\n",
        "  ROUND(AVG(DATEDIFF(SECOND, period_start_time, period_end_time)) / 60.0, 1) AS duracao_minutos\n",
        "FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.observabilidade_runs\n",
        "WHERE COALESCE(period_end_time, ingested_at) >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS\n",
        "  AND period_start_time IS NOT NULL\n",
        "  AND period_end_time IS NOT NULL"
      ],
      "catalog": "workspace",
      "schema": "{{SCHEMA_PREFIX}}_05_apoio"
    },
    {
      "name": "orq_taxa_sucesso",
      "displayName": "Taxa de Sucesso (7 dias)",
      "queryLines": [
        "SELECT\n",
        "  ROUND(SUM(CASE WHEN result_state = 'SUCCEEDED' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS taxa_sucesso_pct\n",
        "FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.observabilidade_runs\n",
        "WHERE COALESCE(period_end_time, ingested_at) >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS"
      ],
      "catalog": "workspace",
      "schema": "{{SCHEMA_PREFIX}}_05_apoio"
    },
    {
      "name": "orq_task_breakdown",
      "displayName": "Breakdown Tempo por Etapa (7 dias)",
      "queryLines": [
        "SELECT\n",
        "  CASE WHEN task_key = 'check_landing_zone' THEN 'Verificacao Landing Zone'\n",
        "    WHEN task_key = 'bronze_bpa' THEN 'Bronze - BPA'\n",
        "    WHEN task_key = 'bronze_bpp' THEN 'Bronze - BPP'\n",
        "    WHEN task_key = 'bronze_dre' THEN 'Bronze - DRE'\n",
        "    WHEN task_key = 'silver_bpa' THEN 'Silver - BPA'\n",
        "    WHEN task_key = 'silver_bpp' THEN 'Silver - BPP'\n",
        "    WHEN task_key = 'silver_dre' THEN 'Silver - DRE'\n",
        "    WHEN task_key = 'verificacao_landing' THEN 'Verificacao Diaria'\n",
        "    ELSE task_key END AS Etapa,\n",
        "  ROUND(AVG(duracao_segundos) / 60.0, 1) AS duracao_minutos,\n",
        "  'Pipeline Completo' AS barra\n",
        "FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.base_unificada\n",
        "WHERE COALESCE(period_end_time, execucao_created_at) >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS\n",
        "  AND duracao_segundos IS NOT NULL AND task_key IS NOT NULL\n",
        "GROUP BY task_key\n",
        "ORDER BY duracao_minutos DESC"
      ],
      "catalog": "workspace",
      "schema": "{{SCHEMA_PREFIX}}_05_apoio"
    },
    {
      "name": "exe_total_registros",
      "displayName": "Total Registros Última Run",
      "queryLines": [
        "SELECT COALESCE(SUM(registros_processados), 0) AS total_registros\n",
        "FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.base_unificada\n",
        "WHERE run_id = (\n",
        "  SELECT run_id FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.observabilidade_runs\n",
        "  WHERE run_id IN (SELECT DISTINCT run_id FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.observabilidade_execucoes WHERE run_id IS NOT NULL)\n",
        "  ORDER BY COALESCE(period_end_time, ingested_at) DESC LIMIT 1)"
      ],
      "catalog": "workspace",
      "schema": "{{SCHEMA_PREFIX}}_05_apoio"
    },
    {
      "name": "exe_cobertura",
      "displayName": "Cobertura Pipeline",
      "queryLines": [
        "SELECT fonte,\n",
        "  MAX(CASE WHEN ano = 2021 THEN\n",
        "    CASE task_result_state WHEN 'SUCCEEDED' THEN '✅' WHEN 'FAILED' THEN '❌' WHEN 'ERROR' THEN '❌' WHEN 'CANCELLED' THEN '⚠️' ELSE '—' END\n",
        "  END) AS `2021`,   MAX(CASE WHEN ano = 2022 THEN\n",
        "    CASE task_result_state WHEN 'SUCCEEDED' THEN '✅' WHEN 'FAILED' THEN '❌' WHEN 'ERROR' THEN '❌' WHEN 'CANCELLED' THEN '⚠️' ELSE '—' END\n",
        "  END) AS `2022`,   MAX(CASE WHEN ano = 2023 THEN\n",
        "    CASE task_result_state WHEN 'SUCCEEDED' THEN '✅' WHEN 'FAILED' THEN '❌' WHEN 'ERROR' THEN '❌' WHEN 'CANCELLED' THEN '⚠️' ELSE '—' END\n",
        "  END) AS `2023`,   MAX(CASE WHEN ano = 2024 THEN\n",
        "    CASE task_result_state WHEN 'SUCCEEDED' THEN '✅' WHEN 'FAILED' THEN '❌' WHEN 'ERROR' THEN '❌' WHEN 'CANCELLED' THEN '⚠️' ELSE '—' END\n",
        "  END) AS `2024`,   MAX(CASE WHEN ano = 2025 THEN\n",
        "    CASE task_result_state WHEN 'SUCCEEDED' THEN '✅' WHEN 'FAILED' THEN '❌' WHEN 'ERROR' THEN '❌' WHEN 'CANCELLED' THEN '⚠️' ELSE '—' END\n",
        "  END) AS `2025`,   MAX(CASE WHEN ano = 2026 THEN\n",
        "    CASE task_result_state WHEN 'SUCCEEDED' THEN '✅' WHEN 'FAILED' THEN '❌' WHEN 'ERROR' THEN '❌' WHEN 'CANCELLED' THEN '⚠️' ELSE '—' END\n",
        "  END) AS `2026`\n",
        "FROM (SELECT fonte, ano, task_result_state,\n",
        "    ROW_NUMBER() OVER (PARTITION BY fonte, ano ORDER BY period_end_time DESC) AS rn\n",
        "  FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.base_unificada WHERE fonte IS NOT NULL AND ano IS NOT NULL) sub\n",
        "WHERE rn = 1 GROUP BY fonte ORDER BY fonte"
      ],
      "catalog": "workspace",
      "schema": "{{SCHEMA_PREFIX}}_05_apoio"
    },
    {
      "name": "qual_fails_24h",
      "displayName": "Fails 24h",
      "queryLines": [
        "SELECT\n",
        "  COUNT(*) AS total_fails\n",
        "FROM\n",
        "  observabilidade_guardrails\n",
        "WHERE\n",
        "  resultado = 'FAIL'\n",
        "  AND ts_check >= (CURRENT_TIMESTAMP() - INTERVAL 24 HOURS)"
      ],
      "catalog": "workspace",
      "schema": "{{SCHEMA_PREFIX}}_05_apoio"
    },
    {
      "name": "qual_checks_ultima_run",
      "displayName": "Checks Última Run",
      "queryLines": [
        "SELECT COUNT(*) AS total_checks\n",
        "FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.observabilidade_guardrails g\n",
        "WHERE g.id_execucao IN (\n",
        "  SELECT e.id_execucao FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.observabilidade_execucoes e\n",
        "  WHERE e.run_id = (\n",
        "    SELECT r.run_id FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.observabilidade_runs r\n",
        "    WHERE r.run_id IN (SELECT DISTINCT run_id FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.observabilidade_execucoes WHERE run_id IS NOT NULL)\n",
        "    ORDER BY COALESCE(r.period_end_time, r.ingested_at) DESC LIMIT 1))"
      ],
      "catalog": "workspace",
      "schema": "{{SCHEMA_PREFIX}}_05_apoio"
    },
    {
      "name": "qual_pass_rate",
      "displayName": "Taxa PASS",
      "queryLines": [
        "SELECT ROUND(SUM(CASE WHEN resultado = 'PASS' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS taxa_pass_pct\n",
        "FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.observabilidade_guardrails g\n",
        "WHERE g.id_execucao IN (\n",
        "  SELECT e.id_execucao FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.observabilidade_execucoes e\n",
        "  WHERE e.run_id = (\n",
        "    SELECT r.run_id FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.observabilidade_runs r\n",
        "    WHERE r.run_id IN (SELECT DISTINCT run_id FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.observabilidade_execucoes WHERE run_id IS NOT NULL)\n",
        "    ORDER BY COALESCE(r.period_end_time, r.ingested_at) DESC LIMIT 1))"
      ],
      "catalog": "workspace",
      "schema": "{{SCHEMA_PREFIX}}_05_apoio"
    },
    {
      "name": "qual_tendencia",
      "displayName": "Tendência Guardrails",
      "queryLines": [
        "SELECT\n",
        "  e.run_id,\n",
        "  g.resultado,\n",
        "  COUNT(*) AS contagem\n",
        "FROM\n",
        "  observabilidade_guardrails g\n",
        "    JOIN observabilidade_execucoes e\n",
        "      ON g.id_execucao = e.id_execucao\n",
        "WHERE\n",
        "  e.run_id IS NOT NULL\n",
        "GROUP BY\n",
        "  e.run_id,\n",
        "  g.resultado\n",
        "ORDER BY\n",
        "  e.run_id"
      ],
      "catalog": "workspace",
      "schema": "{{SCHEMA_PREFIX}}_05_apoio"
    },
    {
      "name": "ing_data_recente",
      "displayName": "Ingestão Mais Recente",
      "queryLines": [
        "SELECT\n",
        "  MAX(ingest_ts) AS data_ingestao_recente\n",
        "FROM\n",
        "  controle_ingestao\n",
        "WHERE\n",
        "  status = 'SUCCESS'"
      ],
      "catalog": "workspace",
      "schema": "{{SCHEMA_PREFIX}}_05_apoio"
    },
    {
      "name": "ing_freshness",
      "displayName": "Freshness por Fonte/Ano",
      "queryLines": [
        "SELECT\n",
        "  fonte,\n",
        "  ano,\n",
        "  last_modified_cvm,\n",
        "  ingest_ts,\n",
        "  versao_ingestao,\n",
        "  status,\n",
        "  DATEDIFF(CURRENT_DATE(), DATE(ingest_ts)) AS dias_desde_ingestao\n",
        "FROM\n",
        "  (\n",
        "    SELECT\n",
        "      fonte,\n",
        "      ano,\n",
        "      last_modified_cvm,\n",
        "      ingest_ts,\n",
        "      versao_ingestao,\n",
        "      status,\n",
        "      ROW_NUMBER() OVER (PARTITION BY fonte, ano ORDER BY ingest_ts DESC) AS rn\n",
        "    FROM\n",
        "      controle_ingestao\n",
        "  )\n",
        "WHERE\n",
        "  rn = 1\n",
        "ORDER BY\n",
        "  dias_desde_ingestao DESC"
      ],
      "catalog": "workspace",
      "schema": "{{SCHEMA_PREFIX}}_05_apoio"
    },
    {
      "name": "ing_bytes_ano",
      "displayName": "Bytes por Ano",
      "queryLines": [
        "SELECT\n",
        "  ano,\n",
        "  SUM(bytes_arquivo) AS total_bytes\n",
        "FROM\n",
        "  (\n",
        "    SELECT\n",
        "      fonte,\n",
        "      ano,\n",
        "      bytes_arquivo,\n",
        "      ROW_NUMBER() OVER (PARTITION BY fonte, ano ORDER BY ingest_ts DESC) AS rn\n",
        "    FROM\n",
        "      controle_ingestao\n",
        "    WHERE\n",
        "      bytes_arquivo IS NOT NULL\n",
        "  ) sub\n",
        "WHERE\n",
        "  rn = 1\n",
        "GROUP BY\n",
        "  ano\n",
        "ORDER BY\n",
        "  ano"
      ],
      "catalog": "workspace",
      "schema": "{{SCHEMA_PREFIX}}_05_apoio"
    },
    {
      "name": "orq_status_hero",
      "displayName": "Status Hero Pipeline",
      "queryLines": [
        "SELECT\n",
        "  CASE\n",
        "    WHEN result_state = 'SUCCEEDED' THEN '✓ SAUDAVEL'\n",
        "    WHEN result_state IS NULL THEN '⏸ AGUARDANDO'\n",
        "    ELSE '⚠ ATENCAO NECESSARIA'\n",
        "  END AS status_visual,\n",
        "  CONCAT('Ultima execucao: ', DATE_FORMAT(period_end_time, 'dd/MM as HH:mm'), ' - Duracao: ',\n",
        "    CASE WHEN DATEDIFF(SECOND, period_start_time, period_end_time) >= 60\n",
        "      THEN CONCAT(FLOOR(DATEDIFF(SECOND, period_start_time, period_end_time) / 60), 'min ', ROUND(DATEDIFF(SECOND, period_start_time, period_end_time) % 60), 's')\n",
        "      ELSE CONCAT(ROUND(DATEDIFF(SECOND, period_start_time, period_end_time)), ' segundos')\n",
        "    END) AS detalhes,\n",
        "  result_state AS status_raw\n",
        "FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.observabilidade_runs\n",
        "WHERE COALESCE(period_end_time, ingested_at) >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS\n",
        "ORDER BY COALESCE(period_end_time, ingested_at) DESC\n",
        "LIMIT 1"
      ]
    },
    {
      "name": "orq_duracao_formatada",
      "displayName": "Histórico Duração Diária (7 dias)",
      "queryLines": [
        "SELECT\n",
        "  DATE_FORMAT(period_end_time, 'dd/MM HH:mm') AS Data,\n",
        "  ROUND(DATEDIFF(SECOND, period_start_time, period_end_time) / 60.0, 1) AS duracao_minutos,\n",
        "  run_id\n",
        "FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.observabilidade_runs\n",
        "WHERE period_end_time >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS\n",
        "  AND period_start_time IS NOT NULL\n",
        "  AND period_end_time IS NOT NULL\n",
        "ORDER BY period_end_time ASC"
      ]
    },
    {
      "name": "orq_execucoes_48h",
      "displayName": "Últimas Execuções (7 dias)",
      "queryLines": [
        "SELECT\n",
        "  job_name AS Pipeline,\n",
        "  DATE_FORMAT(period_end_time, 'dd/MM/yyyy HH:mm') AS `Concluido em`,\n",
        "  CASE WHEN DATEDIFF(SECOND, period_start_time, period_end_time) >= 60\n",
        "    THEN CONCAT(FLOOR(DATEDIFF(SECOND, period_start_time, period_end_time) / 60), 'min ', ROUND(DATEDIFF(SECOND, period_start_time, period_end_time) % 60), 's')\n",
        "    ELSE CONCAT(ROUND(DATEDIFF(SECOND, period_start_time, period_end_time)), ' seg')\n",
        "  END AS `Tempo de Execucao`,\n",
        "  INITCAP(ambiente) AS Ambiente,\n",
        "  CASE WHEN LOWER(trigger_type) = 'cron' THEN 'Automatico' WHEN LOWER(trigger_type) = 'onetime' THEN 'Manual' ELSE trigger_type END AS `Tipo de Execucao`,\n",
        "  CASE WHEN result_state = 'SUCCEEDED' THEN 'Sucesso' WHEN result_state = 'FAILED' THEN 'Falhou' WHEN result_state = 'CANCELLED' THEN 'Cancelado' WHEN result_state = 'ERROR' THEN 'Erro' ELSE result_state END AS Resultado\n",
        "FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.observabilidade_runs\n",
        "WHERE COALESCE(period_end_time, ingested_at) >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS\n",
        "ORDER BY COALESCE(period_end_time, ingested_at) DESC"
      ]
    },
    {
      "name": "filtro_ambiente",
      "displayName": "Filtro: Ambiente",
      "queryLines": [
        "SELECT DISTINCT INITCAP(ambiente) AS Ambiente\n",
        "FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.observabilidade_runs\n",
        "WHERE COALESCE(period_end_time, ingested_at) >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS AND ambiente IS NOT NULL\n",
        "ORDER BY Ambiente"
      ]
    },
    {
      "name": "filtro_tipo_exec",
      "displayName": "Filtro: Tipo de Execução",
      "queryLines": [
        "SELECT DISTINCT\n",
        "  CASE WHEN LOWER(trigger_type) = 'cron' THEN 'Automatico' WHEN LOWER(trigger_type) = 'onetime' THEN 'Manual' ELSE trigger_type END AS `Tipo de Execucao`\n",
        "FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.observabilidade_runs\n",
        "WHERE COALESCE(period_end_time, ingested_at) >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS\n",
        "ORDER BY `Tipo de Execucao`"
      ]
    },
    {
      "name": "exe_heatmap_throughput",
      "displayName": "Heatmap Throughput (fonte × etapa)",
      "queryLines": [
        "WITH ultima_run AS (\n",
        "  SELECT run_id, MAX(period_end_time) AS max_fim_ts\n",
        "  FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.base_unificada\n",
        "  WHERE run_id IS NOT NULL AND registros_processados IS NOT NULL AND registros_processados > 0\n",
        "    AND duracao_segundos IS NOT NULL AND duracao_segundos > 0 AND period_end_time IS NOT NULL\n",
        "  GROUP BY run_id ORDER BY max_fim_ts DESC LIMIT 1)\n",
        "SELECT fonte, etapa,\n",
        "  ROUND(SUM(registros_processados) / NULLIF(SUM(duracao_segundos) / 60.0, 0), 1) AS throughput_reg_min,\n",
        "  SUM(registros_processados) AS total_registros,\n",
        "  ROUND(SUM(duracao_segundos) / 60.0, 1) AS total_duracao_min\n",
        "FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.base_unificada\n",
        "WHERE run_id = (SELECT run_id FROM ultima_run)\n",
        "  AND fonte IS NOT NULL AND etapa IN ('bronze', 'silver')\n",
        "  AND registros_processados IS NOT NULL AND duracao_segundos IS NOT NULL\n",
        "GROUP BY fonte, etapa ORDER BY fonte, etapa"
      ]
    },
    {
      "name": "exe_cobertura_heatmap",
      "displayName": "Cobertura Pipeline (heatmap compacto)",
      "queryLines": [
        "SELECT fonte, ano,\n",
        "  CASE WHEN task_result_state = 'SUCCEEDED' THEN '✅' WHEN task_result_state = 'FAILED' THEN '❌'\n",
        "    WHEN task_result_state = 'ERROR' THEN '❌' WHEN task_result_state = 'CANCELLED' THEN '⚠️' ELSE '❓' END AS status_emoji,\n",
        "  task_result_state AS status_raw\n",
        "FROM (SELECT fonte, ano, task_result_state,\n",
        "    ROW_NUMBER() OVER (PARTITION BY fonte, ano ORDER BY period_end_time DESC) AS rn\n",
        "  FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.base_unificada WHERE fonte IS NOT NULL AND ano IS NOT NULL) sub\n",
        "WHERE rn = 1 ORDER BY fonte, ano"
      ]
    },
    {
      "name": "exe_kpis_contexto",
      "displayName": "KPIs Execução com Contexto",
      "queryLines": [
        "WITH ultima_run AS (\n",
        "  SELECT run_id, MAX(period_end_time) AS max_fim_ts\n",
        "  FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.base_unificada\n",
        "  WHERE run_id IS NOT NULL AND registros_processados IS NOT NULL AND registros_processados > 0\n",
        "    AND duracao_segundos IS NOT NULL AND duracao_segundos > 0 AND period_end_time IS NOT NULL\n",
        "  GROUP BY run_id ORDER BY max_fim_ts DESC LIMIT 1),\n",
        "kpis_ultima AS (\n",
        "  SELECT SUM(registros_processados) AS volume_total,\n",
        "    ROUND(SUM(duracao_segundos) / 60.0, 1) AS duracao_total_min,\n",
        "    ROUND(SUM(registros_processados) / NULLIF(SUM(duracao_segundos) / 60.0, 0), 0) AS throughput_reg_min,\n",
        "    COUNT(DISTINCT id_execucao) AS total_execucoes,\n",
        "    COUNT(DISTINCT CASE WHEN registros_processados > 0 THEN id_execucao END) AS execucoes_completas\n",
        "  FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.base_unificada\n",
        "  WHERE run_id = (SELECT run_id FROM ultima_run) AND registros_processados IS NOT NULL AND duracao_segundos IS NOT NULL),\n",
        "kpis_48h AS (\n",
        "  SELECT AVG(volume_total) AS media_volume_48h,\n",
        "    PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY duracao_total_min) AS p10_duracao,\n",
        "    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duracao_total_min) AS p50_duracao,\n",
        "    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY duracao_total_min) AS p90_duracao,\n",
        "    AVG(throughput_reg_min) AS media_throughput_48h\n",
        "  FROM (SELECT run_id, SUM(registros_processados) AS volume_total,\n",
        "    ROUND(SUM(duracao_segundos) / 60.0, 1) AS duracao_total_min,\n",
        "    ROUND(SUM(registros_processados) / NULLIF(SUM(duracao_segundos) / 60.0, 0), 0) AS throughput_reg_min\n",
        "    FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.base_unificada\n",
        "    WHERE period_end_time >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS\n",
        "      AND registros_processados IS NOT NULL AND registros_processados > 0\n",
        "      AND duracao_segundos IS NOT NULL AND duracao_segundos > 0\n",
        "    GROUP BY run_id) sub)\n",
        "SELECT k.volume_total, k.duracao_total_min, k.throughput_reg_min, k.total_execucoes, k.execucoes_completas,\n",
        "  ROUND(((k.volume_total - m.media_volume_48h) / NULLIF(m.media_volume_48h, 0)) * 100.0, 0) AS variacao_volume_pct,\n",
        "  ROUND(m.media_volume_48h, 0) AS baseline_volume,\n",
        "  ROUND(((k.throughput_reg_min - m.media_throughput_48h) / NULLIF(m.media_throughput_48h, 0)) * 100.0, 0) AS variacao_throughput_pct,\n",
        "  ROUND(m.media_throughput_48h, 0) AS baseline_throughput, 100000 AS alvo_throughput,\n",
        "  ROUND(m.p10_duracao, 1) AS p10_duracao, ROUND(m.p50_duracao, 1) AS p50_duracao, ROUND(m.p90_duracao, 1) AS p90_duracao\n",
        "FROM kpis_ultima k CROSS JOIN kpis_48h m"
      ]
    },
    {
      "name": "exe_insights",
      "displayName": "Insights e Alertas Automáticos",
      "queryLines": [
        "WITH ultima_run AS (\n",
        "  SELECT run_id, MAX(period_end_time) AS max_fim_ts\n",
        "  FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.base_unificada\n",
        "  WHERE run_id IS NOT NULL AND registros_processados IS NOT NULL AND registros_processados > 0\n",
        "    AND duracao_segundos IS NOT NULL AND duracao_segundos > 0 AND period_end_time IS NOT NULL\n",
        "  GROUP BY run_id ORDER BY max_fim_ts DESC LIMIT 1),\n",
        "metricas_ultima AS (\n",
        "  SELECT fonte, etapa,\n",
        "    ROUND(SUM(registros_processados) / NULLIF(SUM(duracao_segundos) / 60.0, 0), 0) AS throughput_atual\n",
        "  FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.base_unificada\n",
        "  WHERE run_id = (SELECT run_id FROM ultima_run) AND fonte IS NOT NULL AND etapa IN ('bronze', 'silver')\n",
        "    AND registros_processados IS NOT NULL AND duracao_segundos IS NOT NULL\n",
        "  GROUP BY fonte, etapa),\n",
        "baselines AS (\n",
        "  SELECT fonte, etapa, ROUND(AVG(throughput_run), 0) AS throughput_baseline\n",
        "  FROM (SELECT fonte, etapa, run_id,\n",
        "    ROUND(SUM(registros_processados) / NULLIF(SUM(duracao_segundos) / 60.0, 0), 0) AS throughput_run\n",
        "    FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.base_unificada\n",
        "    WHERE period_end_time >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS AND fonte IS NOT NULL AND etapa IN ('bronze', 'silver')\n",
        "      AND registros_processados IS NOT NULL AND registros_processados > 0\n",
        "      AND duracao_segundos IS NOT NULL AND duracao_segundos > 0\n",
        "    GROUP BY fonte, etapa, run_id) sub\n",
        "  GROUP BY fonte, etapa),\n",
        "analise AS (\n",
        "  SELECT m.fonte, m.etapa, m.throughput_atual, b.throughput_baseline,\n",
        "    ROUND(((m.throughput_atual - b.throughput_baseline) / NULLIF(b.throughput_baseline, 0)) * 100.0, 0) AS variacao_pct,\n",
        "    CASE WHEN m.throughput_atual < b.throughput_baseline * 0.7 THEN 'CRITICO'\n",
        "      WHEN m.throughput_atual < b.throughput_baseline * 0.85 THEN 'ATENCAO' ELSE 'OK' END AS status\n",
        "  FROM metricas_ultima m INNER JOIN baselines b ON m.fonte = b.fonte AND m.etapa = b.etapa)\n",
        "SELECT CONCAT(fonte, ' / ', etapa) AS fonte_etapa, throughput_atual, throughput_baseline, variacao_pct, status,\n",
        "  CASE WHEN status = 'CRITICO' THEN CONCAT('CRITICO: ', fonte, ' ', etapa, ' com throughput ', ABS(variacao_pct), '% abaixo da baseline')\n",
        "    WHEN status = 'ATENCAO' THEN CONCAT('ATENCAO: ', fonte, ' ', etapa, ' com throughput ', ABS(variacao_pct), '% abaixo da baseline')\n",
        "    ELSE CONCAT(fonte, ' ', etapa, ' dentro do esperado (', variacao_pct, '% vs baseline)') END AS mensagem\n",
        "FROM analise\n",
        "ORDER BY CASE status WHEN 'CRITICO' THEN 1 WHEN 'ATENCAO' THEN 2 ELSE 3 END, variacao_pct"
      ]
    },
    {
      "name": "filtro_runs",
      "displayName": "Filtro: Últimas Runs",
      "queryLines": [
        "SELECT DISTINCT run_id, DATE_FORMAT(period_end_time, 'dd/MM HH:mm') AS run_timestamp,\n",
        "  CONCAT(run_id, ' - ', DATE_FORMAT(period_end_time, 'dd/MM HH:mm')) AS run_label\n",
        "FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.observabilidade_runs\n",
        "WHERE period_end_time IS NOT NULL\n",
        "ORDER BY run_id DESC\n",
        "LIMIT 20"
      ]
    },
    {
      "name": "filtro_fonte",
      "displayName": "Filtro: Fonte",
      "queryLines": [
        "SELECT DISTINCT\n",
        "  fonte\n",
        "FROM\n",
        "  workspace.{{SCHEMA_PREFIX}}_05_apoio.observabilidade_execucoes\n",
        "WHERE\n",
        "  fonte IS NOT NULL\n",
        "ORDER BY\n",
        "  fonte"
      ]
    },
    {
      "name": "filtro_etapa",
      "displayName": "Filtro: Etapa",
      "queryLines": [
        "SELECT DISTINCT\n",
        "  etapa\n",
        "FROM\n",
        "  workspace.{{SCHEMA_PREFIX}}_05_apoio.observabilidade_execucoes\n",
        "WHERE\n",
        "  etapa IS NOT NULL\n",
        "ORDER BY\n",
        "  etapa"
      ]
    },
    {
      "name": "exe_scatter_melhorado",
      "displayName": "Scatter Duração×Registros",
      "queryLines": [
        "SELECT\n",
        "  id_execucao,\n",
        "  etapa,\n",
        "  registros_processados,\n",
        "  duracao_segundos,\n",
        "  ROUND(duracao_segundos / 60.0, 2) AS duracao_minutos,\n",
        "  ROUND(registros_processados / NULLIF(duracao_segundos / 60.0, 0), 1) AS throughput_reg_min,\n",
        "  run_id,\n",
        "  fonte,\n",
        "  ano\n",
        "FROM\n",
        "  workspace.{{SCHEMA_PREFIX}}_05_apoio.observabilidade_execucoes\n",
        "WHERE\n",
        "  registros_processados IS NOT NULL\n",
        "  AND duracao_segundos IS NOT NULL\n",
        "  AND etapa IN ('bronze', 'silver', 'verificacao')\n",
        "ORDER BY\n",
        "  run_id DESC,\n",
        "  etapa"
      ],
      "catalog": "workspace",
      "schema": "{{SCHEMA_PREFIX}}_05_apoio"
    },
    {
      "name": "exe_kpis_periodo",
      "displayName": "KPIs Execução por Período (48h)",
      "queryLines": [
        "WITH run_metrics AS (\n",
        "  SELECT MAX(period_end_time) AS run_period, SUM(registros_processados) AS volume_total,\n",
        "    ROUND(SUM(duracao_segundos) / 60.0, 1) AS duracao_total_min,\n",
        "    ROUND(SUM(registros_processados) / NULLIF(SUM(duracao_segundos) / 60.0, 0), 0) AS throughput_reg_min,\n",
        "    COUNT(DISTINCT id_execucao) AS total_execucoes,\n",
        "    COUNT(DISTINCT CASE WHEN registros_processados > 0 THEN id_execucao END) AS execucoes_completas\n",
        "  FROM workspace.{{SCHEMA_PREFIX}}_05_apoio.base_unificada\n",
        "  WHERE period_end_time >= CURRENT_TIMESTAMP() - INTERVAL 48 HOURS AND run_id IS NOT NULL\n",
        "    AND registros_processados IS NOT NULL AND registros_processados > 0\n",
        "    AND duracao_segundos IS NOT NULL AND duracao_segundos > 0 AND period_end_time IS NOT NULL\n",
        "  GROUP BY run_id),\n",
        "baselines AS (\n",
        "  SELECT ROUND(AVG(volume_total), 0) AS baseline_volume, ROUND(AVG(throughput_reg_min), 0) AS baseline_throughput,\n",
        "    ROUND(AVG(duracao_total_min), 1) AS baseline_duracao, ROUND(AVG(execucoes_completas), 0) AS baseline_completude\n",
        "  FROM run_metrics)\n",
        "SELECT r.run_period, r.volume_total, r.duracao_total_min, r.throughput_reg_min, r.total_execucoes, r.execucoes_completas,\n",
        "  b.baseline_volume, b.baseline_throughput, b.baseline_duracao, b.baseline_completude\n",
        "FROM run_metrics r CROSS JOIN baselines b ORDER BY r.run_period ASC"
      ]
    }
  ],
  "pages": [
    {
      "name": "c625375b",
      "displayName": "Orquestração",
      "layout": [
        {
          "widget": {
            "name": "w1_ultima_run",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "orq_execucoes_48h",
                  "fields": [
                    {
                      "name": "Pipeline",
                      "expression": "`Pipeline`"
                    },
                    {
                      "name": "Concluído em",
                      "expression": "`Concluído em`"
                    },
                    {
                      "name": "Tempo de Execução",
                      "expression": "`Tempo de Execução`"
                    },
                    {
                      "name": "Ambiente",
                      "expression": "`Ambiente`"
                    },
                    {
                      "name": "Tipo de Execução",
                      "expression": "`Tipo de Execução`"
                    },
                    {
                      "name": "Resultado",
                      "expression": "`Resultado`"
                    }
                  ],
                  "disaggregated": true
                }
              }
            ],
            "spec": {
              "frame": {
                "showTitle": true,
                "title": {
                  "value": "Histórico de execuções",
                  "fields": []
                }
              },
              "version": 2,
              "widgetType": "table",
              "encodings": {
                "columns": [
                  {
                    "fieldName": "Pipeline"
                  },
                  {
                    "fieldName": "Concluído em"
                  },
                  {
                    "fieldName": "Tempo de Execução"
                  },
                  {
                    "fieldName": "Ambiente"
                  },
                  {
                    "fieldName": "Tipo de Execução"
                  },
                  {
                    "fieldName": "Resultado"
                  }
                ]
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 0,
            "y": 11,
            "width": 12,
            "height": 8
          }
        },
        {
          "widget": {
            "name": "w2_media_5runs",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "orq_media_5runs",
                  "fields": [
                    {
                      "name": "duracao_minutos",
                      "expression": "`duracao_minutos`"
                    }
                  ],
                  "disaggregated": true
                }
              }
            ],
            "spec": {
              "frame": {
                "showTitle": true,
                "showDescription": false,
                "title": {
                  "value": "Duração média por execução",
                  "fields": []
                },
                "description": {
                  "value": "Minutos por execução",
                  "fields": []
                }
              },
              "version": 2,
              "widgetType": "counter",
              "encodings": {
                "value": {
                  "fieldName": "duracao_minutos",
                  "format": {
                    "type": "number-plain",
                    "abbreviation": "none",
                    "decimalPlaces": {
                      "type": "exact",
                      "places": 0
                    }
                  },
                  "formatTemplate": "{{ @formatted }} minutos"
                }
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 6,
            "y": 2,
            "width": 3,
            "height": 2
          }
        },
        {
          "widget": {
            "name": "w3_taxa_sucesso",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "orq_taxa_sucesso",
                  "fields": [
                    {
                      "name": "taxa_sucesso_pct",
                      "expression": "`taxa_sucesso_pct`"
                    }
                  ],
                  "disaggregated": true
                }
              }
            ],
            "spec": {
              "frame": {
                "showTitle": true,
                "showDescription": false,
                "title": {
                  "value": "Taxa de sucesso das execuções",
                  "fields": []
                },
                "description": {
                  "value": "Percentual de execuções bem-sucedidas",
                  "fields": []
                }
              },
              "version": 2,
              "widgetType": "counter",
              "encodings": {
                "value": {
                  "fieldName": "taxa_sucesso_pct",
                  "format": {
                    "type": "number-plain",
                    "abbreviation": "none",
                    "decimalPlaces": {
                      "type": "exact",
                      "places": 0
                    }
                  },
                  "formatTemplate": "{{ @formatted }}%"
                }
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 9,
            "y": 2,
            "width": 3,
            "height": 2
          }
        },
        {
          "widget": {
            "name": "w5_task_breakdown",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "orq_task_breakdown",
                  "fields": [
                    {
                      "name": "duracao_minutos",
                      "expression": "`duracao_minutos`"
                    },
                    {
                      "name": "Etapa",
                      "expression": "`Etapa`"
                    }
                  ],
                  "disaggregated": true
                }
              }
            ],
            "spec": {
              "frame": {
                "showTitle": true,
                "title": {
                  "value": "Tempo médio por etapa",
                  "fields": []
                }
              },
              "version": 3,
              "widgetType": "bar",
              "encodings": {
                "x": {
                  "fieldName": "duracao_minutos",
                  "displayName": "",
                  "scale": {
                    "type": "quantitative"
                  }
                },
                "y": {
                  "fieldName": "Etapa",
                  "displayName": "",
                  "scale": {
                    "type": "categorical"
                  }
                }
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 8,
            "y": 4,
            "width": 4,
            "height": 7
          }
        },
        {
          "widget": {
            "name": "w4_duracao_tempo",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "orq_duracao_formatada",
                  "fields": [
                    {
                      "name": "Data",
                      "expression": "`Data`"
                    },
                    {
                      "name": "duracao_minutos",
                      "expression": "`duracao_minutos`"
                    }
                  ],
                  "disaggregated": true
                }
              }
            ],
            "spec": {
              "frame": {
                "showTitle": true,
                "title": {
                  "value": "Histórico de duração",
                  "fields": []
                }
              },
              "version": 3,
              "widgetType": "line",
              "encodings": {
                "x": {
                  "fieldName": "Data",
                  "displayName": "",
                  "scale": {
                    "type": "categorical"
                  }
                },
                "y": {
                  "fieldName": "duracao_minutos",
                  "displayName": "",
                  "scale": {
                    "type": "quantitative"
                  }
                }
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 0,
            "y": 4,
            "width": 8,
            "height": 7
          }
        },
        {
          "widget": {
            "name": "hero_status",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "orq_status_hero",
                  "fields": [
                    {
                      "name": "status_visual",
                      "expression": "`status_visual`"
                    }
                  ],
                  "disaggregated": true
                }
              }
            ],
            "spec": {
              "frame": {
                "showTitle": true,
                "title": {
                  "value": "Status do pipeline (últimos 7 dias)",
                  "fields": []
                }
              },
              "version": 2,
              "widgetType": "counter",
              "encodings": {
                "value": {
                  "fieldName": "status_visual"
                }
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 0,
            "y": 0,
            "width": 3,
            "height": 2
          }
        },
        {
          "widget": {
            "name": "filtro_ambiente",
            "queries": [
              {
                "name": "dashboards/01f1badbcdcb11c9b1dee1e37ada16e6/datasets/01f1bc1498c4145db9adc1a9421f44e8_Ambiente",
                "query": {
                  "datasetName": "filtro_ambiente",
                  "fields": [
                    {
                      "name": "Ambiente",
                      "expression": "`Ambiente`"
                    },
                    {
                      "name": "Ambiente_associativity",
                      "expression": "COUNT_IF(`associative_filter_predicate_group`)"
                    }
                  ],
                  "disaggregated": false
                }
              }
            ],
            "spec": {
              "version": 2,
              "frame": {
                "showTitle": true,
                "title": "Ambiente"
              },
              "widgetType": "filter-multi-select",
              "encodings": {
                "fields": [
                  {
                    "fieldName": "Ambiente",
                    "queryName": "dashboards/01f1badbcdcb11c9b1dee1e37ada16e6/datasets/01f1bc1498c4145db9adc1a9421f44e8_Ambiente"
                  }
                ]
              }
            }
          },
          "position": {
            "x": 0,
            "y": 2,
            "width": 3,
            "height": 2
          }
        },
        {
          "widget": {
            "name": "filtro_tipo_exec",
            "queries": [
              {
                "name": "dashboards/01f1badbcdcb11c9b1dee1e37ada16e6/datasets/01f1bc1499091a38990d999172033ecf_Tipo de Execução",
                "query": {
                  "datasetName": "filtro_tipo_exec",
                  "fields": [
                    {
                      "name": "Tipo de Execução",
                      "expression": "`Tipo de Execução`"
                    },
                    {
                      "name": "Tipo de Execução_associativity",
                      "expression": "COUNT_IF(`associative_filter_predicate_group`)"
                    }
                  ],
                  "disaggregated": false
                }
              }
            ],
            "spec": {
              "version": 2,
              "frame": {
                "showTitle": true,
                "title": "Tipo de execução"
              },
              "widgetType": "filter-multi-select",
              "encodings": {
                "fields": [
                  {
                    "fieldName": "Tipo de Execução",
                    "queryName": "dashboards/01f1badbcdcb11c9b1dee1e37ada16e6/datasets/01f1bc1499091a38990d999172033ecf_Tipo de Execução"
                  }
                ]
              }
            }
          },
          "position": {
            "x": 3,
            "y": 2,
            "width": 3,
            "height": 2
          }
        }
      ],
      "pageType": "PAGE_TYPE_CANVAS",
      "layoutVersion": "GRID_V1"
    },
    {
      "name": "execucao",
      "displayName": "Execução",
      "layout": [
        {
          "widget": {
            "name": "w9_cobertura",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "exe_cobertura",
                  "fields": [
                    {
                      "name": "fonte",
                      "expression": "`fonte`"
                    },
                    {
                      "name": "2021",
                      "expression": "`2021`"
                    },
                    {
                      "name": "2022",
                      "expression": "`2022`"
                    },
                    {
                      "name": "2023",
                      "expression": "`2023`"
                    },
                    {
                      "name": "2024",
                      "expression": "`2024`"
                    },
                    {
                      "name": "2025",
                      "expression": "`2025`"
                    },
                    {
                      "name": "2026",
                      "expression": "`2026`"
                    }
                  ],
                  "disaggregated": true
                }
              }
            ],
            "spec": {
              "frame": {
                "showTitle": true,
                "title": {
                  "value": "Cobertura de fontes por ano",
                  "fields": []
                }
              },
              "version": 2,
              "widgetType": "table",
              "encodings": {
                "columns": [
                  {
                    "fieldName": "fonte"
                  },
                  {
                    "fieldName": "2021",
                    "format": {
                      "type": "number-plain",
                      "abbreviation": "none",
                      "decimalPlaces": {
                        "type": "exact",
                        "places": 0
                      }
                    }
                  },
                  {
                    "fieldName": "2022",
                    "format": {
                      "type": "number-plain",
                      "abbreviation": "none",
                      "decimalPlaces": {
                        "type": "exact",
                        "places": 0
                      }
                    }
                  },
                  {
                    "fieldName": "2023",
                    "format": {
                      "type": "number-plain",
                      "abbreviation": "none",
                      "decimalPlaces": {
                        "type": "exact",
                        "places": 0
                      }
                    }
                  },
                  {
                    "fieldName": "2024",
                    "format": {
                      "type": "number-plain",
                      "abbreviation": "none",
                      "decimalPlaces": {
                        "type": "exact",
                        "places": 0
                      }
                    }
                  },
                  {
                    "fieldName": "2025",
                    "format": {
                      "type": "number-plain",
                      "abbreviation": "none",
                      "decimalPlaces": {
                        "type": "exact",
                        "places": 0
                      }
                    }
                  },
                  {
                    "fieldName": "2026",
                    "format": {
                      "type": "number-plain",
                      "abbreviation": "none",
                      "decimalPlaces": {
                        "type": "exact",
                        "places": 0
                      }
                    }
                  }
                ]
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 6,
            "y": 8,
            "width": 6,
            "height": 6
          }
        },
        {
          "widget": {
            "name": "heatmap_throughput",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "exe_heatmap_throughput",
                  "fields": [
                    {
                      "name": "fonte",
                      "expression": "`fonte`"
                    },
                    {
                      "name": "etapa",
                      "expression": "`etapa`"
                    },
                    {
                      "name": "throughput_reg_min",
                      "expression": "`throughput_reg_min`"
                    }
                  ],
                  "cubeGroupingSets": {
                    "sets": [
                      {
                        "fieldNames": [
                          "fonte"
                        ]
                      },
                      {
                        "fieldNames": [
                          "etapa"
                        ]
                      }
                    ]
                  },
                  "disaggregated": true,
                  "orders": [
                    {
                      "direction": "ASC",
                      "expression": "`fonte`"
                    },
                    {
                      "direction": "ASC",
                      "expression": "`etapa`"
                    }
                  ]
                }
              }
            ],
            "spec": {
              "frame": {
                "showTitle": true,
                "title": {
                  "value": "Throughput por fonte e etapa (reg/min)",
                  "fields": []
                }
              },
              "version": 4,
              "widgetType": "pivot",
              "encodings": {
                "rows": {
                  "fields": [
                    {
                      "fieldName": "fonte",
                      "scale": {
                        "type": "categorical"
                      }
                    }
                  ]
                },
                "columns": {
                  "fields": [
                    {
                      "fieldName": "etapa",
                      "scale": {
                        "type": "categorical"
                      }
                    }
                  ]
                },
                "cells": {
                  "fields": [
                    {
                      "fieldName": "throughput_reg_min",
                      "format": {
                        "type": "number-plain",
                        "abbreviation": "none",
                        "decimalPlaces": {
                          "type": "exact",
                          "places": 1
                        }
                      },
                      "cellType": "text",
                      "style": {
                        "type": "color-scale",
                        "colorScale": {
                          "scale": {
                            "type": "quantitative",
                            "colorRamp": {
                              "mode": "single-hue",
                              "baseColor": {
                                "themeColorType": "visualizationColors",
                                "position": 1
                              }
                            }
                          }
                        }
                      }
                    }
                  ],
                  "displayAs": "rows"
                }
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 0,
            "y": 8,
            "width": 6,
            "height": 6
          }
        },
        {
          "widget": {
            "name": "contexto_temporal",
            "multilineTextboxSpec": {
              "lines": [
                "**Janela de dados:** Últimos 7 dias | **Baseline:** Média dos últimos 7 dias"
              ]
            }
          },
          "position": {
            "x": 0,
            "y": 7,
            "width": 12,
            "height": 1
          }
        },
        {
          "widget": {
            "name": "filtro_run",
            "queries": [
              {
                "name": "dashboards/01f1badbcdcb11c9b1dee1e37ada16e6/datasets/01f1bc2a7ef81a6e8a364c9bd9868dac_run_id",
                "query": {
                  "datasetName": "filtro_runs",
                  "fields": [
                    {
                      "name": "run_id",
                      "expression": "`run_id`"
                    },
                    {
                      "name": "run_id_associativity",
                      "expression": "COUNT_IF(`associative_filter_predicate_group`)"
                    }
                  ],
                  "disaggregated": false
                }
              }
            ],
            "spec": {
              "version": 2,
              "frame": {
                "showTitle": true,
                "title": {
                  "value": "Run (id)",
                  "fields": []
                }
              },
              "widgetType": "filter-single-select",
              "encodings": {
                "fields": [
                  {
                    "fieldName": "run_id",
                    "queryName": "dashboards/01f1badbcdcb11c9b1dee1e37ada16e6/datasets/01f1bc2a7ef81a6e8a364c9bd9868dac_run_id"
                  }
                ]
              }
            }
          },
          "position": {
            "x": 0,
            "y": 2,
            "width": 4,
            "height": 2
          }
        },
        {
          "widget": {
            "name": "filtro_fonte",
            "queries": [
              {
                "name": "dashboards/01f1badbcdcb11c9b1dee1e37ada16e6/datasets/01f1bc2a7fa91368baaa3dda2376ec37_fonte",
                "query": {
                  "datasetName": "filtro_fonte",
                  "fields": [
                    {
                      "name": "fonte",
                      "expression": "`fonte`"
                    },
                    {
                      "name": "fonte_associativity",
                      "expression": "COUNT_IF(`associative_filter_predicate_group`)"
                    }
                  ],
                  "disaggregated": false
                }
              }
            ],
            "spec": {
              "version": 2,
              "frame": {
                "showTitle": true,
                "title": {
                  "value": "Fonte",
                  "fields": []
                }
              },
              "widgetType": "filter-multi-select",
              "encodings": {
                "fields": [
                  {
                    "fieldName": "fonte",
                    "queryName": "dashboards/01f1badbcdcb11c9b1dee1e37ada16e6/datasets/01f1bc2a7fa91368baaa3dda2376ec37_fonte"
                  }
                ]
              }
            }
          },
          "position": {
            "x": 4,
            "y": 2,
            "width": 4,
            "height": 2
          }
        },
        {
          "widget": {
            "name": "filtro_etapa",
            "queries": [
              {
                "name": "dashboards/01f1badbcdcb11c9b1dee1e37ada16e6/datasets/01f1bc2a810a1984aacb66039c097bdd_etapa",
                "query": {
                  "datasetName": "filtro_etapa",
                  "fields": [
                    {
                      "name": "etapa",
                      "expression": "`etapa`"
                    },
                    {
                      "name": "etapa_associativity",
                      "expression": "COUNT_IF(`associative_filter_predicate_group`)"
                    }
                  ],
                  "disaggregated": false
                }
              }
            ],
            "spec": {
              "version": 2,
              "frame": {
                "showTitle": true,
                "title": {
                  "value": "Etapa",
                  "fields": []
                }
              },
              "widgetType": "filter-multi-select",
              "encodings": {
                "fields": [
                  {
                    "fieldName": "etapa",
                    "queryName": "dashboards/01f1badbcdcb11c9b1dee1e37ada16e6/datasets/01f1bc2a810a1984aacb66039c097bdd_etapa"
                  }
                ]
              }
            }
          },
          "position": {
            "x": 8,
            "y": 2,
            "width": 4,
            "height": 2
          }
        },
        {
          "widget": {
            "name": "kpi_volume",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "exe_kpis_periodo",
                  "fields": [
                    {
                      "name": "run_period",
                      "expression": "`run_period`"
                    },
                    {
                      "name": "baseline_volume",
                      "expression": "`baseline_volume`"
                    },
                    {
                      "name": "volume_total",
                      "expression": "`volume_total`"
                    }
                  ],
                  "disaggregated": true,
                  "orders": [
                    {
                      "direction": "DESC",
                      "expression": "`run_period`"
                    }
                  ]
                }
              }
            ],
            "spec": {
              "frame": {
                "showTitle": true,
                "showDescription": true,
                "title": {
                  "value": "Volume processado",
                  "fields": []
                },
                "description": {
                  "value": "Registros processados por run",
                  "fields": []
                }
              },
              "version": 2,
              "widgetType": "counter",
              "encodings": {
                "value": {
                  "fieldName": "volume_total",
                  "format": {
                    "type": "number-plain",
                    "abbreviation": "compact",
                    "decimalPlaces": {
                      "type": "exact",
                      "places": 1
                    }
                  }
                },
                "target": {
                  "fieldName": "baseline_volume",
                  "period": {
                    "type": "relative-to-value-period",
                    "offset": 0
                  }
                },
                "period": {
                  "fieldName": "run_period",
                  "hideLabels": true
                }
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 0,
            "y": 4,
            "width": 3,
            "height": 3
          }
        },
        {
          "widget": {
            "name": "kpi_throughput",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "exe_kpis_periodo",
                  "fields": [
                    {
                      "name": "run_period",
                      "expression": "`run_period`"
                    },
                    {
                      "name": "baseline_throughput",
                      "expression": "`baseline_throughput`"
                    },
                    {
                      "name": "throughput_reg_min",
                      "expression": "`throughput_reg_min`"
                    }
                  ],
                  "disaggregated": true,
                  "orders": [
                    {
                      "direction": "DESC",
                      "expression": "`run_period`"
                    }
                  ]
                }
              }
            ],
            "spec": {
              "frame": {
                "showTitle": true,
                "showDescription": true,
                "title": {
                  "value": "Throughput",
                  "fields": []
                },
                "description": {
                  "value": "Registros processados por minuto",
                  "fields": []
                }
              },
              "version": 2,
              "widgetType": "counter",
              "encodings": {
                "value": {
                  "fieldName": "throughput_reg_min",
                  "format": {
                    "type": "number-plain",
                    "abbreviation": "compact",
                    "decimalPlaces": {
                      "type": "exact",
                      "places": 0
                    }
                  }
                },
                "target": {
                  "fieldName": "baseline_throughput",
                  "period": {
                    "type": "relative-to-value-period",
                    "offset": 0
                  }
                },
                "period": {
                  "fieldName": "run_period",
                  "hideLabels": true
                }
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 3,
            "y": 4,
            "width": 3,
            "height": 3
          }
        },
        {
          "widget": {
            "name": "kpi_duracao",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "exe_kpis_periodo",
                  "fields": [
                    {
                      "name": "run_period",
                      "expression": "`run_period`"
                    },
                    {
                      "name": "baseline_duracao",
                      "expression": "`baseline_duracao`"
                    },
                    {
                      "name": "duracao_total_min",
                      "expression": "`duracao_total_min`"
                    }
                  ],
                  "disaggregated": true,
                  "orders": [
                    {
                      "direction": "DESC",
                      "expression": "`run_period`"
                    }
                  ]
                }
              }
            ],
            "spec": {
              "frame": {
                "showTitle": true,
                "showDescription": true,
                "title": {
                  "value": "Duração da run",
                  "fields": []
                },
                "description": {
                  "value": "Minutos por run",
                  "fields": []
                }
              },
              "version": 2,
              "widgetType": "counter",
              "encodings": {
                "value": {
                  "fieldName": "duracao_total_min",
                  "format": {
                    "type": "number-plain",
                    "abbreviation": "none",
                    "decimalPlaces": {
                      "type": "exact",
                      "places": 1
                    }
                  }
                },
                "target": {
                  "fieldName": "baseline_duracao",
                  "period": {
                    "offset": 1,
                    "type": "relative-to-value-period"
                  }
                },
                "period": {
                  "fieldName": "run_period",
                  "hideLabels": true
                }
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 6,
            "y": 4,
            "width": 3,
            "height": 3
          }
        },
        {
          "widget": {
            "name": "kpi_variacao",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "exe_kpis_periodo",
                  "fields": [
                    {
                      "name": "run_period",
                      "expression": "`run_period`"
                    },
                    {
                      "name": "baseline_completude",
                      "expression": "`baseline_completude`"
                    },
                    {
                      "name": "execucoes_completas",
                      "expression": "`execucoes_completas`"
                    }
                  ],
                  "disaggregated": true,
                  "orders": [
                    {
                      "direction": "DESC",
                      "expression": "`run_period`"
                    }
                  ]
                }
              }
            ],
            "spec": {
              "frame": {
                "showDescription": true,
                "showTitle": true,
                "title": {
                  "fields": [],
                  "value": "Completude"
                },
                "description": {
                  "fields": [],
                  "value": "Fontes por run"
                }
              },
              "version": 2,
              "widgetType": "counter",
              "encodings": {
                "value": {
                  "fieldName": "execucoes_completas",
                  "format": {
                    "abbreviation": "none",
                    "decimalPlaces": {
                      "places": 0,
                      "type": "exact"
                    },
                    "type": "number-plain"
                  }
                },
                "target": {
                  "fieldName": "baseline_completude",
                  "period": {
                    "offset": 1,
                    "type": "relative-to-value-period"
                  }
                },
                "period": {
                  "fieldName": "run_period",
                  "hideLabels": true
                }
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 9,
            "y": 4,
            "width": 3,
            "height": 3
          }
        },
        {
          "widget": {
            "name": "w7_registros_series",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "exe_kpis_periodo",
                  "fields": [
                    {
                      "name": "run_period",
                      "expression": "`run_period`"
                    },
                    {
                      "name": "duracao_total_min",
                      "expression": "`duracao_total_min`"
                    }
                  ],
                  "disaggregated": true
                }
              }
            ],
            "spec": {
              "frame": {
                "showTitle": true,
                "title": {
                  "fields": [],
                  "value": "Tendência de duração por run"
                }
              },
              "version": 3,
              "widgetType": "line",
              "encodings": {
                "x": {
                  "fieldName": "run_period",
                  "axis": {
                    "hideTitle": true
                  },
                  "scale": {
                    "type": "temporal"
                  }
                },
                "y": {
                  "fieldName": "duracao_total_min",
                  "axis": {
                    "hideTitle": true
                  },
                  "format": {
                    "abbreviation": "none",
                    "decimalPlaces": {
                      "places": 1,
                      "type": "exact"
                    },
                    "type": "number-plain"
                  },
                  "scale": {
                    "type": "quantitative"
                  }
                }
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 6,
            "y": 14,
            "width": 6,
            "height": 9
          }
        },
        {
          "widget": {
            "name": "w8_scatter",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "exe_scatter_melhorado",
                  "fields": [
                    {
                      "name": "etapa",
                      "expression": "`etapa`"
                    },
                    {
                      "name": "registros_processados",
                      "expression": "`registros_processados`"
                    },
                    {
                      "name": "duracao_segundos",
                      "expression": "`duracao_segundos`"
                    }
                  ],
                  "filters": [
                    {
                      "expression": "`registros_processados` IS NOT NULL"
                    },
                    {
                      "expression": "`duracao_segundos` IS NOT NULL"
                    }
                  ],
                  "disaggregated": true
                }
              }
            ],
            "spec": {
              "frame": {
                "showTitle": true,
                "title": {
                  "value": "Duração (s) × registros por execução",
                  "fields": []
                }
              },
              "version": 3,
              "widgetType": "scatter",
              "encodings": {
                "x": {
                  "fieldName": "registros_processados",
                  "format": {
                    "type": "number-plain",
                    "abbreviation": "compact",
                    "decimalPlaces": {
                      "type": "exact",
                      "places": 0
                    }
                  },
                  "axis": {
                    "hideTitle": true
                  },
                  "scale": {
                    "type": "quantitative"
                  }
                },
                "y": {
                  "fieldName": "duracao_segundos",
                  "format": {
                    "type": "number-plain",
                    "abbreviation": "compact",
                    "decimalPlaces": {
                      "type": "exact",
                      "places": 0
                    }
                  },
                  "axis": {
                    "hideTitle": true
                  },
                  "scale": {
                    "type": "quantitative"
                  }
                },
                "color": {
                  "fieldName": "etapa",
                  "scale": {
                    "type": "categorical"
                  }
                }
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 0,
            "y": 14,
            "width": 6,
            "height": 9
          }
        },
        {
          "widget": {
            "name": "header_status",
            "multilineTextboxSpec": {
              "lines": [
                "## ✓ Pipeline operando dentro da normalidade\n",
                "Comparação: última execução vs média dos últimos 7 dias"
              ]
            }
          },
          "position": {
            "x": 0,
            "y": 0,
            "width": 12,
            "height": 2
          }
        }
      ],
      "pageType": "PAGE_TYPE_CANVAS",
      "layoutVersion": "GRID_V1"
    },
    {
      "name": "qualidade",
      "displayName": "Qualidade",
      "layout": [
        {
          "widget": {
            "name": "w10_fails_24h",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "qual_fails_24h",
                  "fields": [
                    {
                      "name": "total_fails",
                      "expression": "`total_fails`"
                    }
                  ],
                  "disaggregated": true
                }
              }
            ],
            "spec": {
              "frame": {
                "showTitle": true,
                "title": {
                  "value": "Fails nas últimas 24h",
                  "fields": []
                }
              },
              "version": 2,
              "widgetType": "counter",
              "encodings": {
                "value": {
                  "fieldName": "total_fails"
                }
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 0,
            "y": 0,
            "width": 4,
            "height": 3
          }
        },
        {
          "widget": {
            "name": "w11_checks_ultima_run",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "qual_checks_ultima_run",
                  "fields": [
                    {
                      "name": "total_checks",
                      "expression": "`total_checks`"
                    }
                  ],
                  "disaggregated": true
                }
              }
            ],
            "spec": {
              "frame": {
                "showTitle": true,
                "title": {
                  "value": "Total de checks na última run",
                  "fields": []
                }
              },
              "version": 2,
              "widgetType": "counter",
              "encodings": {
                "value": {
                  "fieldName": "total_checks"
                }
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 4,
            "y": 0,
            "width": 4,
            "height": 3
          }
        },
        {
          "widget": {
            "name": "w12_pass_rate",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "qual_pass_rate",
                  "fields": [
                    {
                      "name": "taxa_pass_pct",
                      "expression": "`taxa_pass_pct`"
                    }
                  ],
                  "disaggregated": true
                }
              }
            ],
            "spec": {
              "frame": {
                "showTitle": true,
                "title": {
                  "value": "Taxa de pass (%)",
                  "fields": []
                }
              },
              "version": 2,
              "widgetType": "counter",
              "encodings": {
                "value": {
                  "fieldName": "taxa_pass_pct"
                }
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 8,
            "y": 0,
            "width": 4,
            "height": 3
          }
        },
        {
          "widget": {
            "name": "w13_fails_warns",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "obs_guardrails",
                  "fields": [
                    {
                      "name": "ts_check",
                      "expression": "`ts_check`"
                    },
                    {
                      "name": "etapa",
                      "expression": "`etapa`"
                    },
                    {
                      "name": "fonte",
                      "expression": "`fonte`"
                    },
                    {
                      "name": "ano",
                      "expression": "`ano`"
                    },
                    {
                      "name": "nome_guardrail",
                      "expression": "`nome_guardrail`"
                    },
                    {
                      "name": "tipo_check",
                      "expression": "`tipo_check`"
                    },
                    {
                      "name": "resultado",
                      "expression": "`resultado`"
                    },
                    {
                      "name": "detalhes",
                      "expression": "`detalhes`"
                    }
                  ],
                  "filters": [
                    {
                      "expression": "`resultado` IN ('FAIL', 'WARN')"
                    }
                  ],
                  "disaggregated": true
                }
              }
            ],
            "spec": {
              "frame": {
                "showTitle": true,
                "title": {
                  "value": "Guardrails com fail ou warn",
                  "fields": []
                }
              },
              "version": 2,
              "widgetType": "table",
              "encodings": {
                "columns": [
                  {
                    "fieldName": "ts_check"
                  },
                  {
                    "fieldName": "etapa"
                  },
                  {
                    "fieldName": "fonte"
                  },
                  {
                    "fieldName": "ano"
                  },
                  {
                    "fieldName": "nome_guardrail"
                  },
                  {
                    "fieldName": "tipo_check"
                  },
                  {
                    "fieldName": "resultado"
                  },
                  {
                    "fieldName": "detalhes"
                  }
                ]
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 0,
            "y": 3,
            "width": 8,
            "height": 8
          }
        },
        {
          "widget": {
            "name": "w14_tendencia",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "qual_tendencia",
                  "fields": [
                    {
                      "name": "resultado",
                      "expression": "`resultado`"
                    },
                    {
                      "name": "run_id",
                      "expression": "`run_id`"
                    },
                    {
                      "name": "contagem",
                      "expression": "`contagem`"
                    }
                  ],
                  "disaggregated": true
                }
              }
            ],
            "spec": {
              "frame": {
                "showTitle": true,
                "title": {
                  "value": "Tendência de guardrails por run",
                  "fields": []
                }
              },
              "version": 3,
              "widgetType": "line",
              "encodings": {
                "x": {
                  "fieldName": "run_id",
                  "scale": {
                    "type": "categorical"
                  }
                },
                "y": {
                  "fieldName": "contagem",
                  "scale": {
                    "type": "quantitative"
                  }
                },
                "color": {
                  "fieldName": "resultado",
                  "scale": {
                    "type": "categorical"
                  }
                }
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 8,
            "y": 3,
            "width": 4,
            "height": 8
          }
        }
      ],
      "pageType": "PAGE_TYPE_CANVAS",
      "layoutVersion": "GRID_V1"
    },
    {
      "name": "ingestao",
      "displayName": "Ingestão",
      "layout": [
        {
          "widget": {
            "name": "w15_data_recente",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "ing_data_recente",
                  "fields": [
                    {
                      "name": "data_ingestao_recente",
                      "expression": "`data_ingestao_recente`"
                    }
                  ],
                  "disaggregated": true
                }
              }
            ],
            "spec": {
              "frame": {
                "showTitle": true,
                "title": {
                  "value": "Ingestão mais recente com sucesso",
                  "fields": []
                }
              },
              "version": 2,
              "widgetType": "counter",
              "encodings": {
                "value": {
                  "fieldName": "data_ingestao_recente"
                }
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 0,
            "y": 0,
            "width": 12,
            "height": 3
          }
        },
        {
          "widget": {
            "name": "w16_freshness",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "ing_freshness",
                  "fields": [
                    {
                      "name": "fonte",
                      "expression": "`fonte`"
                    },
                    {
                      "name": "ano",
                      "expression": "`ano`"
                    },
                    {
                      "name": "last_modified_cvm",
                      "expression": "`last_modified_cvm`"
                    },
                    {
                      "name": "ingest_ts",
                      "expression": "`ingest_ts`"
                    },
                    {
                      "name": "versao_ingestao",
                      "expression": "`versao_ingestao`"
                    },
                    {
                      "name": "status",
                      "expression": "`status`"
                    },
                    {
                      "name": "dias_desde_ingestao",
                      "expression": "`dias_desde_ingestao`"
                    }
                  ],
                  "disaggregated": true
                }
              }
            ],
            "spec": {
              "frame": {
                "showTitle": true,
                "title": {
                  "value": "Freshness por fonte e ano (dias desde ingestão)",
                  "fields": []
                }
              },
              "version": 2,
              "widgetType": "table",
              "encodings": {
                "columns": [
                  {
                    "fieldName": "fonte"
                  },
                  {
                    "fieldName": "ano"
                  },
                  {
                    "fieldName": "last_modified_cvm"
                  },
                  {
                    "fieldName": "ingest_ts"
                  },
                  {
                    "fieldName": "versao_ingestao"
                  },
                  {
                    "fieldName": "status"
                  },
                  {
                    "fieldName": "dias_desde_ingestao"
                  }
                ]
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 0,
            "y": 3,
            "width": 8,
            "height": 10
          }
        },
        {
          "widget": {
            "name": "w17_bytes_ano",
            "queries": [
              {
                "name": "main_query",
                "query": {
                  "datasetName": "ing_bytes_ano",
                  "fields": [
                    {
                      "name": "ano",
                      "expression": "`ano`"
                    },
                    {
                      "name": "total_bytes",
                      "expression": "`total_bytes`"
                    }
                  ],
                  "disaggregated": true
                }
              }
            ],
            "spec": {
              "frame": {
                "showTitle": true,
                "title": {
                  "value": "Volume por ano (bytes, última ingestão)",
                  "fields": []
                }
              },
              "version": 3,
              "widgetType": "bar",
              "encodings": {
                "x": {
                  "fieldName": "ano",
                  "scale": {
                    "type": "categorical"
                  }
                },
                "y": {
                  "fieldName": "total_bytes",
                  "scale": {
                    "type": "quantitative"
                  }
                }
              },
              "data": {
                "queryName": "main_query"
              }
            }
          },
          "position": {
            "x": 8,
            "y": 3,
            "width": 4,
            "height": 10
          }
        }
      ],
      "pageType": "PAGE_TYPE_CANVAS",
      "layoutVersion": "GRID_V1"
    },
    {
      "name": "global_filters",
      "displayName": "Global Filters",
      "pageType": "PAGE_TYPE_GLOBAL_FILTERS",
      "layoutVersion": "GRID_V1"
    }
  ],
  "uiSettings": {
    "theme": {
      "widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"
    },
    "applyModeEnabled": false
  }
}