# Especificações Técnicas

## Propósito

Este diretório contém especificações técnicas e padrões de desenvolvimento para projetos de dados.
Documenta decisões arquiteturais, convenções e processos que garantem consistência e manutenibilidade.

## Estrutura

* **arquitetura_medalhao.md**: Fundamentos da arquitetura em camadas (Bronze/Silver/Gold)
  - Princípios fundamentais e contratos por camada
  - Antipadrões arquiteturais e como evitá-los
  - Reprocessabilidade, determinismo e versionamento

* **nomenclaturas.md**: Convenções de naming
  - Notebooks, tabelas, dataframes, variáveis
  - Princípio DRY e numeração para rastreabilidade

* **estrutura_notebooks.md**: Padrões de organização de código
  - Formato de arquivo (.py vs .ipynb)
  - Estrutura de células e separação de responsabilidades
  - Comentários e documentação

* **unity_catalog.md**: Modelagem de schemas e tabelas
  - Padrões de nomenclatura no Unity Catalog
  - Organização por camadas e projetos

* **resiliencia_operacional.md**: Estratégias de tratamento de erros
  - Retry logic com exponential backoff
  - Tratamento granular de erros
  - Checkpointing e tabelas de controle

* **protocolo_atualizacao.md**: Manutenção de documentação
  - Matriz de impactos: quando mudanças em código exigem atualização de documentação
  - Checklist de arquivos por tipo de mudança

* **escolha_sql_pyspark.md**: Critérios de decisão
  - Quando preferir SQL vs PySpark
  - Trade-offs e casos de uso

## Uso

Especificações técnicas documentadas são fundamentais para:

* Onboarding de novos desenvolvedores
* Manutenção de longo prazo
* Consistência em decisões arquiteturais
* Auditoria e reprodutibilidade

Em pipelines de dados, decisões sobre camadas, versionamento e estratégias de gravação
precisam estar documentadas para evitar divergência e garantir reprocessabilidade.