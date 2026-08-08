# Referência de IDs - Projeto CVM Dados Financeiros

**Propósito**: Registro centralizado dos IDs de todos os assets do projeto para uso em automações, scripts e referências cruzadas.

**Data de última atualização**: 2026-07-27

---

## 📁 Estrutura de Pastas

| Pasta | ID | Caminho |
| --- | --- | --- |
| projeto-cvm-dados-financeiros/ | (path) | . |
| 00_documentacao/ | 245806422504864 | .../00_documentacao |
| 01_bronze/ | 245806422504865 | .../01_bronze |
| 02_silver/ | 245806422504866 | .../02_silver |
| 03_gold/ | 245806422504867 | .../03_gold |
| 04_apoio/ | 4053803626820848 | .../04_apoio |
| 00_documentacao/tecnica/ | 245806422504869 | .../00_documentacao/tecnica |
| 00_documentacao/negocio/ | 245806422504868 | .../00_documentacao/negocio |

---

## 📄 Arquivos de Documentação (.md)

| Arquivo | ID | Localização |
| --- | --- | --- |
| README.md | 245806422504860 | Raiz do projeto |
| evolucao_projeto.md | 4053803626820842 | 00_documentacao/ |
| arquitetura.md | 245806422504871 | 00_documentacao/tecnica/ |
| dicionario_dados.md | 245806422504872 | 00_documentacao/negocio/ |
| referencia_ids.md | 1260064971103914 | 00_documentacao/ (este arquivo) |

---

## 📓 Notebooks - Camada Bronze

| Notebook | ID | Descrição |
| --- | --- | --- |
| 101_cvm_dfp_dre | **1231313839399377** | Ingestão DRE (Demonstração Resultado Exercício) |
| 102_cvm_dfp_bpa | 864521697998492 | Ingestão BPA (Balanço Patrimonial Ativo) |

---

## 📓 Notebooks - Camada Silver

| Notebook | ID | Descrição |
| --- | --- | --- |
| 201_cvm_dfp_dre | 245806422504876 | Transformação e limpeza DRE |
| 202_cvm_dfp_bpa | 1260064971103913 | Transformação e limpeza BPA |

---

## 📓 Notebooks - Camada Gold

*(Pasta vazia - sem notebooks implementados)*

---

## 📓 Notebooks - Apoio (04_apoio/)

| Notebook | ID | Descrição |
| --- | --- | --- |
| 000_orquestrador_pipeline | 1260064971103906 | Orquestrador: detecção inteligente de períodos |
| 001_ddl_create_tables | 4053803626820852 | Criação de schemas e tabelas UC |
| 002_ddl_controle_ingestao | 1260064971103905 | Criação tabela de controle de ingestão |
| 003_download_cvm_para_landing | 1260064971103907 | Download arquivos CVM para Landing Zone |
| 099_ddl_table_comments | 4053803626820853 | Documentação de tabelas com COMMENT ON |

---

## 📄 Arquivos de Código (.py)

| Arquivo | ID | Descrição |
| --- | --- | --- |
| config_parametros.py | 1260064971103904 | Configurações centralizadas do pipeline |

---

## ⚠️ Notas Importantes

* **IDs são imutáveis**: Cada asset recebe um ID único no momento da criação que nunca muda
* **Recreação gera novo ID**: Se um notebook/arquivo for deletado e recriado, receberá um novo ID
* **Atualização deste arquivo**: Sempre que criar/deletar assets do projeto, atualizar este registro
* **Notebooks destacados**: IDs em negrito são os mais críticos para o pipeline principal

---

## 🔄 Histórico de Mudanças

### 2026-07-27 - Padronização de numeração (04_apoio) + Auditoria de completude
* Corrigida numeração dos notebooks de apoio para seguir padrão de 3 dígitos obrigatórios
* Removida duplicidade: 000_ddl_create_tables → 001_ddl_create_tables
* Removida duplicidade: 999_ddl_table_comments → 099_ddl_table_comments
* Removidas entradas de arquivos .sql inexistentes (00_ddl_create_tables.sql, 99_ddl_table_comments.sql)
* Adicionado notebook faltante: 202_cvm_dfp_bpa (ID: 1260064971103913)
* Auditoria completa: 25/25 IDs validados e corretos

### 2026-07-23 - Criação inicial
* Captura completa de todos os IDs do projeto
* Motivação: ID desatualizado do notebook 101 causou erro em sessão anterior
* Criado arquivo centralizado para prevenir inconsistências futuras