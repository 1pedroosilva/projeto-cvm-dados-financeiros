# Escolha entre SQL e PySpark

## Quando Preferir SQL

* **Consultas analíticas diretas** (agregações, joins, filtros simples)
* **Delta Lake operations nativas** (MERGE, UPDATE, DELETE)
* **Melhor performance** com motor Photon otimizado para SQL
* **Maior legibilidade** para analistas de negócio
* **Queries ad-hoc e exploratórias**

## Quando Preferir PySpark

* **Lógica complexa e iterativa** (loops, condicionais, controle programático)
* **Integração com bibliotecas Python** (ML, pandas, APIs)
* **Transformações complexas** em múltiplas etapas
* **Necessidade de variáveis** e reutilização de código

## Regra Geral

**Combinar ambos conforme o caso de uso** - ambos compilam para o mesmo motor Spark no Databricks.

**Escolher baseado em**:
* Legibilidade
* Manutenibilidade
* Contexto do usuário final (analistas vs engenheiros)

---

💡 **Ao implementar mudanças, consulte `.agent_instructions/protocolo_atualizacao.md` (ID: 4053803626820843) para atualizar documentações afetadas.**