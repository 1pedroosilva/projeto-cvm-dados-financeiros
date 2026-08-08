# Dicionário de Dados - Projeto CVM Dados Financeiros

## Contexto de Negócio

### O que é a CVM?

A **Comissão de Valores Mobiliários (CVM)** é uma autarquia federal brasileira vinculada ao Ministério da Economia, responsável por:
* Regulamentar e fiscalizar o mercado de capitais brasileiro
* Proteger investidores
* Garantir transparência nas informações corporativas
* Regular empresas de capital aberto (ações negociadas em bolsa)

### Demonstrações Financeiras Padronizadas (DFP)

As **DFP** são relatórios financeiros **obrigatórios** que todas as companhias abertas brasileiras devem apresentar anualmente à CVM, contendo:
* Balanço Patrimonial (BP)
* Demonstração do Resultado do Exercício (DRE)
* Demonstração dos Fluxos de Caixa (DFC)
* Demonstração das Mutações do Patrimônio Líquido (DMPL)
* Demonstração do Valor Adicionado (DVA)
* Notas explicativas

**Periodicidade**: Anual (relatório consolidado do exercício fiscal)

**Público**: Todos os dados são públicos e disponíveis no [Portal de Dados Abertos da CVM](https://dados.cvm.gov.br/)

## Demonstração do Resultado do Exercício (DRE)

### O que é a DRE?

A **DRE** é um relatório contábil que demonstra:
* **Receitas** geradas pela empresa no período
* **Custos e despesas** incorridos
* **Resultado líquido** (lucro ou prejuízo)

### DRE Consolidada vs. Individual

* **DRE Consolidada**: Inclui resultados da empresa controladora + todas as controladas (visão do grupo econômico)
* **DRE Individual**: Apenas os resultados da empresa controladora

**Neste projeto**: Trabalhamos apenas com **DRE Consolidada** para ter visão completa do grupo empresarial.

### Estrutura da DRE

A DRE segue uma estrutura hierárquica padronizada:

```
Receita de Venda de Bens e/ou Serviços
(-) Custo dos Bens e/ou Serviços Vendidos
= Resultado Bruto

(-) Despesas Operacionais
  (-) Despesas com Vendas
  (-) Despesas Gerais e Administrativas
  (+/-) Outras Receitas/Despesas Operacionais
= Resultado Antes do Resultado Financeiro e Tributos

(+/-) Resultado Financeiro
  (+) Receitas Financeiras
  (-) Despesas Financeiras
= Resultado Antes dos Tributos sobre Lucro

(-) Imposto de Renda e Contribuição Social sobre Lucro
= Resultado Líquido das Operações Continuadas

(+/-) Resultado Líquido de Operações Descontinuadas
= Resultado Líquido do Período
```

## Estrutura dos Dados Fonte (CVM)

### Formato de Entrega

* **Formato**: Arquivo ZIP contendo múltiplos CSVs
* **URL**: `https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_{ANO}.zip`
* **Arquivo DRE Consolidada**: `dfp_cia_aberta_DRE_con_{ANO}.csv`
* **Encoding**: ISO-8859-1 (Latin-1)
* **Separador**: `;` (ponto e vírgula)

### Colunas Principais (CSV Original)

**Identificadores**:
* `CNPJ_CIA`: CNPJ da companhia (cadastro nacional de pessoa jurídica)
* `DENOM_CIA`: Nome/denominação social da companhia
* `CD_CVM`: Código de identificação da companhia na CVM

**Período**:
* `DT_REFER`: Data de referência do documento (formato: YYYY-MM-DD)
* `DT_INI_EXERC`: Data de início do exercício fiscal
* `DT_FIM_EXERC`: Data de fim do exercício fiscal

**Conta Contábil**:
* `CD_CONTA`: Código da conta contábil (estrutura hierárquica)
* `DS_CONTA`: Descrição da conta contábil
* `ORDEM_EXERC`: Ordem de exercício (Último = exercício mais recente, Penúltimo = exercício anterior)

**Valores**:
* `VL_CONTA`: Valor da conta contábil (em unidade monetária)
* `ST_CONTA_FIXA`: Indica se a conta é fixa na estrutura da DRE

**Metadados**:
* `VERSAO`: Versão do documento (uma empresa pode reapresentar demonstrações corrigidas)
* `GRUPO_DFP`: Tipo de demonstração (DF Consolidada - Demonstração do Resultado)
* `MOEDA`: Moeda dos valores (geralmente REAL)
* `ESCALA_MOEDA`: Escala monetária (MIL = milhares de reais, UNIDADE = reais)

## Conceitos de Negócio

### Companhia Aberta

Empresa cujas ações são negociadas publicamente em bolsa de valores (B3 - Brasil, Bolsa, Balcão).

**Obrigações**:
* Prestar contas à CVM regularmente
* Publicar demonstrações financeiras padronizadas
* Manter transparência com investidores

### Exercício Fiscal

Período de 12 meses usado para fins contábeis e fiscais. No Brasil:
* Geralmente coincide com o ano civil (01/01 a 31/12)
* Algumas empresas podem ter exercício diferente (ex: julho a junho)

### Consolidação

Processo de combinar demonstrações financeiras de:
* **Controladora**: Empresa mãe
* **Controladas**: Empresas subsidiárias (mais de 50% de participação)

**Objetivo**: Mostrar o grupo econômico como uma única entidade.

### Reapresentação

Empresas podem **reapresentar** demonstrações financeiras corrigidas quando:
* Identificam erros
* Mudanças em normas contábeis
* Reclassificações

**Campo VERSAO**: Identifica qual versão do documento (sempre usar a versão mais recente)

## Hierarquia de Contas

### Estrutura de Código de Conta

O campo `CD_CONTA` segue uma estrutura hierárquica:

* **Nível 1**: `1` = Conta principal
* **Nível 2**: `1.01` = Subconta
* **Nível 3**: `1.01.01` = Subconta de subconta
* **Nível 4**: `1.01.01.01` = Detalhamento maior

**Exemplo**:
```
3       - Resultado Antes dos Tributos sobre Lucro
3.05    - Tributos sobre Lucro
3.05.01 - Imposto de Renda e Contribuição Social sobre Lucro
3.06    - Resultado Líquido das Operações Continuadas
```

### Contas Fixas vs. Variáveis

* **Contas Fixas** (`ST_CONTA_FIXA = S`): Estrutura obrigatória da DRE, presente em todas as empresas
* **Contas Variáveis** (`ST_CONTA_FIXA = N`): Detalhamentos específicos de cada empresa

## Regras de Negócio

### Dados Consolidados

* Sempre trabalhar com **DRE Consolidada** (`dfp_cia_aberta_DRE_con_{ANO}.csv`)
* Não misturar com DRE individual para evitar dupla contagem

### Versão Mais Recente

* Uma empresa pode ter múltiplas versões do mesmo documento
* **Regra**: Sempre usar a **última versão** (MAX(VERSAO) por empresa/período)

### Comparação Temporal

* Campo `ORDEM_EXERC`:
  - `Último` = Exercício corrente (ano sendo reportado)
  - `Penúltimo` = Exercício anterior (para comparação YoY)

### Valores Monetários

* **Atenção**: Verificar campo `ESCALA_MOEDA`
* Se `ESCALA_MOEDA = MIL`, multiplicar `VL_CONTA` por 1.000 para obter valor real
* Se `ESCALA_MOEDA = UNIDADE`, usar `VL_CONTA` diretamente

## Casos de Uso

### Análise de Rentabilidade

* **Margem Bruta**: (Resultado Bruto / Receita Líquida) × 100
* **Margem Líquida**: (Resultado Líquido / Receita Líquida) × 100
* **EBITDA**: Resultado antes de juros, impostos, depreciação e amortização

### Comparação Setorial

* Comparar métricas entre empresas do mesmo setor
* Identificar outliers (empresas com performance excepcional ou ruim)
* Benchmarking setorial

### Análise Temporal

* Evolução de receitas ao longo dos anos
* Crescimento de lucro líquido
* Identificação de tendências (crescimento/queda sustentada)

## Referências

* **Portal de Dados Abertos CVM**: https://dados.cvm.gov.br/
* **Manual de Dados Abertos CVM**: https://dados.cvm.gov.br/documents/
* **Lei das S.A. (Lei 6.404/76)**: Base legal para demonstrações financeiras
* **Normas CPC**: Comitê de Pronunciamentos Contábeis (padrões contábeis brasileiros)

## Glossário

* **CVM**: Comissão de Valores Mobiliários
* **DFP**: Demonstrações Financeiras Padronizadas
* **DRE**: Demonstração do Resultado do Exercício
* **CNPJ**: Cadastro Nacional da Pessoa Jurídica
* **CIA**: Companhia (empresa)
* **B3**: Brasil, Bolsa, Balcão (bolsa de valores brasileira)
* **EBITDA**: Earnings Before Interest, Taxes, Depreciation and Amortization
* **YoY**: Year over Year (comparação ano sobre ano)