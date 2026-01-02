Nome do projeto: Sales Data Engineering with PySpark

Problema de negócio:
A empresa possui dados de vendas espalhados em arquivos CSV e precisa de um Data Warehouse para análises de faturamento, produtos e clientes.

Objetivo técnico:
- Construir um pipeline end-to-end
- Usar PySpark
- Criar um modelo estrela
- Disponibilizar dados para análises SQL

Essas regras vão guiar todo o pipeline:
- Apenas pedidos com status = 'FINALIZADO' entram nas vendas
- valor_total_item = quantidade * valor_unitario
- Datas sempre em formato yyyy-MM-dd
- IDs nunca podem ser nulos
- Valores monetários não podem ser negativos