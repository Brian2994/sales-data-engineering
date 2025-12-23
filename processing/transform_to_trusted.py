from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("SalesDataTransformation") \
    .getOrCreate()

# ----------------------------
# Leitura da camada processed
# ----------------------------
clientes_df = spark.read.parquet("data/processed/clientes")
produtos_df = spark.read.parquet("data/processed/produtos")
pedidos_df = spark.read.parquet("data/processed/pedidos")
itens_df = spark.read.parquet("data/processed/itens_pedido")

# ----------------------------
# Limpeza básica
# ----------------------------
clientes_df = clientes_df.dropna(subset=["id_cliente"])
produtos_df = produtos_df.dropna(subset=["id_produto"])
pedidos_df = pedidos_df.dropna(subset=["id_pedido", "id_cliente"])
itens_df = itens_df.dropna(subset=["id_pedido", "id_produto"])

# ----------------------------
# Filtrar pedidos finalizados
# ----------------------------
pedidos_finalizados_df = pedidos_df.filter(col("status") == "FINALIZADO")

# ----------------------------
# Join pedidos + itens
# ----------------------------
vendas_df = pedidos_finalizados_df.join(
    itens_df,
    on="id_pedido",
    how="inner"
)

# ----------------------------
# Calcular valor total do item
# ----------------------------
vendas_df = vendas_df.withColumn(
    "valor_total",
    col("quantidade") * col("valor_unitario")
)

# ----------------------------
# Remover valores inválidos
# ----------------------------
vendas_df = vendas_df.filter(
    (col("quantidade") > 0) &
    (col("valor_unitario") >= 0) &
    (col("valor_total") >= 0)
)

# ----------------------------
# Join com dimensões
# ----------------------------
vendas_df = vendas_df.join(clientes_df, on="id_cliente", how="left") \
                     .join(produtos_df, on="id_produto", how="left")

# ----------------------------
# Selecionar colunas finais
# ----------------------------
vendas_trusted_df = vendas_df.select(
    "id_pedido",
    "data_pedido",
    "id_cliente",
    "cidade",
    "estado",
    "id_produto",
    "categoria",
    "quantidade",
    "valor_unitario",
    "valor_total"
)

# ----------------------------
# Salvar camada trusted
# ----------------------------
vendas_trusted_df.write.mode("overwrite").parquet("data/trusted/vendas")

clientes_df.write.mode("overwrite").parquet("data/trusted/dim_clientes")
produtos_df.write.mode("overwrite").parquet("data/trusted/dim_produtos")

print("Transformação concluída com sucesso!")