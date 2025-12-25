from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, dayofweek

spark = SparkSession.builder \
    .appName("BuildSalesDW") \
    .getOrCreate()

# ----------------------------
# Leitura da camada trusted
# ----------------------------
vendas_df = spark.read.parquet("data/trusted/vendas")

# ----------------------------
# DIM CLIENTE
# ----------------------------
dim_cliente_df = vendas_df.select(
    "id_cliente",
    "cidade",
    "estado"
).dropDuplicates(["id_cliente"])

# ----------------------------
# DIM PRODUTO
# ----------------------------
dim_produto_df = vendas_df.select(
    "id_produto",
    "categoria"
).dropDuplicates(["id_produto"])

# ----------------------------
# DIM TEMPO
# ----------------------------
dim_tempo_df = vendas_df.select(
    col("data_pedido").alias("data")
).dropDuplicates()

dim_tempo_df = dim_tempo_df \
    .withColumn("ano", year("data")) \
    .withColumn("mes", month("data")) \
    .withColumn("dia", dayofmonth("data")) \
    .withColumn("dia_semana", dayofweek("data"))

# ----------------------------
# FATO VENDAS
# ----------------------------
fato_vendas_df = vendas_df.select(
    "id_pedido",
    "id_cliente",
    "id_produto",
    col("data_pedido".alias("data")),
    "quantidade",
    "valor_total"
)

# ----------------------------
# Salvar tabelas DW (Parquet)
# ----------------------------
dim_cliente_df.write.mode("overwrite").parquet("data/warehouse/dim_cliente")
dim_produto_df.write.mode("overwrite").parquet("data/warehouse/dim_produto")
dim_tempo_df.write.mode("overwrite").parquet("data/warehouse/dim_tempo")
fato_vendas_df.write.mode("overwrite").parquet("data/warehouse/fato_vendas")

print("Data Warehouse modelado com sucesso!")