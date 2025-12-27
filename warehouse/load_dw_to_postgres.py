from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appNome("LoadDWToPostgres") \
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.1"
    ) \
    .getOrCreate()

# ----------------------------
# Configuração JDBC
# ----------------------------
jdbc_url = "jdbc:postgresql://localhost:5432/sales_dw"
jdbc_properties = {
    "user": "dw_user",
    "password": "dw_pass",
    "driver": "org.postgresql.Driver"
}

# ----------------------------
# Leitura dos dados DW
# ----------------------------
dim_cliente_df = spark.read.parquet("data/warehouse/dim_cliente")
dim_produto_df = spark.read.parquet("data/warehouse/dim_produto")
dim_tempo_df = spark.read.parquet("data/warehouse/dim_tempo")
fato_vendas_df = spark.read.parquet("data/warehouse/fato_vendas")

# ----------------------------
# Escrita no PostgreSQL
# ----------------------------
dim_cliente_df.write.jdbc(
    url=jdbc_url,
    table="dim_cliente",
    mode="overwrite",
    properties=jdbc_properties
)

dim_produto_df.write.jdbc(
    url=jdbc_url,
    table="dim_produto",
    mode="overwrite",
    properties=jdbc_properties
)

dim_tempo_df.write.jdbc(
    url=jdbc_url,
    table="dim_tempo",
    mode="overwrite",
    properties=jdbc_properties
)

fato_vendas_df.write.jdbc(
    url=jdbc_url,
    table="fato_vendas",
    mode="overwrite",
    properties=jdbc_properties
)

print("Carga no PostgreSQL concluída com sucesso!")