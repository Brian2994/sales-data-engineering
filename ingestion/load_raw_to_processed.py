from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# Inicializa Spark
spark = SparkSession.builder.appName("SalesDataIngestion").getOrCreate()

# ---------------------------------
# Definição de schemas
# ---------------------------------
schema_clientes = StructType([
    StructField("id_cliente", IntegerType(), True),
    StructField("nome", StringType(), True),
    StructField("cidade", StringType(), True),
    StructField("estado", StringType(), True),
    StructField("data_cadastro", DateType(), True)
])

schema_produtos = StructType([
    StructField("id_produto", IntegerType(), True),
    StructField("categoria", StringType(), True),
    StructField("preco", DoubleType(), True)
])

schema_pedidos = StructType([
    StructField("id_pedido", IntegerType(), True),
    StructField("id_cliente", IntegerType(), True),
    StructField("data_pedido", DateType(), True),
    StructField("status", StringType(), True)
])

schema_itens_pedido = StructType([
    StructField("id_pedido", IntegerType(), True),
    StructField("id_produto", IntegerType(), True),
    StructField("quantidade", IntegerType(), True),
    StructField("valor_unitario", DoubleType(), True)
])

# ---------------------------------
# Leitura dos CSVs
# ---------------------------------
clientes_df = spark.read.csv("data/raw/clientes.csv", header=True, schema=schema_clientes)
produtos_df = spark.read.csv("data/raw/produtos.csv", header=True, schema=schema_produtos)
pedidos_df = spark.read.csv("data/raw/pedidos.csv", header=True, schema=schema_pedidos)
itens_pedido_df = spark.read.csv("data/raw/itens_pedido.csv", header=True, schema=schema_itens_pedido)

# ---------------------------------
# Salvar em Parquet (processed)
# ---------------------------------
clientes_df.write.mode("overwrite").parquet("data/processed/clientes")
produtos_df.write.mode("overwrite").parquet("data/processed/produtos")
pedidos_df.write.mode("overwrite").parquet("data/processed/pedidos")
itens_pedido_df.write.mode("overwrite").parquet("data/processed/itens_pedido")

print("Ingestão concluída com sucesso!")