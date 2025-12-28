from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("DQChecks") \
    .getOrCreate()

df = spark.read.parquet("data/trusted/vendas")

# IDs nulos
assert df.filter(col("id_cliente").isNull()).count() == 0

# Valores negativos
assert df.filter(col("valor_total") < 0).count() == 0

# Quantidade inválida
assert df.filter(col("quantidade") <= 0).count() == 0

print("Data Quality OK ✅")