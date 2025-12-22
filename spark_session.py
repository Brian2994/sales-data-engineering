from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("SalesDataProject") \
        .getOrCreate()
    
    spark.range(10).show()
    
    spark.stop()

if __name__ == "__main__":
    main()