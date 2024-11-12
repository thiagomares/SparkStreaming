import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, FloatType

url_mysql = os.environ.get("MYSQL_URL")
tabela = os.environ.get("MYSQL_TABLE")
usuario = os.environ.get("MYSQL_USER")
senha = os.environ.get("MYSQL_PASSWORD")

directory_path = "dados"

spark = SparkSession.builder.appName("job1").getOrCreate()

schema = StructType([
    StructField("IDVenda", IntegerType(), True),
    StructField("IDVendedor", IntegerType(), True),
    StructField("IDCliente", IntegerType(), True),
    StructField("Data", DateType(), True),
    StructField("Total", FloatType(), True)
])

df = spark.readStream.option('sep', ';').option('header', 'false').schema(schema).csv(directory_path)

query = df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", directory_path + "/parquet_output") \
    .option("checkpointLocation", directory_path + "/checkpoint_parquet") \
    .start()

query.awaitTermination()
