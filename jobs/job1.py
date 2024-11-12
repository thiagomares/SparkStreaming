import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, FloatType

# Configura as variáveis para conexão MySQL
url_mysql = os.environ.get("MYSQL_URL")
tabela = os.environ.get("MYSQL_TABLE")
usuario = os.environ.get("MYSQL_USER")
senha = os.environ.get("MYSQL_PASSWORD")

# Diretório onde estão armazenados os arquivos CSV
directory_path = "dados"  # Certifique-se de que seja o caminho relativo ou absoluto para sua pasta "dados"

# Cria a sessão Spark
spark = SparkSession.builder.appName("job1").getOrCreate()

schema = StructType([
    StructField("IDVenda", IntegerType(), True),      # IDVenda como inteiro
    StructField("IDVendedor", IntegerType(), True),   # IDVendedor como inteiro
    StructField("IDCliente", IntegerType(), True),    # IDCliente como inteiro
    StructField("Data", DateType(), True),            # Data como tipo Date
    StructField("Total", FloatType(), True)           # Total como tipo Float
])

# Lê os arquivos CSV de forma contínua (streaming)
df = spark.readStream.option('sep', ';').option('header', 'false').schema(schema).csv(directory_path)

# Escreve o DataFrame no formato Parquet na mesma pasta onde os CSV estão
query = df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", directory_path + "/parquet_output") \
    .option("checkpointLocation", directory_path + "/checkpoint_parquet") \
    .start()

# Espera o término do stream
query.awaitTermination()
