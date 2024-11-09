import os
from pyspark.sql import SparkSession

# Obtenha as variáveis de ambiente para configurar a sessão do Spark e o MySQL
access_key = os.environ.get("AWS_ACCESS_KEY_ID")
secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
s3_endpoint = os.environ.get("S3_ENDPOINT")
bucket_name = os.environ.get("S3_BUCKET_NAME")
file_key = os.environ.get("S3_FILE_KEY")
url_mysql = os.environ.get("MYSQL_URL")
tabela = os.environ.get("MYSQL_TABLE")
usuario = os.environ.get("MYSQL_USER")
senha = os.environ.get("MYSQL_PASSWORD")


spark = SparkSession.builder.appName("job1") \
    .config("spark.hadoop.fs.s3a.access.key", access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
    .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint) \
    .getOrCreate()

s3_path = f's3a://{bucket_name}/{file_key}'


df_streaming = spark.readStream \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(s3_path)

query = df_streaming.writeStream \
    .outputMode("append") \
    .foreachBatch(lambda df, epochId: df.write \
        .format("jdbc") \
        .option("url", url_mysql) \
        .option("dbtable", tabela) \
        .option("user", usuario) \
        .option("password", senha) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()) \
    .start()

query.awaitTermination()
