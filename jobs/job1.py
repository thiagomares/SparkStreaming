from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("job1") \
    .config("spark.hadoop.fs.s3a.access.key", "AKIAZI2LDILVTCRC5Y5I") \
    .config("spark.hadoop.fs.s3a.secret.key", "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") \
    .config("spark.hadoop.fs.s3a.endpoint", "ss3.sa-east-1.amazonaws.com") \
    .getOrCreate()
    
bucket_name = 'bucketestudosengdados'
file_key = 'mes/dia/hora/13/'
s3_path = f's3a://{bucket_name}/{file_key}'

df_streaming = spark.readStream \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(s3_path)

url_mysql = "jdbc:mysql://mysql:3306/airflow"
tabela = "dados"
usuario = "root"
senha = "root_password"

# Escreva o stream para o MySQL
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
